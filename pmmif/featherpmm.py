# -*- coding: utf-8 -*-
"""
featherpmm.py: Extends feather file format with a paired file
that contains extra metadata. If the feather file is /path/to/foo.feather,
the metadata file is /path/to/foo.pmm.
"""

from __future__ import division

import os
import datetime
import numpy as np
import sys

try:
    import feather
except ImportError:
    feather = None

from pmmif import pmm

PANDAS_STRING_DTYPE = np.dtype('O')
PANDAS_BOOL_DTYPE = np.dtype('bool')
PANDAS_FLOAT_DTYPE = np.dtype('float64')
NULL_STRING = '∅'
NULL_SUFFIX = '_' + NULL_STRING
UTF8 = 'UTF-8'

isPython2 = sys.version_info.major < 3
if isPython2:
    bytes_type = str                # type: type
    unicode_type = unicode          # type: type
else:
    bytes_type = bytes
    unicode_type = str


class Dataset(object):
    """
    Container for a Pandas dataframe and a metadata object from PMM.

    Metadata fulfils two functions:
        1. It allows association of 'intended' types of columns, in cases
           for which Pandas forces promotion. For example:
                - Integer columns that contain nulls are promoted to float
                  in Pandas, but can be marked as 'integer' in the metadata.
                - Boolean columns that contain nulls are promoted to Object
                  in Pandas, but can be marked as 'boolean' in the metadata.
        2. It allows additional annotations to be associated with the
           dataframe, and with individual columns of the dataframe.
                - Tags (with optional values)
                - Descriptions

    A dataset object has two attributes:
          - df    is an ordinary Pandas dataframe
          - md    is a PMM Metadata object
    """
    def __init__(self, df, md=None, name=None):
        """
        Create Dataset object.
          -  df   is a Pandas dataframe.
          -  md   is a PMM metadata object, and is optional.
          -  name is the name to be associated with the dataset, for the
                  case where no metadata is provided.

        If no metadata is provided, the metadata is inferred from the
        dataframe.
        """
        self.df = df
        self.md = md or _create_pmm_metadata(df, name)

    def add_field(self, name, col, pmmtype=None):
        """
        Add a new field to the dataset.

        Adds the field to the Pandas dataframe, and declares its type
        in the PMM metadata.

        The type, if provided, must be one of: 'boolean', 'integer',
        'real', 'string', 'datestamp'. If the type is not specified, the
        type is inferred from the dataframe.

        The advantages of using add_field, as opposed to just creating it
        directly in the dataframe, are:
            - the option of specifying an intended type, even if Pandas has
              promoted the type in the dataframe.
            - creating a corresponding entry in the metadata, to facilitate
              tagging, descriptions, etc.
        """
        self.df[name] = col
        self.declare_field(name, pmmtype)

    def declare_field(self, name, pmmtype=None):
        """
        Declare the type of a field in the dataset, which must already
        exist in the Pandas dataframe.

        This is intended for use when a dataframe has been created without
        any metadata (for example, from a CSV file), and then more detailed
        type information needs to be declared for existing fields.

        The type, if provided, must be one of the types described for
        add_field, above.
        """
        if self.df[name].isnull().all():  # all null or no records
            if pmmtype == 'string':
                self.df[name] = self.df[name].astype(np.dtype('O'))
            elif pmmtype == 'datestamp':
                self.df[name] = (self.df[name]
                                     .astype(np.dtype('datetime64[ns]')))
        fieldMetadata = _create_pmm_field(self.df[name], pmmtype=pmmtype)
        self.md.add_field(name, fieldMetadata)

    def tag_field(self, colname, tagname, value=None):
        """
        Add a tag to a field. The tag can optionally have a value, but
        by default does not.

        The field must already exist in the metadata.

        If a value is provided, it must be of one of the following Python
        types:
            - None
            - bool
            - int
            - float
            - str
            - datetime.datetime
        or it can be a (potentially nested) list or dictionary over these
        types with string keys.
        """
        self.md[colname].tags[tagname] = value

    def tag_dataset(self, tagname, value=None):
        """
        Add a tag to the dataset. The tag can optionally have a value, but
        by default does not. If a value is provided, it must have one of
        the types described in tag_field above.
        """
        self.md.tags[tagname] = value

    def update_metadata(self):
        """
        Update the metadata to bring it into line with the dataset.

        After calling this method, all of the fields that exist in the
        dataset will now exist in the metadata too, and the metadata will
        not contain any fields that do not appear in the dataframe.

        It will infer types in the metadata for any fields that do not
        already have metadata, but will not alter the types of existing
        fields in the metadata.
        """
        _reset_fields_from_dataframe(self)

    def merge_metadata(self, other, fields=None):
        """
        After merging another dataset in (via pd.merge), we don't have any of
        the metadata associated with the fields that have come in from that
        other dataset. This function takes the 'other metadata' and brings it
        into the metadata for the main dataset. The new metadata wins if the
        fields exists in both if:
            1. the fields parameter is left empty, or
            2. the fields parameter is a list but field is not in the list

        """
        newfields = [fx.name for fx in other.md.fields]
        for f in newfields:
            if (fields is None) or (f in fields):
                if f in [fx.name for fx in self.md.fields]:
                    del self.md.fields[[fx.name
                                        for fx in self.md.fields].index(f)]
        _add_metadata_from_other_dataset(self.md, other.md)
        _reset_fields_from_dataframe(self)

    def append(self, other):
        """
        Append another dataset to an existing one.

        The second dataframe is appended to the first one, and the metadata
        for any fields that only exist in the second one is added to the
        first one.
        """
        self.df = self.df.append(other.df, ignore_index=True)
        _add_metadata_from_other_dataset(self.md, other.md)
        _reset_fields_from_dataframe(self)


def read_dataframe(featherpath):
    """
    Similar to feather.read_dataframe except that it also reads the
    corresponding .pmm file, if present, and returns a Dataset
    object rather than a dataframe.

    The Dataset object contains the Pandas dataframe in its df attribute,
    and the metadata in its md attribute.
    """
    if feather is None:
        raise Exception('Feather-format is not available')
    df = feather.read_dataframe(featherpath)
    pmmpath, datasetname = _split_feather_path(featherpath)
    if os.path.exists(pmmpath):
        md = pmm.load(pmmpath)
    else:
        md = _create_pmm_metadata(df, datasetname)
    df = _recover_problematical_all_null_columns(Dataset(df, md))
    return Dataset(df, md)


def write_dataframe(dataset, featherpath):
    """
    Similar to feather.write_dataframe except that it also writes a
    corresponding .pmm file, and expects a Dataset object rather than a
    dataframe.

    The Dataset object contains the Pandas dataframe in its df attribute,
    and the metadata in its md attribute.
    """
    if feather is None:
        raise Exception('Feather-format is not available')
    pmmpath, datasetname = _split_feather_path(featherpath)
    if dataset.md is None:
        dataset.md = _create_pmm_metadata(dataset.df, datasetname)

    _reset_fields_from_dataframe(dataset)

    df = _sanitize_problematical_all_null_columns(dataset)
    try:
        feather.write_dataframe(df, featherpath)
        dataset.md.save(pmmpath)
    except:
        # feather leaves dud files around if it fails to write
        if os.path.exists(featherpath):
            os.remove(featherpath)
        if os.path.exists(pmmpath):
            os.remove(pmmpath)
        raise


#
# The rest of the functions below are internal to this module, and should not
# be called from outside.
#


def _sanitize_problematical_all_null_columns(ds):
    """
    Feather doesn't like all-null string columns or all-null boolean columns,
    so this method transforms them before saving.
    They are transformed into float64 fields with NaN et every value,
    and the pandas column name gets a '_∅t' appended, where t is a
    type indicator --- b for boolean, s for string or u for unknown.
    """
    origdf, md = ds.df, ds.md
    df = origdf[list(origdf)]       # Copye
    nTransformed = 0
    fieldnames = list(df.columns)
    nRecords = len(df.index)
    dfIsUnicode = fieldnames and type(fieldnames[0]) == unicode_type
    fn = _unicode_definite if dfIsUnicode else _utf8_definite
    null_suffix = fn(NULL_SUFFIX)
    for i, f in enumerate(fieldnames):
        if (df[f].dtype not in (PANDAS_STRING_DTYPE, PANDAS_BOOL_DTYPE)
                or df[f].notnull().sum() > 0):  # includes bools with nulls
            continue
        if df[f].dtype == PANDAS_BOOL_DTYPE and nRecords > 0:
            continue
        typeChar = ('b' if md[f].type == 'boolean'
                        else 's' if md[f].type == 'string'
                        else 'u')
        altname = f + null_suffix + typeChar
        if altname in fieldnames:
            continue  # already there. Whatever...
            # restore any all-null string fields
        df[altname] = np.array([np.nan] * nRecords,
                               dtype=PANDAS_FLOAT_DTYPE)
        nTransformed += 1
        fieldnames[i] = altname
    if nTransformed > 0:
        df = df[fieldnames]
    return df


def _recover_problematical_all_null_columns(ds):
    """
    Feather doesn't like all-null string columns or all-null boolean columns,
    so they are sanitized before saving; this untransforms them.
    """
    df, md = ds.df, ds.md
    nTransformed = 0
    fieldnames = list(df.columns)
    nRecords = len(df.index)
    dfIsUnicode = fieldnames and type(fieldnames[0]) == unicode_type
    fn = _unicode_definite if dfIsUnicode else _utf8_definite
    null_suffix = fn(NULL_SUFFIX)
    for i, f in enumerate(fieldnames):
        if f[:-1].endswith(null_suffix) and all(np.isnan(df[f])):
            suffixLen = len(null_suffix) + 1
            suffix = f[-suffixLen:]
            truename = f[:-suffixLen]
            typeChar  = suffix[-1]
            if truename in df:
                continue  # Both there; makes no sense; leave
            # restore any all-null string fields
            type_ = PANDAS_STRING_DTYPE
            if (typeChar == 'b' and nRecords == 0):
                type_ = PANDAS_BOOL_DTYPE
            df[truename] = np.array(np.ones(nRecords) * np.nan, type_)
            nTransformed += 1
            fieldnames[i] = truename
    if nTransformed > 0:
        df = df[fieldnames]
    return df


def _split_feather_path(path):
    """
    Returns path to the corresponding PMM file and the dataset name
    from the feather path given.
    """
    pathbody, ext = os.path.splitext(path)
    if ext.startswith('.feather'):
        pmmpath = pathbody + '.pmm' + ext[8:]
    else:
        pmmpath = pathbody + '.pmm'
    datasetname = os.path.split(pathbody)[1]
    return pmmpath, datasetname


def _create_pmm_metadata(df, name):
    """
    Creates a vanilla PMM Metadata struture from a data frame.
    """
    nRecords = len(df.index)
    fields = [_create_pmm_field(df[col]) for col in df]
    return pmm.Metadata(name, nRecords, fields, tags={})


def _create_pmm_field(col, pmmtype=None):
    """
    Creates a PMM Field object from a column.
    """
    return pmm.Field(col.name, _pmm_type(col, pmmtype), role='',
                     tags={}, stats={})


def _pmm_type(col, pmmtype=None):
    """
    Maps Pandas types to PMM types
    """
    if pmmtype:
        if pmmtype not in pmm.FIELD_TYPES:
            raise ValueError('Unknown PMM type: %s:' % pmmtype)
        return pmmtype
    s = str(col.dtype)
    if s == 'bool':
        return 'boolean'
    elif s.startswith('int'):
        return 'integer'
    elif s.startswith('float'):
        return 'real'
    elif s == 'object':
        someindex = col.first_valid_index()
        if someindex is not None:
            somevalue = col[someindex]
            if type(somevalue) is bool:
                return 'boolean'
        return 'string'
    elif s.startswith('date'):
        return 'datestamp'
    else:
        raise TypeError('Unknown type: %s [%s]' % (s, repr(col.dtype)))


def _reset_fields_from_dataframe(dataset):
    """
    Make some effort to ensure that the metadata matches the data
    before writing.
    """
    df, md = dataset.df, dataset.md
    dfFieldnames = list(df)
    mdFieldnames = [f.name for f in md.fields]

    dfOnlyFieldnames = set(dfFieldnames) - set(mdFieldnames)
    mdOnlyFieldnames = set(mdFieldnames) - set(dfFieldnames)

    for f in dfOnlyFieldnames:
        md.fields.append(_create_pmm_field(df[f]))
    assert(mdOnlyFieldnames.intersection(set([f.name for f in md.fields]))
           == mdOnlyFieldnames)

    # Remove metadata for fields that aren't in the dataframe
    for f in mdOnlyFieldnames:
        del md.fields[[fx.name for fx in md.fields].index(f)]

    orderOK = True
    for i, f in enumerate(list(md.fields)):
        if not f.name == dfFieldnames[i]:
            orderOK = False
    if not orderOK:
        md.fields = [md[f] for f in dfFieldnames]
    md.fieldcount = len(md.fields)
    md.recordcount = len(df)


def _add_metadata_from_other_dataset(md, othermd):
    """
    Extend md with fields from othermd not present in md.
    (No consistency checking.)
    """
    fields = [f.name for f in md.fields]
    otherFields = [f.name for f in othermd.fields]
    otherOnlyFields = set(otherFields) - set(fields)
    for f in otherOnlyFields:
        md.fields.append(othermd[f])
    md.fieldcount = len(md.fields)


def _unicode_definite(s):
    """
    Converts a string to unicode if it isn't.
    Assumes encoding is UTF-8.
    Stringifies (as unicode) non strings.
    """
    return unicode_type(s, UTF8) if type(s) == bytes_type else unicode_type(s)


def _utf8_definite(s):
    """
    Converts a string to UTF-8 if it is unicode.
    Otherwise just returns the string.
    Stringifies non strings.
    """
    return s.encode(UTF8) if type(s) == unicode_type else bytes_type(s)


def _utf8_definite_object(s):
    """
    Converts all unicode within scalar or object, recursively, to unicode.
    Handles lists, tuples and dictionaries, as well as scalars.
    """
    if type(s) == unicode_type:
        return s.encode(UTF8)
    elif type(s) == list:
        return [_utf8_definite_object(v) for v in s]
    elif type(s) == tuple:
        return tuple([_utf8_definite_object(v) for v in s])
    elif type(s) == dict:
        return {_utf8_definite_object(k): _utf8_definite_object(v)
                for (k, v) in s.items()}
    return s


def _unicode_definite_object(s):
    """
    Converts all bytes within scalar or object, recursively, to unicode.
    Handles lists, tuples and dictionaries, as well as scalars.
    """
    if type(s) == bytes_type:
        return s.decode(UTF8)
    elif type(s) == list:
        return [_unicode_definite_object(v) for v in s]
    elif type(s) == tuple:
        return tuple([_unicode_definite_object(v) for v in s])
    elif type(s) == dict:
        return {unicode_definite_object(k): _unicode_definite_object(v)
                for (k, v) in s.items()}
    return s


if isPython2:
    _str_definite_object = _utf8_definite_object
    _str_definite = _utf8_definite
else:
    _str_definite_object = _unicode_definite_object
    _str_definite = _unicode_definite
