#
# Predictive Modelling Metadata Interchange Format
#
# Requires OrderedDict (available in collections in Python 2.7 and backported)
#
from __future__ import division
from __future__ import unicode_literals
import json
import datetime
import sys
from collections import OrderedDict, Counter

PMM_VERSION = '0.1'
DEFAULT_DATE_TAG_FORMAT = '%Y-%m-%d %H:%M:%S'

if sys.version_info.major < 3:
    STRING_TYPE = unicode
else:
    STRING_TYPE = str



class PMMError(Exception):
    pass


class TAG(object):
    CATEGORICAL = 'categorical'
    ORDINAL = 'ordinal'
    UNIQUE = 'unique'

    MAXIMIZE = 'maximize'
    MINIMIZE = 'minimize'


class ROLE(object):
    INDEPENDENT = 'independent'         # left-hand side; predictor
    DEPENDENT = 'dependent'             # right-hand side; outcome
    TREATMENT = 'treatment'             # specifies which treatment, if any
    WEIGHT = 'weight'                   # weight field of some kind
    AUXILIARY = 'auxiliary'             # auxiliary field e.g. value field
    VALIDATION = 'validation'           # field specifies cross-validation
                                        #   partition
    IGNORE = 'ignore'                   # ignore other unspecified field
    UNSPECIFIED = ''                    #

    ROLES = (INDEPENDENT, DEPENDENT, TREATMENT, WEIGHT, AUXILIARY,
             VALIDATION, IGNORE, UNSPECIFIED)


FIELD_TYPES = ['boolean', 'integer', 'real', 'string', 'datestamp']


class PMMType(object):
    """
    Template class used to specify each element of the PMM format.

    Typically, you simply need to subclass PMMType and list the required,
    defaulted, and optional arguments. This will provide a constructor with the
    normal Python style (i.e. supply required and defaulted arguments with a
    combination of positional and keyword args, and optional arguments using
    keyword ags).  You also get a serializable() method that turns the
    element into an ordered dictionary (to write out as JSON say), and which
    can normally be passed back to the constructor as keyword arguments to
    reconstruct the instance, e.g. elt2 = MyElt(**elt.serializable())

    In more complex cases you can override the constructor and serializable
    methods.
    """

    required = OrderedDict(())   # name -> type
    defaulted = OrderedDict(())  # name -> (type, default)
    optional = OrderedDict(())   # name -> type
    # note use type=any to avoid specifying a specific type for an attribute

    def __init__(self, *args, **kwargs):
        """
        Generic initializer to set up class with positional and keyword
        arguments corresponding to the required, defaulted and optional args
        defined for the class
        """

        # convenience class attribute with type for each attribute
        if not hasattr(self.__class__, 'typemap'):
            self.__class__.typemap = dict([(name, type_)
                                          for (name, (type_, default_))
                                              in self.defaulted.items()])
            self.__class__.typemap.update(self.required)
            self.__class__.typemap.update(self.optional)

        # first do the keyword arguments
        for key, val in kwargs.items():
            self._setattr(key, val)

        # then do the positional paramters
        keys = list(self.required.keys()) + list(self.defaulted.keys())
        if len(args) > len(keys):
            raise PMMError('Constructor for %s takes at most %d '
                           'positional arguments, %d given'
                           % (self.__class__.__name__, len(keys), len(args)))

        for key, val in zip(keys, args):
            self._setattr(key, val)

        # now do any remaining defaulted args
        for key, (type_, default_) in self.defaulted.items():
            if not hasattr(self, key):
                self._setattr(key, default_)

        # check that we've got all the required args
        for key in self.required:
            if not hasattr(self, key) or self.__dict__[key] is None:
                raise PMMError('Constructor for %s missing required '
                               'argument %s' % (self.__class__.__name__,
                                key))

    def _setattr(self, key, val):
        if key not in self.typemap:
            raise PMMError('Unknown attribute %s for class %s'
                           % (key, self.__class__.__name__))
        elif val is None:
            return

        def convert(val, type_):
            # if type_ is a list of types, process the inside of the list
            if (type(type_) in (tuple, list)
                    and len(type_) == 1
                    and type(val) in (tuple, list)):
                return [convert(v, type_[0]) for v in val]

            # if type_ is unspecified or val already matches, we're good
            elif type_ is any or type(val) == type_:
                return val

            # if it's a PMMType instantiated with a dict, treat as kwargs
            elif issubclass(type_, PMMType) and type(val) == dict:
                return type_(**val)

            elif type_ == str and sys.version_info.major < 3:
                return (val.encode('UTF-8') if type(val) == unicode
                        else type_(val))

            # otherwise let type_ try to construct an instance from val
            # (will raise an error if it's incompatible)
            else:
                return type_(val)

        setattr(self, key, convert(val, self.typemap[key]))

    def serializable(self):
        """
        Return a serializable (typically ordered) dict representing self
        """

        # helper to recursively serialize children
        def serialize(val):
            if hasattr(val, 'serializable'):
                return val.serializable()
            elif type(val) in (list, tuple):
                return [serialize(v) for v in val]
            else:
                return val

        dct = OrderedDict([])
        for key in (list(self.required.keys()) + list(self.defaulted.keys())
                    + list(self.optional.keys())):
            if hasattr(self, key):
                dct[key] = serialize(getattr(self, key))
        return dct


class FlatFileFormat(PMMType):
    defaulted = OrderedDict([
            ('encoding', (str, 'UTF-8')),
            ('separator', (str, ',')),
            ('quote', (str, '"')),
            ('escape', (str, '\\')),
            ('nullmarker', (str, '')),
            ('headerrowcount', (int, 1)),
            ('dateformat', (str, None)),
    ])
    # TODO: Date format


class FlatFile(PMMType):
    required = OrderedDict([
            ('name', str),
            ('format', FlatFileFormat)
    ])


class Data(PMMType):
    # at some point we might allow a choice of representations
    required = OrderedDict([
            ('flatfile', FlatFile)
    ])


class Stats(PMMType):
    optional = OrderedDict([
            ('nnulls', int),
            ('nuniques', int),
            ('min', any),     # abuse the any() function to allow any type
            ('max', any),
            ('mean', float),
    ])


class Field(PMMType):
    required = OrderedDict([
            ('name', str),
            ('type', str),
            ('role', str),
            ('tags', dict),      # dictionary of values
            ('stats', Stats),
    ])
    optional = OrderedDict([
            ('values', [any]),  # list of arbitrary type (field dependent)
            ('longname', str),
            ('description', str),
    ])


class Metadata(PMMType):
    required = OrderedDict([
            ('pmmversion', str),       # omitted in direct constructor
            ('name', str),
            ('recordcount', int),
            ('fieldcount', int),       # omitted in direct constructor
            ('fields', [Field]),       # list of Field ojects
            ('tags', dict),            # dictionary of values
    ])
    optional = OrderedDict([
            ('data', Data),
            ('description', str),
            ('creator', str),
            ('contributor', str),
            ('permissions', str),
            ('datetagformat', str),
    ])

    def __init__(self, *args, **kwargs):
        """
        Instantiate without pmmversion and fieldcount when constructing
        directly, e.g.

            Metadata(name, recordcount, fields, ...)

        """

        if len(args) >= 2:
            # automatically construct the pmmversion and fieldcount args
            args = list(args)
            args.insert(0, PMM_VERSION)
            args.insert(3, len(args[3]))

        super(Metadata, self).__init__(*args, **kwargs)

        if self.fieldcount != len(self.fields):
            raise PMMError('Metadata fieldcount %d <> number of fields %d'
                           % (self.fieldcount, len(self.fields)))

        if float(self.pmmversion) != float(PMM_VERSION):
            raise PMMError("Can't handle pmmversion %s (vs %s)"
                           % (self.pmmversion, PMM_VERSION))


    def toJSON(self):
        self.convert_all_date_tags()
        self.order_all_tags()
        jsonStr = json.dumps(self.serializable(), indent=4)
        self.unconvert_all_date_tags()
        return u'\n'.join(L.rstrip() for L in jsonStr.splitlines())

    def order_all_tags(self):
        self.tags = self.order_tags(getattr(self, 'tags', {}))
        for field in self.fields:
            field.tags = self.order_tags(getattr(field, 'tags', {}))

    def order_tags(self, tags):
        d = OrderedDict()
        for tag in sorted(tags):
            d[tag] = tags[tag]
        return d

    def validate_fields(self):
        """
        Validate that all fields have names and types,
        and that the types are allowed PMM types.
        Also checks that names are unique, though does not place any
        other restrictions on the names.
        """
        c = Counter()
        for i, f in enumerate(self.fields):
            if not hasattr(f, 'name'):
                raise PMMError('Unknown name for field %d' % i)
            if not hasattr(f, 'type'):
                raise PMMError('Missing type for field %s' % f.name)
            if not f.type in FIELD_TYPES:
                raise PMMError('Unknown type %s for field %s' % (f.type,
                                                                 f.name))
            c[f.name] += 1
        if any(v > 1 for v in c.values()):
            raise PMMError('Not all field names are unique:',
                           ' '.join(k for k in c if c[k] > 1))

    def validate(self):
        """
        Validate various aspects of a PMM file.
        """
        self.validate_fields()

    def convert_all_date_tags(self):
        datetagformat = getattr(self, 'datetagformat', DEFAULT_DATE_TAG_FORMAT)
        nDates = self.convert_date_tags(getattr(self, 'tags', OrderedDict()),
                                        datetagformat)
        for f in self.fields:
            nDates += self.convert_date_tags(getattr(f, 'tags', OrderedDict()),
                                             datetagformat)
        if nDates:
            self.datetagformat = datetagformat  # ensure set

    def convert_date_tags(self, tags, fmt):
        n = 0
        for tag in tags:
            val = tags[tag]
            if type(val) == datetime.datetime:
                tags[tag] = val.strftime(fmt)
                n += 1
        return n

    def unconvert_all_date_tags(self):
        datetagformat = getattr(self, 'datetagformat', DEFAULT_DATE_TAG_FORMAT)
        self.unconvert_date_tags(getattr(self, 'tags', OrderedDict()),
                                 datetagformat)
        for f in self.fields:
            self.unconvert_date_tags(getattr(f, 'tags', OrderedDict()),
                                     datetagformat)

    def unconvert_date_tags(self, tags, fmt):
        for tag in tags:
            val = tags[tag]
            if type(val) == STRING_TYPE:
                try:
                    tags[tag] = datetime.datetime.strptime(val, fmt)
                except ValueError:
                    pass

    def save(self, path):
        with open(path, 'w') as f:
            f.write(self.toJSON())

    def __getitem__(self, name):
        for field in self.fields:
            if field.name == name:
                return field
        raise KeyError(name)

    def add_field(self, name, fieldMetadata):
        if getattr(fieldMetadata, 'name', None) is None:
            raise Exception('Attempt to add %s metadata with no name: %s'
                            % (name, str(fieldMetadata)))
        for i, field in enumerate(self.fields):
            if field.name == name:
                self.fields[i] = fieldMetadata
                return
        self.fields.append(fieldMetadata)


def interpret_all_date_tags(m):
    datetagformat = getattr(m, 'datetagformat', None)
    if not datetagformat:
        return
    interpret_date_tags(getattr(m, 'tags', OrderedDict()) , datetagformat)
    for f in getattr(m, 'fields', OrderedDict()):
        interpret_date_tags(getattr(f, 'tags', OrderedDict()), datetagformat)


def interpret_date_tags(tags, datetagformat):
    for tag in tags:
        v = tags[tag]
        if type(v) == STRING_TYPE:
            try:
                dt = datetime.datetime.strptime(v, datetagformat)
                tags[tag] = dt
            except ValueError:
                pass


def load(path):
    """
    Reads a PMM file from the path given.
    Returns a PMM Metadata object.
    """
    with open(path) as f:
        jsonText = f.read()
    return loads(jsonText)


def loads(jsonText):
    """
    Converts the JSON text given into a Metadata object.
    """
    m = Metadata(**json.loads(jsonText))
    interpret_all_date_tags(m)
    return m


