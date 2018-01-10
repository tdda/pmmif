# -*- coding: utf-8 -*-
"""
testpmm.py: tests for pmm.py
"""
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import os
import json
import tempfile
import unittest

from pprint import pprint as pp

from pmmif.pmm import (Metadata, Field, Stats, Data, FlatFile, FlatFileFormat,
                       ROLE, TAG, load)

HILLSTROM_PMM_VERSION = '0.1'


class LoadTests(unittest.TestCase):
    indir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')
    outdir = tempfile.gettempdir()
    def testRoundTrip(self):
        for pmmfile in ['hillstrom.pmm', 'victorlo.pmm']:
            fullpath = os.path.join(self.indir, pmmfile)
            m = load(fullpath)
            dupfile = os.path.join(self.outdir, pmmfile)
            with open(dupfile,'w') as f:
                f.write(m.toJSON())
            with open(fullpath) as f:
                original = f.read()
            with open(dupfile) as f:
                copy = f.read()
            if not copy == original:
                path = os.path.join(self.outdir, 'failed-' + pmmfile)
                with open(path, 'w') as f:
                    f.write(copy)
                    print('Actual failing output written to %s.' % path)
            self.assertEqual(original, copy)
            try:
                os.unlink(dupfile)
            except:
                pass

    def testLoad(self):
        path = os.path.join(self.indir, 'hillstrom.pmm')
        m = load(path)

        # Basic Metadata

        self.assertEqual(set(m.__dict__.keys()),
                         {'name',
                          'recordcount',
                          'fields',
                          'fieldcount',
                          'data',
                          'pmmversion',
                          'tags'})
        self.assertEqual(m.name, 'hillstrom')
        self.assertEqual(m.recordcount, 64000)
        self.assertEqual(m.fieldcount, 12)
        self.assertEqual(m.pmmversion, HILLSTROM_PMM_VERSION)

        # The data section describes a single flat file
        # named hillstrom.csv with the following format

        self.assertEqual(list(m.data.__dict__.keys()), ['flatfile'])
        f = m.data.flatfile
        self.assertEqual(set(f.__dict__.keys()), {'name', 'format'})
        self.assertEqual(f.name, 'hillstrom.csv')
        fmt = f.format
        self.assertEqual(set(fmt.__dict__.keys()),
                         {
                             'encoding',
                             'escape',
                             'headerrowcount',
                             'nullmarker',
                             'quote',
                             'separator',
                         })

        self.assertEqual(fmt.encoding, 'UTF-8')
        self.assertEqual(fmt.escape, '\\')
        self.assertEqual(fmt.headerrowcount, 1)
        self.assertEqual(fmt.nullmarker, '')
        self.assertEqual(fmt.quote, '"')
        self.assertEqual(fmt.separator, ',')

        # There are 12 fields
        fields = m.fields
        self.assertEqual(len(fields), 12)
        self.assertEqual([f.name for f in fields],
                         [
                             'recency',
                             'history_segment',
                             'history',
                             'mens',
                             'womens',
                             'zip_code',
                             'newbie',
                             'channel',
                             'segment',
                             'visit',
                             'conversion',
                             'spend',
                         ])
        self.assertEqual([f.type for f in fields],
                         [
                             'integer',
                             'string',
                             'real',
                             'boolean',
                             'boolean',
                             'string',
                             'boolean',
                             'string',
                             'string',
                             'boolean',
                             'boolean',
                             'real',
                         ])



def buildVictorLo():
    m = Metadata('victorlo', 99999,
        [
            Field('age','real', ROLE.INDEPENDENT, {}, Stats()),
            Field('trade','integer', ROLE.INDEPENDENT, {TAG.CATEGORICAL: None},
                                     Stats()),
            Field('wealth','real', ROLE.INDEPENDENT, {}, Stats()),
            Field('asset','real', ROLE.INDEPENDENT, {}, Stats()),
            Field('homevalue','real', ROLE.INDEPENDENT, {}, Stats()),
            Field('trt_flg','integer', ROLE.TREATMENT, {}, Stats()),
            Field('respond','integer', ROLE.DEPENDENT, {}, Stats()),
            Field('trn_flg','integer', ROLE.VALIDATION, {}, Stats()),
        ],
        data=Data(FlatFile('VictorLoDRA.dat', FlatFileFormat())),
        description="Synthetic dataset for True Lift paper",
        creator="Victor Lo",
        permissions="Public",
        tags={})

    outpath = os.path.join(LoadTests.outdir, 'victorlo.pmm')
    with open(outpath, 'w') as f:
        f.write(m.toJSON())


class ConstructTests(unittest.TestCase):
    def testVictorLo(self):
        buildVictorLo()



if __name__ == '__main__':
    unittest.main()


