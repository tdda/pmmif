from __future__ import print_function

PMMIF_MAJOR_VERSION = 0
PMMIF_MINOR_VERSION = 1
PMMIF_EDIT = 1

PMMIF_VERSION = '%d.%d.%02d' % (PMMIF_MAJOR_VERSION, PMMIF_MINOR_VERSION,
                                PMMIF_EDIT)

if __name__ == '__main__':
    print(PMMIF_VERSION)
