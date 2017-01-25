import os
from setuptools import setup, find_packages

from pmmif.version import version as __version__

def read(fname):
    # read contents of file
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

def data(path, pathitems):
    # build list of additional files to package up from a subdirectory
    names = []
    for relpath in pathitems:
        subpath = path + [relpath]
        dirname = os.path.join(*subpath)
        for name in os.listdir(dirname):
            pathname = os.path.join(relpath, name)
            fullpathname = os.path.join(dirname, name)
            if os.path.isdir(fullpathname):
                names.extend(data(path, [pathname]))
            else:
                names.append(pathname)
    return names

setup(
    name='pmmif',
    version=__version__,
    author='Stochastic Solutions Limited',
    author_email='info@StochasticSolutions.com',
    description='Predictive Modelling Metadata Interchange Format',
    long_description=read('README.md'),
    license='MIT',
    url='http://www.tdda.info',
    download_url='https://github.com/tdda/pmmif',
    keywords='predictive modelling metadata',
    packages=find_packages(),
    package_data={
        'pmmif': ['README.md', 'LICENSE.txt'] + data(['pmmif'],
                                                     ['doc', 'data']),
    },
    zip_safe=False,
    install_requires=['numpy>=1.9'],
)
