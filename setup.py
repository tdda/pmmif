from setuptools import setup, find_packages
setup(
    name = 'pmmif',
    version = '@@VERSION@@',
    author = 'Stochastic Solutions Limited',
    description = 'Predictive Modelling Metadata Interchange Format',
    license = 'MIT',
    url = 'http://www.tdda.info',
    keywords = 'predictive modelling metadata',
    namespace_packages = ['pmmif'],
    packages = find_packages(),
    package_data = {
        '': [ 'data/*.*', 'doc/Makefile', 'doc/source/*.*'],
    },
    zip_safe=False,
    install_requires = ['numpy>=1.9'],
)
