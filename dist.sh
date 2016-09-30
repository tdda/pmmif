#!/bin/sh
#
# Distribution script for PMMIF.
#
# Builds eggs and wheels, for both Python2 and Python3.
#
# After running this script, these distribution files will appear in a
# 'dist' subdirectory.
#

name=pmmif
v=`python version.py`

QUIET=--q

rm -fr dist eggs wheels
allfiles=`ls`

mkdir dist
mkdir dist/pmmif
cp -r $allfiles dist/pmmif

cd dist

echo "__import__('pkg_resources').declare_namespace(__name__)" > pmmif/__init__.py
echo "__version__ = '$v'" >> pmmif/__init__.py

sed -e "s/@@VERSION@@/$v/" < pmmif/setup.py > setup.py

v0=$v
case $v in
    *0?) v=`echo $v | sed -e s/\\.0/./`
esac

function eggs() {
    py=$1

    PYVERSION=`python$py --version 2>&1 | sed -e 's/.* //' -e 's/\.[0-9]*$//'`
    python$py setup.py bdist_egg --exclude-source-files $QUIET

    eggname=$name-$v-py$PYVERSION.egg
    eggfile=dist/$eggname
    if [ ! -f $eggfile ]
    then
        eggname=$name-$v0-py$PYVERSION.egg
        eggfile=dist/$eggname
    fi

    wheelpysuffix=`echo $PYVERSION | sed -e 's/\.//'`
    wheelfile=$name-$v-py$wheelpysuffix-none-any.whl

    if [ -f $eggfile ]
    then
        echo "Created Python$py egg $eggfile"
        wheel convert $eggfile
        mv $eggfile $eggname
        if [ ! -f $wheelfile ]
        then
            wheelfile=$name-$v0-py$wheelpysuffix-none-any.whl
        fi
        if [ -f $wheelfile ]
        then
            echo "Created Python$py wheel $wheelfile"
        else
            echo "*** Failed to create Python$py wheel $wheelfile" 1>&2
        fi
    else
        echo "*** Failed to create Python$py egg $eggfile" 1>&2
    fi
}

eggs 2
eggs 3

rm -fr pmmif build dist *.egg-info setup.py
