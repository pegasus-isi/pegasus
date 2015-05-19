#!/bin/bash

set -e
set -x

tarfile=$1
libdir=$2
package=$3

tarname=${tarfile/.tar.gz//}

mkdir -p $libdir

tar xzf ${tarfile}

pushd $tarname > /dev/null
python setup.py install_lib -d $libdir > /dev/null
popd > /dev/null

rm -rf $tarname

# Touch the package so that make knows it is up-to-date
touch $libdir/$package

