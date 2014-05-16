#!/bin/bash

set -e
set -x

dir=$(cd $(dirname $0) && pwd)

cd $dir

if [ -z "$libdir" ]; then
    prefix=$(cd $dir/../.. && pwd)
    libdir=${prefix}/lib/pegasus/externals/python
fi

function clean_externals {
    rm -rf $libdir
    rm -rf pysqlite-2.6.0
    rm -rf boto-2.5.2
    rm -rf SQLAlchemy-0.7.6
    rm -rf SleekXMPP-1.2.5
    rm -rf pyasn1-0.1.7
    rm -rf pyasn1-modules-0.0.5
    rm -rf dnspython-1.11.1
}

function install_lib {
    tarname=$1
    tar xzf ${tarname}.tar.gz
    pushd $tarname
    python setup.py install_lib -d $libdir
    popd
    rm -rf $tarname
}

function install_externals {
    # Only install pysqlite for python 2.3 and 2.4
    if python -V 2>&1 | grep -qE 'ython 2\.[3-4]'; then
        install_lib pysqlite-2.6.0
    fi
    install_lib boto-2.5.2
    install_lib SQLAlchemy-0.7.6
    install_lib SleekXMPP-1.2.5
    install_lib pyasn1-0.1.7
    install_lib pyasn1-modules-0.0.5
    install_lib dnspython-1.11.1
}

command=$1

if [ -z "$command" ]; then
    clean_externals
    install_externals
elif [ "$command" == "clean" ]; then
    clean_externals
elif [ "$command" == "install" ]; then
    clean_externals
    install_externals
else
    echo "Invalid command: $command"
    exit 1
fi

