#!/bin/bash

set -e
set -x

declare -a packages

packages+=("Flask-0.10")
packages+=("Flask-Cache-0.13.1")
packages+=("Flask-SQLAlchemy-0.16")
packages+=("Jinja2-2.7")
packages+=("MarkupSafe-0.18")
packages+=("SQLAlchemy-0.8.0")
packages+=("WTForms-1.0.3")
packages+=("Werkzeug-0.9.3")
packages+=("boto-2.5.2")
packages+=("passlib-1.6.1")
packages+=("requests-1.2.3")
packages+=("itsdangerous-0.21")

# Only install pysqlite for python 2.3 and 2.4
if python -V 2>&1 | grep -qE 'ython 2\.[3-4]'; then
    packages+=("pysqlite-2.6.0")
fi

dir=$(cd $(dirname $0) && pwd)

cd $dir

if [ -z "$libdir" ]; then
    prefix=$(cd $dir/../.. && pwd)
    libdir=${prefix}/lib/pegasus/externals/python
fi

function clean_externals {
    rm -rf $libdir
    for package in "${packages[@]}"; do
        rm -rf $package
    done
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
    for package in "${packages[@]}"; do
        install_lib $package
    done
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

