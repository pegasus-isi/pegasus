#!/bin/bash

set -e

declare -a packages

# PM-997: OSX El Capitan and later do not have OpenSSL headers required to
# compile the pyOpenSSL package. Fortunately, they already have pyOpenSSL
# installed, so we don't need to install it ourselves.
PYOPENSSL="Yes"
if [ -x "/usr/bin/sw_vers" ]; then
    OSX=$(/usr/bin/sw_vers -productVersion)
    MAJOR=$(echo $OSX | cut -d. -f1)
    MINOR=$(echo $OSX | cut -d. -f2)
    # El Capitan is version 10.11
    if [ "$MAJOR" -eq 10 ] && [ "$MINOR" -ge 11 ]; then
        PYOPENSSL="No"
    fi
fi

if python -V 2>&1 | grep -qE 'ython 2\.[3-4]'; then
    # Install alternative dependencies for python 2.4
    packages+=("pysqlite-2.6.0")
    packages+=("SQLAlchemy-0.7.6")
else
    # For other python >= 2.5, install the service dependencies
    packages+=("Flask-0.10")
    packages+=("Flask-Cache-0.13.1")
    packages+=("Flask-SQLAlchemy-0.16")
    packages+=("Jinja2-2.7")
    packages+=("MarkupSafe-0.18")
    packages+=("WTForms-1.0.3")
    packages+=("Werkzeug-0.9.3")
    packages+=("requests-2.18.4")
    packages+=("itsdangerous-0.21")
    packages+=("boto-2.38.0")
    packages+=("SQLAlchemy-0.8.0")
    packages+=("pam-0.1.4")
    if [ "$PYOPENSSL" == "Yes" ]; then
        packages+=("pyOpenSSL-0.13")
    else
        echo "Skipping pyOpenSSL..."
    fi
    if [ -x "$(which pg_config 2>/dev/null)" ]; then
        packages+=("psycopg2-2.6")
    else
        echo "WARNING: pg_config not found: skipping python postgresql library" >&2
    fi
    if [ -x "$(which mysql_config 2>/dev/null)" ]; then
        packages+=("MySQL-python-1.2.5")
    else
        echo "WARNING: mysql_config not found: skipping python mysql library" >&2
    fi
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
    echo "Installing $tarname..."
    tar xzf ${tarname}.tar.gz
    pushd $tarname > /dev/null
    python setup.py install_lib -d $libdir > /dev/null
    popd > /dev/null
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

