#!/usr/bin/env python3


import argparse
import os
import sys
from glob import glob
from os.path import abspath, dirname, exists, join, normpath


def _perl_dump(**kw):
    """."""
    print(
        """my $pegasus_bin_dir = "%(bin_dir)s";
my $pegasus_conf_dir = "%(conf_dir)s";
my $pegasus_java_dir = "%(java_dir)s";
my $pegasus_perl_dir = "%(perl_dir)s";
my $pegasus_python_dir = "%(python_dir)s";
my $pegasus_python_externals_dir = "%(python_externals_dir)s";
my $pegasus_share_dir = "%(share_dir)s";
my $pegasus_schema_dir = "%(schema_dir)s";
unshift(@INC, $pegasus_perl_dir);
"""
        % kw,
        end="",
    )


def _perl_hash(**kw):
    """."""
    print(
        """use vars qw(%%pegasus);
%%pegasus =
    ( bin => "%(conf_dir)s"
    , conf => "%(conf_dir)s"
    , java => "%(java_dir)s"
    , perl => "%(perl_dir)s"
    , python => "%(python_dir)s"
    , pyexts => "%(python_externals_dir)s"
    , share => "%(share_dir)s"
    , schema => "%(schema_dir)s"
    );
unshift( @INC, $pegasus{perl} );
"""
        % kw,
        end="",
    )


def _python_hash(**kw):
    """."""
    print(
        """pegasus_bin_dir = "%(bin_dir)s"
pegasus_conf_dir = "%(conf_dir)s"
pegasus_java_dir = "%(java_dir)s"
pegasus_perl_dir = "%(perl_dir)s"
pegasus_python_dir = "%(python_dir)s"
pegasus_python_externals_dir = "%(python_externals_dir)s"
pegasus_share_dir = "%(share_dir)s"
pegasus_schema_dir = "%(schema_dir)s"
"""
        % kw,
        end="",
    )


def _sh_dump(**kw):
    """."""
    print(
        """PEGASUS_BIN_DIR="%(bin_dir)s";
export PEGASUS_BIN_DIR
PEGASUS_CONF_DIR="%(conf_dir)s"
export PEGASUS_CONF_DIR
PEGASUS_JAVA_DIR="%(java_dir)s"
export PEGASUS_JAVA_DIR
PEGASUS_PERL_DIR="%(perl_dir)s"
export PEGASUS_PERL_DIR
PEGASUS_PYTHON_DIR="%(python_dir)s"
export PEGASUS_PYTHON_DIR
PEGASUS_PYTHON_EXTERNALS_DIR="%(python_externals_dir)s"
export PEGASUS_PYTHON_EXTERNALS_DIR
PEGASUS_SHARE_DIR="%(share_dir)s"
export PEGASUS_SHARE_DIR
PEGASUS_SCHEMA_DIR="%(schema_dir)s"
export PEGASUS_SCHEMA_DIR
CLASSPATH="%(classpath)s"
export CLASSPATH
"""
        % kw,
        end="",
    )


def _get_bin_dir(exe):
    bin_dir = normpath(join(dirname(abspath(exe)), "bin"))

    while not exists(bin_dir):
        bin_dir = normpath(join(bin_dir, "..", "..", "bin"))

    return bin_dir


def _main(
    version=False,
    perl_dump=False,
    perl_hash=False,
    python_hash=False,
    sh_dump=False,
    bin=False,
    conf=False,
    java=False,
    perl=False,
    python=False,
    python_externals=False,
    schema=False,
    r=False,
    classpath=False,
    noeoln=False,
):
    """."""
    _version = "@PEGASUS_VERSION@"

    bin_dir = _get_bin_dir(sys.argv[0])
    base_dir = dirname(bin_dir)

    lib = "@LIBDIR@"  # lib64 for 64bit RPMS
    if lib.startswith("@"):
        lib = "lib"

    python_lib = "@PYTHON_LIBDIR@"
    if python_lib.startswith("@"):
        python_lib = "lib/pegasus/python"

    conf_dir = join(base_dir, "etc")
    share_dir = join(base_dir, "share", "pegasus")
    java_dir = join(share_dir, "java")
    perl_dir = join(base_dir, lib, "pegasus", "perl")
    python_dir = join(base_dir, python_lib)
    python_externals_dir = join(base_dir, lib, "pegasus", "externals", "python")
    schema_dir = join(share_dir, "schema")
    r_dir = "".join(sorted(glob(join(share_dir, "r", "*.tar.gz"))))

    # for development - running out of a source checkout
    test = join(base_dir, "build", "classes")
    extra_classpath = test if exists(test) else ""

    # in native packaging mode, some directories move
    if base_dir == "/usr":
        conf_dir = "/etc/pegasus"

    # classpath
    jars = sorted(glob(join(java_dir, "*.jar")))
    if extra_classpath:
        jars.insert(0, extra_classpath)

    _classpath = ":".join(jars)
    if "CLASSPATH" in os.environ:
        _classpath += ":" + os.environ["CLASSPATH"]

    # construct aws batch classpath
    aws_jars = sorted(glob(join(java_dir, "aws", "*.jar")))
    _classpath += ":" + ":".join(aws_jars)

    eol = "" if noeoln else "\n"

    if version:
        print(_version, end=eol)
    elif perl_dump:
        _perl_dump(
            bin_dir=bin_dir,
            conf_dir=conf_dir,
            java_dir=java_dir,
            perl_dir=perl_dir,
            python_dir=python_dir,
            python_externals_dir=python_externals_dir,
            share_dir=share_dir,
            schema_dir=schema_dir,
        )
    elif perl_hash:
        _perl_hash(
            bin_dir=bin_dir,
            conf_dir=conf_dir,
            java_dir=java_dir,
            perl_dir=perl_dir,
            python_dir=python_dir,
            python_externals_dir=python_externals_dir,
            share_dir=share_dir,
            schema_dir=schema_dir,
        )
    elif python_hash:
        _python_hash(
            bin_dir=bin_dir,
            conf_dir=conf_dir,
            java_dir=java_dir,
            perl_dir=perl_dir,
            python_dir=python_dir,
            python_externals_dir=python_externals_dir,
            share_dir=share_dir,
            schema_dir=schema_dir,
        )
    elif sh_dump:
        _sh_dump(
            bin_dir=bin_dir,
            conf_dir=conf_dir,
            java_dir=java_dir,
            perl_dir=perl_dir,
            python_dir=python_dir,
            python_externals_dir=python_externals_dir,
            share_dir=share_dir,
            schema_dir=schema_dir,
            classpath=_classpath,
        )
    elif bin:
        print(bin_dir, end=eol)
    elif conf:
        print(conf_dir, end=eol)
    elif java:
        print(java_dir, end=eol)
    elif perl:
        print(perl_dir, end=eol)
    elif python:
        print(python_dir, end=eol)
    elif python_externals:
        print(python_externals_dir, end=eol)
    elif schema:
        print(schema_dir, end=eol)
    elif r:
        print(r_dir, end=eol)
    elif classpath:
        print(_classpath, end=eol)
    else:
        # Code should not reach here.
        pass


def main():
    """."""
    parser = argparse.ArgumentParser(
        description="This is NOT an application to configure Pegasus, but an application to query the current Pegasus installation."
    )

    parser.add_argument(
        "--version",
        "-V",
        action="store_true",
        help="Print Pegasus version information and exit.",
    )

    parser.add_argument(
        "--perl-dump",
        action="store_true",
        help="Dumps all settings in perl format as separate variables.",
    )
    parser.add_argument(
        "--perl-hash",
        action="store_true",
        help="Dumps all settings in perl format as single perl hash.",
    )
    parser.add_argument(
        "--python-hash",
        action="store_true",
        help="Dumps all settings in python format.",
    )
    parser.add_argument(
        "--sh-dump", action="store_true", help="Dumps all settings in shell format.",
    )

    parser.add_argument(
        "--bin",
        action="store_true",
        help="Print the directory containing Pegasus binaries.",
    )
    parser.add_argument(
        "--conf",
        action="store_true",
        help="Print the directory containing configuration files.",
    )
    parser.add_argument(
        "--java", action="store_true", help="Print the directory containing the jars.",
    )
    parser.add_argument(
        "--perl",
        action="store_true",
        help="Print the directory to include into your PERL5LIB.",
    )
    parser.add_argument(
        "--python",
        action="store_true",
        help="Print the directory to include into your PYTHONPATH.",
    )
    parser.add_argument(
        "--python-externals",
        action="store_true",
        help="Print the directory to the external Python libraries.",
    )
    parser.add_argument(
        "--schema", action="store_true", help="Print the directory containing schemas.",
    )
    parser.add_argument(
        "--r",
        action="store_true",
        help="Print the path to the R DAX API source package.",
    )
    parser.add_argument(
        "--classpath",
        action="store_true",
        help="Builds a classpath containing the Pegasus jars.",
    )

    parser.add_argument(
        "--noeoln",
        action="store_true",
        help="Do not produce a end-of-line after output. This is useful when being called from non-shell backticks in scripts.",
    )

    # Ensure at least one option is passed
    if len(sys.argv) < 2:
        parser.print_help()
        sys.exit(1)

    args = parser.parse_args()
    _main(**vars(args))


if __name__ == "__main__":
    main()
