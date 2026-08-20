#!/usr/bin/env python3


import argparse
import os
import sys
from glob import glob
from os.path import dirname, join

from Pegasus import data
from Pegasus.cli._paths import get_bin_dir


def _python_hash(**kw):
    """."""
    print(
        """pegasus_bin_dir = "{bin_dir}"
pegasus_conf_dir = "{conf_dir}"
pegasus_java_dir = "{java_dir}"
pegasus_python_dir = "{python_dir}"
pegasus_python_externals_dir = "{python_externals_dir}"
pegasus_share_dir = "{share_dir}"
pegasus_schema_dir = "{schema_dir}"
""".format(**kw),
        end="",
    )


def _sh_dump(**kw):
    """."""
    print(
        """PEGASUS_BIN_DIR="{bin_dir}";
export PEGASUS_BIN_DIR
PEGASUS_CONF_DIR="{conf_dir}"
export PEGASUS_CONF_DIR
PEGASUS_JAVA_DIR="{java_dir}"
export PEGASUS_JAVA_DIR
PEGASUS_PYTHON_DIR="{python_dir}"
export PEGASUS_PYTHON_DIR
PEGASUS_PYTHON_EXTERNALS_DIR="{python_externals_dir}"
export PEGASUS_PYTHON_EXTERNALS_DIR
PEGASUS_SHARE_DIR="{share_dir}"
export PEGASUS_SHARE_DIR
PEGASUS_SCHEMA_DIR="{schema_dir}"
export PEGASUS_SCHEMA_DIR
CLASSPATH="{classpath}"
export CLASSPATH
""".format(**kw),
        end="",
    )


def _get_version():
    """
    Get version from package metadata
    """
    from importlib.metadata import PackageNotFoundError, version

    try:
        return version("pegasus-wms")
    except PackageNotFoundError:
        return "unknown"


def _main(
    version=False,
    python_hash=False,
    sh_dump=False,
    bin=False,
    conf=False,
    java=False,
    python=False,
    python_externals=False,
    schema=False,
    r=False,
    classpath=False,
    noeoln=False,
):
    """."""
    _version = _get_version()

    # where the console scripts live - resolved from the install, not from
    # sys.argv[0], which main._run_script rewrites to a bare script name
    bin_dir = get_bin_dir()
    base_dir = dirname(bin_dir)

    # <site-packages>/Pegasus/data, holding everything that used to live
    # under $PEGASUS_HOME/{share/pegasus,etc}
    data_dir = data.__path__[0]
    conf_dir = join(data_dir, "etc")
    share_dir = data_dir
    java_dir = join(data_dir, "java")
    # the directory Pegasus itself is importable from
    python_dir = dirname(dirname(data_dir))
    python_externals_dir = python_dir
    schema_dir = join(data_dir, "schema")
    r_dir = "".join(sorted(glob(join(data_dir, "r", "*.tar.gz"))))

    # in native packaging mode, some directories move
    if base_dir == "/usr":
        conf_dir = "/etc/pegasus"

    # classpath
    jars = sorted(glob(join(java_dir, "*.jar")))

    _classpath = ":".join(jars)
    if "CLASSPATH" in os.environ:
        _classpath += ":" + os.environ["CLASSPATH"]

    # construct aws batch classpath
    aws_jars = sorted(glob(join(java_dir, "aws", "*.jar")))
    _classpath += ":" + ":".join(aws_jars)

    eol = "" if noeoln else "\n"

    if version:
        print(_version, end=eol)
    elif python_hash:
        _python_hash(
            bin_dir=bin_dir,
            conf_dir=conf_dir,
            java_dir=java_dir,
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
        "--python-hash",
        action="store_true",
        help="Dumps all settings in python format.",
    )
    parser.add_argument(
        "--sh-dump",
        action="store_true",
        help="Dumps all settings in shell format.",
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
        "--java",
        action="store_true",
        help="Print the directory containing the jars.",
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
        "--schema",
        action="store_true",
        help="Print the directory containing schemas.",
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
