"""pegasus config — query the current Pegasus installation.

This is a rewrite of the original pegasus-config.py to use importlib.resources
for locating package data instead of filesystem-relative paths.
"""

import os
import shutil
import sys
from glob import glob
from pathlib import Path

import click


def _get_data_dir():
    """Locate the Pegasus data directory using importlib.resources."""
    try:
        from importlib.resources import files

        data_dir = Path(str(files("Pegasus.data")))
        if data_dir.is_dir():
            return data_dir
    except (ImportError, TypeError, ModuleNotFoundError):
        pass

    # Fallback: relative to this file
    cli_dir = Path(__file__).resolve().parent
    pegasus_dir = cli_dir.parent

    # Check for src/Pegasus/data/
    data_dir = pegasus_dir / "data"
    if data_dir.is_dir():
        return data_dir

    # Check for repo-root share/pegasus/ (development mode)
    candidate = pegasus_dir
    for _ in range(10):
        share_dir = candidate / "share" / "pegasus"
        if share_dir.is_dir():
            return share_dir
        candidate = candidate.parent

    return None


def _get_paths():
    """Compute all Pegasus installation paths."""
    data_dir = _get_data_dir()

    if data_dir is None:
        click.echo("Error: Cannot locate Pegasus data directory", err=True)
        sys.exit(1)

    # Determine layout
    if (data_dir / "java").is_dir() and data_dir.name == "data":
        # New layout: src/Pegasus/data/
        base_dir = data_dir.parent.parent.parent  # src/Pegasus/data -> src/Pegasus -> src -> root
        bin_dir = str(Path(shutil.which("pegasus") or "").parent) if shutil.which("pegasus") else str(data_dir / "bin")
        conf_dir = str(data_dir / "etc")
        java_dir = str(data_dir / "java")
        python_dir = str(data_dir.parent.parent)  # src/
        python_externals_dir = python_dir  # No separate externals in new layout
        share_dir = str(data_dir)
        schema_dir = str(data_dir / "schema")
    else:
        # Old layout: share/pegasus/
        base_dir = data_dir.parent.parent
        bin_dir = str(base_dir / "bin")
        conf_dir = str(base_dir / "etc")
        java_dir = str(data_dir / "java")
        python_dir = str(base_dir / "lib" / "pegasus" / "python")
        python_externals_dir = str(base_dir / "lib" / "pegasus" / "externals" / "python")
        share_dir = str(data_dir)
        schema_dir = str(data_dir / "schema")

        # Native packaging mode
        if str(base_dir) == "/usr":
            conf_dir = "/etc/pegasus"

    return {
        "bin_dir": bin_dir,
        "conf_dir": conf_dir,
        "java_dir": java_dir,
        "python_dir": python_dir,
        "python_externals_dir": python_externals_dir,
        "share_dir": share_dir,
        "schema_dir": schema_dir,
    }


def _build_classpath(java_dir):
    """Build Java classpath from JARs directory."""
    jars = sorted(glob(os.path.join(java_dir, "*.jar")))
    aws_jars = sorted(glob(os.path.join(java_dir, "aws", "*.jar")))
    all_jars = jars + aws_jars

    classpath = ":".join(all_jars)
    env_classpath = os.environ.get("CLASSPATH", "")
    if env_classpath:
        classpath = classpath + ":" + env_classpath

    return classpath


@click.command("config")
@click.option("--version", "-V", "show_version", is_flag=True, help="Print Pegasus version information and exit.")
@click.option("--python-hash", is_flag=True, help="Dumps all settings in python format.")
@click.option("--sh-dump", is_flag=True, help="Dumps all settings in shell format.")
@click.option("--bin", "show_bin", is_flag=True, help="Print the directory containing Pegasus binaries.")
@click.option("--conf", "show_conf", is_flag=True, help="Print the directory containing configuration files.")
@click.option("--java", "show_java", is_flag=True, help="Print the directory containing the jars.")
@click.option("--python", "show_python", is_flag=True, help="Print the directory to include into your PYTHONPATH.")
@click.option("--python-externals", is_flag=True, help="Print the directory to the external Python libraries.")
@click.option("--schema", "show_schema", is_flag=True, help="Print the directory containing schemas.")
@click.option("--r", "show_r", is_flag=True, help="Print the path to the R DAX API source package.")
@click.option("--classpath", "show_classpath", is_flag=True, help="Builds a classpath containing the Pegasus jars.")
@click.option("--noeoln", is_flag=True, help="Do not produce an end-of-line after output.")
def config(
    show_version, python_hash, sh_dump, show_bin, show_conf,
    show_java, show_python, python_externals, show_schema,
    show_r, show_classpath, noeoln,
):
    """Query the current Pegasus installation paths and configuration."""
    # Version from package metadata
    _version = "5.2.0"
    try:
        from importlib.metadata import version as pkg_version
        _version = pkg_version("pegasus-wms")
    except (ImportError, Exception):
        pass

    paths = _get_paths()
    eol = "" if noeoln else "\n"

    if show_version:
        print(_version, end=eol)
    elif python_hash:
        print(
            'pegasus_bin_dir = "%(bin_dir)s"\n'
            'pegasus_conf_dir = "%(conf_dir)s"\n'
            'pegasus_java_dir = "%(java_dir)s"\n'
            'pegasus_python_dir = "%(python_dir)s"\n'
            'pegasus_python_externals_dir = "%(python_externals_dir)s"\n'
            'pegasus_share_dir = "%(share_dir)s"\n'
            'pegasus_schema_dir = "%(schema_dir)s"\n' % paths,
            end="",
        )
    elif sh_dump:
        classpath = _build_classpath(paths["java_dir"])
        print(
            'PEGASUS_BIN_DIR="%(bin_dir)s";\n'
            "export PEGASUS_BIN_DIR\n"
            'PEGASUS_CONF_DIR="%(conf_dir)s"\n'
            "export PEGASUS_CONF_DIR\n"
            'PEGASUS_JAVA_DIR="%(java_dir)s"\n'
            "export PEGASUS_JAVA_DIR\n"
            'PEGASUS_PYTHON_DIR="%(python_dir)s"\n'
            "export PEGASUS_PYTHON_DIR\n"
            'PEGASUS_PYTHON_EXTERNALS_DIR="%(python_externals_dir)s"\n'
            "export PEGASUS_PYTHON_EXTERNALS_DIR\n"
            'PEGASUS_SHARE_DIR="%(share_dir)s"\n'
            "export PEGASUS_SHARE_DIR\n"
            'PEGASUS_SCHEMA_DIR="%(schema_dir)s"\n'
            "export PEGASUS_SCHEMA_DIR\n" % paths,
            end="",
        )
        print(
            'CLASSPATH="%s"\nexport CLASSPATH\n' % classpath,
            end="",
        )
    elif show_bin:
        print(paths["bin_dir"], end=eol)
    elif show_conf:
        print(paths["conf_dir"], end=eol)
    elif show_java:
        print(paths["java_dir"], end=eol)
    elif show_python:
        print(paths["python_dir"], end=eol)
    elif python_externals:
        print(paths["python_externals_dir"], end=eol)
    elif show_schema:
        print(paths["schema_dir"], end=eol)
    elif show_r:
        # R support - look for R tarball
        r_dir = ""
        data_dir = _get_data_dir()
        if data_dir:
            r_tarballs = sorted(glob(str(data_dir / "r" / "*.tar.gz")))
            r_dir = "".join(r_tarballs)
        print(r_dir, end=eol)
    elif show_classpath:
        print(_build_classpath(paths["java_dir"]), end=eol)
    else:
        # No option specified, show help
        ctx = click.get_current_context()
        click.echo(ctx.get_help())
        ctx.exit(1)
