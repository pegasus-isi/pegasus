import os
import subprocess
import sys

import pytest

from Pegasus import cli

PYTHON_INTERPRETER = sys.executable
PEGASUS_INTEGRITY = cli.__path__[0] + "/pegasus-integrity.py"


@pytest.fixture(scope="package")
def data_files(resource_path_root):
    cwd = os.getcwd()
    os.chdir(str(resource_path_root / "pegasus-integrity"))
    yield
    os.chdir(cwd)


@pytest.mark.parametrize(
    "args,expected",
    [
        ("--generate=data.1", 0),
        ("--generate=data.1:data.2", 0),
        ("--generate-xml=data.1", 0),
        ("--generate-xml=foo1=data.1:foo2=data.2", 0),
    ],
)
def test_generate(data_files, args, expected):
    rv = subprocess.run([PYTHON_INTERPRETER, PEGASUS_INTEGRITY, args])

    assert rv.returncode == expected


@pytest.mark.parametrize(
    "args,expected", [("--verify=data.1", 0), ("--verify=data.1:foo.2=data.2", 0),],
)
def test_verify(data_files, args, expected):
    rv = subprocess.run([PYTHON_INTERPRETER, PEGASUS_INTEGRITY, args])

    assert rv.returncode == expected


def test_verify_multiple_stdin(data_files):
    print(cli.__path__)
    rv = subprocess.run(
        [PYTHON_INTERPRETER, PEGASUS_INTEGRITY, "--print-timings", "--verify=stdin"],
        input=b"data.1:foo.2=data.2",
    )

    assert rv.returncode == 0
