import logging
import os
from configparser import DEFAULTSECT
from pathlib import Path
from tempfile import NamedTemporaryFile

import pytest

from Pegasus.api.properties import Properties


@pytest.fixture(scope="function")
def props():
    return Properties()


def test_ls(capsys, props):
    try:
        Properties.ls("pegasus.pmc")
        captured = capsys.readouterr().out
        assert (
            captured
            == """pegasus.pmc_priority
pegasus.pmc_request_cpus
pegasus.pmc_request_memory
pegasus.pmc_task_arguments
"""
        )

        Properties.ls()
        Properties.ls("nothing")
        props.ls()
    except Exception:
        pytest.raises("should not have failed")


@pytest.mark.parametrize(
    "k, v",
    [
        ("pegasus.mode", "development"),
        ("pegasus.catalog.replica.db.user", "alpha"),
        ("pegasus.catalog.replica.db.useSSL", True),
        ("pegasus.ppn", 100),
        ("pegasus.catalog.site.file", Path("dir/regular.txt")),
        ("pegasus.catalog.site.file", Path("dir/with space.txt")),
    ],
)
def test_set_item(k, v, props):
    props[k] = v
    assert props[k] == str(v)


@pytest.mark.parametrize(
    "property_list,expected_result",
    [
        (Properties._props, True),
        (Properties._pattern_props, True),
        (["invalid", "Pegasus.mode", "pegasus.catalog.replica.db."], False),
    ],
)
def test__check_key(property_list, expected_result):
    for property in property_list:
        result = Properties._check_key(property)
        assert (
            result == expected_result
        ), "Properties._check_key('{}') returned {} but {} was expected".format(
            property, result, expected_result
        )


@pytest.mark.parametrize(
    "k, v",
    [
        ("invalid", "development"),
        ("Pegasus.mode", "development"),
        ("pegasus.catalog.replica.db.", "alpha"),
    ],
)
def test_set_item_fail(caplog, k, v, props):
    props[k] = v
    # TODO
    print("hi", caplog.record_tuples)

    assert caplog.record_tuples == [
        (
            "Pegasus.api.properties",
            logging.WARNING,
            "Unrecognized property key: '{}' has been set to '{}'".format(k, v),
        )
    ]


@pytest.mark.parametrize(
    "k, v",
    [
        ("pegasus.mode", "development"),
        ("pegasus.catalog.replica.db.user", "alpha"),
        ("pegasus.catalog.replica.db.useSSL", True),
        ("pegasus.ppn", 100),
        ("pegasus.catalog.site.file", Path("dir/regular.txt")),
        ("pegasus.catalog.site.file", Path("dir/with space.txt")),
    ],
)
def test_get_item(k, v, props):
    props[k] = v
    assert props[k] == str(v)


@pytest.mark.parametrize(
    "k, v",
    [
        ("pegasus.mode", "development"),
        ("pegasus.catalog.replica.db.user", "alpha"),
        ("pegasus.catalog.replica.db.useSSL", True),
    ],
)
def test_del_item(k, v, props):
    props[k] = v
    del props[k]

    assert k not in props._conf[DEFAULTSECT]


def test_write_str_filename(props):
    with NamedTemporaryFile(mode="w+") as f:
        props["pegasus.mode"] = "development"
        props["globus.queue"] = "main"
        props.write(f.name)

        f.seek(0)
        assert (
            f.read()
            == """pegasus.mode = development
globus.queue = main

"""
        )


def test_write_str_filename_ensure_key_case_preserved(props):
    with NamedTemporaryFile(mode="w+") as f:
        props["env.PEGASUS_HOME"] = "HOME"
        props["env.pegasus_home"] = "home"
        props.write(f)

        f.seek(0)
        assert (
            f.read()
            == """env.PEGASUS_HOME = HOME
env.pegasus_home = home

"""
        )


def test_write_file(props):
    with NamedTemporaryFile(mode="w+") as f:
        props["pegasus.mode"] = "development"
        props.write(f)

        f.seek(0)
        assert f.read() == "pegasus.mode = development\n\n"


def test_write_invalid_file(props):
    with pytest.raises(TypeError) as e:
        props.write(123)

    assert "invalid file: 123" in str(e)


def test_write_default_file(props):
    props["pegasus.mode"] = "development"
    props.write()

    EXPECTED_DEFAULT_FILE = "pegasus.properties"
    with open(EXPECTED_DEFAULT_FILE) as f:
        assert f.read() == "pegasus.mode = development\n\n"

    os.remove(EXPECTED_DEFAULT_FILE)
