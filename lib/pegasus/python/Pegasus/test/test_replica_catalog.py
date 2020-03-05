import json
from tempfile import NamedTemporaryFile, TemporaryFile

import pytest

import Pegasus
from Pegasus import yaml
from Pegasus.api.replica_catalog import ReplicaCatalog
from Pegasus.replica_catalog import _to_rc, dump, dumps, load, loads


@pytest.fixture(scope="module")
def rc_as_dict():
    return {
        "pegasus": "5.0",
        "replicas": [{"lfn": "a", "pfn": "/a", "site": "local", "regex": False}],
    }


def test_to_rc(rc_as_dict):

    expected = ReplicaCatalog().add_replica("local", "a", "/a", regex=False)
    result = _to_rc(rc_as_dict)

    assert result.replicas == expected.replicas


def test_load(mocker, rc_as_dict):
    mocker.patch("Pegasus.yaml.load", return_value=rc_as_dict)
    with TemporaryFile() as f:
        rc = load(f)
        Pegasus.yaml.load.assert_called_once_with(f)

        assert len(rc.replicas) == 1
        assert ("local", "a", "/a", False) in rc.replicas


def test_loads(mocker, rc_as_dict):
    mocker.patch("Pegasus.yaml.load", return_value=rc_as_dict)
    rc = loads(json.dumps(rc_as_dict))

    assert len(rc.replicas) == 1
    assert ("local", "a", "/a", False) in rc.replicas


def test_dump(mocker):
    mocker.patch("Pegasus.api.writable.Writable.write")
    rc = ReplicaCatalog()
    with NamedTemporaryFile(mode="w") as f:
        dump(rc, f, _format="yml")
        Pegasus.api.writable.Writable.write.assert_called_once_with(f, _format="yml")


def test_dumps(rc_as_dict):
    rc = ReplicaCatalog().add_replica("local", "a", "/a", regex=False)
    assert yaml.load(dumps(rc)) == rc_as_dict
