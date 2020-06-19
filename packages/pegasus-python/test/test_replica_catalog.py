import json
from tempfile import NamedTemporaryFile, TemporaryFile

import pytest

import Pegasus
from Pegasus import yaml
from Pegasus.api.replica_catalog import _PFN, ReplicaCatalog, _ReplicaCatalogEntry
from Pegasus.replica_catalog import _to_rc, dump, dumps, load, loads


@pytest.fixture(scope="module")
def rc_as_dict():
    return {
        "pegasus": "5.0",
        "replicas": [
            {
                "lfn": "a",
                "pfns": [{"site": "local", "pfn": "/a"}, {"site": "condorpool", "pfn": "/a"}],
                "checksum": {"sha256": "abc123"},
                "metadata": {"key": "value"}
            },
            {
                "lfn": "b*",
                "pfns": [{"site": "local", "pfn": "/b"}],
                "metadata": {"key": "value"},
                "regex": True
            }
        ]
    }

@pytest.fixture(scope="function")
def rc():
    return ReplicaCatalog()\
        .add_replica("local", "a", "/a", checksum={"sha256": "abc123"}, metadata={"key": "value"})\
        .add_replica("condorpool", "a", "/a")\
        .add_regex_replica("local", "b*", "/b", metadata={"key": "value"})

def test_to_rc(rc, rc_as_dict):
    result = _to_rc(rc_as_dict)

    assert result.entries[("a", False)].lfn == rc.entries[("a", False)].lfn
    assert result.entries[("a", False)].pfns == rc.entries[("a", False)].pfns
    assert result.entries[("a", False)].metadata == rc.entries[("a", False)].metadata
    assert result.entries[("a", False)].checksum == rc.entries[("a", False)].checksum

    assert result.entries[("b*", True)].lfn == rc.entries[("b*", True)].lfn
    assert result.entries[("b*", True)].pfns == rc.entries[("b*", True)].pfns
    assert result.entries[("b*", True)].metadata == rc.entries[("b*", True)].metadata


def test_load(mocker, rc_as_dict):
    mocker.patch("Pegasus.yaml.load", return_value=rc_as_dict)
    with TemporaryFile() as f:
        rc = load(f)
        Pegasus.yaml.load.assert_called_once_with(f)

        assert len(rc.entries) == 2
        assert rc.entries[("a", False)].lfn == "a"
        assert rc.entries[("a", False)].pfns == {_PFN("local", "/a"), _PFN("condorpool", "/a")}
        assert rc.entries[("a", False)].metadata == {"key": "value"}
        assert rc.entries[("a", False)].checksum == {"sha256": "abc123"}

        assert rc.entries[("b*", True)].lfn == "b*"
        assert rc.entries[("b*", True)].pfns == {_PFN("local", "/b")}
        assert rc.entries[("b*", True)].metadata == {"key": "value"}


def test_loads(mocker, rc_as_dict):
    mocker.patch("Pegasus.yaml.load", return_value=rc_as_dict)
    rc = loads(json.dumps(rc_as_dict))

    assert len(rc.entries) == 2
    assert rc.entries[("a", False)].lfn == "a"
    assert rc.entries[("a", False)].pfns == {_PFN("local", "/a"), _PFN("condorpool", "/a")}
    assert rc.entries[("a", False)].metadata == {"key": "value"}
    assert rc.entries[("a", False)].checksum == {"sha256": "abc123"}

    assert rc.entries[("b*", True)].lfn == "b*"
    assert rc.entries[("b*", True)].pfns == {_PFN("local", "/b")}
    assert rc.entries[("b*", True)].metadata == {"key": "value"}

def test_dump(mocker):
    mocker.patch("Pegasus.api.writable.Writable.write")
    rc = ReplicaCatalog()
    with NamedTemporaryFile(mode="w") as f:
        dump(rc, f, _format="yml")
        Pegasus.api.writable.Writable.write.assert_called_once_with(f, _format="yml")


def test_dumps(rc):
    result = _to_rc(yaml.load(dumps(rc)))
    
    assert len(result.entries) == 2
    assert result.entries[("a", False)].lfn == "a"
    assert result.entries[("a", False)].pfns == {_PFN("local", "/a"), _PFN("condorpool", "/a")}
    assert result.entries[("a", False)].metadata == {"key": "value"}
    assert result.entries[("a", False)].checksum == {"sha256": "abc123"}

    assert result.entries[("b*", True)].lfn == "b*"
    assert result.entries[("b*", True)].pfns == {_PFN("local", "/b")}
    assert result.entries[("b*", True)].metadata == {"key": "value"}