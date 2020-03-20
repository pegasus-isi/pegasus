import json
from pathlib import Path
from tempfile import NamedTemporaryFile

import pytest
import yaml
from jsonschema import validate

from Pegasus.api.errors import DuplicateError
from Pegasus.api.writable import _CustomEncoder
from Pegasus.api.replica_catalog import (
    PEGASUS_VERSION,
    File,
    ReplicaCatalog,
    _ReplicaCatalogEntry,
)


class TestFile:
    @pytest.mark.parametrize("lfn", [("a"), ("例")])
    def test_valid_file(self, lfn: str):
        assert File(lfn)

    @pytest.mark.parametrize("lfn", [(1), (list())])
    def test_invalid_file(self, lfn: str):
        with pytest.raises(TypeError) as e:
            File(lfn)

        assert "invalid lfn: {lfn}".format(lfn=lfn) in str(e)

    def test_tojson_no_metadata(self):
        assert File("lfn").__json__() == {"lfn": "lfn"}

    def test_eq(self):
        assert File("a") == File("a")
        assert File("a") != File("b")
        assert File("a") != 1

    def test_tojson_with_metdata(self, convert_yaml_schemas_to_json, load_schema):
        result = File("lfn").add_metadata(key="value").__json__()
        expected = {
            "lfn": "lfn",
            "metadata": {"key": "value"},
        }

        file_schema = load_schema("rc-5.0.json")["$defs"]["file"]
        validate(instance=result, schema=file_schema)

        assert result == expected


class Test_ReplicaCatalogEntry:
    @pytest.mark.parametrize(
        "site, lfn, pfn, regex, checksum_type, checksum_value",
        [
            ("local", "f.a", "/f.a", False, None, None),
            ("local", "f.a", "/f.a", True, None, None),
            ("local", "f.a", "/f.a", False, "sha256", "abc123"),
        ],
    )
    def test_valid_entry(self, site, lfn, pfn, regex, checksum_type, checksum_value):
        assert _ReplicaCatalogEntry(
            site, lfn, pfn, regex, checksum_type, checksum_value
        )

    def test_regex_and_checksum_in_entry(self):
        with pytest.raises(ValueError) as e:
            _ReplicaCatalogEntry(
                "local",
                "f.a",
                "/f.a",
                regex=True,
                checksum_type="sha256",
                checksum_value="abc123",
            )

        assert "Checksum values cannot be used with a regex" in str(e)

    def test_invalid_checksum(self):
        with pytest.raises(ValueError) as e:
            _ReplicaCatalogEntry(
                "local", "f.a", "/f.a", checksum_type="sha256", checksum_value=None
            )

        assert "Checksum usage in replica catalog requires" in str(e)

    @pytest.mark.parametrize(
        "a,b",
        [
            (
                _ReplicaCatalogEntry("local", "f.a", "/f.a"),
                _ReplicaCatalogEntry("local", "f.a", "/f.a"),
            ),
            (
                _ReplicaCatalogEntry("local", "f.a", "/f.a", True),
                _ReplicaCatalogEntry("local", "f.a", "/f.a", True),
            ),
            (
                _ReplicaCatalogEntry(
                    "local",
                    "f.a",
                    "/f.a",
                    checksum_type="sha256",
                    checksum_value="abc123",
                ),
                _ReplicaCatalogEntry(
                    "local",
                    "f.a",
                    "/f.a",
                    checksum_type="sha256",
                    checksum_value="abc123",
                ),
            ),
        ],
    )
    def test_eq(self, a, b):
        assert a == b

    @pytest.mark.parametrize(
        "result, expected",
        [
            (
                _ReplicaCatalogEntry("local", "f.a", "/f.a").__json__(),
                {"site": "local", "lfn": "f.a", "pfn": "/f.a",},
            ),
            (
                _ReplicaCatalogEntry("local", "f.a", "/f.a", regex=True).__json__(),
                {"site": "local", "lfn": "f.a", "pfn": "/f.a", "regex": True},
            ),
            (
                _ReplicaCatalogEntry(
                    "local",
                    "f.a",
                    "/f.a",
                    checksum_type="sha256",
                    checksum_value="abc123",
                ).__json__(),
                {
                    "site": "local",
                    "lfn": "f.a",
                    "pfn": "/f.a",
                    "checksum": {"type": "sha256", "value": "abc123"},
                },
            ),
        ],
    )
    def test_tojson(self, result, expected):
        assert result == expected


class TestReplicaCatalog:
    @pytest.mark.parametrize(
        "replica", [("lfn", "pfn", "site", True), (File("lfn"), "pfn", "site", True)]
    )
    def test_add_replica(self, replica: tuple):
        rc = ReplicaCatalog()
        rc.add_replica(*replica)
        assert len(rc.replicas) == 1

    def test_add_duplicate_replica(self):
        rc = ReplicaCatalog()
        with pytest.raises(DuplicateError) as e:
            rc.add_replica("site", "lfn", "pfn", True)
            rc.add_replica("site", File("lfn"), "pfn", True)

        assert (
            "entry: ReplicaEntry(site=site, lfn=lfn, pfn=pfn, regex=True, checksum_type=None, checksum_value=None)"
            in str(e)
        )

    def test_add_invalid_replica(self):
        rc = ReplicaCatalog()
        with pytest.raises(TypeError) as e:
            rc.add_replica("site", set(), "pfn")

        assert "invalid lfn: {lfn}".format(lfn=set()) in str(e)

    def test_tojson(self, convert_yaml_schemas_to_json, load_schema):
        rc = ReplicaCatalog()
        rc.add_replica("site1", "lfn1", "pfn1")
        rc.add_replica("site2", "lfn2", "pfn2", True)
        rc.add_replica(
            "site3", "lfn3", "pfn3", checksum_type="sha256", checksum_value="abc123"
        )

        expected = {
            "pegasus": PEGASUS_VERSION,
            "replicas": [
                {"lfn": "lfn1", "pfn": "pfn1", "site": "site1"},
                {"lfn": "lfn2", "pfn": "pfn2", "site": "site2", "regex": True},
                {
                    "lfn": "lfn3",
                    "pfn": "pfn3",
                    "site": "site3",
                    "checksum": {"type": "sha256", "value": "abc123"},
                },
            ],
        }
        expected["replicas"] = sorted(expected["replicas"], key=lambda d: d["lfn"])

        result = json.loads(json.dumps(rc, cls=_CustomEncoder))
        result["replicas"] = sorted(result["replicas"], key=lambda d: d["lfn"])

        rc_schema = load_schema("rc-5.0.json")
        validate(instance=result, schema=rc_schema)

        assert result == expected

    @pytest.mark.parametrize(
        "_format, loader", [("json", json.load), ("yml", yaml.safe_load)]
    )
    def test_write(self, _format, loader):
        rc = ReplicaCatalog()
        rc.add_replica("site1", "lfn1", "pfn1", True).add_replica(
            "site2", "lfn2", "pfn2", True
        )

        expected = {
            "pegasus": PEGASUS_VERSION,
            "replicas": [
                {"lfn": "lfn1", "pfn": "pfn1", "site": "site1", "regex": True},
                {"lfn": "lfn2", "pfn": "pfn2", "site": "site2", "regex": True},
            ],
        }
        expected["replicas"] = sorted(expected["replicas"], key=lambda d: d["lfn"])

        with NamedTemporaryFile(mode="r+") as f:
            rc.write(f, _format=_format)
            f.seek(0)
            result = loader(f)

        result["replicas"] = sorted(result["replicas"], key=lambda d: d["lfn"])

        assert result == expected

    def test_write_default(self):
        expected_file = Path("replicas.yml")
        ReplicaCatalog().write()

        try:
            expected_file.unlink()
        except FileNotFoundError:
            pytest.fail("could not find {}".format(expected_file))
