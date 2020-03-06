import json
from tempfile import NamedTemporaryFile

import pytest
import yaml
from jsonschema import validate

from Pegasus.api.errors import DuplicateError
from Pegasus.api.replica_catalog import PEGASUS_VERSION, File, ReplicaCatalog


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

        assert "entry: {replica}".format(replica=("site", "lfn", "pfn", True)) in str(e)

    def test_add_invalid_replica(self):
        rc = ReplicaCatalog()
        with pytest.raises(TypeError) as e:
            rc.add_replica("site", set(), "pfn")

        assert "invalid lfn: {lfn}".format(lfn=set()) in str(e)

    def test_tojson(self, convert_yaml_schemas_to_json, load_schema):
        rc = ReplicaCatalog()
        rc.add_replica("site1", "lfn1", "pfn1", True)
        rc.add_replica("site2", "lfn2", "pfn2", True)

        expected = {
            "pegasus": PEGASUS_VERSION,
            "replicas": [
                {"lfn": "lfn1", "pfn": "pfn1", "site": "site1", "regex": True},
                {"lfn": "lfn2", "pfn": "pfn2", "site": "site2", "regex": True},
            ],
        }
        expected["replicas"] = sorted(expected["replicas"], key=lambda d: d["lfn"])

        result = rc.__json__()
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
