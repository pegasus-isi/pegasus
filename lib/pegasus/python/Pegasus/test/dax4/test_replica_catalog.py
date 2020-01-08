import os
import json

import pytest

from Pegasus.dax4.replica_catalog import File
from Pegasus.dax4.replica_catalog import ReplicaCatalog
from Pegasus.dax4.replica_catalog import PEGASUS_VERSION
from Pegasus.dax4.writable import FileFormat
from Pegasus.dax4.errors import NotFoundError
from Pegasus.dax4.errors import DuplicateError
from Pegasus.dax4.mixins import Namespace


class TestFile:
    @pytest.mark.parametrize("lfn", [("a"), ("例")])
    def test_valid_file(self, lfn: str):
        File(lfn)

    @pytest.mark.parametrize("lfn", [(1), (list())])
    def test_invalid_file(self, lfn: str):
        with pytest.raises(ValueError):
            File(lfn)

    def test_tojson_no_metadata(self):
        assert File("lfn").__json__() == {"lfn": "lfn"}

    def test_eq(self):
        assert File("a") == File("a")
        assert File("a") != File("b")
        assert File("a") != 1

    def test_tojson_with_metdata(self):
        assert File("lfn").add_metadata("key", "value").__json__() == {
            "lfn": "lfn",
            "metadata": {"key": "value"},
        }


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
        with pytest.raises(DuplicateError):
            rc.add_replica("lfn", "pfn", "site", True)
            rc.add_replica(File("lfn"), "pfn", "site", True)

    def test_add_invalid_replica(self):
        rc = ReplicaCatalog()
        with pytest.raises(ValueError):
            rc.add_replica(set(), "pfn", "site")

    @pytest.mark.parametrize(
        "replica", [("lfn", "pfn", "site", False), (File("lfn"), "pfn", "site", True)]
    )
    def test_has_replica(self, replica: tuple):
        rc = ReplicaCatalog()
        rc.add_replica(*replica)

        assert rc.has_replica(*replica) == True
        assert rc.has_replica("lfn2", "pfn", "site", False) == False

    def test_has_invalid_replica(self):
        rc = ReplicaCatalog()
        with pytest.raises(ValueError):
            rc.has_replica(set(), "pfn", "ste")

    def test_remove_replica(self):
        rc = ReplicaCatalog()
        replica = ("lfn", "pfn", "site")
        rc.add_replica(*replica)
        assert rc.has_replica(*replica) == True

        rc.remove_replica(*replica)
        assert len(rc.replicas) == 0
        assert rc.has_replica(*replica) == False

        with pytest.raises(NotFoundError):
            rc.remove_replica(*replica)

    def test_remove_invalid_replica(self):
        rc = ReplicaCatalog()
        with pytest.raises(ValueError):
            rc.remove_replica(set(), "pfn", "site")

    def test_tojson(self):
        rc = ReplicaCatalog()
        rc.add_replica("lfn1", "pfn1", "site1", True)
        rc.add_replica("lfn2", "pfn2", "site2", True)

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

        assert result == expected

    def test_write(self):
        rc = ReplicaCatalog()
        rc.add_replica("lfn1", "pfn1", "site1", True)
        rc.add_replica("lfn2", "pfn2", "site2", True)

        expected = {
            "pegasus": PEGASUS_VERSION,
            "replicas": [
                {"lfn": "lfn1", "pfn": "pfn1", "site": "site1", "regex": True},
                {"lfn": "lfn2", "pfn": "pfn2", "site": "site2", "regex": True},
            ],
        }
        expected["replicas"] = sorted(expected["replicas"], key=lambda d: d["lfn"])

        test_output_filename = os.path.join(
            os.path.dirname(os.path.realpath(__file__)), "ReplicaCatalogTestOutput.json"
        )

        rc.write(non_default_filepath=test_output_filename, file_format=FileFormat.JSON)

        with open(test_output_filename, "r") as f:
            result = json.load(f)
            result["replicas"] = sorted(result["replicas"], key=lambda d: d["lfn"])

        assert result == expected

        # cleanup
        os.remove(test_output_filename)
