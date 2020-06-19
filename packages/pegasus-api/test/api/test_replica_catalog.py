import json
import re
from pathlib import Path
from tempfile import NamedTemporaryFile
from collections import OrderedDict


import pytest
from jsonschema import validate

import yaml

from Pegasus.api.errors import DuplicateError
from Pegasus.api.replica_catalog import (
    PEGASUS_VERSION,
    _PFN,
    File,
    ReplicaCatalog,
    _ReplicaCatalogEntry,
)
from Pegasus.api.writable import _CustomEncoder
from conftest import _tojson

class TestFile:
    @pytest.mark.parametrize("lfn,size", [("a", None), ("例", 2048)])
    def test_valid_file(self, lfn: str, size: int):
        assert File(lfn, size)

    @pytest.mark.parametrize("lfn", [(1), (list())])
    def test_invalid_file(self, lfn: str):
        with pytest.raises(TypeError) as e:
            File(lfn)

        assert "invalid lfn: {lfn}".format(lfn=lfn) in str(e)

    @pytest.mark.parametrize(
        "lfn,size,expected",
        [
            ("f1", None, {"lfn": "f1"}),
            ("f2", 2048, {"lfn": "f2", "size": 2048, "metadata": {"size": 2048}}),
        ],
    )
    def test_tojson_no_metadata(self, lfn, size, expected):
        assert File(lfn, size).__json__() == expected

    def test_eq(self):
        assert File("a") == File("a")
        assert File("a") != File("b")
        assert File("a") != 1

    def test_tojson_with_metdata(self, convert_yaml_schemas_to_json, load_schema):
        result = File("lfn", size=2048).add_metadata(key="value").__json__()
        expected = {
            "lfn": "lfn",
            "metadata": {"key": "value", "size": 2048},
            "size": 2048,
        }

        file_schema = load_schema("rc-5.0.json")["$defs"]["file"]
        validate(instance=result, schema=file_schema)

        assert result == expected


class Test_ReplicaCatalogEntry:
    @pytest.mark.parametrize(
        "lfn, pfns, checksum, metadata, regex",
        [
            ("f.a", {_PFN("local", "/f.a")}, None, None, False),
            ("f.a", {_PFN("local", "/f.a")}, {"sha245": "abc123"}, None, False),
            (
                "f.a",
                {_PFN("local", "/f.a")},
                {"sha245": "abc123"},
                {"owner": "ryan"},
                False,
            ),
            ("f.a", {_PFN("local", "/f.a")}, None, None, True),
            ("f.a", {_PFN("local", "/f.a")}, None, {"owner", "ryan"}, True),
        ],
    )
    def test_valid_entry(self, lfn, pfns, checksum, metadata, regex):
        assert _ReplicaCatalogEntry(
            lfn, pfns, checksum=checksum, metadata=metadata, regex=regex
        )

    @pytest.mark.parametrize(
        "result, expected",
        [
            (
                _tojson(_ReplicaCatalogEntry("f.a", {_PFN("local", "/f.a")})),
                {"lfn": "f.a", "pfns": [{"site": "local", "pfn": "/f.a"}]},
            ),
            (
                _tojson(
                    _ReplicaCatalogEntry(
                        "f.a", {_PFN("local", "/f.a")}, checksum={"sha256": "abc123"}
                    )
                ),
                {
                    "lfn": "f.a",
                    "pfns": [{"site": "local", "pfn": "/f.a"}],
                    "checksum": {"sha256": "abc123"},
                },
            ),
            (
                _tojson(
                    _ReplicaCatalogEntry(
                        "f.a",
                        {_PFN("local", "/f.a")},
                        checksum={"sha256": "abc123"},
                        metadata={"owner": "ryan"},
                    )
                ),
                {
                    "lfn": "f.a",
                    "pfns": [{"site": "local", "pfn": "/f.a"}],
                    "checksum": {"sha256": "abc123"},
                    "metadata": {"owner": "ryan"},
                },
            ),
            (
                _tojson(
                    _ReplicaCatalogEntry("f.a", {_PFN("local", "/f.a")}, regex=True)
                ),
                {
                    "lfn": "f.a",
                    "pfns": [{"site": "local", "pfn": "/f.a"}],
                    "regex": True,
                },
            ),
        ],
    )
    def test_tojson(self, result, expected):
        assert result == expected


class TestReplicaCatalog:
    def test_add_replica_str_as_lfn(self):
        rc = ReplicaCatalog()
        rc.add_replica("local", "f.a", "/f.a")
        assert _tojson(rc.entries[("f.a", False)]) == {
            "lfn": "f.a",
            "pfns": [{"site": "local", "pfn": "/f.a"}],
        }

    def test_add_replica_pfn_with_path_obj(self):
        rc = ReplicaCatalog()
        rc.add_replica("local", "test_replica_catalog", Path(__file__))

        # ensure that the path was resolved
        assert "packages/pegasus-api/test/api/test_replica_catalog.py" in rc.entries[("test_replica_catalog", False)].pfns.pop().pfn

    def test_add_replica_pfn_with_invalid_path_obj(self, tmpdir):
        test_dir = tmpdir.mkdir("test")
        
        rc = ReplicaCatalog()
        with pytest.raises(ValueError) as e:
            rc.add_replica("local", "f.a", Path(str(test_dir)))

        assert "Invalid pfn: {}, the given path".format(test_dir) in str(e)


    # TODO: why does this test break the following tests???
    '''
    def test_add_replica_file_as_lfn(self):
        rc = ReplicaCatalog()
        f = File("f.a", size=1024).add_metadata(creator="ryan")
        rc.add_replica("local", f, "/f.a")

        assert _tojson(rc.entries[("f.a", False)]) == {
            "lfn": "f.a",
            "pfns": [{"site": "local", "pfn": "/f.a"}],
            "metadata": {"size": 1024, "creator": "ryan"},
        }
    '''

    def test_add_multiple_replicas(self):
        rc = ReplicaCatalog()
        rc.add_replica("local", "f.a", "/f.a")
        rc.add_replica("local", "f.b", "/f.b")
        assert _tojson(rc.entries[("f.a", False)]) == {
            "lfn": "f.a",
            "pfns": [{"site": "local", "pfn": "/f.a"}],
        }

        assert _tojson(rc.entries[("f.b", False)]) == {
            "lfn": "f.b",
            "pfns": [{"site": "local", "pfn": "/f.b"}],
        }

        assert len(rc.entries) == 2

    def test_add_replica_multiple_pfns(self):
        rc = ReplicaCatalog()
        rc.add_replica("local", "f.a", "/f.a")
        rc.add_replica("condorpool", "f.a", "/f.a")
        rc.add_replica("condorpool", "f.a", "/f.a")

        assert len(rc.entries) == 1
        assert rc.entries[("f.a", False)].pfns == {
            _PFN("local", "/f.a"),
            _PFN("condorpool", "/f.a"),
        }

        assert rc.entries[("f.a", False)].metadata == {}

    def test_add_replica_multiple_pfns_checksums_and_metadata(self):
        rc = ReplicaCatalog()
        rc.add_replica(
            "local",
            "f.a",
            "/f.a",
            checksum={"sha256": "abc"},
            metadata={"creator": "ryan"},
        )

        rc.add_replica(
            "condorpool", "f.a", "/f.a", metadata={"size": 1024},
        )

        f_a_entry = rc.entries[("f.a", False)]
        assert f_a_entry.metadata == {"size": 1024, "creator": "ryan"}
        assert f_a_entry.checksum == {"sha256": "abc"}

    def test_add_replica_with_invalid_checksum(self):
        rc = ReplicaCatalog()
        with pytest.raises(ValueError) as e:
            rc.add_replica("local", "f.a", "/f.a", checksum={"md5": "123"})

        assert "Invalid checksum: md5" in str(e)

    def test_add_regex_replica(self):
        rc = ReplicaCatalog()
        rc.add_regex_replica("local", "*.txt", "/path")

        assert _tojson(rc.entries[("*.txt", True)]) == {
            "lfn": "*.txt",
            "pfns": [{"site": "local", "pfn": "/path"}],
            "regex": True
        }

    def test_add_duplicate_regex_replica(self):
        rc = ReplicaCatalog()
        rc.add_regex_replica("local", "*.txt", "/path")

        with pytest.raises(DuplicateError) as e:
            rc.add_regex_replica("local", "*.txt", "/path")
        
        assert "Pattern: *.txt already exists" in str(e)

    def test_add_regex_replica_with_metadata(self):
        rc = ReplicaCatalog()
        rc.add_regex_replica("local", "*.txt", "/path", metadata={"creator": "ryan"})

        assert _tojson(rc.entries[("*.txt", True)]) == {
            "lfn": "*.txt",
            "pfns": [{"site": "local", "pfn": "/path"}],
            "metadata": {"creator": "ryan"},
            "regex": True
        }

    def test_tojson(self):
        rc = ReplicaCatalog()
        rc.add_replica("local", "f.a", "/f.a", checksum={"sha256": "123"}, metadata={"size": 1024})
        rc.add_regex_replica("local", "*.txt", "/path", metadata={"creator": "ryan"})

        assert _tojson(rc) == {
                'pegasus': '5.0', 
                'replicas': [
                    {
                        'lfn': 'f.a', 
                        'pfns': [{'site': 'local', 'pfn': '/f.a'}], 
                        'checksum': {'sha256': '123'}, 
                        'metadata': {'size': 1024}
                    }, 
                    {
                        'lfn': '*.txt', 
                        'pfns': [{'site': 'local', 'pfn': '/path'}], 
                        'metadata': {'creator': 'ryan'}, 
                        'regex': True
                    }, 
                ]
            }

    @pytest.mark.parametrize(
        "_format, loader", [("json", json.load), ("yml", yaml.safe_load)]
    )
    def test_write(self, _format, loader):
        rc = ReplicaCatalog()
        f_a = File("f.a", size=1024).add_metadata(creator="ryan")
        rc.add_replica("local", f_a, "/f.a", checksum={"sha256": "123"}, metadata={"extra": "metadata"})
        rc.add_replica("condorpool", f_a, "/f.a")
        rc.add_replica("local", "f.b", "/f.b")
        rc.add_regex_replica("local", "*.txt", "/path", metadata={"creator": "ryan"})

        expected = {
            'pegasus': '5.0', 
            'replicas': [
                {
                    'lfn': 'f.a', 
                    'pfns': [{'site': 'local', 'pfn': '/f.a'}, {'site': 'condorpool', 'pfn': '/f.a'}], 
                    'checksum': {'sha256': '123'}, 
                    'metadata': {'extra': 'metadata', 'size': 1024, 'creator': 'ryan'}
                }, 
                {
                    'lfn': 'f.b', 
                    'pfns': [{'site': 'local', 'pfn': '/f.b'}], 
                    'metadata': {'size': 1024, 'creator': 'ryan'}
                }, 
                {
                    'lfn': '*.txt', 
                    'pfns': [{'site': 'local', 'pfn': '/path'}], 
                    'metadata': {'creator': 'ryan'}, 'regex': True}
            ]
        }
        expected["replicas"][0]["pfns"] = sorted( 
            expected["replicas"][0]["pfns"], key=lambda pfn: pfn["site"]
        )

        with NamedTemporaryFile(mode="r+") as f:
            rc.write(f, _format=_format)
            f.seek(0)
            result = loader(f)

        result["replicas"][0]["pfns"] = sorted( 
            result["replicas"][0]["pfns"], key=lambda pfn: pfn["site"]
        )

        assert result == expected

    def test_write_default(self):
        expected_file = Path("replicas.yml")
        ReplicaCatalog().write()

        try:
            expected_file.unlink()
        except FileNotFoundError:
            pytest.fail("could not find {}".format(expected_file))

    def test_replica_catalog_ordering_on_yml_write(self):
        ReplicaCatalog().write()

        EXPECTED_FILE = Path("replicas.yml")

        with EXPECTED_FILE.open() as f:
            # reading in as str so ordering of keys is not disrupted
            # when loaded into a dict
            result = f.read()

        EXPECTED_FILE.unlink()

        """
        Check that rc keys have been ordered as follows:
        - pegasus
        - replicas
        """
        p = re.compile(r"pegasus: '5.0'[\w\W]+replicas:[\w\W]+")
        assert p.match(result) is not None

