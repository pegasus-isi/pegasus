import getpass
import json
import re
from pathlib import Path
from tempfile import NamedTemporaryFile

import pytest
import yaml
from conftest import _tojson
from jsonschema import validate

from Pegasus.api.errors import DuplicateError
from Pegasus.api.replica_catalog import _PFN, File, ReplicaCatalog, _ReplicaCatalogEntry


class Test_PFN:
    def test_valid_pfn(self):
        assert _PFN(site="local", pfn="/file.txt")

    @pytest.mark.parametrize(
        "pfn,other,result",
        [
            (
                _PFN(site="local", pfn="/file.txt"),
                _PFN(site="local", pfn="/file.txt"),
                True,
            ),
            (
                _PFN(site="condorpool", pfn="/file.txt"),
                _PFN(site="local", pfn="/file.txt"),
                False,
            ),
            (
                _PFN(site="local", pfn="/file1.txt"),
                _PFN(site="local", pfn="/file.txt"),
                False,
            ),
            (_PFN(site="local", pfn="/file.txt"), "pfn", False),
        ],
    )
    def test_eq(self, pfn, other, result):
        assert (pfn == other) == result

    def test_hash(self):
        pfn = _PFN(site="local", pfn="/file.txt")
        assert hash(pfn) == hash(("local", "/file.txt"))

    def test_repr(self):
        pfn = _PFN(site="local", pfn="/file.txt")
        assert repr(pfn) == "<_PFN site: local, pfn: /file.txt>"

    def test_tojson(self):
        pfn = _PFN(site="local", pfn="/file.txt")
        assert pfn.__json__() == {"site": "local", "pfn": "/file.txt"}


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
        "lfn,size,for_planning,expected",
        [
            ("f1", None, True, {"lfn": "f1", "forPlanning": True}),
            ("f2", 2048, False, {"lfn": "f2", "size": 2048, "metadata": {"size": 2048}}),
            ("f3", 1024, True, {"lfn": "f3", "size": 1024, "forPlanning": True, "metadata": {"size": 1024}})
        ],
    )
    def test_tojson_no_metadata(self, lfn, size, for_planning, expected):
        assert File(lfn, size, for_planning).__json__() == expected

    def test_eq(self):
        assert File("a") == File("a")
        assert File("a") != File("b")
        assert File("a") != 1

    def test_repr(self):
        assert repr(File("a")) == "<File a>"

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
        
    def test_tojson_forplanning_with_metdata(self):
        result = File("subwf_tc.yml", size=1024, for_planning=True).add_metadata(creator="zaiyan").__json__()
        expected = {
            "lfn": "subwf_tc.yml",
            "metadata": {"creator": "zaiyan", "size": 1024},
            "size": 1024,
            "forPlanning": True
        }
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
        rc.add_replica("local", "test_replica_catalog", Path("/file"))

        assert rc.entries[("test_replica_catalog", False)].pfns.pop().pfn == "/file"

    def test_add_replica_pfn_with_invalid_path_object(self):
        rc = ReplicaCatalog()
        with pytest.raises(ValueError) as e:
            rc.add_replica("local", "f.a", Path("file"))

        assert "Invalid pfn: file, the given path must be an absolute path" in str(e)

    def test_add_replica_pfn_as_file_object(self):
        rc = ReplicaCatalog()
        with pytest.raises(TypeError) as e:
            rc.add_replica(site="local", lfn="test_lfn", pfn=File("badfile"))

        assert (
            "Invalid pfn: badfile, the given pfn must be a str or pathlib.Path"
            in str(e)
        )

    def test_add_replica_file_as_lfn(self):
        rc = ReplicaCatalog()
        f = File("f.a", size=1024).add_metadata(creator="ryan")
        rc.add_replica("local", f, "/f.a")

        assert _tojson(rc.entries[("f.a", False)]) == {
            "lfn": "f.a",
            "pfns": [{"site": "local", "pfn": "/f.a"}],
            "metadata": {"size": 1024, "creator": "ryan"},
        }

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

    @pytest.mark.parametrize(
        "given_pfn, expected_pfn",
        [("/path", "/path"), (Path("/path/[0]"), "/path/[0]")],
    )
    def test_add_regex_replica(self, given_pfn, expected_pfn):
        rc = ReplicaCatalog()
        rc.add_regex_replica("local", "*.txt", given_pfn)

        assert _tojson(rc.entries[("*.txt", True)]) == {
            "lfn": "*.txt",
            "pfns": [{"site": "local", "pfn": expected_pfn}],
            "regex": True,
        }

    def test_add_duplicate_regex_replica(self):
        rc = ReplicaCatalog()
        rc.add_regex_replica("local", "*.txt", "/path")

        with pytest.raises(DuplicateError) as e:
            rc.add_regex_replica("local", "*.txt", "/path")

        assert "Pattern: *.txt already exists" in str(e)

    def test_add_invalid_pfn_regex_replica(self):
        rc = ReplicaCatalog()
        with pytest.raises(ValueError) as e:
            rc.add_regex_replica("local", "*.txt", Path("not_an_absolute_path"))

        assert "Invalid pfn: not_an_absolute_path" in str(e)

    def test_add_regex_replica_with_metadata(self):
        rc = ReplicaCatalog()
        rc.add_regex_replica("local", "*.txt", "/path", metadata={"creator": "ryan"})

        assert _tojson(rc.entries[("*.txt", True)]) == {
            "lfn": "*.txt",
            "pfns": [{"site": "local", "pfn": "/path"}],
            "metadata": {"creator": "ryan"},
            "regex": True,
        }

    def test_tojson(self, convert_yaml_schemas_to_json, load_schema):
        rc = ReplicaCatalog()
        rc.add_replica(
            "local",
            "f.a",
            "/f.a",
            checksum={"sha256": "123"},
            metadata={"size": 1024, "㐦": "㐦"},
        )
        rc.add_regex_replica("local", "*.txt", "/path", metadata={"creator": "ryan"})
        result = _tojson(rc)
        expected = {
            "pegasus": "5.0.4",
            "replicas": [
                {
                    "lfn": "f.a",
                    "pfns": [{"site": "local", "pfn": "/f.a"}],
                    "checksum": {"sha256": "123"},
                    "metadata": {"size": 1024, "㐦": "㐦"},
                },
                {
                    "lfn": "*.txt",
                    "pfns": [{"site": "local", "pfn": "/path"}],
                    "metadata": {"creator": "ryan"},
                    "regex": True,
                },
            ],
        }

        assert result == expected

        rc_schema = load_schema("rc-5.0.json")
        validate(instance=result, schema=rc_schema)

    @pytest.mark.parametrize(
        "_format, loader", [("json", json.load), ("yml", yaml.safe_load)]
    )
    def test_write(self, _format, loader):
        rc = ReplicaCatalog()
        f_a = File("f.a", size=1024).add_metadata(creator="ryan")
        rc.add_replica(
            "local",
            f_a,
            "/f.a",
            checksum={"sha256": "123"},
            metadata={"extra": "metadata"},
        )
        rc.add_replica("condorpool", f_a, "/f.a")
        rc.add_replica("local", "f.b", "/f.b")
        rc.add_regex_replica("local", "*.txt", "/path", metadata={"creator": "ryan"})

        expected = {
            "pegasus": "5.0.4",
            "replicas": [
                {
                    "lfn": "f.a",
                    "pfns": [
                        {"site": "local", "pfn": "/f.a"},
                        {"site": "condorpool", "pfn": "/f.a"},
                    ],
                    "checksum": {"sha256": "123"},
                    "metadata": {"extra": "metadata", "size": 1024, "creator": "ryan"},
                },
                {"lfn": "f.b", "pfns": [{"site": "local", "pfn": "/f.b"}],},
                {
                    "lfn": "*.txt",
                    "pfns": [{"site": "local", "pfn": "/path"}],
                    "metadata": {"creator": "ryan"},
                    "regex": True,
                },
            ],
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

        assert "createdOn" in result["x-pegasus"]
        assert result["x-pegasus"]["createdBy"] == getpass.getuser()
        assert result["x-pegasus"]["apiLang"] == "python"
        del result["x-pegasus"]
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
        p = re.compile(r"x-pegasus:[\w\W]+pegasus: 5.0.4[\w\W]+replicas:[\w\W]+")
        assert p.match(result) is not None
