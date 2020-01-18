import json

import pytest
from jsonschema import validate

from Pegasus.dax4.site_catalog import OperationType
from Pegasus.dax4.site_catalog import FileServer
from Pegasus.dax4.site_catalog import DirectoryType
from Pegasus.dax4.site_catalog import Directory

from Pegasus.dax4.mixins import Namespace


class TestFileServer:
    def test_valid_file_server(self):
        FileServer("url", OperationType.PUT)

    def test_invlaid_file_server(self):
        with pytest.raises(ValueError):
            FileServer("url", "put")

    def test_tojson_with_profiles(self, convert_yaml_schemas_to_json, load_schema):
        result = (
            FileServer("url", OperationType.PUT)
            .add_profile(Namespace.ENV, "key", "value")
            .__json__()
        )

        expected = {
            "url": "url",
            "operation": "put",
            "profiles": {"env": {"key": "value"}},
        }

        file_server_schema = load_schema("sc-5.0.json")["$defs"]["fileServer"]
        validate(instance=result, schema=file_server_schema)

        assert result == expected


class TestDirectory:
    def test_valid_directory(self):
        Directory(DirectoryType.LOCAL_SCRATCH, "/path")

    def test_invalid_directory(self):
        with pytest.raises(ValueError):
            Directory("invalid type", "/path")

    def test_add_valid_file_server(self):
        d = Directory(DirectoryType.LOCAL_SCRATCH, "/path")
        d.add_file_server(FileServer("url", OperationType.PUT))

    def test_add_invalid_file_server(self):
        d = Directory(DirectoryType.LOCAL_SCRATCH, "/path")
        with pytest.raises(ValueError):
            d.add_file_server(123)

    def test_chaining(self):
        a = Directory(DirectoryType.LOCAL_SCRATCH, "/path")
        b = a.add_file_server(FileServer("url", OperationType.PUT)).add_file_server(
            FileServer("url", OperationType.GET)
        )

        assert id(a) == id(b)

    def test_tojson(self):
        result = (
            Directory(DirectoryType.LOCAL_SCRATCH, "/path")
            .add_file_server(FileServer("url", OperationType.PUT))
            .__json__()
        )

        expected = {
            "type": "localScratch",
            "path": "/path",
            "fileServers": [FileServer("url", OperationType.PUT).__json__()],
        }

        assert result == expected

