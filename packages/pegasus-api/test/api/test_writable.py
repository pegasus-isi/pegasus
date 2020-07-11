import getpass
import json
import os
from tempfile import TemporaryFile

import pytest
import yaml

from Pegasus.api.writable import Writable, _CustomEncoder, _filter_out_nones


@pytest.fixture(scope="function")
def writable_obj():
    def _writable_obj():
        class Item:
            def __init__(self, n):
                self.name = "item" + str(n)

            def __json__(self):
                return {"name": self.name}

        class Container(Writable):
            _DEFAULT_FILENAME = "container.yml"

            def __init__(self):
                self.name = "container⽷"
                self.items = [Item(i) for i in range(3)]

            def __json__(self):
                return {"name": self.name, "items": self.items}

        return Container()

    return _writable_obj()


@pytest.fixture(scope="function")
def expected():
    return {
        "x-pegasus": {
            "createdBy": getpass.getuser(),
            "createdOn": "now",
            "apiLang": "python",
        },
        "name": "container⽷",
        "items": [{"name": "item0"}, {"name": "item1"}, {"name": "item2"}],
    }


class Test_CustomEncoder:
    def test_json(self, writable_obj, expected):
        result = json.loads(json.dumps(writable_obj, cls=_CustomEncoder))

        # delete file info as it is only included when object is written
        # but not converted to json
        del expected["x-pegasus"]

        assert result == expected


class TestWritable:
    def test_write_using_defaults(self, writable_obj, expected, mocker):
        writable_obj.write()
        with open("container.yml") as f:
            result = yaml.safe_load(f)

            # TODO: check that keys in x-pegasus exist and then del x-pegasus
            # setting dates to be the same as it won't be safe to compare them
            # and this is simpler than trying to mocker.patch datetime
            expected["x-pegasus"]["createdOn"] = result["x-pegasus"]["createdOn"]
            assert result == expected

        os.remove("container.yml")

    @pytest.mark.parametrize(
        "file, _format, loader",
        [
            ("file.yml", "yml", yaml.safe_load),
            ("file.yaml", "yaml", yaml.safe_load),
            ("file", "yml", yaml.safe_load),
            ("file.123", "yml", yaml.safe_load),
            ("file.yml", "json", yaml.safe_load),
            ("file.json", "json", json.load),
            ("file", "json", json.load),
            ("file.123", "json", json.load),
        ],
    )
    def test_write_using_str_input(self, writable_obj, expected, file, _format, loader):
        writable_obj.write(file, _format=_format)
        with open(file) as f:
            result = loader(f)

            # setting dates to be the same as it won't be safe to compare them
            # and this is simpler than trying to mocker.patch datetime
            expected["x-pegasus"]["createdOn"] = result["x-pegasus"]["createdOn"]
            assert result == expected

        os.remove(file)

    @pytest.mark.parametrize(
        "file, _format, loader",
        [
            ("file.yml", "yml", yaml.safe_load),
            ("file.yaml", "yaml", yaml.safe_load),
            ("file", "yml", yaml.safe_load),
            ("file.123", "yml", yaml.safe_load),
            ("file.yml", "json", yaml.safe_load),
            ("file.json", "json", json.load),
            ("file", "json", json.load),
            ("file.123", "json", json.load),
        ],
    )
    def test_write_using_file_input(
        self, writable_obj, expected, file, _format, loader
    ):
        with open(file, "w+") as f:
            writable_obj.write(f, _format=_format)
            f.seek(0)
            result = loader(f)

            # setting dates to be the same as it won't be safe to compare them
            # and this is simpler than trying to mocker.patch datetime
            expected["x-pegasus"]["createdOn"] = result["x-pegasus"]["createdOn"]
            assert result == expected

        os.remove(file)

    def test_write_invalid_format(self, writable_obj):
        with pytest.raises(ValueError):
            writable_obj.write("abc", _format="123")

    def test_write_invalid_file_type(self, writable_obj):
        with pytest.raises(TypeError) as e:
            writable_obj.write(1)

        assert "1 must be of type str or file object" in str(e)

    @pytest.mark.parametrize(
        "file, _format, loader",
        [(TemporaryFile, "yml", yaml.safe_load), (TemporaryFile, "json", json.load)],
    )
    def test_write_stream_without_name(
        self, writable_obj, expected, file, _format, loader
    ):
        with file(mode="w+") as f:
            writable_obj.write(f, _format=_format)
            f.seek(0)
            result = loader(f)

            # setting dates to be the same as it won't be safe to compare them
            # and this is simpler than trying to mocker.patch datetime
            expected["x-pegasus"]["createdOn"] = result["x-pegasus"]["createdOn"]
            assert result == expected


def test_filter_out_nones():
    d = {"a": 1, "b": None}

    assert _filter_out_nones(d) == {"a": 1}


def test_filter_out_nones_invalid_type():
    with pytest.raises(TypeError) as e:
        _filter_out_nones(123)
