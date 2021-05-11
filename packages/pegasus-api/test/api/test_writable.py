import getpass
import json
import os
from io import StringIO
from pathlib import Path
from tempfile import NamedTemporaryFile, TemporaryFile

import pytest
import yaml

from Pegasus.api.errors import PegasusError
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
                Writable.__init__(self)

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

    def test_json_attribute_not_callable(self, writable_obj):
        writable_obj.__json__ = "not a callable"
        with pytest.raises(TypeError) as e:
            json.loads(json.dumps(writable_obj, cls=_CustomEncoder))

        assert "__json__ is not callable" in str(e)

    def test_json_default_encoder(self, mocker):
        class NotJSONSerializableObj:
            pass

        # default implementation json.JSONEncoder.default should raise TypeError
        with pytest.raises(TypeError) as e:
            json.loads(json.dumps(NotJSONSerializableObj(), cls=_CustomEncoder))

        # ensure we are not getting the TypeError raised by _CustomEncoder.default
        assert "__json__ is not callable" not in str(e)


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
        [
            (TemporaryFile, "yml", yaml.safe_load),
            (TemporaryFile, "json", json.load),
            (StringIO, "yaml", yaml.safe_load),
        ],
    )
    def test_write_stream_without_name(
        self, writable_obj, expected, file, _format, loader
    ):

        kwargs = {"mode": "w+"}
        if type(file) == type(StringIO):
            kwargs = {}

        with file(**kwargs) as f:
            writable_obj.write(f, _format=_format)
            f.seek(0)
            result = loader(f)

            # setting dates to be the same as it won't be safe to compare them
            # and this is simpler than trying to mocker.patch datetime
            expected["x-pegasus"]["createdOn"] = result["x-pegasus"]["createdOn"]
            assert result == expected

    def test_internal_write_invalid_format(self, writable_obj):
        """Testing Writable._write"""
        with pytest.raises(ValueError) as e:
            writable_obj._write("file", "bad_format")

        assert "invalid _ext: bad_format" in str(e)

    def test_set_path_given_default_filename(self, writable_obj):
        writable_obj.write()
        assert writable_obj._path == str(Path(writable_obj._DEFAULT_FILENAME).resolve())
        assert writable_obj.path == Path(writable_obj._DEFAULT_FILENAME).resolve()
        writable_obj._path = None
        os.remove(str(Path(writable_obj._DEFAULT_FILENAME).resolve()))

    def test_set_path_given_str_filename(self, writable_obj):
        writable_obj.write("filename")
        assert writable_obj._path == str(Path("filename").resolve())
        assert writable_obj.path == Path("filename").resolve()
        writable_obj._path = None
        os.remove("filename")

    def test_set_path_given_NamedTemporaryFile(self, writable_obj):
        f = NamedTemporaryFile(mode="w")
        writable_obj.write(f)
        assert writable_obj._path == str(Path(f.name).resolve())
        assert writable_obj.path == Path(f.name).resolve()
        writable_obj._path = None
        f.close()

    def test_set_path_given_TemporaryFile(self, writable_obj):
        f = TemporaryFile(mode="w")
        writable_obj.write(f)
        assert writable_obj._path == None
        with pytest.raises(PegasusError) as e:
            writable_obj.path

        assert "Container.write(filename)" in str(e)
        writable_obj._path = None
        f.close()

    def test_set_path_given_StringIO(self, writable_obj):
        f = StringIO()
        writable_obj.write(f)
        assert writable_obj._path == None
        with pytest.raises(PegasusError) as e:
            writable_obj.path

        assert "Container.write(filename)" in str(e)
        f.close()

    def test_get_path_exception_message(self, writable_obj):
        writable_obj.write(StringIO())

        with pytest.raises(PegasusError) as e:
            writable_obj.path

        assert "Container.write(filename)" in str(e)


def test_filter_out_nones():
    d = {"a": 1, "b": None}

    assert _filter_out_nones(d) == {"a": 1}


def test_filter_out_nones_invalid_type():
    with pytest.raises(TypeError) as e:
        _filter_out_nones(123)
