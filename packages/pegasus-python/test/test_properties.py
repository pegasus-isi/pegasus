from configparser import DEFAULTSECT
from tempfile import TemporaryFile

from Pegasus.api.properties import Properties
from Pegasus.properties import *


def test_load():
    with TemporaryFile(mode="w+") as f:
        f.write("a = b\nc = d\n\n")
        f.seek(0)
        props = load(f)

    assert props._conf[DEFAULTSECT] == {"a": "b", "c": "d"}


def test_loads():
    s = "a = b\nc = d\n\n"
    props = loads(s)

    assert props._conf[DEFAULTSECT] == {"a": "b", "c": "d"}


def test_dump():
    props = Properties()
    props["a"] = "b"
    props["c"] = "d"

    with TemporaryFile(mode="w+") as f:
        dump(props, f)
        f.seek(0)
        assert f.read() == "a = b\nc = d\n\n"


def test_dumps():
    props = Properties()
    props["a"] = "b"
    props["c"] = "d"

    assert dumps(props) == "a = b\nc = d\n\n"
