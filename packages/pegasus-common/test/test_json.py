import io
from enum import Enum
from pathlib import Path
from uuid import UUID

import pytest

from Pegasus.json import dump_all, dumps, load_all, loads


class _Color(Enum):
    RED = 1


class _Html:
    def __html__(self):
        return "html"


class _Json:
    def __json__(self):
        return "json"


@pytest.mark.parametrize(
    "s, expected",
    [
        ('{"key": 1}', 1),
        ('{"key": "2018-10-10"}', "2018-10-10"),
        ('{"key": "yes"}', "yes"),
        ('{"key": true}', True),
    ],
)
def test_loads(s, expected):
    """Test :meth:`Pegasus.json.loads`."""
    rv = loads(s)
    assert type(rv["key"]) == type(expected)
    assert rv["key"] == expected


@pytest.mark.parametrize(
    "obj, expected",
    [
        ({"key": 1}, '{"key": 1}'),
        ({"key": "2018-10-10"}, '{"key": "2018-10-10"}'),
        ({"key": "yes"}, '{"key": "yes"}'),
        ({"key": True}, '{"key": true}'),
        ({"key": Path("./aaa")}, '{"key": "aaa"}'),
        ({"key": Path("../aaa")}, '{"key": "../aaa"}'),
    ],
)
def test_dumps(obj, expected):
    """Test :meth:`Pegasus.json.dumps`."""
    assert dumps(obj) == expected


@pytest.mark.parametrize(
    "obj, expected", [('{"key": 1}\n{"key": 2}', [{"key": 1}, {"key": 2}])],
)
def test_load_all(obj, expected):
    """Test :meth:`Pegasus.json.load_all`."""
    assert list(load_all(obj)) == expected
    assert list(load_all(io.StringIO(obj))) == expected


@pytest.mark.parametrize(
    "obj, expected",
    [
        ({"key": 1}, '{"key": 1}\n'),
        ({"key": _Color.RED}, '{"key": "RED"}\n'),
        (
            {"key": UUID("{12345678-1234-5678-1234-567812345678}")},
            '{"key": "12345678-1234-5678-1234-567812345678"}\n',
        ),
        ({"key": _Html()}, '{"key": "html"}\n'),
        ({"key": _Json()}, '{"key": "json"}\n'),
        ({"key": "2018-10-10"}, '{"key": "2018-10-10"}\n'),
        ({"key": "yes"}, '{"key": "yes"}\n'),
        ({"key": True}, '{"key": true}\n'),
        ({"key": Path("./aaa")}, '{"key": "aaa"}\n'),
        ({"key": Path("../aaa")}, '{"key": "../aaa"}\n'),
    ],
)
def test_dump_all(obj, expected):
    """Test :meth:`Pegasus.json.dumps`."""
    assert dump_all([obj]) == expected

    out = io.StringIO()
    dump_all([obj], out)
    assert out.getvalue() == expected

    with pytest.raises(TypeError) as e:
        dump_all([obj], 1)
    assert "s must either be None or an open text file" in str(e.value)
