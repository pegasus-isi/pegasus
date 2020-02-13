import pytest

from Pegasus.yaml import dumps, loads


@pytest.mark.parametrize(
    "s, expected", [("key: 1", 1), ("key: 2018-10-10", "2018-10-10")]
)
def test_loads(s, expected):
    """Test :meth:`Pegasus.yaml.loads`."""
    rv = loads(s)
    assert type(rv["key"]) == type(expected)
    assert rv["key"] == expected


@pytest.mark.parametrize(
    "obj, expected",
    [({"key": 1}, "key: 1\n"), ({"key": "2018-10-10"}, "key: '2018-10-10'\n")],
)
def test_dumps(obj, expected):
    """Test :meth:`Pegasus.yaml.dumps`."""
    assert dumps(obj) == expected
