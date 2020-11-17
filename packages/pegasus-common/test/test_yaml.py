from collections import OrderedDict
from pathlib import Path

import pytest

from Pegasus.yaml import dumps, loads


@pytest.mark.parametrize(
    "s, expected",
    [
        ("key: 1", 1),
        ("key: 2018-10-10", "2018-10-10"),
        ("key: yes", "yes"),
        ("key: true", True),
    ],
)
def test_loads(s, expected):
    """Test :meth:`Pegasus.yaml.loads`."""
    rv = loads(s)
    assert type(rv["key"]) == type(expected)
    assert rv["key"] == expected


@pytest.mark.parametrize(
    "obj, expected",
    [
        ({"key": 1}, "key: 1\n"),
        ({"key": "2018-10-10"}, "key: '2018-10-10'\n"),
        ({"key": "yes"}, "key: 'yes'\n"),
        ({"key": True}, "key: true\n"),
        ({"key": Path("./aaa")}, "key: aaa\n"),
        ({"key": Path("../aaa")}, "key: ../aaa\n"),
        (OrderedDict([(1, 1), (2, 2)]), "1: 1\n2: 2\n"),
        (OrderedDict([(1, OrderedDict([(2, 2)])), (3, 3)]), "1:\n  2: 2\n3: 3\n"),
    ],
)
def test_dumps(obj, expected):
    """Test :meth:`Pegasus.yaml.dumps`."""
    assert dumps(obj) == expected
