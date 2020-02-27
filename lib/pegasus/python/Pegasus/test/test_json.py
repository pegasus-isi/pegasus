# -*- coding: utf-8 -*-

from pathlib import Path

import pytest

from Pegasus.json import dump_all, dumps, loads


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
    "obj, expected",
    [
        ({"key": 1}, '{"key": 1}\n'),
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
