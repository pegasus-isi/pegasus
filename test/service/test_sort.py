__author__ = "Rajiv Mayani"

import pytest

from Pegasus.service._sort import InvalidSortError, sort_parse


@pytest.mark.parametrize("e", ["a.a", "a1.a", "a_.a"])
def test_valid_prefix(e):
    r = sort_parse(e)
    assert ".".join(r[0][:-1]) == e


@pytest.mark.parametrize("e", [".", ".a", "1.a", "1a.a", "a-.a"])
def test_invalid_prefix(e):
    with pytest.raises(InvalidSortError):
        sort_parse(e)


@pytest.mark.parametrize("e", ["a", "a1", "a_", "a.a", "a.a1", "a.a_"])
def test_valid_identifier(e):
    r = sort_parse(e)
    assert ".".join(r[0][:-1]) == e


@pytest.mark.parametrize("e", ["a.", "a.0", "a.0a", "a.a-"])
def test_invalid_identifier(e):
    with pytest.raises(InvalidSortError):
        sort_parse(e)


@pytest.mark.parametrize("e", ["a, b, c"])
def test_tuple_input(e):
    r = sort_parse(e)
    assert isinstance(r, list)
    assert len(r) == 3
    assert ".".join(r[0][:-1]) == "a"
    assert ".".join(r[1][:-1]) == "b"
    assert ".".join(r[2][:-1]) == "c"


@pytest.mark.parametrize("e", ["c1, -c2, +c3"])
def test_unary_tuple_input(e):
    r = sort_parse(e)
    assert isinstance(r, list)
    assert len(r) == 3
    assert ".".join(r[0][:-1]) == "c1"
    assert r[0][-1] == "ASC"
    assert ".".join(r[1][:-1]) == "c2"
    assert r[1][-1] == "DESC"
    assert ".".join(r[2][:-1]) == "c3"
    assert r[2][-1] == "ASC"


@pytest.mark.parametrize("e", ["name"])
def test_name_input(e):
    r = sort_parse(e)
    assert isinstance(r, list)
    assert ".".join(r[0][:-1]) == e


@pytest.mark.parametrize("e", ["+name", "-name"])
def test_unary_name_input(e):
    r = sort_parse(e)
    assert isinstance(r, list)
    assert ".".join(r[0][:-1]) == "name"
