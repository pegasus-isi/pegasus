import pytest
from enum import Enum

from Pegasus.api._utils import _get_class_enum_member_str
from Pegasus.api._utils import _get_enum_str


def test__get_class_enum_member_str():
    class _Constants(Enum):
        A = "a"
        B = "b"
        C = "c"

    class Grades:
        A = _Constants.A
        B = _Constants.B
        C = _Constants.C

        def __init__(self):
            pass

        def func(self):
            pass

    result = _get_class_enum_member_str(Grades, _Constants)
    expected = "Grades.<A | B | C>"

    assert result == expected


def test__get_enum_str():
    class Constants(Enum):
        A = "a"
        B = "b"

    result = _get_enum_str(Constants)
    expected = "Constants.<A | B>"

    assert result == expected


def test_invalid__get_enum_str():
    with pytest.raises(TypeError) as e:
        _get_enum_str(int)

    assert "invalid enum_cls: {_type}".format(_type=int) in str(e)

