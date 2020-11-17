import inspect
from enum import Enum
from functools import wraps


def _get_class_enum_member_str(_cls, _type):
    """Internal function for documentation purposes. Given the following classes:

    .. code-block:: python

        class _GridType(Enum):
            GT5 = "gt5"
            PBS = "pbs"

        class Grid:
            GT5 = _GridType.GT5
            PBS = _GridType.PBS
            num = 1

            def __init__(self):
                pass

            def __json__(self):
                pass

    Calling _get_class_enum_member_str(Grid, _GridType) would return the string:
    "Grid.<GT5 | PBS>"


    :param _cls: the target class
    :type _cls: type
    :param _type: class variable types to extract
    :type _type: type
    :return: a str formatted as _cls.__name__ + ".<member1 | member 2 .. | member n>"
    :rtype: str
    """
    enums = []

    for name, member in _cls.__dict__.items():
        if not inspect.isfunction(member):
            if isinstance(member, _type):
                enums.append(name)

    enums.sort()
    return _cls.__name__ + ".<{members}>".format(members=" | ".join(enums))


def _get_enum_str(enum_cls):
    """Internal function for documentation purposes. Given the following enum class:

    .. code-block:: python

        class Scheduler(Enum):
            PBS = "pbs"
            LSF = "lsf"

    Calling _get_enum_str(Scheduler) would return the string:
    "Scheduler.<PBS | LSF>"

    :param enum_cls: the target class that inherits from Enum
    :type enum_cls: type
    :return: a str formatted as enum_cls.__name__ + ".<member 1 | member 2 | .. | member n>"
    :rtype: str
    :raises TypeError:
    """

    if not issubclass(enum_cls, Enum):
        raise TypeError(
            "invalid enum_cls: {cls}; enum_cls must be a subclass of Enum".format(
                cls=enum_cls
            )
        )

    return enum_cls.__name__ + ".<{members}>".format(
        members=" | ".join(sorted(enum_cls._member_names_))
    )


def _chained(f):
    """Method decorator to allow chaining. Methods decorated by this should
    not return anything. If they do an exception will be thrown."""

    @wraps(f)
    def wrapper(self, *args, **kwargs):
        assert f(self, *args, **kwargs) == None
        return self

    return wrapper
