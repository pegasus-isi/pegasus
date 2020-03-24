"""
:mod:`properties` exposes an API to serialize and deserialize Pegasus's properties file.

Basic Usage::

    >>> from Pegasus import properties
    >>> properties.loads("... ")
    ...

    >>> print(properties.dumps( ... ))
    ' ... '

.. moduleauthor:: Ryan Tanaka <tanaka@isi.edu>
.. moduleauthor:: Rajiv Mayani <mayani@isi.edu>
"""

import io
from configparser import DEFAULTSECT, ConfigParser
from typing import TextIO

from Pegasus.api.properties import Properties

__all__ = (
    "load",
    "loads",
    "dump",
    "dumps",
)


def load(fp: TextIO, *args, **kwargs) -> Properties:
    """
    Deserialize ``fp`` (a ``.read()``-supporting file-like object containing a Properties document) to a :py:class:`~Pegasus.api.properties.Properties` object.

    :param fp: file like object to load from
    :type fp: TextIO
    :return: deserialized Properties object
    :rtype: Properties
    """

    conf = ConfigParser()
    conf.read_string("[{}]\n".format(DEFAULTSECT) + fp.read())
    props = Properties()
    props._conf = conf

    return props


def loads(s: str, *args, **kwargs) -> Properties:
    """
    Deserialize ``s`` (a ``str``, ``bytes`` or ``bytearray`` instance containing a Properties document) to a :py:class:`~Pegasus.api.properties.Properties` object.

    :param s: string to load from
    :type s: str
    :return: deserialized Properties object
    :rtype: Properties
    """
    conf = ConfigParser()
    conf.read_string("[{}]\n".format(DEFAULTSECT) + s)
    props = Properties()
    props._conf = conf

    return props


def dump(obj: Properties, fp: TextIO, *args, **kwargs) -> None:
    """
    Serialize ``obj`` as a :py:class:`~Pegasus.api.properties.Properties` formatted stream to ``fp`` (a ``.write()``-supporting file-like object).

    :param obj: Properties to serialize
    :type obj: Properties
    :param fp: file like object to serialize to
    :type fp: TextIO
    :rtype: NoReturn
    """
    obj.write(fp)


def dumps(obj: Properties, *args, **kwargs) -> str:
    """
    Serialize ``obj`` to a :py:class:`~Pegasus.api.properties.Properties` formatted ``str``.

    :param obj: Properties to serialize
    :type obj: Properties
    :return: Properties serialized as a string
    :rtype: str
    """
    with io.StringIO() as s:
        obj.write(s)
        return s.getvalue()
