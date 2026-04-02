"""
:mod:`kickstart` exposes an API to serialize and deserialize Pegasus's kickstart file.

Basic Usage::

    >>> from Pegasus import kickstart
    >>> kickstart.loads("... ")
    ...

    >>> print(kickstart.dumps( ... ))
    ' ... '

.. moduleauthor:: Mats Rynge <rynge@isi.edu>
.. moduleauthor:: Rajiv Mayani <mayani@isi.edu>
"""

from typing import Dict, TextIO

__all__ = (
    "load",
    "loads",
    "dump",
    "dumps",
)


def load(fp: TextIO, *args, **kwargs) -> Dict:
    """
    Deserialize ``fp`` (a ``.read()``-supporting file-like object containing a Kickstart document) to a Python object.

    :param fp: File-like object to read from.
    :type fp: TextIO
    :return: Deserialized kickstart data.
    :rtype: Dict
    """


def loads(s: str, *args, **kwargs) -> Dict:
    """
    Deserialize ``s`` (a ``str``, ``bytes`` or ``bytearray`` instance containing a Kickstart document) to a Python object.

    :param s: String containing a Kickstart document.
    :type s: str
    :return: Deserialized kickstart data.
    :rtype: Dict
    """


def dump(obj: Dict, fp: TextIO, *args, **kwargs) -> None:
    """
    Serialize ``obj`` as a Kickstart formatted stream to ``fp`` (a ``.write()``-supporting file-like object).

    :param obj: Kickstart data to serialize.
    :type obj: Dict
    :param fp: File-like object to write to.
    :type fp: TextIO
    :rtype: None
    """


def dumps(obj: Dict, *args, **kwargs) -> str:
    """
    Serialize ``obj`` to a Kickstart formatted ``str``.

    :param obj: Kickstart data to serialize.
    :type obj: Dict
    :return: Kickstart document as a string.
    :rtype: str
    """
