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

    [extended_summary]

    :param fp: [description]
    :type fp: TextIO
    :return: [description]
    :rtype: Dict
    """


def loads(s: str, *args, **kwargs) -> Dict:
    """
    Deserialize ``s`` (a ``str``, ``bytes`` or ``bytearray`` instance containing a Kickstart document) to a Python object.

    [extended_summary]

    :param s: [description]
    :type s: str
    :return: [description]
    :rtype: Dict
    """


def dump(obj: Dict, fp: TextIO, *args, **kwargs) -> None:
    """
    Serialize ``obj`` as a Kickstart formatted stream to ``fp`` (a ``.write()``-supporting file-like object).

    [extended_summary]

    :param obj: [description]
    :type obj: Dict
    :param fp: [description]
    :type fp: TextIO
    :return: [description]
    :rtype: NoReturn
    """


def dumps(obj: Dict, *args, **kwargs) -> str:
    """
    Serialize ``obj`` to a Kickstart formatted ``str``.

    [extended_summary]

    :param obj: [description]
    :type obj: Dict
    :return: [description]
    :rtype: str
    """
