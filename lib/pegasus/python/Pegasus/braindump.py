# -*- coding: utf-8 -*-
"""
:mod:`braindump` exposes an API to serialize and deserialize Pegasus's braindump file.

Basic Usage::

    >>> from Pegasus import braindump
    >>> braindump.loads("user: mayani\ngrid_dn: null\n")
    {'grid_dn': None, 'user': 'mayani'}

    >>> print(braindump.dumps({"grid_dn": None, "user": "mayani"}))
    '{"grid_dn": null, "user": "mayani"}'

.. moduleauthor:: Rajiv Mayani <mayani@isi.edu>
"""

from typing import Dict, TextIO

from Pegasus import yaml

__all__ = (
    "load",
    "loads",
    "dump",
    "dumps",
)


def load(fp: TextIO, *args, **kwargs) -> Dict:
    """
    Deserialize ``fp`` (a ``.read()``-supporting file-like object containing a JSON document) to a Python object.

    [extended_summary]

    :param fp: [description]
    :type fp: TextIO
    :return: [description]
    :rtype: Dict
    """
    return yaml.load(fp, *args, **kwargs)


def loads(s: str, *args, **kwargs) -> Dict:
    """
    Deserialize ``s`` (a ``str``, ``bytes`` or ``bytearray`` instance containing a JSON document) to a Python object.

    [extended_summary]

    :param s: [description]
    :type s: str
    :return: [description]
    :rtype: Dict
    """
    return yaml.loads(s, *args, **kwargs)


def dump(obj: Dict, fp: TextIO, *args, **kwargs) -> None:
    """
    Serialize ``obj`` as a JSON formatted stream to ``fp`` (a ``.write()``-supporting file-like object).

    [extended_summary]

    :param obj: [description]
    :type obj: Dict
    :param fp: [description]
    :type fp: TextIO
    :return: [description]
    :rtype: NoReturn
    """
    yaml.dump(obj, fp, *args, **kwargs)


def dumps(obj: Dict, *args, **kwargs) -> str:
    """
    Serialize ``obj`` to a JSON formatted ``str``.

    [extended_summary]

    :param obj: [description]
    :type obj: Dict
    :return: [description]
    :rtype: str
    """
    return yaml.dumps(obj, *args, **kwargs)
