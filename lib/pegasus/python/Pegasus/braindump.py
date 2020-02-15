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

from pathlib import Path
from typing import Dict, TextIO

import attr

from Pegasus import yaml

__all__ = (
    "load",
    "loads",
    "dump",
    "dumps",
    "Braindump",
)


@attr.s(slots=True, kw_only=True)
class Braindump:
    """
    Data class representing Braindump file.

    .. todo::

        Use :mod:`dataclasses` instead of :mod:`attr`
    """

    user = attr.ib(type=str, default=None)  # type: str
    grid_dn = attr.ib(type=str, default=None)  # type: str
    submit_hostname = attr.ib(type=str, default=None)  # type: str
    root_wf_uuid = attr.ib(type=str, default=None)  # type: str
    wf_uuid = attr.ib(type=str, default=None)  # type: str
    dax = attr.ib(type=str, default=None)  # type: str
    dax_label = attr.ib(type=str, default=None)  # type: str
    dax_index = attr.ib(type=str, default=None)  # type: str
    dax_version = attr.ib(type=str, default=None)  # type: str
    pegasus_wf_name = attr.ib(type=str, default=None)  # type: str
    timestamp = attr.ib(type=str, default=None)  # type: str
    basedir = attr.ib(type=str, default=None)  # type: str
    submit_dir = attr.ib(type=str, default=None)  # type: str
    planner = attr.ib(type=str, default=None)  # type: str
    planner_version = attr.ib(type=str, default=None)  # type: str
    pegasus_build = attr.ib(type=str, default=None)  # type: str
    planner_arguments = attr.ib(type=str, default=None)  # type: str
    jsd = attr.ib(type=str, default=None)  # type: str
    rundir = attr.ib(type=str, default=None)  # type: str
    bindir = attr.ib(type=str, default=None)  # type: str
    vogroup = attr.ib(type=str, default=None)  # type: str
    uses_pmc = attr.ib(type=str, default=None)  # type: str
    properties = attr.ib(type=str, default=None)  # type: str
    condor_log = attr.ib(type=str, default=None)  # type: str
    dag = attr.ib(type=str, default=None)  # type: str
    type = attr.ib(type=str, default=None)  # type: str
    notify = attr.ib(type=str, default=None)  # type: str


def load(fp: TextIO, *args, **kwargs) -> Braindump:
    """
    Deserialize ``fp`` (a ``.read()``-supporting file-like object containing a Braindump document) to a Python object.

    [extended_summary]

    :param fp: [description]
    :type fp: TextIO
    :return: [description]
    :rtype: Dict
    """
    return loads(fp.read())


def loads(s: str, *args, **kwargs) -> Braindump:
    """
    Deserialize ``s`` (a ``str``, ``bytes`` or ``bytearray`` instance containing a Braindump document) to a Python object.

    [extended_summary]

    :param s: [description]
    :type s: str
    :return: [description]
    :rtype: Dict
    """
    _dict = yaml.load(s, *args, **kwargs)

    if not isinstance(_dict, dict):
        raise ValueError("Invalid braindump file.")

    return Braindump(**_dict)


def dump(obj: Dict, fp: TextIO, *args, **kwargs) -> None:
    """
    Serialize ``obj`` as a Braindump formatted stream to ``fp`` (a ``.write()``-supporting file-like object).

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
    Serialize ``obj`` to a Braindump formatted ``str``.

    [extended_summary]

    :param obj: [description]
    :type obj: Dict
    :return: [description]
    :rtype: str
    """
    return yaml.dumps(obj, *args, **kwargs)
