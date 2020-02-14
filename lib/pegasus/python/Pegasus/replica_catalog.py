# -*- coding: utf-8 -*-

import json

from Pegasus import yaml
from Pegasus.api.writable import _CustomEncoder
from Pegasus.api.replica_catalog import ReplicaCatalog
from Pegasus.api.errors import PegasusError

"""
:mod:`workflow` exposes an API to serialize and deserialize Pegasus's workflow file.

Basic Usage::

    >>> from Pegasus import Workflow
    >>> Workflow.loads("... ")
    ...

    >>> print(Workflow.dumps( ... ))
    ' ... '

.. moduleauthor:: Ryan Tanaka <tanaka@isi.edu>
.. moduleauthor:: Rajiv Mayani <mayani@isi.edu>
"""

from typing import Dict, TextIO

__all__ = (
    "load",
    "loads",
    "dump",
    "dumps",
)

def _to_rc(d: dict) -> ReplicaCatalog:
    """Convert dict to ReplicaCatalog
    
    :param d: ReplicaCatalog represented as a dict
    :type d: dict
    :raises PegasusError: encountered error parsing 
    :return: a ReplicaCatalog object based on d
    :rtype: ReplicaCatalog
    """
    rc = ReplicaCatalog()

    try:
        for r in d["replicas"]:
            rc.add_replica(r["lfn"], r["pfn"], r["site"], r["regex"]) 
    except KeyError:
        raise PegasusError("error parsing {}".format(rc))

    return rc

def load(fp: TextIO, *args, **kwargs) -> ReplicaCatalog:
    """
    Deserialize ``fp`` (a ``.read()``-supporting file-like object containing a Workflow document) to a :py:class:`~Pegasus.api.replica_catalog.ReplicaCatalog` object.

    :param fp: file like object to load from 
    :type fp: TextIO
    :return: deserialized ReplicaCatalog object
    :rtype: ReplicaCatalog
    """
    return _to_rc(yaml.load(fp))


def loads(s: str, *args, **kwargs) -> ReplicaCatalog:
    """
    Deserialize ``s`` (a ``str``, ``bytes`` or ``bytearray`` instance containing a Workflow document) to a :py:class:`~Pegasus.api.replica_catalog.ReplicaCatalog` object.

    :param s: string to load from 
    :type s: str
    :return: deserialized ReplicaCatalog object
    :rtype: ReplicaCatalog
    """
    return _to_rc(yaml.load(s))



def dump(obj: ReplicaCatalog, fp: TextIO, *args, **kwargs) -> None:
    """
    Serialize ``obj`` as a :py:class:`~Pegasus.api.replica_catalog.ReplicaCatalog` formatted stream to ``fp`` (a ``.write()``-supporting file-like object).

    :param obj: ReplicaCatalog to serialize
    :type obj: ReplicaCatalog
    :param fp: file like object to serialize to
    :type fp: TextIO
    :rtype: NoReturn
    """
    obj.write(fp)


def dumps(obj: ReplicaCatalog, *args, **kwargs) -> str:
    """
    Serialize ``obj`` to a :py:class:`~Pegasus.api.replica_catalog.ReplicaCatalog` formatted ``str``.

    :param obj: ReplicaCatalog to serialize
    :type obj: ReplicaCatalog
    :return: ReplicaCatalog serialized as a string
    :rtype: str
    """
    return json.dumps(obj, cls=_CustomEncoder)
