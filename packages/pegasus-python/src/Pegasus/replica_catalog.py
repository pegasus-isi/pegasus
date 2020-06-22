"""
:mod:`replica_catalog` exposes an API to serialize and deserialize Pegasus's replica catalog file.

Basic Usage::

    >>> from Pegasus import replica_catalog
    >>> replica_catalog.loads("... ")
    ...

    >>> print(replica_catalog.dumps( ... ))
    ' ... '

.. moduleauthor:: Ryan Tanaka <tanaka@isi.edu>
.. moduleauthor:: Rajiv Mayani <mayani@isi.edu>
"""

from io import StringIO
from typing import TextIO

from Pegasus import yaml
from Pegasus.api.errors import PegasusError
from Pegasus.api.replica_catalog import _PFN, ReplicaCatalog

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
            lfn = r["lfn"]
            pfns = {_PFN(i["site"], i["pfn"]) for i in r["pfns"]}

            checksum = r.get("checksum") if r.get("checksum") else {}
            metadata = r.get("metadata") if r.get("metadata") else {}

            regex = r.get("regex")
            for pfn in pfns:
                if regex:
                    rc.add_regex_replica(pfn.site, lfn, pfn.pfn, metadata=metadata)
                else:
                    rc.add_replica(
                        pfn.site, lfn, pfn.pfn, metadata=metadata, checksum=checksum
                    )

    except KeyError:
        raise PegasusError("error parsing {}".format(d))

    return rc


def load(fp: TextIO, *args, **kwargs) -> ReplicaCatalog:
    """
    Deserialize ``fp`` (a ``.read()``-supporting file-like object containing a ReplicaCatalog document) to a :py:class:`~Pegasus.api.replica_catalog.ReplicaCatalog` object.

    :param fp: file like object to load from
    :type fp: TextIO
    :return: deserialized ReplicaCatalog object
    :rtype: ReplicaCatalog
    """
    return _to_rc(yaml.load(fp))


def loads(s: str, *args, **kwargs) -> ReplicaCatalog:
    """
    Deserialize ``s`` (a ``str``, ``bytes`` or ``bytearray`` instance containing a ReplicaCatalog document) to a :py:class:`~Pegasus.api.replica_catalog.ReplicaCatalog` object.

    :param s: string to load from
    :type s: str
    :return: deserialized ReplicaCatalog object
    :rtype: ReplicaCatalog
    """
    return _to_rc(yaml.load(s))


def dump(obj: ReplicaCatalog, fp: TextIO, _format="yml", *args, **kwargs) -> None:
    """
    Serialize ``obj`` as a :py:class:`~Pegasus.api.replica_catalog.ReplicaCatalog` formatted stream to ``fp`` (a ``.write()``-supporting file-like object).

    :param obj: ReplicaCatalog to serialize
    :type obj: ReplicaCatalog
    :param fp: file like object to serialize to
    :type fp: TextIO
    :param _format: format to write to if fp does not have an extension; can be one of ["yml" | "yaml" | "json"], defaults to "yml"
    :type _format: str
    :rtype: NoReturn
    """
    obj.write(fp, _format=_format)


def dumps(obj: ReplicaCatalog, _format="yml", *args, **kwargs) -> str:
    """
    Serialize ``obj`` to a :py:class:`~Pegasus.api.replica_catalog.ReplicaCatalog` formatted ``str``.

    :param obj: ReplicaCatalog to serialize
    :type obj: ReplicaCatalog
    :param _format: format to write to if fp does not have an extension; can be one of ["yml" | "yaml" | "json"], defaults to "yml"
    :type _format: str
    :return: ReplicaCatalog serialized as a string
    :rtype: str
    """
    with StringIO() as s:
        obj.write(s, _format=_format)
        s.seek(0)
        return s.read()
