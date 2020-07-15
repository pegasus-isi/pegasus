"""
:mod:`transformation_catalog` exposes an API to serialize and deserialize Pegasus's transformation catalog file.

Basic Usage::

    >>> from Pegasus import transformation_catalog
    >>> transformation_catalog.loads("... ")
    ...

    >>> print(transformation_catalog.dumps( ... ))
    ' ... '

.. moduleauthor:: Ryan Tanaka <tanaka@isi.edu>
.. moduleauthor:: Rajiv Mayani <mayani@isi.edu>
"""

from collections import defaultdict
from io import StringIO
from typing import TextIO

from Pegasus import yaml
from Pegasus.api.errors import PegasusError
from Pegasus.api.site_catalog import OS, Arch
from Pegasus.api.transformation_catalog import (
    Container,
    Transformation,
    TransformationCatalog,
    TransformationSite,
)

__all__ = (
    "load",
    "loads",
    "dump",
    "dumps",
)


def _to_tc(d: dict) -> TransformationCatalog:
    """Convert dict to TransformationCatalog

    :param d: TransformationCatalog represented as a dict
    :type d: dict
    :raises PegasusError: encountered error parsing
    :return: a TransformationCatalog object based on d
    :rtype: TransformationCatalog
    """

    try:
        tc = TransformationCatalog()

        # add transformations
        for tr in d["transformations"]:
            tr_to_add = Transformation(
                tr["name"],
                tr.get("namespace"),
                tr.get("version"),
                checksum=tr.get("checksum"),
            )

            # add transformation sites
            for s in tr["sites"]:
                site_to_add = TransformationSite(
                    s["name"],
                    s["pfn"],
                    True if s["type"] == "stageable" else False,
                    bypass_staging=s.get("bypass"),
                    arch=getattr(Arch, s.get("arch").upper())
                    if s.get("arch")
                    else None,
                    os_type=getattr(OS, s.get("os.type").upper())
                    if s.get("os.type")
                    else None,
                    os_release=s.get("os.release"),
                    os_version=s.get("os.version"),
                    container=s.get("container"),
                )

                # add profiles
                if s.get("profiles"):
                    site_to_add.profiles = defaultdict(dict, s.get("profiles"))

                # add metadata
                if s.get("metadata"):
                    site_to_add.metadata = s.get("metadata")

                # add site to this tr
                tr_to_add.add_sites(site_to_add)

            # add requires
            if tr.get("requires"):
                tr_to_add.requires = set(tr.get("requires"))

            # add profiles
            if tr.get("profiles"):
                tr_to_add.profiles = defaultdict(dict, tr.get("profiles"))

            # add hooks
            if tr.get("hooks"):
                tr_to_add.hooks = defaultdict(list, tr.get("hooks"))

            # add metadata
            if tr.get("metadata"):
                tr_to_add.metadata = tr.get("metadata")

            # add tr to tc
            tc.add_transformations(tr_to_add)

        # add containers
        if "containers" in d:
            for cont in d["containers"]:
                cont_to_add = Container(
                    cont["name"],
                    getattr(Container, cont["type"].upper()),
                    cont["image"],
                    mounts=cont.get("mounts"),
                    image_site=cont.get("image.site"),
                    checksum=cont.get("checksum"),
                    bypass_staging=cont.get("bypass"),
                )

                # add profiles
                if cont.get("profiles"):
                    cont_to_add.profiles = defaultdict(dict, cont.get("profiles"))

                # add cont to tc
                tc.add_containers(cont_to_add)

        return tc

    except KeyError:
        raise PegasusError("error parsing {}".format(d))


def load(fp: TextIO, *args, **kwargs) -> TransformationCatalog:
    """
    Deserialize ``fp`` (a ``.read()``-supporting file-like object containing a TransformationCatalog document) to a :py:class:`~Pegasus.api.transformation_catalog.TransformationCatalog` object.

    :param fp: file like object to load from
    :type fp: TextIO
    :return: deserialized TransformationCatalog object
    :rtype: TransformationCatalog
    """
    return _to_tc(yaml.load(fp))


def loads(s: str, *args, **kwargs) -> TransformationCatalog:
    """
    Deserialize ``s`` (a ``str``, ``bytes`` or ``bytearray`` instance containing a TransformationCatalog document) to a :py:class:`~Pegasus.api.transformation_catalog.TransformationCatalog` object.

    :param s: string to load from
    :type s: str
    :return: deserialized TransformationCatalog object
    :rtype: TransformationCatalog
    """
    return _to_tc(yaml.load(s))


def dump(
    obj: TransformationCatalog, fp: TextIO, _format="yml", *args, **kwargs
) -> None:
    """
    Serialize ``obj`` as a :py:class:`~Pegasus.api.transformation_catalog.TransformationCatalog` formatted stream to ``fp`` (a ``.write()``-supporting file-like object).

    :param obj: TransformationCatalog to serialize
    :type obj: TransformationCatalog
    :param fp: file like object to serialize to
    :type fp: TextIO
    :param _format: format to write to if fp does not have an extension; can be one of ["yml" | "yaml" | "json"], defaults to "yml"
    :type _format: str
    :rtype: NoReturn
    """
    obj.write(fp, _format=_format)


def dumps(obj: TransformationCatalog, _format="yml", *args, **kwargs) -> str:
    """
    Serialize ``obj`` to a :py:class:`~Pegasus.api.transformation_catalog.TransformationCatalog` formatted ``str``.

    :param obj: TransformationCatalog to serialize
    :type obj: TransformationCatalog
    :param _format: format to write to if fp does not have an extension; can be one of ["yml" | "yaml" | "json"], defaults to "yml"
    :type _format: str
    :return: TransformationCatalog serialized as a string
    :rtype: str
    """
    with StringIO() as s:
        obj.write(s, _format=_format)
        s.seek(0)
        return s.read()
