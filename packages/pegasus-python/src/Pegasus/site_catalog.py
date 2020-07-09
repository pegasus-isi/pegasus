from collections import defaultdict
from io import StringIO
from typing import TextIO

from Pegasus import yaml
from Pegasus.api.errors import PegasusError
from Pegasus.api.site_catalog import (
    OS,
    Arch,
    Directory,
    FileServer,
    Grid,
    Operation,
    Scheduler,
    Site,
    SiteCatalog,
    SupportedJobs,
    _DirectoryType,
)

"""
:mod:`site_catalog` exposes an API to serialize and deserialize Pegasus's site catalog file.

Basic Usage::

    >>> from Pegasus import site_catalog
    >>> site_catalog.loads("... ")
    ...

    >>> print(site_catalog.dumps( ... ))
    ' ... '

.. moduleauthor:: Ryan Tanaka <tanaka@isi.edu>
.. moduleauthor:: Rajiv Mayani <mayani@isi.edu>
"""


__all__ = (
    "load",
    "loads",
    "dump",
    "dumps",
)


def _to_sc(d: dict) -> SiteCatalog:
    """Convert dict to SiteCatalog

    :param d: SiteCatalog represented as a dict
    :type d: dict
    :raises PegasusError: encountered error parsing
    :return: a SiteCatalog object based on d
    :rtype: SiteCatalog
    """

    try:
        sc = SiteCatalog()

        for s in d["sites"]:
            site = Site(
                s["name"],
                arch=getattr(Arch, s.get("arch").upper()) if s.get("arch") else None,
                os_type=getattr(OS, s.get("os.type").upper())
                if s.get("os.type")
                else None,
                os_release=s.get("os.release"),
                os_version=s.get("os.version"),
            )

            # add directories
            for _dir in s["directories"]:

                dir_type = None
                for enum_name, enum in _DirectoryType.__members__.items():
                    if _dir["type"] == enum.value:
                        dir_type = enum_name
                        break

                directory = Directory(getattr(Directory, dir_type), _dir["path"])

                # add file servers
                for fs in _dir["fileServers"]:
                    file_server = FileServer(
                        fs["url"], getattr(Operation, fs["operation"].upper())
                    )

                    # add profiles
                    if fs.get("profiles"):
                        file_server.profiles = defaultdict(dict, fs.get("profiles"))

                    # add file server to this directory
                    directory.add_file_servers(file_server)

                # add directory to this site
                site.add_directories(directory)

            # add grids
            if s.get("grids"):
                for gr in s.get("grids"):
                    grid = Grid(
                        getattr(Grid, gr["type"].upper()),
                        gr["contact"],
                        getattr(Scheduler, gr["scheduler"].upper()),
                        job_type=getattr(SupportedJobs, gr.get("jobtype").upper())
                        if gr.get("jobtype")
                        else None,
                    )

                    # add grid to this site
                    site.add_grids(grid)

            # add profiles
            if s.get("profiles"):
                site.profiles = defaultdict(dict, s.get("profiles"))

            # add site to sc
            sc.add_sites(site)

        return sc

    except KeyError:
        raise PegasusError("error parsing {}".format(d))


def load(fp: TextIO, *args, **kwargs) -> SiteCatalog:
    """
    Deserialize ``fp`` (a ``.read()``-supporting file-like object containing a SiteCatalog document) to a :py:class:`~Pegasus.api.site_catalog.SiteCatalog` object.

    :param fp: file like object to load from
    :type fp: TextIO
    :return: deserialized SiteCatalog object
    :rtype: SiteCatalog
    """
    return _to_sc(yaml.load(fp))


def loads(s: str, *args, **kwargs) -> SiteCatalog:
    """
    Deserialize ``s`` (a ``str``, ``bytes`` or ``bytearray`` instance containing a SiteCatalog document) to a :py:class:`~Pegasus.api.site_catalog.SiteCatalog` object.

    :param s: string to load from
    :type s: str
    :return: deserialized SiteCatalog object
    :rtype: SiteCatalog
    """
    return _to_sc(yaml.load(s))


def dump(obj: SiteCatalog, fp: TextIO, _format="yml", *args, **kwargs) -> None:
    """
    Serialize ``obj`` as a :py:class:`~Pegasus.api.site_catalog.SiteCatalog` formatted stream to ``fp`` (a ``.write()``-supporting file-like object).

    :param obj: SiteCatalog to serialize
    :type obj: SiteCatalog
    :param fp: file like object to serialize to
    :type fp: TextIO
    :param _format: format to write to if fp does not have an extension; can be one of ["yml" | "yaml" | "json"], defaults to "yml"
    :type _format: str
    :rtype: NoReturn
    """
    obj.write(fp, _format=_format)


def dumps(obj: SiteCatalog, _format="yml", *args, **kwargs) -> str:
    """
    Serialize ``obj`` to a :py:class:`~Pegasus.api.site_catalog.SiteCatalog` formatted ``str``.

    :param obj: SiteCatalog to serialize
    :type obj: SiteCatalog
    :param _format: format to write to if fp does not have an extension; can be one of ["yml" | "yaml" | "json"], defaults to "yml"
    :type _format: str
    :return: SiteCatalog serialized as a string
    :rtype: str
    """
    with StringIO() as s:
        obj.write(s, _format=_format)
        s.seek(0)
        return s.read()
