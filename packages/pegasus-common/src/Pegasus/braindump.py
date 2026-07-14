r""":mod:`braindump` exposes an API to serialize and deserialize Pegasus's braindump file.

Basic Usage::

    >>> from Pegasus import braindump
    >>> braindump.loads("user: mayani\ngrid_dn: null\n")
    {'grid_dn': None, 'user': 'mayani'}

    >>> print(braindump.dumps({"grid_dn": None, "user": "mayani"}))
    '{"grid_dn": null, "user": "mayani"}'

.. moduleauthor:: Rajiv Mayani <mayani@isi.edu>
"""

import typing
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import TextIO

from Pegasus import yaml

__all__ = (
    "load",
    "loads",
    "dump",
    "dumps",
    "Braindump",
)


typing._ClassVar = typing.ClassVar


@dataclass()
class Braindump:
    """Data class representing Braindump file.

    .. todo::

        Use :mod:`dataclasses` instead of :mod:`attr`
    """

    def __post_init__(self):
        """Convert string path fields to :class:`pathlib.Path` and coerce ``uses_pmc`` to bool."""
        self.dax = Path(self.dax) if self.dax else self.dax
        self.basedir = Path(self.basedir) if self.basedir else self.basedir
        self.submit_dir = Path(self.submit_dir) if self.submit_dir else self.submit_dir
        self.planner = Path(self.planner) if self.planner else self.planner
        self.bindir = Path(self.bindir) if self.bindir else self.bindir
        self.uses_pmc = None if self.uses_pmc is None else bool(self.uses_pmc)

    #: The username of the user that ran pegasus-plan
    user: str | None = field(default=None)

    #: The Distinguished Name in the proxy
    grid_dn: str | None = field(default=None)

    #: The hostname of the submit host
    submit_hostname: str | None = field(default=None)

    #: The workflow uuid of the root workflow
    root_wf_uuid: str | None = field(default=None)

    #: The workflow uuid of the current workflow i.e the one whose submit directory
    #: the braindump file is.
    wf_uuid: str | None = field(default=None)

    #: The path to the dax file
    dax: Path | None = field(default=None)

    #: The label attribute in the adag element of the dax
    dax_label: str | None = field(default=None)

    #: The index in the dax.
    dax_index: str | None = field(default=None)

    #: The version of the DAX schema that DAX referred to.
    dax_version: str | None = field(default=None)

    #: The workflow name constructed by pegasus when planning
    pegasus_wf_name: str | None = field(default=None)

    #: The timestamp when planning occurred
    timestamp: str | None = field(default=None)

    #: The base submit directory
    basedir: Path | None = field(default=None)

    #: The full path for the submit directory
    submit_dir: Path | None = field(default=None)

    #: The planner used to construct the executable workflow. always pegasus
    planner: Path | None = field(default=None)

    #: The versions of the planner
    planner_version: str | None = field(default=None)

    #: The arguments with which the planner is invoked.
    planner_arguments: str | None = field(default=None)

    #: The build timestamp
    pegasus_build: str | None = field(default=None)

    #: The path to the jobstate file
    jsd: str | None = field(default=None)

    #: The rundir in the numbering scheme for the submit directories
    rundir: str | None = field(default=None)

    #: The bin directory of the pegasus installation
    bindir: Path | None = field(default=None)

    #: The vo group to which the user belongs to. Defaults to pegasus
    vogroup: str | None = field(default=None)

    #: Whether the workflow uses PMC
    uses_pmc: bool | None = field(default=None)

    #: The full path to the properties file in the submit directory
    properties: str | None = field(default=None)

    #: The full path to condor common log in the submit directory
    condor_log: str | None = field(default=None)

    #: The basename of the dag file created
    dag: str | None = field(default=None)

    #: The type of executable workflow. Can be dag | shell
    type: str | None = field(default=None)

    #: The notify file that contains any notifications that need to be sent
    #: for the workflow.
    notify: str | None = field(default=None)

    #: Set in PMC mode.
    script: str | None = field(default=None)

    #: The application this workflow belongs to
    app: str | None = field(default=None)


def load(fp: TextIO, *args, **kwargs) -> Braindump:
    """Deserialize ``fp`` (a ``.read()``-supporting file-like object containing a Braindump document) to a :class:`Braindump` object.

    :param fp: readable file-like object containing a YAML-formatted braindump document
    :type fp: TextIO
    :return: populated Braindump dataclass instance
    :rtype: Braindump
    """
    return loads(fp.read())


def loads(s: str, *args, **kwargs) -> Braindump:
    """Deserialize ``s`` (a ``str``, ``bytes``, or ``bytearray`` containing a Braindump document) to a :class:`Braindump` object.

    :param s: YAML-formatted braindump string
    :type s: str
    :return: populated Braindump dataclass instance
    :rtype: Braindump
    :raises ValueError: if ``s`` does not parse to a YAML mapping
    """
    _dict = yaml.load(s, *args, **kwargs)

    if not isinstance(_dict, dict):
        raise ValueError("Invalid braindump file.")

    return Braindump(**_dict)


def dump(obj: Braindump, fp: TextIO, *args, **kwargs) -> None:
    """Serialize ``obj`` as a YAML-formatted braindump stream to ``fp`` (a ``.write()``-supporting file-like object).

    :param obj: Braindump instance to serialize
    :type obj: Braindump
    :param fp: writable file-like object to write the YAML output to
    :type fp: TextIO
    """
    yaml.dump(asdict(obj), fp, *args, **kwargs)


def dumps(obj: Braindump, *args, **kwargs) -> str:
    """Serialize ``obj`` to a YAML-formatted braindump ``str``.

    :param obj: Braindump instance to serialize
    :type obj: Braindump
    :return: YAML-formatted braindump string
    :rtype: str
    """
    return yaml.dumps(asdict(obj), *args, **kwargs)
