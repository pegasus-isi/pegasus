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

from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, TextIO

from Pegasus import yaml

__all__ = (
    "load",
    "loads",
    "dump",
    "dumps",
    "Braindump",
)


@dataclass()
class Braindump:
    """
    Data class representing Braindump file.

    .. todo::

        Use :mod:`dataclasses` instead of :mod:`attr`
    """

    def __post_init__(self):
        self.dax = Path(self.dax) if self.dax else self.dax
        self.basedir = Path(self.basedir) if self.basedir else self.basedir
        self.submit_dir = Path(self.submit_dir) if self.submit_dir else self.submit_dir
        self.planner = Path(self.planner) if self.planner else self.planner
        self.bindir = Path(self.bindir) if self.bindir else self.bindir
        self.uses_pmc = None if self.uses_pmc is None else bool(self.uses_pmc)

    #: The username of the user that ran pegasus-plan
    user: str = field(default=None)

    #: The Distinguished Name in the proxy
    grid_dn: str = field(default=None)

    #: The hostname of the submit host
    submit_hostname: str = field(default=None)  # type: str

    #: The workflow uuid of the root workflow
    root_wf_uuid: str = field(default=None)  # type: str

    #: The workflow uuid of the current workflow i.e the one whose submit directory
    #: the braindump file is.
    wf_uuid: str = field(default=None)  # type: str

    #: The path to the dax file
    dax: Path = field(default=None)  # type: Path

    #: The label attribute in the adag element of the dax
    dax_label: str = field(default=None)  # type: str

    #: The index in the dax.
    dax_index: str = field(default=None)  # type: str

    #: The version of the DAX schema that DAX referred to.
    dax_version: str = field(default=None)  # type: str

    #: The workflow name constructed by pegasus when planning
    pegasus_wf_name: str = field(default=None)  # type: str

    #: The timestamp when planning occured
    timestamp: str = field(default=None)  # type: str

    #: The base submit directory
    basedir: Path = field(default=None)  # type: Path

    #: The full path for the submit directory
    submit_dir: Path = field(default=None)  # type: Path

    #: The planner used to construct the executable workflow. always pegasus
    planner: Path = field(default=None)  # type: Path

    #: The versions of the planner
    planner_version: str = field(default=None)  # type: str

    #: The arguments with which the planner is invoked.
    planner_arguments: str = field(default=None)  # type: str

    #: The build timestamp
    pegasus_build: str = field(default=None)  # type: str

    #: The path to the jobstate file
    jsd: str = field(default=None)  # type: str

    #: The rundir in the numbering scheme for the submit directories
    rundir: str = field(default=None)  # type: str

    #: The bin directory of the pegasus installation
    bindir: Path = field(default=None)  # type: Path

    #: The vo group to which the user belongs to. Defaults to pegasus
    vogroup: str = field(default=None)  # type: str

    #: Whether the workflow uses PMC
    uses_pmc: bool = field(default=None)  # type: Optional[bool]

    #: The full path to the properties file in the submit directory
    properties: str = field(default=None)  # type: str

    #: The full path to condor common log in the submit directory
    condor_log: str = field(default=None)  # type: str

    #: The basename of the dag file created
    dag: str = field(default=None)  # type: str

    #: The type of executable workflow. Can be dag | shell
    type: str = field(default=None)  # type: str

    #: The notify file that contains any notifications that need to be sent
    #: for the workflow.
    notify: str = field(default=None)  # type: str

    #: Set in PMC mode.
    script: str = field(default=None)  # type: str


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


def dump(obj: Braindump, fp: TextIO, *args, **kwargs) -> None:
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
    yaml.dump(attr.asdict(obj), fp, *args, **kwargs)


def dumps(obj: Braindump, *args, **kwargs) -> str:
    """
    Serialize ``obj`` to a Braindump formatted ``str``.

    [extended_summary]

    :param obj: [description]
    :type obj: Dict
    :return: [description]
    :rtype: str
    """
    return yaml.dumps(attr.asdict(obj), *args, **kwargs)
