from collections import OrderedDict
from pathlib import Path
from typing import Dict, Optional, Set, Union

from ._utils import _chained
from .errors import DuplicateError
from .mixins import MetadataMixin
from .writable import Writable, _filter_out_nones

PEGASUS_VERSION = "5.0.4"

__all__ = ["File", "ReplicaCatalog"]


class _PFN:
    """A physical file name comprising site and path"""

    def __init__(self, site, pfn):
        self.site = site
        self.pfn = pfn

    def __eq__(self, other):
        if isinstance(other, _PFN):
            return self.site == other.site and self.pfn == other.pfn
        return False

    def __hash__(self):
        return hash((self.site, self.pfn))

    def __repr__(self):
        return "<_PFN site: {}, pfn: {}>".format(self.site, self.pfn)

    def __json__(self):
        return OrderedDict([("site", self.site), ("pfn", self.pfn)])


class File(MetadataMixin):
    """
    A workflow File. This class is used to represent the inputs and outputs of a
    :py:class:`~Pegasus.api.workflow.Job`.

    .. code-block:: python

        # Example
        input_file = File("data.txt").add_metadata(creator="ryan")

    """

    def __init__(self, lfn: str, size: Optional[int] = None, for_planning: Optional[bool] = False):
        """
        :param lfn: a unique logical filename
        :type lfn: str
        :param size: size in bytes, defaults to None
        :type size: int
        :param for_planning: indicate that a file is to be used for planning purposes
        :type for_planning: bool
        """
        if not isinstance(lfn, str):
            raise TypeError(
                "invalid lfn: {lfn}; lfn must be of type str".format(lfn=lfn)
            )

        self.metadata = OrderedDict()
        self.lfn = lfn
        self.size = size
        if size:
            self.metadata["size"] = size
        if for_planning:
            self.for_planning = for_planning
        else:
            self.for_planning = None

    def __str__(self):
        return self.lfn

    def __hash__(self):
        return hash(self.lfn)

    def __eq__(self, other):
        if isinstance(other, File):
            return self.lfn == other.lfn
        return False

    def __repr__(self):
        return "<{} {}>".format(self.__class__.__name__, self.lfn)

    def __json__(self):
        return _filter_out_nones(
            OrderedDict(
                [
                    ("lfn", self.lfn),
                    ("metadata", self.metadata if len(self.metadata) > 0 else None),
                    ("size", self.size),
                    ("forPlanning", self.for_planning)
                ]
            )
        )


class _ReplicaCatalogEntry:
    def __init__(
        self,
        lfn: str,
        pfns: Set[_PFN],
        checksum: Optional[Dict[str, str]] = None,
        metadata: Optional[Dict[str, Union[int, str, float]]] = None,
        regex: bool = False,
    ):
        self.lfn = lfn
        self.pfns = pfns
        self.checksum = checksum or OrderedDict()
        self.metadata = metadata or OrderedDict()
        self.regex = regex

    def __json__(self):
        return _filter_out_nones(
            OrderedDict(
                [
                    ("lfn", self.lfn),
                    ("pfns", [pfn for pfn in self.pfns]),
                    ("checksum", self.checksum if len(self.checksum) > 0 else None),
                    ("metadata", self.metadata if len(self.metadata) > 0 else None),
                    ("regex", self.regex if self.regex else None),
                ]
            )
        )


class ReplicaCatalog(Writable):
    """Maintains a mapping of logical filenames to physical filenames. Any input
       files to the workflow are specified here so that Pegasus knows where to
       obtain them.

        .. code-block:: python

            # Example
            if1 = File("if")
            if2 = File("if2")

            rc = ReplicaCatalog()\\
                .add_replica("local", if1, "/nfs/u2/ryan/data.csv")\\
                .add_replica("local", "if2", "/nfs/u2/ryan/data2.csv")\\
                .write()
    """

    _DEFAULT_FILENAME = "replicas.yml"

    _SUPPORTED_CHECKSUMS = {"sha256"}

    def __init__(self):
        Writable.__init__(self)

        # Using key = (<lfn or pattern>, <is_regex>) to preserve insertion
        # order of entries while distinguishing between regex and
        # non regex entries
        self.entries = OrderedDict()

    @_chained
    def add_regex_replica(
        self,
        site: str,
        pattern: str,
        pfn: Union[str, Path],
        metadata: Optional[Dict[str, Union[int, str, float]]] = None,
    ):
        r"""
        add_regex_replica(self, site: str, pattern: str, pfn: Union[str, Path], metadata: Optional[Dict[str, Union[int, str, float]]] = None)
        Add an entry to this replica catalog using a regular expression pattern.
        Note that regular expressions should follow Java regular expression syntax
        as the underlying code that handles this catalog is Java based.

            .. code-block:: python

                # Example 1: Match f<any-character>a i.e. faa, f.a, f0a, etc.
                rc.add_regex_replica("local", "f.a", "/Volumes/data/input/f.a")

                # Example 2: Using groupings
                rc.add_regex_replica("local", "alpha\.(csv|txt|xml)", "/Volumes/data/input/[1]/[0]")

                # If the file being looked up is alpha.csv, the pfn for the file will be
                # generated as /Volumes/data/input/csv/alpha.csv

                # Example 3: Specifying a default location for all lfns that don't match any
                # regular expressions. Note that this should be the last entry into the replica
                # catalog if used.

                rc.add_regex_replica("local", ".*", Path("/Volumes/data") / "input/[0]")


        :param site: the site at which this replica (file) resides
        :type site: str
        :param pattern: regular expression used to match a file
        :type pattern: str
        :param pfn: path to the file (may also be a pattern as shown in the example above)
        :type pfn: Union[str, Path]
        :param metadata: any metadata to be associated with the matched files, for example: :code:`{"creator": "pegasus"}`, defaults to None
        :type metadata: Optional[Dict[str, Union[int, str, float]]]
        :raises DuplicateError: Duplicate patterns with different PFNs are currently not supported
        """

        metadata = metadata or OrderedDict()

        # restricting pattern to single pfn (may be relaxed in future release)
        if (pattern, True) in self.entries:
            raise DuplicateError(
                "Pattern: {} already exists in this replica catalog".format(pattern)
            )

        # handle Path obj if given for pfn
        if isinstance(pfn, Path):
            if not pfn.is_absolute():
                raise ValueError(
                    "Invalid pfn: {}, the given pfn must be an absolute path".format(
                        pfn
                    )
                )

            pfn = str(pfn)

        self.entries[(pattern, True)] = _ReplicaCatalogEntry(
            lfn=pattern, pfns={_PFN(site, pfn)}, metadata=metadata, regex=True
        )

    @_chained
    def add_replica(
        self,
        site: str,
        lfn: Union[str, File],
        pfn: Union[str, Path],
        checksum: Optional[Dict[str, str]] = None,
        metadata: Optional[Dict[str, Union[int, str, float]]] = None,
    ):
        """
        add_replica(self, site: str, lfn: Union[str, File], pfn: Union[str, Path], checksum: Optional[Dict[str, str]] = None, metadata: Optiona[Dict[str, Union[int, str, float]]] = None)
        Add an entry to this replica catalog.

            .. code-block:: python

                # Example 1
                f = File("in.txt").add_metadata(creator="pegasus")
                rc.add_replica("local", f, Path(".").resolve() / "in.txt")

                # Example 2: Adding metadata and a checksum
                rc.add_replica(
                    "local",
                    "in.txt",
                    "/home/ryan/wf/in.txt",
                    checksum={"sha256": "abc123"},
                    metadata={"creator": "pegasus"}
                )

                # Example 3: Adding multiple pfns for the same lfn (metadata and checksum will be
                # updated for that lfn if given.
                rc.add_replica("local", "in.txt", Path(".").resolve() / "in.txt")
                rc.add_replica("condorpool", "in.txt", "/path/to/file/in.txt")

        :param site: the site at which this replica (file) resides
        :type site: str
        :param lfn: logical file name
        :type lfn: Union[str, File]
        :param pfn: physical file name such as :code:`Path("f.txt").resolve()`, :code:`/home/ryan/file.txt`, or :code:`http://pegasus.isi.edu/file.txt`
        :type pfn: Union[str, Path]
        :param checksum: Dict containing checksums for this file. Currently only sha256 is given. This should be entered as :code:`{"sha256": <value>}`, defaults to None
        :type checksum: Optional[Dict[str, str]], optional
        :param metadata: metadata key value pairs associated with this lfn such as :code:`{"created": "Thu Jun 18 22:18:36 PDT 2020", "owner": "pegasus"}`, defaults to None
        :type metadata: Optional[Dict[str, Union[int, str, float]]], optional
        :raises ValueError: if pfn is given as a :code:`pathlib.Path`, it must be an absolute path
        :raises ValueError: an unsupported checksum type was given
        """

        # handle Path obj if given for pfn
        if isinstance(pfn, Path):
            if not pfn.is_absolute():
                raise ValueError(
                    "Invalid pfn: {}, the given path must be an absolute path".format(
                        str(pfn)
                    )
                )

            pfn = str(pfn)

        if isinstance(pfn, File):
            raise TypeError(
                "Invalid pfn: {}, the given pfn must be a str or pathlib.Path".format(
                    pfn
                )
            )

        metadata = metadata or OrderedDict()
        checksum = checksum or OrderedDict()

        # File might contain metadata that should be included
        if isinstance(lfn, File):
            if lfn.metadata:
                metadata.update(lfn.metadata)

            lfn = lfn.lfn

        # ensure supported checksum type given
        if len(checksum) > 0:
            for checksum_type in checksum:
                if checksum_type.lower() not in ReplicaCatalog._SUPPORTED_CHECKSUMS:
                    raise ValueError(
                        "Invalid checksum: {}, supported checksum types are: {}".format(
                            checksum_type, ReplicaCatalog._SUPPORTED_CHECKSUMS
                        )
                    )

        # if an entry with the given lfn already exists, update it
        # else create and add a new one
        if (lfn, False) in self.entries:
            self.entries[(lfn, False)].pfns.add(_PFN(site, pfn))
            self.entries[(lfn, False)].checksum.update(checksum)
            self.entries[(lfn, False)].metadata.update(metadata)
        else:
            self.entries[(lfn, False)] = _ReplicaCatalogEntry(
                lfn,
                {_PFN(site, pfn)},
                checksum=checksum,
                metadata=metadata,
                regex=False,
            )

    def __json__(self):
        return OrderedDict(
            [
                ("pegasus", PEGASUS_VERSION),
                ("replicas", [v for _, v in self.entries.items()]),
            ]
        )
