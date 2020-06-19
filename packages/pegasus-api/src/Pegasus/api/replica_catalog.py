from pathlib import Path
from collections import OrderedDict
from typing import Union, List, Dict, Optional, Set

from ._utils import _chained
from .errors import DuplicateError
from .mixins import MetadataMixin
from .writable import Writable, _filter_out_nones

PEGASUS_VERSION = "5.0"

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
        return {"site": self.site, "pfn": self.pfn}


class File(MetadataMixin):
    """
    A workflow File. This class is used to represent
    :py:class:`~Pegasus.api.workflow.Job` inputs and outputs.
    """

    def __init__(self, lfn: str, size: Optional[int]=None):
        """
        :param lfn: a unique logical filename
        :type lfn: str
        :param size: size in bytes, defaults to None
        :type size: int
        """
        if not isinstance(lfn, str):
            raise TypeError(
                "invalid lfn: {lfn}; lfn must be of type str".format(lfn=lfn)
            )

        self.metadata = dict()
        self.lfn = lfn
        self.size = size
        if size:
            self.metadata["size"] = size

    def __str__(self):
        return self.lfn

    def __hash__(self):
        return hash(self.lfn)

    def __eq__(self, other):
        if isinstance(other, File):
            return self.lfn == other.lfn
        return False

    def __json__(self):
        return _filter_out_nones(
            {
                "lfn": self.lfn,
                "metadata": self.metadata if len(self.metadata) > 0 else None,
                "size": self.size,
            }
        )


class _ReplicaCatalogEntry:
    def __init__(
        self,
        lfn: str,
        pfns: Set[_PFN],
        checksum: Dict[str, str] = dict(),
        metadata: Dict[str, Union[int, str, float]] = dict(),
        regex: bool = False,
    ):
        self.lfn = lfn
        self.pfns = pfns
        self.checksum = checksum
        self.metadata = metadata
        self.regex = regex

    def __json__(self):
        return _filter_out_nones(
            {
                "lfn": self.lfn,
                "pfns": [pfn for pfn in self.pfns],
                "checksum": self.checksum if len(self.checksum) > 0 else None,
                "metadata": self.metadata if len(self.metadata) > 0 else None,
                "regex": self.regex if self.regex else None,
            }
        )


class ReplicaCatalog(Writable):
    """Maintains a mapping of logical filenames to physical filenames

        .. code-block:: python

            # Example
            if1 = File("if")
            if2 = File("if2")

            (ReplicaCatalog()
                .add_replica("local", if1, "/nfs/u2/ryan/data.csv")
                .add_replica("local", "if2", "/nfs/u2/ryan/data2.csv")
                .write())
    """

    _DEFAULT_FILENAME = "replicas.yml"

    _SUPPORTED_CHECKSUMS = {"sha256"}

    def __init__(self):
        # Using key = (<lfn or pattern>, <is_regex>) to preserve insertion
        # order of entries while distinguishing between regex and
        # non regex entries
        self.entries = OrderedDict()

    @_chained
    def add_regex_replica(
        self,
        site: str,
        pattern: str,
        pfn: str,
        metadata: Dict[str, Union[int, str, float]] = {},
    ):
        """[summary]

        :param site: [description]
        :type site: str
        :param pattern: [description]
        :type pattern: str
        :param pfn: [description]
        :type pfn: str
        :param metadata: [description]
        :type metadata: Dict[str, Union[int, str, float]]
        :raises DuplicateError: [description]
        """

        # restricting pattern to single pfn (may be relaxed in future release)
        if (pattern, True) in self.entries:
            raise DuplicateError(
                "Pattern: {} already exists in this replica catalog".format(pattern)
            )

        self.entries[(pattern, True)] = _ReplicaCatalogEntry(
            lfn=pattern, pfns={_PFN(site, pfn)}, metadata=metadata, regex=True
        )

    @_chained
    def add_replica(
        self,
        site: str,
        lfn: Union[str, File],
        pfn: Union[str, Path],
        checksum: Dict[str, str] = dict(),
        metadata: Dict[str, Union[int, str, float]] = dict(),
    ):
        """[summary]

        :param site: [description]
        :type site: str
        :param lfn: [description]
        :type lfn: Union[str, File]
        :param pfn: [description]
        :type pfn: str
        :param checksum: [description], defaults to {}
        :type checksum: Dict[str, str], optional
        :param metadata: [description], defaults to {}
        :type metadata: Dict[str, Union[int, str, float]], optional
        :raises ValueError: [description]
        :raises DuplicateError: [description]
        :raises ValueError: [description]
        """


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
                ("replicas", [v for _, v in self.entries.items()])
            ]
        )
