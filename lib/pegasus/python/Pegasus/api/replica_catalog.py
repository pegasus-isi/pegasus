from ._utils import _chained
from .errors import DuplicateError
from .mixins import MetadataMixin
from .writable import Writable, _filter_out_nones

PEGASUS_VERSION = "5.0"

__all__ = ["File", "ReplicaCatalog"]


class File(MetadataMixin):
    """
    A workflow File. This class is used to represent 
    :py:class:`~Pegasus.api.workflow.Job` inputs and outputs.
    """

    def __init__(self, lfn):
        """
        :param lfn: a unique logical filename 
        :type lfn: str
        """
        if not isinstance(lfn, str):
            raise TypeError(
                "invalid lfn: {lfn}; lfn must be of type str".format(lfn=lfn)
            )

        self.metadata = dict()
        self.lfn = lfn

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
                "metadata": dict(self.metadata) if len(self.metadata) > 0 else None,
            }
        )


class _ReplicaCatalogEntry:
    """Internal class used to represent a Replica Catalog entry"""

    def __init__(
        self, site, lfn, pfn, regex=False, checksum_type=None, checksum_value=None
    ):
        self.site = site
        self.lfn = lfn
        self.pfn = pfn
        self.regex = regex
        self.checksum_type = None
        self.checksum_value = None

        if checksum_type and checksum_value:
            if self.regex:
                raise ValueError("Checksum values cannot be used with a regex entry")
            else:
                self.checksum_type = checksum_type
                self.checksum_value = checksum_value
        elif bool(checksum_type) ^ bool(checksum_value):
            raise ValueError(
                "Checksum usage in replica catalog requires that both checksum_type and checksum_value be set"
            )

    def __str__(self):
        return "ReplicaEntry(site={}, lfn={}, pfn={}, regex={}, checksum_type={}, checksum_value={})".format(
            self.site,
            self.lfn,
            self.pfn,
            self.regex,
            self.checksum_type,
            self.checksum_value,
        )

    def __hash__(self):
        return hash(
            (
                self.site,
                self.lfn,
                self.pfn,
                self.regex,
                self.checksum_type,
                self.checksum_value,
            )
        )

    def __eq__(self, other):
        if isinstance(other, _ReplicaCatalogEntry):
            return (
                self.site == other.site
                and self.lfn == other.lfn
                and self.pfn == other.pfn
                and self.regex == other.regex
                and self.checksum_type == other.checksum_type
                and self.checksum_value == other.checksum_value
            )
        else:
            raise ValueError(
                "_ReplicaCatalogEntry cannot be compared with {}".format(type(other))
            )

    def __json__(self):
        return _filter_out_nones(
            {
                "site": self.site,
                "lfn": self.lfn,
                "pfn": self.pfn,
                "regex": self.regex if self.regex else None,
                "checksum": {"type": self.checksum_type, "value": self.checksum_value}
                if self.checksum_type and self.checksum_value
                else None,
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

    def __init__(self):
        self.replicas = set()

    @_chained
    def add_replica(
        self, site, lfn, pfn, regex=False, checksum_type=None, checksum_value=None
    ):
        """Add an entry to the replica catalog. If regex=True, checksum_type and
        checksum_value cannot be used. If a checksum of this replica is to be used, 
        both checksum_type and checksum_value must be specified.

        .. code-block:: python

            # Example 1
            rc.add_replica("local", "f.a", "file:///Volumes/data/input/f.a")

            # Example 2
            # refers to 'f<any char>a' such as faa, f.a, f0a, etc.
            rc.add_replica("local", "f.a", "file:///Volumes/data/input/f.a", regex="true")

            # Example 3
            rc.add_replica("local", 
                           "f.a", 
                           "file:///lfs1/input-data/f.a", 
                           checksum_type="sha256", 
                           checksum_value="ca8ed5988cb4ca0b67c45fd80fd17423aba2a066ca8a63a4e1c6adab067a3e92")
        
        :param site: site at which this file resides
        :type site: str
        :param lfn: logical filename or :py:class:`~Pegasus.api.replica_catalog.File`
        :type lfn: str or File
        :param pfn: physical file name 
        :type pfn: str
        :param regex: whether or not the lfn is a regex pattern, defaults to False
        :type regex: bool, optional
        :param checksum_type: the checksum type, currently only type of sha256 is supported
        :type checksum_type: str, optional
        :param checksum_value: the checksum for the file
        :type checksum_value: str, optional
        :raises DuplicateError: an entry with the same parameters already exists in the catalog
        :raises TypeError: lfn must be of type :py:class:`~Pegasus.api.replica_catalog.File` or str
        :return: self
        """
        if not isinstance(lfn, File) and not isinstance(lfn, str):
            raise TypeError(
                "invalid lfn: {lfn}; lfn must be of type File or str".format(lfn=lfn)
            )

        if isinstance(lfn, File):
            lfn = lfn.lfn

        replica = _ReplicaCatalogEntry(
            site,
            lfn,
            pfn,
            regex=regex,
            checksum_type=checksum_type,
            checksum_value=checksum_value,
        )
        if replica in self.replicas:
            raise DuplicateError(
                "entry: {replica} already exists in this ReplicaCatalog".format(
                    replica=replica
                )
            )
        else:
            self.replicas.add(replica)

    def __json__(self):
        return {
            "pegasus": PEGASUS_VERSION,
            "replicas": [r for r in self.replicas],
        }
