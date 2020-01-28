import json

import yaml

from .writable import Writable
from .writable import _filter_out_nones
from .errors import DuplicateError
from .errors import NotFoundError
from .mixins import MetadataMixin

PEGASUS_VERSION = "5.0"

__all__ = ["File", "ReplicaCatalog"]


class File(MetadataMixin):
    """
    A workflow File. This class is used to represent 
    :py:class:`~Pegasus.dax4.workflow.Job` inputs and outputs.
    """

    def __init__(self, lfn):
        """
        :param lfn: a unique logical filename 
        :type lfn: str
        """
        if not isinstance(lfn, str):
            raise ValueError("lfn must be a string")

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


class ReplicaCatalog(Writable):
    """Maintains a mapping of logical filenames to physical filenames"""

    def __init__(self):
        self.replicas = set()

    def add_replica(self, lfn, pfn, site, regex=False):
        """Add an entry to the replica catalog
        
        .. code-block:: python

            # Example
            if1 = File("if")
            if2 = File("if2")

            rc = (
                ReplicaCatalog()
                .add_replica(if1, "/nfs/u2/ryan/data.csv", "local")
                .add_replica("if2", "/nfs/u2/ryan/data2.csv", "local")
            )

        :param lfn: logical filename or :py:class:`~Pegasus.dax4.replica_catalog.File`
        :type lfn: str or File
        :param pfn: physical file name 
        :type pfn: str
        :param site: site at which this file resides
        :type site: str
        :param regex: whether or not the lfn is a regex pattern, defaults to False
        :type regex: bool, optional
        :raises DuplicateError: an entry with the same parameters already exists in the catalog
        :raises ValueError: lfn must be of type :py:class:`~Pegasus.dax4.replica_catalog.File` or str
        :return: self
        """
        if not isinstance(lfn, File) and not isinstance(lfn, str):
            raise ValueError("lfn must be File or str")

        if isinstance(lfn, File):
            lfn = lfn.lfn

        replica = (lfn, pfn, site, regex)
        if replica in self.replicas:
            raise DuplicateError("duplicate replica catalog entry {}".format(replica))
        else:
            self.replicas.add(replica)

        return self

    def __json__(self):
        return {
            "pegasus": PEGASUS_VERSION,
            "replicas": [
                {"lfn": r[0], "pfn": r[1], "site": r[2], "regex": r[3]}
                for r in self.replicas
            ],
        }
