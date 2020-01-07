import json

import yaml

from .writable import Writable, filter_out_nones, FileFormat
from .errors import DuplicateError, NotFoundError
from .mixins import MetadataMixin

PEGASUS_VERSION = "5.0"


class File(MetadataMixin):
    """A workflow File"""

    def __init__(self, lfn):
        """Constuctor
        
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
        return filter_out_nones(
            {
                "lfn": self.lfn,
                "metadata": dict(self.metadata) if len(self.metadata) > 0 else None,
            }
        )


class ReplicaCatalog(Writable):
    """ReplicaCatalog class which maintains a mapping of logical filenames
    to physical filenames. This mapping is a one to many relationship.
    """

    def __init__(self):
        """Constructor"""
        self.replicas = set()

    def add_replica(self, lfn, pfn, site, regex=False):
        """Add an entry to the replica catalog
        
        :param lfn: logical filename or File object
        :type lfn: str|File
        :param pfn: physical file name 
        :type pfn: str
        :param site: site at which this file resides
        :type site: str
        :param regex: whether or not the lfn is a regex pattern, defaults to False
        :type regex: bool, optional
        :raises DuplicateError: an entry with the same parameters already exists in the catalog
        :raises ValueError: lfn must be of type File or str
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

    def remove_replica(self, lfn, pfn, site, regex=False):
        """Remove a replica with the given lfn, pfn, site, and regex value
        
        :param lfn: logical filename or File object
        :type lfn: str|File
        :param pfn: physical file name 
        :type pfn: str
        :param site: site at which this file resides
        :type site: str
        :param regex: whether or not the lfn is a regex pattern, defaults to False
        :type regex: bool, optional
        :raises NotFoundError: Replica(lfn, pfn, site, regex) has not been added to this catalog
        :raises ValueError: lfn must be of type File or str
        :return: self
        :rtype: ReplicaCatalog
        """
        if not isinstance(lfn, File) and not isinstance(lfn, str):
            raise ValueError("lfn must be File or str")

        if isinstance(lfn, File):
            lfn = lfn.lfn

        replica = (lfn, pfn, site, regex)
        if not self.has_replica(*replica):
            raise NotFoundError(
                "replica with lfn: {0}, pfn: {1}, site: {2}, regex: {3} does not exist".format(
                    *replica
                )
            )

        self.replicas.remove(replica)

        return self

    def has_replica(self, lfn, pfn, site, regex=False):
        """Check if a Replica with the following properties exists in this catalog
        
        :param lfn: logical filename or File
        :type lfn: str|File
        :param pfn: physical file name 
        :type pfn: str
        :param site: site at which this file resides
        :type site: str
        :param regex: whether or not the lfn is a regex pattern, defaults to False
        :type regex: bool, optional
        :raises ValueError: lfn must be of type File or str
        :return: whether or not (lfn, pfn, site, regex) has been added to this catalog
        :rtype: bool
        """
        if not isinstance(lfn, File) and not isinstance(lfn, str):
            raise ValueError("lfn must be File or str")

        if isinstance(lfn, File):
            lfn = lfn.lfn

        return (lfn, pfn, site, regex) in self.replicas

    def __json__(self):
        return {
            "pegasus": PEGASUS_VERSION,
            "replicas": [
                {"lfn": r[0], "pfn": r[1], "site": r[2], "regex": r[3]}
                for r in self.replicas
            ],
        }
