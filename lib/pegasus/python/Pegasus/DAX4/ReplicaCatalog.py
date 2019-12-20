import json

import yaml

from .Writable import Writable, filter_out_nones, FileFormat
from .Errors import DuplicateError, NotFoundError
from .Mixins import MetadataMixin

PEGASUS_VERSION = "5.0"


class File(MetadataMixin):
    """A workflow File"""

    def __init__(self, lfn):
        """Constuctor
        
        :param lfn: a unique logical filename 
        :type lfn: str
        """
        self.metadata = dict()
        self.lfn = lfn

    def __str__(self):
        return self.lfn

    def __hash__(self):
        return hash(self.lfn)

    def __json__(self):
        return filter_out_nones(
            {
                "lfn": self.lfn,
                "metadata": dict(self.metadata) if len(self.metadata) > 0 else None,
            }
        )


class Replica:
    """A ReplicaCatalog entry"""

    def __init__(self, lfn, pfn, site, regex=False):
        """Constructor
        
        :param lfn: File or logical filename
        :type lfn: File|str
        :param pfn: physical filename
        :type pfn: str
        :param site: site at which this file(replica) resides
        :type site: str
        :param regex: whether or not to evaluate this lfn and pfn as a regex, defaults to False
        :type regex: bool, optional
        """
        if isinstance(lfn, File):
            # lfn is a File object
            self.lfn = lfn.lfn
        else:
            self.lfn = lfn

        self.pfn = pfn
        self.site = site
        self.regex = regex

    def __json__(self):
        return {
            "lfn": self.lfn,
            "pfn": self.pfn,
            "site": self.site,
            "regex": self.regex,
        }

    def __hash__(self):
        return hash((self.lfn, self.pfn, self.site, self.regex))


class ReplicaCatalog(Writable):
    """ReplicaCatalog class which maintains a mapping of logical filenames
    to physical filenames. This mapping is a one to many relationship.
    """

    def __init__(self):
        """Constructor"""
        self.replicas = set()

    def add_replica(self, lfn, pfn, site, regex=False):
        """Add an entry to the replica catalog
        
        :param lfn: logical filename
        :type lfn: str
        :param pfn: physical file name 
        :type pfn: str
        :param site: site at which this file resides
        :type site: str
        :param regex: whether or not the lfn is a regex pattern, defaults to False
        :type regex: bool, optional
        :raises DuplicateError: an entry with the same parameters already exists in the catalog
        """
        r = Replica(lfn, pfn, site, regex)
        if r in self.replicas:
            raise DuplicateError("Duplicate replica catalog entry {}".format(r))
        else:
            self.replicas.add(r)

        return self

    def remove_replica(self, lfn, pfn, site, regex=False):
        """Remove a replica with the given lfn, pfn, site, and regex value
        
        :param lfn: logical filename
        :type lfn: str
        :param pfn: physical file name 
        :type pfn: str
        :param site: site at which this file resides
        :type site: str
        :param regex: whether or not the lfn is a regex pattern, defaults to False
        :type regex: bool, optional
        :raises NotFoundError: Replica(lfn, pfn, site, regex) has not been added to this catalog
        :return: self
        :rtype: ReplicaCatalog
        """
        args = (lfn, pfn, site, regex)
        if not self.has_replica(*args):
            raise NotFoundError(
                "replica with lfn: {0}, pfn: {1}, site: {2}, regex: {3} does not exist".format(
                    lfn, pfn, site, regex
                )
            )

        self.replicas.remove(Replica(*args))

        return self

    def has_replica(self, lfn, pfn, site, regex=False):
        """Check if a Replica with the following properties exists in this catalog
        
        :param lfn: logical filename
        :type lfn: str
        :param pfn: physical file name 
        :type pfn: str
        :param site: site at which this file resides
        :type site: str
        :param regex: whether or not the lfn is a regex pattern, defaults to False
        :type regex: bool, optional
        :return: whether or not Replica(lfn, pfn, site, regex) has been added to this catalog
        :rtype: bool
        """
        return Replica(lfn, pfn, site, regex) in self.replicas

    def __json__(self):
        return {
            "pegasus": PEGASUS_VERSION,
            "replicas": [r.__json__() for r in self.replicas],
        }
