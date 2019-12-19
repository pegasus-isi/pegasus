import json

import yaml

from .Encoding import filter_out_nones, FileFormat, CustomEncoder
from .Errors import DuplicateError, NotFoundError

PEGASUS_VERSION = "5.0"

# TODO: MetadataMixin
class File:
    def __init__(self, lfn):
        self.lfn = lfn

    def __json__(self):
        return {"lfn": self.lfn}

    def __str__(self):
        return self.lfn

    def __hash__(self):
        return hash(self.lfn)


class Replica:
    def __init__(self, lfn, pfn, site, regex=False):
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


class ReplicaCatalog:
    """ReplicaCatalog class which maintains a mapping of logical filenames
    to physical filenames. This mapping is a one to many relationship.
    """

    def __init__(self, default_filepath="ReplicaCatalog"):
        """Constructor
        
        :param filepath: filepath to write this catalog to, defaults to "ReplicaCatalog.yml"
        :type filepath: str, optional
        """
        self.default_filepath = default_filepath
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
        """[summary]
        
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

    def write(self, non_default_filepath="", file_format=FileFormat.YAML):
        """Write this catalog, formatted in YAML, to a file
        
        :param filepath: path to which this catalog will be written, defaults to self.filepath if filepath is "" or None
        :type filepath: str, optional
        """
        if not isinstance(file_format, FileFormat):
            raise ValueError("invalid file format {}".format(file_format))

        path = self.default_filepath
        if non_default_filepath != "":
            path = non_default_filepath
        else:
            if file_format == FileFormat.YAML:
                path = ".".join([self.default_filepath, FileFormat.YAML.value])
            elif file_format == FileFormat.JSON:
                path = ".".join([self.default_filepath, FileFormat.JSON.value])

        with open(path, "w") as file:
            if file_format == FileFormat.YAML:
                yaml.dump(CustomEncoder().default(self), file)
            elif file_format == FileFormat.JSON:
                json.dump(self, file, cls=CustomEncoder, indent=4)

    def __json__(self):
        return {
            "pegasus": PEGASUS_VERSION,
            "replicas": [r.__json__() for r in self.replicas],
        }
