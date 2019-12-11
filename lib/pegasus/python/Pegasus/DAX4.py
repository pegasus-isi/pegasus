# TODO: licensing stuff
# TODO: example workflow for help(DAX4)
"""API for generating and running Pegasus workflows.
"""

__author__ = "Pegasus Team"
__version__ = "4.0"

import json
from collections import namedtuple
from dataclasses import dataclass, asdict
from enum import Enum

import yaml

# TODO: pydocstyle everything

# TODO: look at pyyaml Dumper, and instead of using YAMLAble, implement a
# __json__() function
# Inside the catalog.write function, use json encoder
# https://gist.github.com/claraj/3b2b95a62c5ba6860c03b5c737c214ab
# yaml.dump(DeliveryEncoder().default(catalog), stream)
# json.dump(catalog, cls=DeliveryEncoder, stream)

"""
class DeliveryEncoder(json.JSONEncoder):
	def default(self, obj):
		
		if isinstance(obj, Date):
			return "whatever spec we come up with for Date such as ISO8601"
        elif isinstance(obj, Path):
            return obj.resolve
        elif hasattr(obj, "__json__"):
            if callable(obj.__json__):
                return obj.__json__()
            else:
                raise TypeError or something along those lines 

		return json.JSONEncoder.default(self, obj) # default, if not Delivery object. Caller's problem if this is not serialziable.

"""


def todict(_dict, cls):
    # https://pypi.org/project/dataclasses-fromdict/
    raise NotImplementedError()


# TODO: transformation catalog, replica catalog, adag

# TODO: print to stdout, executive summary of generated workflow which includes
# things such as: executables, num jobs, replicas, sites, etc..
# This way a user can glance over the summary and re-generate the workflow if
# something looks off (e.g. they expected to add 10 jobs, but actually added 9)

# TODO: decide which symbols to expose
# __all__ = []

# --- PEGASUS VERSION ----------------------------------------------------------
PEGASUS_VERSION = 5.0

# --- ERRORS -------------------------------------------------------------------
# TODO: document errors
class DAX4Error(Exception):
    pass


class DuplicateError(DAX4Error):
    pass


class NotFoundError(DAX4Error):
    pass


class FormatError(DAX4Error):
    pass


class ParseError(DAX4Error):
    pass


# --- FILE FORMAT --------------------------------------------------------------
class FileFormat(Enum):
    JSON = "json"
    YAML = "yml"


# --- JSON ---------------------------------------------------------------------
class CustomEncoder(json.JSONEncoder):
    def default(self, obj):
        # TODO: handle instance of Date and Path
        if hasattr(obj, "__json__"):
            if callable(obj.__json__):
                return obj.__json__()
            else:
                raise TypeError("__json__ is not callable for {}".format(obj))

        return json.JSONEncoder.default(self, obj)


def filter_out_nones(_dict):
    if not isinstance(_dict, dict):
        raise ValueError(
            "a dict must be passed to this function, not {}".format(type(_dict))
        )

    return {key: value for key, value in _dict.items() if value is not None}


'''
# --- Metadata -----------------------------------------------------------------
class _MetadataMixin:
    def add_metadata(self, key, value):
        """Add metadata as a key value pair to this object
        
        :param key: key
        :type key: str
        :param value: value
        :type value: str
        :raises DuplicateError: metadata keys must be unique
        """
        if key in self.metadata:
            raise DuplicateError
        else:
            self.metadata[key] = value

    def update_metadata(self, key, value):
        """Update metadata
        
        :param key: key
        :type key: str
        :param value: value
        :type value: str
        :raises NotFoundError: key not found 
        """
        if key not in self.metadata:
            raise NotFoundError
        else:
            self.metadata[key] = value

    def has_metadata(self, key):
        """Check if metadata with the given key exists for this object
        
        :param key: key
        :type key: str
        :return: whether or not the given metadata key exists for this object
        :rtype: bool
        """
        return key in self.metadata

    def clear_metadata(self):
        """Clear all the metadata given to this object"""
        self.metadata.clear()


# --- Hooks --------------------------------------------------------------------
class EventType(Enum):
    """Event type on which a hook will be triggered"""

    NEVER = "never"
    START = "start"
    ERROR = "error"
    SUCCESS = "success"
    END = "end"
    ALL = "all"


class _HookMixin:
    """Derived class can have hooks assigned to it. This currently supports
    shell hooks, and will be extended to web hooks etc.
    """

    def __init__(self):
        """Constructor"""
        self.hooks = defaultdict()

        # num hooks as if self.hooks was flattened
        self.num_hooks = 0

    def add_shell_hook(self, event_type, cmd):
        """Add a shell hook
        
        :param event_type: an event type defined in DAX4.EventType
        :type event_type: str
        :param cmd: shell command
        :type cmd: str
        """
        if not isinstance(event_type, EventType):
            raise ValueError("event_type must be one of EventType")

        self.hooks[ShellHook.__hook_type__].append(ShellHook(event_type.value, cmd))
        self.num_hooks += 1

    def __len__(self):
        return self.num_hooks

    def _YAMLify(self):
        yaml_obj = dict()

        """
        group hooks together s.t. we have the following:
        {
            "shell": [
                {"_on": <EventType.xxxx>, "cmd": <shell command>},
                ...
            ]
        }
        """
        for hook_type, items in self.hooks.items():
            yaml_obj[hook_type] = [hook._YAMLify() for hook in items]

        return yaml_obj


class Hook(_YAMLAble):
    """Base class that specific hook types will inherit from"""

    def __init__(self, event_type):
        if not isinstance(event_type, EventType):
            raise ValueError("event_type must be one of EventType")

        self.on = event_type.value


class ShellHook(Hook):
    """A hook that executes a shell command"""

    __hook_type__ = "shell"

    def __init__(self, event_type, cmd):
        Hook.__init__(self, event_type.value)
        self.cmd = cmd

    def _YAMLify(self):
        return {"_on": self.on, "cmd": self.cmd}


# --- Profiles -----------------------------------------------------------------
class Namespace(Enum):
    """
    Profile Namespace values recognized by Pegasus. See Executable,
    Transformation, and Job.
    """

    PEGASUS = "pegasus"
    CONDOR = "condor"
    DAGMAN = "dagman"
    ENV = "env"
    HINTS = "hints"
    GLOBUS = "globus"
    SELECTOR = "selector"
    STAT = "stat"


class ProfileMixin(_YAMLAble):
    """Deriving class can have Profiles assigned to it"""

    # a profile entry
    Profile = namedtuple("Profile", ["namespace", "key", "value"])

    def __init__(self):
        """Constructor"""
        self.profiles = set()

    def add_profile(self, namespace, key, value):
        """Add a profile to this object
        
        :param namespace: a namespace defined in DAX4.Namespace
        :type namespace: str (defined in DAX4.Namespace)
        :param key: key
        :type key: str
        :param value: value
        :type value: str
        :raises DuplicateError: profiles must be unique
        """
        if not isinstance(namespace, Namespace):
            raise ValueError("namespace must be one of Namespace")

        p = Profile(namespace.value, key, value)

        if self.has_profile(namespace.value, key, value):
            raise DuplicateError("Duplicate profile %s" % p)
        self.profiles.add(p)

    def has_profile(self, namespace, key, value):
        """Check if a profile with the given namespace, key, and value exists
        
        :param namespace: a namespace defined in DAX4.Namespace
        :type namespace: str (defined in DAX4.Namespace)
        :param key: key
        :type key: str
        :param value: value
        :type value: str
        :raises DuplicateError: profiles must be unique
        :return: True if it exists, else false
        :rtype: bool
        """
        if not isinstance(namespace, Namespace):
            raise ValueError("namespace must be one of Namespace")

        p = Profile(namespace.value, key, value)
        return p in self.profiles

    def remove_profile(self, namespace, key, value):
        """Remove profile from this object"""
        if not isinstance(namespace, Namespace):
            raise ValueError("namespace must be one of Namespace")

        p = Profile(namespace.value, key, value)

        if not self.has_profile(p):
            raise NotFoundError("Profile not found", p)
        self.profiles.remove(p)

    def clear_profiles(self):
        """Remove all profiles from this object"""
        self.profiles.clear()

    def _YAMLify(self):
        yaml_obj = dict()

        """
        group profiles together s.t. we have the following:
        {
            "ns1": {
                "key1": "value",
                "key2"" "value
            },
            "ns2": {...},
            ...
        }
        """
        for p in self.profiles:
            if p.namespace.value not in yaml_obj:
                yaml_obj[p.namespace.value] = {p.key: p.value}
            else:
                yaml_obj[p.namespace.value][p.key] = p.value

        return yaml_obj
'''

# --- Sites --------------------------------------------------------------------
class Architecture(Enum):
    """Architecture types"""

    X86 = "x86"
    X86_64 = "x86_64"
    PPC = "ppc"
    PPC_64 = "ppc_64"
    IA64 = "ia64"
    SPARCV7 = "sparcv7"
    SPARCV9 = "sparcv9"
    AMD64 = "amd64"


class SiteCatalog:
    pass


# --- Replicas -----------------------------------------------------------------
@dataclass(frozen=True)
class Replica:
    lfn: str = None
    pfn: str = None
    site: str = None
    regex: bool = False

    def __json__(self):
        return filter_out_nones(asdict(self))


class ReplicaCatalog:
    """ReplicaCatalog class which maintains a mapping of logical filenames
    to physical filenames. This mapping is a one to many relationship.
    """

    def __init__(self, filepath="ReplicaCatalog.yml"):
        """Constructor
        
        :param filepath: filepath to write this catalog to, defaults to "ReplicaCatalog.yml"
        :type filepath: str, optional
        """
        self.filepath = filepath
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

        path = self.filepath
        if non_default_filepath != "" or non_default_filepath != None:
            path = non_default_filepath

        with open(path, "w") as file:
            if file_format == FileFormat.YAML:
                yaml.dump(CustomEncoder().default(self), file)
            elif file_format == FileFormat.JSON:
                json.dump(self, file, cls=CustomEncoder)
            else:
                raise ValueError("invalid file format {}".format(file_format))

    def __json__(self):
        return {
            "pegasus": PEGASUS_VERSION,
            "replicas": [r.__json__() for r in self.replicas],
        }


'''
# --- Transformations ----------------------------------------------------------
class TransformationType(Enum):
    """Specifies the type of the transformation. STAGEABLE denotes that it can
    be shipped around as a file. INSTALLED denotes that the transformation is
    installed on a specified machine, and that it cannot be shipped around as
    a file to be executed somewher else. 
    """

    STAGEABLE = "stageable"
    INSTALLED = "installed"


class TransformationSite:
    """Site specific information about a Transformation. Transformations will contain
    at least one TransformationSite object which includes, at minimum, the name of the site,
    the transformation's pfn on that site and whether or not it is installed or stageable at
    that site.  
    """

    def __init__(
        self,
        name,
        pfn,
        type,
        arch=None,
        ostype=None,
        osrelease=None,
        osversion=None,
        glibc=None,
        container=None,
    ):
        """Constructor
        
        :param name: site name associated with this transformation
        :type name: str
        :param pfn: physical file name
        :type pfn: str
        :param type: TransformationType.STAGEABLE or TransformationType.INSTALLED
        :type type: TransformationType
        :param arch: Architecture that this transformation was compiled for, defaults to None
        :type arch: Architecture, optional
        :param ostype: Name of os that this transformation was compiled for, defaults to None
        :type ostype: str, optional
        :param osrelease: Release of os that this transformation was compiled for, defaults to None, defaults to None
        :type osrelease: str, optional
        :param osversion: Version of os that this transformation was compiled for, defaults to None, defaults to None
        :type osversion: str, optional
        :param glibc: Version of glibc this transformation was compiled against, defaults to None
        :type glibc: str, optional
        :param container: specify the container to use, optional
        :type container: str 
        """

        self.name = name
        self.pfn = pfn

        if not isinstance(type, TransformationType):
            raise ValueError("type must be one of TransformationType")

        self.type = type.value

        if not isinstance(arch, Arch):
            raise ValueError("arch must be one of Arch")

        self.arch = arch.value

        self.os = os
        self.osrelease = osrelease
        self.osversion = osversion
        self.glibc = glibc
        self.container = container

    def __json__(self):
        site_as_dict = {"name": self.name, "pfn": self.pfn, "type": self.type}

        if self.arch is not None:
            site_as_dict["arch"] = self.arch

        if self.os is not None:
            site_as_dict["os.type"] = self.os

        if self.osrelease is not None:
            site_as_dict["os.release"] = self.osrelease

        if self.osversion is not None:
            site_as_dict["os.version"] = self.osversion

        if self.glibc is not None:
            site_as_dict["glibc"] = self.glibc

        if self.container is not None:
            site_as_dict["container"] = self.container

        if len(self.profiles) > 0:
            site_as_dict["profiles"] = ProfileMixin._YAMLify(self)

        return site_as_dict


class ContainerType(Enum):
    """Container types recognized by Pegasus"""

    DOCKER = "docker"
    SINGULARITY = "singularity"
    SHIFTER = "shifter"


class Container(_YAMLAble, ProfileMixin):
    def __init__(self, name, container_type, image, mount, image_site=None):
        """Constructor
        
        :param name: name of this container
        :type name: str
        :param container_type: a type defined in ContainerType
        :type container_type: ContainerType
        :param image: image, such as 'docker:///rynge/montage:latest'
        :type image: str
        :param mount: mount, such as '/Volumes/Work/lfs1:/shared-data/:ro'
        :type mount: str
        :param image_site: optional site attribute to tell pegasus which site tar file exists, defaults to None
        :type image_site: str, optional
        :raises ValueError: container_type must be one of ContainerType
        """
        ProfileMixin.__init__(self)

        self.name = name

        if not isinstance(container_type, ContainerType):
            raise ValueError("container_type must be one of ContainerType")

        self.container_type = container_type.value
        self.image = image
        self.mount = mount
        self.image_site = image_site

    def _YAMLify(self):
        container_as_dict = {
            "name": self.name,
            "type": self.container_type,
            "image": self.image,
            "mount": self.mount,
        }

        if self.image_site is not None:
            container_as_dict["imageSite"] = self.image_site

        if len(self.profiles) > 0:
            container_as_dict["profiles"] = ProfileMixin._YAMLify()

        return container_as_dict


class Transformation(_YAMLAble, ProfileMixin, HookMixin):
    """A transformation, which can be a standalone executable, or one that
        requires other executables. Transformations can reside on one or
        more sites where they are either stageable (a binary that can be shipped
        around) or installed.
    """

    def __init__(
        self, name, namespace=None, version=None,
    ):
        """Constructor

        :param name: Logical name of executable
        :type name: str
        :param namespace: Transformation namespace
        :type namespace: str, optional
        :param version: Transformation version, defaults to None
        :type version: str, optional
        """
        ProfileMixin.__init__(self)
        HookMixin.__init__(self)

        self.name = name
        self.namespace = namespace
        self.version = version
        self.sites = dict()
        self.requires = list()

    def add_site(
        self,
        name,
        pfn,
        type,
        arch=None,
        ostype=None,
        osrelease=None,
        osversion=None,
        glibc=None,
        container=None,
    ):
        """Add a TransformationSite to this Transformation
        
        :param name: site name associated with this transformation
        :type name: str
        :param pfn: physical file name
        :type pfn: str
        :param type: TransformationType.STAGEABLE or TransformationType.INSTALLED
        :type type: TransformationType
        :param arch: Architecture that this transformation was compiled for, defaults to None
        :type arch: Architecture, optional
        :param os: Name of os that this transformation was compiled for, defaults to None
        :type os: str, optional
        :param osrelease: Release of os that this transformation was compiled for, defaults to None, defaults to None
        :type osrelease: str, optional
        :param osversion: Version of os that this transformation was compiled for, defaults to None, defaults to None
        :type osversion: str, optional
        :param glibc: Version of glibc this transformation was compiled against, defaults to None
        :type glibc: str, optional
        :param container: specify the container to use, optional
        :type container: str 
        :raises DuplicateError: a site with this is already associated to this Transformation
        """

        if name in self.sites:
            raise DuplicateError

        if not isinstance(type, TransformationType):
            raise ValueError("type must be one of TransformationType")

        if arch is not None:
            if not isinstance(arch, Arch):
                raise ValueError("arch must be one of Arch")

        self.sites[name] = TransformationSite(
            name, pfn, type, arch, ostype, osrelease, osversion, glibc, container,
        )

        return self

    def get_site(self, name):
        """Retrieve a TransformationSite object associated with this 
        Transformation by site name

        
        :param name: site name
        :type name: str
        :raises NotFoundError: the site has not been added for this Transformation
        :return: the TransformationSite object associated with this Transformation 
        :rtype: TransformationSite
        """
        if name not in self.sites:
            raise NotFoundError

        return self.sites[name]

    def has_site(self, name):
        """Check if a site has been added for this Transformation
        
        :param name: site name
        :type name: str
        :return: True if site has been added, else False
        :rtype: bool
        """
        return name in self.sites

    def remove_site(self, name):
        """Remove the given site from this Transformation
        
        :param name: name of site to be removed
        :type name: str
        :raises NotFoundError: the site has not been added for this Transformation 
        """
        if name not in self.sites:
            raise NotFoundError

        del self.sites[name]

    def add_site_profile(self, site, namespace, key, value):
        """Add a Profile to a TransformationSite that is associated with this Transformation
        
        :param site: site name
        :type site: str
        :param namespace: a namespace defined in DAX4.Namespace
        :type namespace: str (defined in DAX4.Namespace)
        :param key: key
        :type key: str
        :param value: value
        :type value: str
        :raises NotFoundError: site was not found for this transformation
        :raises ValueError: namespace must be one of Namespace
        :return: self
        :rtype: Transformation
        """
        if site not in self.sites:
            raise NotFoundError

        if not isinstance(namespace, Namespace):
            raise ValueError("namespace must be one of Namespace")

        self.sites[site].add_profile(namespace, key, value)

        return self

    def _YAMLify(self):
        transformation_as_dict = dict()
        transformation_as_dict["name"] = self.name

        if self.namespace is not None:
            transformation_as_dict["namespace"] = self.namespace

        if self.version is not None:
            transformation_as_dict["version"] = self.version

        transformation_as_dict["sites"] = [
            site._YAMLify() for name, site in self.sites.items()
        ]

        if len(self.requires) > 0:
            transformation_as_dict["requires"] = self.requires

        if len(self.profiles) > 0:
            transformation_as_dict["profiles"] = ProfileMixin._YAMLify(self)

        if len(self.hooks) > 0:
            transformation_as_dict["hooks"] = HookMixin._YAMLify(self)

    def __str__(self):
        return u"<Transformation %s::%s:%s>" % (self.namespace, self.name, self.version)


class TransformationCatalog(_YAMLAble):
    """TransformationCatalog class maintains a list a Transformations, site specific
    Transformation information, and a list of containers
    """

    def __init__(self, filepath="TransformationCatalog.yml"):
        """Constructor
        
        :param filepath: filepath to write this catalog to , defaults to "TransformationCatalog.yml"
        :type filepath: str, optional
        """
        self.transformations = dict()
        self.containers = dict()
        self.filepath = filepath

    def add_transformation(self, name, namespace=None, version=None):
        """Add a Transformation to this TransformationCatalog
        
        :param name: Transformation name 
        :type name: str
        :param namespace: a namespace this Transformation belongs to, defaults to None
        :type namespace: str, optional
        :param version: version of this Transformation, defaults to None
        :type version: str, optional
        :raises DuplicateError: a Transformation with this (name, namespace, version) already exists
        :return: the added Transformation object 
        :rtype: Transformation
        """
        if (name, namespace, version) in self.transformations:
            raise DuplicateError

        t = Transformation(name, namespace, version)
        self.transformations[(name, namespace, version)] = t

        return t

    def get_transformation(self, name, namespace=None, version=None):
        """Retrieve a transformation from this catalog by the key (name, namespace, version)

        :param name: Transformation name 
        :type name: str
        :param namespace: a namespace this Transformation belongs to, defaults to None
        :type namespace: str, optional
        :param version: version of this Transformation, defaults to None
        :type version: str, optional
        :raises NotFoundError: Transformation with (name, namespace, key) does not exist
        :return: the Transformation with the given (name, namespace, key)
        :rtype: Transformation
        """
        if (name, namespace, version) not in self.transformations:
            raise NotFoundError

        return self.transformations[(name, namespace, version)]

    def remove_transformation(self, name, namespace=None, version=None):
        pass

    def add_container(self, name, container_type, image, mount, image_site=None):
        """Retrieve a container by its name
        
        :param name: name of this container
        :type name: str
        :param container_type: a type defined in ContainerType
        :type container_type: ContainerType
        :param image: image, such as 'docker:///rynge/montage:latest'
        :type image: str
        :param mount: mount, such as '/Volumes/Work/lfs1:/shared-data/:ro'
        :type mount: str
        :param image_site: optional site attribute to tell pegasus which site tar file exists, defaults to None
        :type image_site: str, optional
        :raises DuplicateError: a Container with this name already exists
        :raises ValueError: container_type must be one of ContainerType
        :return: the added container object
        :rtype: Container
        """
        if name in self.containers:
            raise DuplicateError

        if not isinstance(container_type, ContainerType):
            raise ValueError("container_type must be one of ContainerType")

        c = Container(name, container_type, image, mount, image_site)
        self.containers[name] = c

        return c

    def get_container(self, name):
        """Retrieve a container from this catalog by its name
        
        :param name: Container name
        :type name: str
        :raises NotFoundError: a Container by this name does not exist in this catalog
        :return: the Container with the given name
        :rtype: Container
        """
        if name not in self.containers:
            raise NotFoundError

        return self.containers[name]

    def remove_container(self, name):
        pass

    def write(self, non_default_filepath=""):
        """Write this catalog, formatted in YAML, to a file
        
        :param filepath: path to which this catalog will be written, defaults to self.filepath if filepath is "" or None
        :type filepath: str, optional
        """
        catalog = self._YAMLify()

        # when written out to a separate file, the catalog must contain
        # the pegasus api version
        catalog["pegasus"] = PEGASUS_VERSION

        path = (
            self.filepath
            if non_default_filepath == "" or non_default_filepath == None
            else non_default_filepath
        )
        with open(path, "w") as file:
            yaml.dump(catalog, file)

    def _YAMLify(self):
        tc_as_dict = {
            "transformations": [t._YAMLify() for key, t in self.transformations.items()]
        }

        if len(self.containers) > 0:
            tc_as_dict["containers"] = [
                c._YAMLify() for key, c in self.containers.items()
            ]

        return tc_as_dict
'''

# --- Workflow -----------------------------------------------------------------


class AbstractJob:
    pass


class Job(AbstractJob):
    pass


class DAX(AbstractJob):
    pass


class Workflow:
    pass
