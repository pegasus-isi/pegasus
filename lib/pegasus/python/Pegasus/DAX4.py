# TODO: licensing stuff
# TODO: example workflow for help(DAX4)
"""API for generating and running Pegasus workflows.
"""

__author__ = "Pegasus Team"
__version__ = "4.0"

import json
from collections import namedtuple, defaultdict
from dataclasses import dataclass, asdict
from enum import Enum

import yaml

# TODO: pydocstyle everything

# TODO: pegasus.conf (define schema, then add functionality here)


def todict(_dict, cls):
    # https://pypi.org/project/dataclasses-fromdict/
    raise NotImplementedError()


# TODO: print to stdout, executive summary of generated workflow which includes
# things such as: executables, num jobs, replicas, sites, etc..
# This way a user can glance over the summary and re-generate the workflow if
# something looks off (e.g. they expected to add 10 jobs, but actually added 9)

# TODO: decide which symbols to expose
# __all__ = []

# --- pegasus api version ------------------------------------------------------
PEGASUS_VERSION = "5.0"

# --- errors -------------------------------------------------------------------
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


# --- file format --------------------------------------------------------------
class FileFormat(Enum):
    JSON = "json"
    YAML = "yml"


# --- json ---------------------------------------------------------------------
class CustomEncoder(json.JSONEncoder):
    def default(self, obj):
        # TODO: handle instance of Date and Path
        """
        if isinstance(obj, Date):
			return "whatever spec we come up with for Date such as ISO8601"
        elif isinstance(obj, Path):
            return obj.resolve
        """

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
# --- metadata -----------------------------------------------------------------
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


# --- hooks --------------------------------------------------------------------
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


'''
# --- profiles -----------------------------------------------------------------
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


class ProfileMixin:
    """Deriving class can have Profiles assigned to it"""

    def add_profile(self, namespace, key, value):
        """Add a profile to this object
        
        :param namespace: a namespace defined in DAX4.Namespace
        :type namespace: str (defined in DAX4.Namespace)
        :param key: key
        :type key: str
        :param value: value
        :type value: str
        :raises DuplicateError: profiles must be unique
        :return: self
        :rtype: type(self)
        """
        if not isinstance(namespace, Namespace):
            raise ValueError("namespace must be one of Namespace")

        if namespace.value in self.profiles:
            if key in self.profiles[namespace.value]:
                raise DuplicateError(
                    f"Duplicate profile with namespace: {namespace.value}, key: {key}, value: {value}"
                )

        self.profiles[namespace.value][key] = value

        return self

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

        is_found = False
        if namespace.value in self.profiles:
            if key in self.profiles[namespace.value]:
                is_found = True

        return is_found

    def remove_profile(self, namespace, key, value):
        """Remove a profile from this object
        
        :param namespace: a namespace defined in DAX4.Namespace
        :type namespace: str (defined in DAX4.Namespace)
        :param key: key
        :type key: str
        :param value: value
        :type value: str
        :raises ValueError: Namespace must be one of DAX4.Namespace
        :raises NotFoundError: given profile with namespace key and value is not found 
        :return: self
        :rtype: type(self)
        """
        if not isinstance(namespace, Namespace):
            raise ValueError("namespace must be one of Namespace")

        if not self.has_profile(namespace, key, value):
            raise NotFoundError(
                "Profile with namespace: {}, key: {}, value: {} not found".format(
                    namespace.value, key, value
                )
            )

        del self.profiles[namespace.value][key]

        # Removing namespaces with no k,v pairs
        if len(self.profiles[namespace.value]) == 0:
            del self.profiles[namespace.value]

        return self

    def clear_profiles(self):
        """Remove all profiles from this object
        
        :return: self
        :rtype: type(self)
        """
        self.profiles.clear()

        return self


# --- sites --------------------------------------------------------------------
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


# --- replicas -----------------------------------------------------------------
@dataclass(frozen=True)
class File:
    lfn: str = None

    def __json__(self):
        return asdict(self)

    def __str__(self):
        return self.lfn


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


# --- transformations ----------------------------------------------------------
class TransformationType(Enum):
    """Specifies the type of the transformation. STAGEABLE denotes that it can
    be shipped around as a file. INSTALLED denotes that the transformation is
    installed on a specified machine, and that it cannot be shipped around as
    a file to be executed somewher else. 
    """

    STAGEABLE = "stageable"
    INSTALLED = "installed"


class TransformationSite(ProfileMixin):
    """Site specific information about a Transformation. Transformations will contain
    at least one TransformationSite object which includes, at minimum, the name of the site,
    the transformation's pfn on that site and whether or not it is installed or stageable at
    that site.  
    """

    def __init__(
        self,
        name,
        pfn,
        transformation_type,
        arch=None,
        os_type=None,
        os_release=None,
        os_version=None,
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
        :param os_type: Name of os that this transformation was compiled for, defaults to None
        :type os_type: str, optional
        :param os_release: Release of os that this transformation was compiled for, defaults to None, defaults to None
        :type os_release: str, optional
        :param os_version: Version of os that this transformation was compiled for, defaults to None, defaults to None
        :type os_version: str, optional
        :param glibc: Version of glibc this transformation was compiled against, defaults to None
        :type glibc: str, optional
        :param container: specify the container to use, optional
        :type container: str 
        """

        self.name = name
        self.pfn = pfn

        if not isinstance(transformation_type, TransformationType):
            raise ValueError("type must be one of TransformationType")

        self.transformation_type = transformation_type.value

        if arch is not None:
            if not isinstance(arch, Architecture):
                raise ValueError("arch must be one of Arch")
            else:
                self.arch = arch.value

        self.os_type = os_type
        self.os_release = os_release
        self.os_version = os_version
        self.glibc = glibc
        self.container = container

        self.profiles = defaultdict(dict)

    def __json__(self):

        return filter_out_nones(
            {
                "name": self.name,
                "pfn": self.pfn,
                "type": self.transformation_type,
                "arch": self.arch,
                "os.type": self.os_type,
                "os.release": self.os_release,
                "os.version": self.os_version,
                "glibc": self.glibc,
                "container": self.container,
                "profiles": dict(self.profiles) if len(self.profiles) > 0 else None,
            }
        )


class ContainerType(Enum):
    """Container types recognized by Pegasus"""

    DOCKER = "docker"
    SINGULARITY = "singularity"
    SHIFTER = "shifter"


class Container(ProfileMixin):
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
        self.name = name

        if not isinstance(container_type, ContainerType):
            raise ValueError("container_type must be one of ContainerType")

        self.container_type = container_type.value
        self.image = image
        self.mount = mount
        self.image_site = image_site

        self.profiles = defaultdict(dict)

    def __json__(self):
        return filter_out_nones(
            {
                "name": self.name,
                "type": self.container_type,
                "image": self.image,
                "mount": self.mount,
                "imageSite": self.image_site,
                "profiles": dict(self.profiles) if len(self.profiles) > 0 else None,
            }
        )


# class Transformation(ProfileMixin, HookMixin)
class Transformation(ProfileMixin):
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
        self.name = name
        self.namespace = namespace
        self.version = version
        self.sites = dict()
        self.requires = set()

        self.profiles = defaultdict(dict)
        self.hooks = dict()

    def add_site(
        self,
        name,
        pfn,
        transformation_type,
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
            raise DuplicateError(
                "Site {0} already exists for transformation {1}".format(name, self.name)
            )

        if not isinstance(transformation_type, TransformationType):
            raise ValueError("type must be one of TransformationType")

        if arch is not None:
            if not isinstance(arch, Architecture):
                raise ValueError("arch must be one of Arch")

        self.sites[name] = TransformationSite(
            name,
            pfn,
            transformation_type,
            arch,
            ostype,
            osrelease,
            osversion,
            glibc,
            container,
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
            raise NotFoundError(
                "Site {0} not found for transformation {1}".format(name, self.name)
            )

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
            raise NotFoundError(
                "Site {0} not found for transformation {1}".format(name, self.name)
            )

        del self.sites[name]

    def add_requirement(self, transformation_name):
        """Add a requirement to this Transformation
        
        :param transformation_name: name of the Transformation that this transformation requires
        :type transformation_name: str
        :raises DuplicateError: this requirement already exists
        :return: self
        :rtype: Transformation
        """
        if transformation_name in self.requires:
            raise DuplicateError(
                "Transformation {0} already requires {1}".format(
                    self.name, transformation_name
                )
            )

        self.requires.add(transformation_name)

        return self

    def has_requirement(self, transformation_name):
        """Check if this Transformation requires the given transformation
        
        :param transformation_name: Transformation name to check
        :type transformation_name: str
        :return: whether or not this Transformation requires transformation_name
        :rtype: bool
        """
        return transformation_name in self.requires

    def remove_requirement(self, transformation_name):
        """Remove a requirement from this Transformation
        
        :param transformation_name: name of the Transformation to be removed from the list of requirements
        :type transformation_name: str
        :raises NotFoundError: this requirement does not exist
        """
        if not self.has_requirement(transformation_name):
            raise NotFoundError(
                "Transformation {0} does not have requirement {1}".format(
                    self.name, transformation_name
                )
            )

        self.requires.remove(transformation_name)

        return self

    def __json__(self):
        return filter_out_nones(
            {
                "namespace": self.namespace,
                "name": self.name,
                "version": self.version,
                "requires": list(self.requires) if len(self.requires) > 0 else None,
                "profiles": dict(self.profiles) if len(self.profiles) > 0 else None,
                "hooks": self.hooks if len(self.hooks) > 0 else None,
            }
        )

    def __str__(self):
        return "<Transformation {0}::{1}:{2}>".format(
            self.namespace, self.name, self.version
        )


class TransformationCatalog:
    """TransformationCatalog class maintains a list a Transformations, site specific
    Transformation information, and a list of containers
    """

    def __init__(self, default_filepath="TransformationCatalog"):
        """Constructor
        
        :param filepath: filepath to write this catalog to , defaults to "TransformationCatalog.yml"
        :type filepath: str, optional
        """
        self.default_filepath = default_filepath
        self.transformations = dict()
        self.containers = dict()

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
        if self.has_transformation(name, namespace, version):
            raise DuplicateError(
                "Transformation with name: {0}, namespace: {1}, version: {2} already exists".format(
                    name, namespace, version
                )
            )

        t = Transformation(name, namespace, version)
        self.transformations[(name, namespace, version)] = t

        return self

    def get_transformation(self, name, namespace=None, version=None):
        """Retrieve a Transformation from this catalog by the key (name, namespace, version)

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
        if not self.has_transformation(name, namespace, version):
            raise NotFoundError(
                "Transformation with name: {0}, namespace: {1}, version: {2} does not exist".format(
                    name, namespace, version
                )
            )

        return self.transformations[(name, namespace, version)]

    def has_transformation(self, name, namespace=None, version=None):
        """Check if a Transformation with (name, namespace, version) exists in this catalog
        
        :param name: Transformation name 
        :type name: str
        :param namespace: a namespace this Transformation belongs to, defaults to None
        :type namespace: str, optional
        :param version: version of this Transformation, defaults to None
        :type version: str, optional
        :return: whether or not a Transformation with (name, namespace, version) exists 
        :rtype: bool
        """
        return (name, namespace, version) in self.transformations

    def remove_transformation(self, name, namespace=None, version=None):
        """Remove the given Transformation from this catalog
        
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
        if not self.has_transformation(name, namespace, version):
            raise NotFoundError(
                "Transformation with namespace: {0}, name: {1}, version: {2} does not exist".format(
                    namespace, name, version
                )
            )

        del self.transformations[(name, namespace, version)]

        return self

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
        :return: self
        :rtype: TransformationCatalog
        """
        if self.has_container(name):
            raise DuplicateError("Container {0} already exists".format(name))

        if not isinstance(container_type, ContainerType):
            raise ValueError("container_type must be one of ContainerType")

        self.containers[name] = Container(
            name, container_type, image, mount, image_site
        )

        return self

    def has_container(self, name):
        """Check if a container exists in this catalog
        
        :param name: name of the container
        :type name: str
        :return: wether or not the container exists in this catalog
        :rtype: bool
        """
        return name in self.containers

    def get_container(self, name):
        """Retrieve a container from this catalog by its name
        
        :param name: Container name
        :type name: str
        :raises NotFoundError: a Container by this name does not exist in this catalog
        :return: the Container with the given name
        :rtype: Container
        """
        if not self.has_container(name):
            raise NotFoundError(
                "Container {0} does not exist in this catalog".format(name)
            )

        return self.containers[name]

    def remove_container(self, name):
        """Remove a conatiner with the given name
        
        :param name: container name
        :type name: str
        :raises NotFoundError: the Container with the given name does not exist in this catalog
        :return: self
        :rtype: TransformationCatalog
        """
        if not self.has_container(name):
            raise NotFoundError(
                "Container {0} does not exist in this catalog".format(name)
            )

        del self.containers[name]

        return self

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
        return filter_out_nones(
            {
                "pegasus": PEGASUS_VERSION,
                "transformations": [
                    t.__json__() for key, t in self.transformations.items()
                ],
                "containers": [c.__json__() for key, c in self.containers]
                if len(self.containers) > 0
                else None,
            }
        )


# --- workflow -----------------------------------------------------------------
@dataclass(frozen=True)
class JobInput:
    file: File
    _type: str = "input"

    def __json__(self):
        return {
            "type": self._type, 
            "lfn": self.file.lfn
        }
            


@dataclass(frozen=True)
class JobOutput:
    file: File
    _type: str = "output"
    stage_out: bool = True
    register_replica: bool = False

    def __json__(self):
        return {
            "lfn": self.file.lfn,
            "type": self._type,
            "stageOut": self.stage_out,
            "registerReplica": self.register_replica,
        }


@dataclass
class JobDependency:
    _id: str
    children: set

    def __json__(self):
        return {"id": self._id, "children": list(self.children)}


# TODO: metadata
# TODO: hooks
# TODO: profiles
class AbstractJob:
    def __init__(self, _id=None, node_label=None):
        self._id = _id
        self.node_label = node_label

        # self.stdout = None
        # self.stderr = None
        # self.stdin = None

        # self.hooks = defaultdict(dict)
        # self.profiles = defaultdict(dict)
        # self.metadata = dict()

    def __json__(self):
        raise NotImplementedError()


class Job(AbstractJob):
    def __init__(
        self,
        transformation,
        *arguments,
        _id=None,
        node_label=None,
        namespace=None,
        version=None,
    ):
        self.namespace = None
        self.version = None
        self.transformation = transformation
        self.arguments = arguments
        self.inputs = set()
        self.outputs = set()

        AbstractJob.__init__(self, _id=_id, node_label=node_label)

    def add_inputs(self, *input_files):
        for file in input_files:
            self.inputs.add(JobInput(file))

        return self

    def add_outputs(self, *output_files, stage_out=True, register_replica=False):
        for file in output_files:
            self.outputs.add(
                JobOutput(file, stage_out=stage_out, register_replica=register_replica)
            )

        return self

    def __json__(self):
        return filter_out_nones(
            {
                "namespace": self.namespace,
                "version": self.version,
                "name": self.transformation.name,
                "id": self._id,
                "nodeLabel": self.node_label,
                "arguments": [
                    arg.__json__() if isinstance(arg, File) else arg
                    for arg in self.arguments
                ],
                "uses": [_input.__json__() for _input in self.inputs]
                + [output.__json__() for output in self.outputs],
            }
        )


class DAX(AbstractJob):
    pass


class DAG(AbstractJob):
    pass


# TODO: profiles
# TODO: hooks
# TODO: metadata
class Workflow:
    def __init__(self, name, default_filepath="Workflow"):

        self.name = name
        self.default_filepath = default_filepath

        self.jobs = dict()
        self.dependencies = defaultdict(JobDependency)

        # sequence unique to this workflow only
        self.sequence = 1

        """
        TBD, its a touchy subject...
        self.site_catalog = None
        self.transformation_catalog = None
        self.replica_catalog = None
        """

    def add_job(
        self,
        transformation,
        *arguments,
        _id=None,
        namespace=None,
        version=None,
        node_label=None,
    ):
        if _id == None:
            _id = self.get_next_job_id()
        elif _id in self.jobs:
            raise DuplicateError("Job with id {0} already exists".format(_id))
        else:
            self.jobs[_id] = Job(
                transformation,
                *arguments,
                _id=_id,
                namespace=namespace,
                version=version,
                node_label=node_label,
            )

    def get_job(self, _id):
        if not _id in self.jobs:
            raise NotFoundError("Job with id {0} does not exist".format(_id))

        return self.jobs[_id]

    def get_next_job_id(self):
        next_id = None
        while next_id != None or next_id in self.jobs:
            next_id = "ID{:07d}".format(self.sequence)
            self.sequence += 1

        return next

    """
    TBD, its a touchy subject...
    def add_site_catalog(self, site_catalog):
        pass

    def add_replica_catalog(self, replica_catalog):
        pass

    def add_transformation_catalog(self, transformation_catalog):
        pass
    """

    def add_dependency(self, parent_id, *children_ids):
        children_ids = set(children_ids)
        if parent_id in self.dependencies:
            if not self.dependencies[parent_id].children.isdisjoint(children_ids):
                raise DuplicateError(
                    "A dependency already exists between parentid: {0} and children_ids: {1}".format(
                        parent_id, children_ids
                    )
                )

            self.dependencies[parent_id].children.update(children_ids)

        self.dependencies[parent_id] = JobDependency(parent_id, children_ids)

        return self

    def _infer_dependencies(self):
        pass

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
        # TODO: remove 'pegasus' from tc, rc, sc

        return filter_out_nones(
            {
                "pegasus": PEGASUS_VERSION,
                "name": self.name,
                "jobs": [job.__json__() for _id, job in self.jobs.items()],
                "jobDependencies": [
                    dependency.__json__()
                    for _id, dependency in self.dependencies.items()
                ]
                if len(self.dependencies) > 0
                else None,
            }
        )

