import json
from enum import Enum
from collections import defaultdict

import yaml

from .mixins import ProfileMixin, HookMixin, MetadataMixin
from .site_catalog import Arch, OSType
from .writable import filter_out_nones, Writable
from .errors import DuplicateError, NotFoundError

PEGASUS_VERSION = "5.0"


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
        :type os_type: OSType, optional
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
            if not isinstance(arch, Arch):
                raise ValueError("arch must be one of Arch")
            else:
                self.arch = arch.value
        else:
            self.arch = None

        if os_type is not None:
            if not isinstance(os_type, OSType):
                raise ValueError("os_type must be one of OSType")
            else:
                self.os_type = os_type.value
        else:
            self.os_type = None

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


class Transformation(ProfileMixin, HookMixin, MetadataMixin):
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

        self.hooks = defaultdict(list)
        self.profiles = defaultdict(dict)
        self.metadata = dict()

    def get_key(self):
        return (self.name, self.namespace, self.version)

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
            if not isinstance(arch, Arch):
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

        return self

    def add_site_profile(self, site_name, namespace, key, value):
        """Add a profile to a TransformationSite with site_name 
        
        :param site_name: the name of the site to which the profile is to be added
        :type site_name: str
        :param namespace: a namespace defined in DAX4.Namespace
        :type namespace: str (defined in DAX4.Namespace)
        :param key: key
        :type key: str
        :param value: value
        :type value: str
        :raises NotFoundError: the given site_name was not found
        :return: self
        :rtype: Transformation
        """
        if site_name not in self.sites:
            raise NotFoundError(
                "Site {0} not found for transformation {1}".format(site_name, self.name)
            )

        self.sites[site_name].add_profile(namespace, key, value)

        return self

    def add_requirement(self, required_transformation, namespace=None, version=None):
        """Add a requirement to this Transformation
        
        :param required_transformation: Transformation that this transformation requires
        :type required_transformation: Transformation
        :raises DuplicateError: this requirement already exists
        :return: self
        :rtype: Transformation
        """
        if isinstance(required_transformation, Transformation):
            key = required_transformation.get_key()
        elif isinstance(required_transformation, str):
            key = (required_transformation, namespace, version)
        else:
            raise ValueError(
                "required_transformation must be of type Transformation or str"
            )

        if key in self.requires:
            raise DuplicateError(
                "Transformation {0} already requires Transformation {1}".format(
                    self.name, key
                )
            )

        self.requires.add(key)

        return self

    def has_requirement(self, transformation, namespace=None, version=None):
        """Check if this Transformation requires the given transformation
        
        :param transformation: the Transformation to check for 
        :type transformation: Transformation
        :return: whether or not this Transformation requires the given Transformation
        :rtype: bool
        """
        if isinstance(transformation, Transformation):
            key = transformation.get_key()
        elif isinstance(transformation, str):
            key = (transformation, namespace, version)
        else:
            raise ValueError(
                "required_transformation must be of type Transformation or str"
            )

        return key in self.requires

    def remove_requirement(self, transformation, namespace=None, version=None):
        """Remove a requirement from this Transformation
        
        :param transformation: the Transformation to be removed from the list of requirements
        :type transformation: Transformation
        :raises NotFoundError: this requirement does not exist
        """

        if isinstance(transformation, Transformation):
            key = transformation.get_key()
        elif isinstance(transformation, str):
            key = (transformation, namespace, version)
        else:
            raise ValueError(
                "required_transformation must be of type Transformation or str"
            )

        if not self.has_requirement(transformation, namespace, version):
            raise NotFoundError(
                "Transformation {0} does not have requirement {1}".format(
                    self.name, str(transformation)
                )
            )

        self.requires.remove(key)

        return self

    def __json__(self):
        # TODO: implement yaml dumper that rajiv suggested so that you don't have to loop through and call Object.__json__()...
        return filter_out_nones(
            {
                "namespace": self.namespace,
                "name": self.name,
                "version": self.version,
                "requires": [req[0] for req in self.requires]
                if len(self.requires) > 0
                else None,
                "sites": [site.__json__() for name, site in self.sites.items()],
                "profiles": dict(self.profiles) if len(self.profiles) > 0 else None,
                "hooks": {
                    hook_name: [hook.__json__() for hook in values]
                    for hook_name, values in self.hooks.items()
                }
                if len(self.hooks) > 0
                else None,
                "metadata": self.metadata if len(self.metadata) > 0 else None,
            }
        )

    def __str__(self):
        return "<Transformation {0}::{1}:{2}>".format(
            self.namespace, self.name, self.version
        )

    def __hash__(self):
        return hash(self.get_key())

    def __eq__(self, other):
        if isinstance(other, Transformation):
            return self.get_key() == other.get_key()
        raise ValueError(
            "Transformation cannot be compared with {0}".format(type(other))
        )


class TransformationCatalog(Writable):
    """TransformationCatalog class maintains a list a Transformations, site specific
    Transformation information, and a list of containers
    """

    def __init__(self):
        """Constructor"""
        self.transformations = dict()
        self.containers = dict()

    def add_transformations(self, *transformations):
        """Add one or more Transformations to this catalog
        
        :raises ValueError: argument(s) must be of type Transformation
        :raises DuplicateError: Transformation already exists in this catalog
        """
        for tr in transformations:
            if not isinstance(tr, Transformation):
                raise ValueError("input must be of type Transformation")

            if self.has_transformation(tr):
                raise DuplicateError("transformation already exists in catalog")

            self.transformations[tr.get_key()] = tr

        return self

    def has_transformation(self, transformation, namespace=None, version=None):
        """Check if this catalog contains the given Transformation
        
        :param transformation: the Transformation to check for 
        :type transformation: Transformation
        :return: whether or not this Transformation exists in this catalog
        :rtype: bool
        """
        if isinstance(transformation, Transformation):
            key = transformation.get_key()
        elif isinstance(transformation, str):
            key = (transformation, namespace, version)
        else:
            raise ValueError(
                "required_transformation must be of type Transformation or str"
            )
        return key in self.transformations

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

    def __json__(self):
        return filter_out_nones(
            {
                "pegasus": PEGASUS_VERSION,
                "transformations": [
                    t.__json__() for key, t in self.transformations.items()
                ],
                "containers": [c.__json__() for key, c in self.containers.items()]
                if len(self.containers) > 0
                else None,
            }
        )
