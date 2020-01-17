import json
from enum import Enum
from collections import defaultdict

import yaml

from .mixins import ProfileMixin
from .mixins import HookMixin
from .mixins import MetadataMixin
from .site_catalog import Arch
from .site_catalog import OSType
from .writable import _filter_out_nones
from .writable import Writable
from .errors import DuplicateError
from .errors import NotFoundError

PEGASUS_VERSION = "5.0"

__all__ = [
    "TransformationType",
    "ContainerType",
    "Transformation",
    "TransformationCatalog",
]


class TransformationType(Enum):
    """Specifies the type of the transformation. **STAGEABLE** denotes that it can
    be staged from one site to another. **INSTALLED** denotes that the transformation is
    installed on a specified machine, and that it cannot be staged and executed on
    other sites. 
    """

    STAGEABLE = "stageable"
    INSTALLED = "installed"


class _TransformationSite(ProfileMixin, MetadataMixin):
    """Site specific information about a :py:class:`~Pegasus.dax4.transformation_catalog.Transformation`. 
    A :py:class:`~Pegasus.dax4.transformation_catalog.Transformation` must contain at least one
    _TransformationSite. 
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
        """
        :param name: name of the site at which this :py:class:`~Pegasus.dax4.transformation_catalog.Transformation` resides
        :type name: str
        :param pfn: physical file name
        :type pfn: str
        :param type: a transformation type defined in :py:class:`~Pegasus.dax4.transformation_catalog.TransformationType`
        :type type: TransformationType
        :param arch: architecture that this :py:class:`~Pegasus.dax4.transformation_catalog.Transformation` was compiled for (defined in :py:class:`~Pegasus.dax4.site_catalog.Arch`), defaults to None
        :type arch: Arch, optional
        :param os_type: name of os that this :py:class:`~Pegasus.dax4.transformation_catalog.Transformation` was compiled for (defined in :py:class:`~Pegasus.dax4.site_catalog.OSType`), defaults to None
        :type os_type: OSType, optional
        :param os_release: release of os that this :py:class:`~Pegasus.dax4.transformation_catalog.Transformation` was compiled for, defaults to None, defaults to None
        :type os_release: str, optional
        :param os_version: version of os that this :py:class:`~Pegasus.dax4.transformation_catalog.Transformation` was compiled for, defaults to None, defaults to None
        :type os_version: str, optional
        :param glibc: version of glibc this :py:class:`~Pegasus.dax4.transformation_catalog.Transformation` was compiled against, defaults to None
        :type glibc: str, optional
        :param container: specify the name of the container to use, optional
        :type container: str 
        :raises ValueError: transformation_type must be one of :py:class:`~Pegasus.dax4.transformation_catalog.TransformationType`
        :raises ValueError: arch must be one of :py:class:`~Pegasus.dax4.site_catalog.Arch`
        :raises ValueError: os_type must be one of :py:class:`~Pegasus.dax4.site_catalog.OSType`
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
        self.metadata = dict()

    def __json__(self):

        return _filter_out_nones(
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
                "metadata": dict(self.metadata) if len(self.metadata) > 0 else None,
            }
        )


class ContainerType(Enum):
    """Container types recognized by Pegasus"""

    DOCKER = "docker"
    SINGULARITY = "singularity"
    SHIFTER = "shifter"


class Container(ProfileMixin):
    """Describes a container that can be added to the :py:class:`~Pegasus.dax4.transformation_catalog.TransformationCatalog`

    .. code-block:: python

        c = Container("centos-pegasus", ContainerType.DOCKER, "docker:///ryan/centos-pegasus:latest", ["/Volumes/Work/lfs1:/shared-data/:ro"])\
                .add_profile(Namespace.ENV, "JAVA_HOME", "/usr/bin/java")
            
    """

    def __init__(self, name, container_type, image, mounts=None, image_site=None):
        """
        :param name: name of this container
        :type name: str
        :param container_type: a container type defined in :py:class:`~Pegasus.dax4.transformation_catalog.ContainerType`
        :type container_type: ContainerType
        :param image: image, such as 'docker:///rynge/montage:latest'
        :type image: str
        :param mounts: list of mount strings such as ['/Volumes/Work/lfs1:/shared-data/:ro', ...]
        :type mounts: list
        :param image_site: optional site attribute to tell pegasus which site tar file exists, defaults to None
        :type image_site: str, optional
        :raises ValueError: container_type must be one of :py:class:`~Pegasus.dax4.transformation_catalog.ContainerType`
        """
        self.name = name

        if not isinstance(container_type, ContainerType):
            raise ValueError("container_type must be one of ContainerType")

        self.container_type = container_type.value
        self.image = image
        self.mounts = mounts
        self.image_site = image_site

        self.profiles = defaultdict(dict)

    def __json__(self):
        return _filter_out_nones(
            {
                "name": self.name,
                "type": self.container_type,
                "image": self.image,
                "mounts": self.mounts,
                "image.site": self.image_site,
                "profiles": dict(self.profiles) if len(self.profiles) > 0 else None,
            }
        )


class Transformation(ProfileMixin, HookMixin, MetadataMixin):
    """A transformation, which can be a standalone executable, or one that
    requires other executables. Transformations can reside on one or
    more sites where they are either **STAGEABLE** (a binary that can be shipped
    around) or **INSTALLED**.

    .. code-block:: python

        # Example
        foo = (Transformation("foo")
                .add_site("local", "/nfs/u2/ryan/bin/foo", TransformationType.STAGEABLE, arch=Arch.X86_64, ostype=OSType.LINUX)
                .add_site_profile("local", Namespace.ENV, "JAVA_HOME", "/usr/bin/java")
                .add_requirement("bar")) 
    """

    def __init__(
        self, name, namespace=None, version=None,
    ):
        """
        :param name: logical name of the transformation
        :type name: str
        :param namespace: transformation namespace
        :type namespace: str, optional
        :param version: transformation version, defaults to None
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

    def _get_key(self):
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
        """Add a transformation site to this :py:class:`~Pegasus.dax4.transformation_catalog.Transformation`
        
        :param name: name of the site at which this :py:class:`~Pegasus.dax4.transformation_catalog.Transformation` resides
        :type name: str
        :param pfn: physical file name
        :type pfn: str
        :param type: a transformation type defined in :py:class:`~Pegasus.dax4.transformation_catalog.TransformationType`
        :type type: TransformationType
        :param arch: architecture that this :py:class:`~Pegasus.dax4.transformation_catalog.Transformation` was compiled for (defined in :py:class:`~Pegasus.dax4.site_catalog.Arch`), defaults to None
        :type arch: Arch, optional
        :param os_type: name of os that this :py:class:`~Pegasus.dax4.transformation_catalog.Transformation` was compiled for (defined in :py:class:`~Pegasus.dax4.site_catalog.OSType`), defaults to None
        :type os_type: OSType, optional
        :param os_release: release of os that this :py:class:`~Pegasus.dax4.transformation_catalog.Transformation` was compiled for, defaults to None, defaults to None
        :type os_release: str, optional
        :param os_version: version of os that this :py:class:`~Pegasus.dax4.transformation_catalog.Transformation` was compiled for, defaults to None, defaults to None
        :type os_version: str, optional
        :param glibc: version of glibc this :py:class:`~Pegasus.dax4.transformation_catalog.Transformation` was compiled against, defaults to None
        :type glibc: str, optional
        :param container: specify the name of the container to use, optional
        :type container: str 
        :raises ValueError: transformation_type must be one of :py:class:`~Pegasus.dax4.transformation_catalog.TransformationType`
        :raises ValueError: arch must be one of :py:class:`~Pegasus.dax4.site_catalog.Arch`
        :raises DuplicateError: a site with this is already associated to this :py:class:`~Pegasus.dax4.transformation_catalog.Transformation`
        :return: self
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

        self.sites[name] = _TransformationSite(
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
        """Check if a site with the given name has been added for this 
        :py:class:`~Pegasus.dax4.transformation_catalog.Transformation`
        
        :param name: site name
        :type name: str
        :return: True if site has been added, else False
        :rtype: bool
        """
        return name in self.sites

    def remove_site(self, name):
        """Remove the given site from this :py:class:`~Pegasus.dax4.transformation_catalog.Transformation`
        
        :param name: name of site to be removed
        :type name: str
        :raises NotFoundError: the site has not been added for this :py:class:`~Pegasus.dax4.transformation_catalog.Transformation`
        :return: self
        """
        if name not in self.sites:
            raise NotFoundError(
                "Site {0} not found for transformation {1}".format(name, self.name)
            )

        del self.sites[name]

        return self

    def add_site_profile(self, site_name, namespace, key, value):
        """Add a profile to a transformation site with the given site name
        
        :param site_name: the name of the site to which the profile is to be added
        :type site_name: str
        :param namespace: a namespace defined in :py:class:`~Pegasus.dax4.mixins.Namespace`
        :type namespace: Namespace
        :param key: key
        :type key: str
        :param value: value
        :type value: str
        :raises NotFoundError: the given site_name was not found
        :return: self
        """
        if site_name not in self.sites:
            raise NotFoundError(
                "Site {0} not found for transformation {1}".format(site_name, self.name)
            )

        self.sites[site_name].add_profile(namespace, key, value)

        return self

    def add_site_metadata(self, site_name, key, value):
        """Add metadata to a transformation site with the given site name
        
        :param site_name: the name of the site to which the metadata is to be added
        :type site_name: str
        :param key: key
        :type key: str
        :param value: value
        :type value: str
        :raises NotFoundError: the given site_name was not found
        :return: self
        """
        if site_name not in self.sites:
            raise NotFoundError(
                "Site {0} not found for transformation {1}".format(site_name, self.name)
            )

        self.sites[site_name].add_metadata(key, value)

        return self

    def add_requirement(self, required_transformation, namespace=None, version=None):
        """Add a requirement to this Transformation. Specify the other
        transformation, identified by name, namespace, and version, that this 
        transformation depends upon. If a :py:class:`~Pegasus.dax4.transformation_catalog.Transformation`
        is passed in for *required_transformation*, then namespace and version
        are ignored. 
        
        :param required_transformation: transformation that this transformation requires
        :type required_transformation: str or Transformation
        :raises DuplicateError: this requirement already exists
        :raises ValueError: required_transformation must be of type :py:class:`~Pegasus.dax4.transformation_catalog.Transformation` or str
        :return: self
        """
        if isinstance(required_transformation, Transformation):
            key = required_transformation._get_key()
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
        """Check if this :py:class:`~Pegasus.dax4.transformation_catalog.Transformation` 
        requires the given transformation. If a :py:class:`~Pegasus.dax4.transformation_catalog.Transformation`
        is passed in for *required_transformation*, then namespace and version
        are ignored. 

        :param transformation: the Transformation to check for 
        :type transformation: Transformation or str        
        :raises ValueError: required_transformation must be of type :py:class:`~Pegasus.dax4.transformation_catalog.Transformation` or str
        :return: whether or not this transformation requires the given transformation
        :rtype: bool
        """
        if isinstance(transformation, Transformation):
            key = transformation._get_key()
        elif isinstance(transformation, str):
            key = (transformation, namespace, version)
        else:
            raise ValueError(
                "required_transformation must be of type Transformation or str"
            )

        return key in self.requires

    def remove_requirement(self, transformation, namespace=None, version=None):
        """Remove a requirement from this :py:class:`~Pegasus.dax4.transformation_catalog.Transformation`.
        If a :py:class:`~Pegasus.dax4.transformation_catalog.Transformation`
        is passed in for *required_transformation*, then namespace and version
        are ignored. 
        
        :param transformation: the :py:class:`~Pegasus.dax4.transformation_catalog.Transformation` to be removed from the list of requirements
        :type transformation: Transformation
        :raises NotFoundError: this requirement does not exist
        :raises ValueError: required_transformation must be of type :py:class:`~Pegasus.dax4.transformation_catalog.Transformation` or str
        :return: self
        """

        if isinstance(transformation, Transformation):
            key = transformation._get_key()
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
        return _filter_out_nones(
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
        return hash(self._get_key())

    def __eq__(self, other):
        if isinstance(other, Transformation):
            return self._get_key() == other._get_key()
        raise ValueError(
            "Transformation cannot be compared with {0}".format(type(other))
        )


class TransformationCatalog(Writable):
    """Maintains a list a :py:class:`~Pegasus.dax4.transformation_catalog.Transformations`, site specific
    transformation information, and a list of containers

    .. code-block:: python

        # Example
        foo = (Transformation("foo")
                .add_site("local", "/nfs/u2/ryan/bin/foo", TransformationType.STAGEABLE, arch=Arch.X86_64, ostype=OSType.LINUX)
                .add_site_profile("local", Namespace.ENV, "JAVA_HOME", "/usr/bin/java")
                .add_site_metadata("local", "size", 2048)
                .add_requirement("bar"))

        bar = (Transformation("bar")
                .add_site("local", "/nfs/u2/ryan/bin/bar", TransformationType.STAGEABLE, arch=Arch.X86_64, ostype=OSType.LINUX))

        tc = (TransformationCatalog()
                .add_transformation(foo)
                .add_transformation(bar)
                .add_container("centos-pegasus", ContainerType.DOCKER, "docker:///rynge/centos-pegasus:latest", mounts=["/Volumes/Work/lfs1:/shared-data/:ro"]))
    """

    def __init__(self):
        self.transformations = dict()
        self.containers = dict()

    def add_transformation(self, *transformations):
        """Add one or more :py:class:`~Pegasus.dax4.transformation_catalog.Transformations` to this catalog
        
        :param transformations: the :py:class:`~Pegasus.dax4.transformation_catalog.Transformations` to be added
        :raises ValueError: argument(s) must be of type :py:class:`~Pegasus.dax4.transformation_catalog.Transformations`
        :raises DuplicateError: Transformation already exists in this catalog
        :return: self 
        """
        for tr in transformations:
            if not isinstance(tr, Transformation):
                raise ValueError("input must be of type Transformation")

            if self.has_transformation(tr):
                raise DuplicateError("transformation already exists in catalog")

            self.transformations[tr._get_key()] = tr

        return self

    def has_transformation(self, transformation, namespace=None, version=None):
        """Check if this catalog contains the given :py:class:`~Pegasus.dax4.transformation_catalog.Transformation`.
        If a :py:class:`~Pegasus.dax4.transformation_catalog.Transformation`
        is passed in for *required_transformation*, then namespace and version
        are ignored. 

        :param transformation: the :py:class:`~Pegasus.dax4.transformation_catalog.Transformations` to check for 
        :type transformation: Transformation
        :return: whether or not the given :py:class:`~Pegasus.dax4.transformation_catalog.Transformations` exists in this catalog
        :raises ValueError: required_transformation must be of type :py:class:`~Pegasus.dax4.transformation_catalog.Transformation` or str
        :rtype: bool
        """
        if isinstance(transformation, Transformation):
            key = transformation._get_key()
        elif isinstance(transformation, str):
            key = (transformation, namespace, version)
        else:
            raise ValueError(
                "required_transformation must be of type Transformation or str"
            )
        return key in self.transformations

    def add_container(self, container):
        """Add a :py:class:`~Pegasus.dax4.transformation_catalog.Container` to this catalog
        
        :param container: the :py:class:`~Pegasus.dax4.transformation_catalog.Container` to be added
        :type container: Container
        :raises ValueError: container must be of type :py:class:`~Pegasus.dax4.transformation_catalog.Container`
        :raises DuplicateError: a container with the same name already exists in this catalog
        :return: self
        """

        if not isinstance(container, Container):
            raise ValueError("container must be of type Container")

        if self.has_container(container.name):
            raise DuplicateError("Container {0} already exists".format(container.name))

        self.containers[container.name] = container

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
        """Remove a conatiner with the given name from this catalog
        
        :param name: container name
        :type name: str
        :raises NotFoundError: the Container with the given name does not exist in this catalog
        :return: self
        """
        if not self.has_container(name):
            raise NotFoundError(
                "Container {0} does not exist in this catalog".format(name)
            )

        del self.containers[name]

        return self

    def __json__(self):
        return _filter_out_nones(
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
