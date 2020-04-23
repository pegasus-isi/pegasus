from collections import OrderedDict, defaultdict
from enum import Enum

from .errors import DuplicateError
from .mixins import HookMixin, MetadataMixin, ProfileMixin
from .site_catalog import OS, Arch
from .writable import Writable, _filter_out_nones

from Pegasus.api._utils import _chained, _get_class_enum_member_str, _get_enum_str

PEGASUS_VERSION = "5.0"

__all__ = [
    "Transformation",
    "TransformationSite",
    "TransformationCatalog",
]


class TransformationSite(ProfileMixin, MetadataMixin):
    """Site specific information about a :py:class:`~Pegasus.api.transformation_catalog.Transformation`.
    A :py:class:`~Pegasus.api.transformation_catalog.Transformation` must contain at least one
    TransformationSite.
    """

    def __init__(
        self,
        name,
        pfn,
        is_stageable,
        arch=None,
        os_type=None,
        os_release=None,
        os_version=None,
        glibc=None,
        container=None,
    ):
        """
        :param name: name of the site at which this :py:class:`~Pegasus.api.transformation_catalog.Transformation` resides
        :type name: str
        :param pfn: physical file name
        :type pfn: str
        :param is_stageable: whether or not this transformation is stageable or installed
        :type type: bool
        :param arch: architecture that this :py:class:`~Pegasus.api.transformation_catalog.Transformation` was compiled for (defined in :py:class:`~Pegasus.api.site_catalog.Arch`), defaults to None
        :type arch: Arch, optional
        :param os_type: name of os that this :py:class:`~Pegasus.api.transformation_catalog.Transformation` was compiled for (defined in :py:class:`~Pegasus.api.site_catalog.OS`), defaults to None
        :type os_type: OS, optional
        :param os_release: release of os that this :py:class:`~Pegasus.api.transformation_catalog.Transformation` was compiled for, defaults to None, defaults to None
        :type os_release: str, optional
        :param os_version: version of os that this :py:class:`~Pegasus.api.transformation_catalog.Transformation` was compiled for, defaults to None, defaults to None
        :type os_version: str, optional
        :param glibc: version of glibc this :py:class:`~Pegasus.api.transformation_catalog.Transformation` was compiled against, defaults to None
        :type glibc: str, optional
        :param container: specify the name of the container to use, optional
        :type container: str
        :raises ValueError: arch must be one of :py:class:`~Pegasus.api.site_catalog.Arch`
        :raises ValueError: os_type must be one of :py:class:`~Pegasus.api.site_catalog.OS`
        """

        self.name = name
        self.pfn = pfn
        self.transformation_type = "stageable" if is_stageable else "installed"

        if arch is not None:
            if not isinstance(arch, Arch):
                raise TypeError(
                    "invalid arch: {arch}; arch must be one of {enum_str}".format(
                        arch=arch, enum_str=_get_enum_str(Arch)
                    )
                )
            else:
                self.arch = arch.value
        else:
            self.arch = None

        if os_type is not None:
            if not isinstance(os_type, OS):
                raise TypeError(
                    "invalid os_type: {os_type}; os_type must be one of {enum_str}".format(
                        os_type=os_type, enum_str=_get_enum_str(OS)
                    )
                )
            else:
                self.os_type = os_type.value
        else:
            self.os_type = None

        self.os_release = os_release
        self.os_version = os_version
        self.glibc = glibc

        container_name = None
        if container:
            if isinstance(container, Container):
                container_name = container.name
            elif isinstance(container, str):
                container_name = container
            else:
                raise TypeError(
                    "invalid container: {container}; container must be of type Container or str (the container name)".format(
                        container=container
                    )
                )
        self.container = container_name

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


class _ContainerType(Enum):
    """Container types recognized by Pegasus"""

    DOCKER = "docker"
    SINGULARITY = "singularity"
    SHIFTER = "shifter"


class Container(ProfileMixin):
    """Describes a container that can be added to the :py:class:`~Pegasus.api.transformation_catalog.TransformationCatalog`

    .. code-block:: python

        # Example
        c = Container("centos-pegasus", Container.DOCKER, "docker:///ryan/centos-pegasus:latest", ["/Volumes/Work/lfs1:/shared-data/:ro"])

    """

    DOCKER = _ContainerType.DOCKER
    SINGULARITY = _ContainerType.SINGULARITY
    SHIFTER = _ContainerType.SHIFTER

    def __init__(self, name, container_type, image, mounts=None, image_site=None):
        """
        :param name: name of this container
        :type name: str
        :param container_type: a container type defined in :py:class:`~Pegasus.api.transformation_catalog.Container`
        :type container_type: _ContainerType
        :param image: image, such as 'docker:///rynge/montage:latest'
        :type image: str
        :param mounts: list of mount strings such as ['/Volumes/Work/lfs1:/shared-data/:ro', ...]
        :type mounts: list
        :param image_site: optional site attribute to tell pegasus which site tar file exists, defaults to None
        :type image_site: str, optional
        :raises ValueError: container_type must be one of :py:class:`~Pegasus.api.transformation_catalog._ContainerType`
        """
        self.name = name

        if not isinstance(container_type, _ContainerType):
            raise TypeError(
                "invalid container_type: {container_type}; container_type must be one of {enum_str}".format(
                    container_type=container_type,
                    enum_str=_get_class_enum_member_str(Container, _ContainerType),
                )
            )

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
    requires other executables. 
    """

    def __init__(
        self,
        name,
        namespace=None,
        version=None,
        site=None,
        pfn=None,
        is_stageable=None,
        arch=None,
        os_type=None,
        os_release=None,
        os_version=None,
        glibc=None,
        container=None,
    ):
        """
        When a transformation resides on a single site, the
        syntax in Example 1 can be used where the args: site, pfn, and is_stageable is
        provided to the constructor. If site, pfn, and is_stageable is specified, then
        the args: arch, os_type, os_release, os_version, glibc, and container, are
        applied to the site, else they are ignored. When a transformation resides
        multiple sites, the syntax in Example 2 can be used where multiple
        TransformationSite objects can be added.

        .. code-block:: python

            # Example 1: transformation that resides on a single site
            preprocess = Transformation(
                    "preprocess",
                    namespace="pegasus",
                    version="4.0",
                    site=CONDOR_POOL,
                    pfn=PEGASUS_LOCATION,
                    is_stageable=False
                )

            # Example 2: transformation that resides on multiple sites
            preprocess = (Transformation("preprocess", namespace="pegasus", version="4.0")
                            .add_sites(
                                TransformationSite(
                                    CONDOR_POOL,
                                    PEGASUS_LOCATION,
                                    is_stageable=False,
                                    arch=Arch.X86_64,
                                    os_type=OS.LINUX),

                                ...
                            ))


        :param name: the logical name of the transformation
        :type name: str
        :param namespace: the namespace that this transformation belongs to, defaults to None
        :type namespace: str, optional
        :param version: the version of this transformation (e.g. "1.1"), defaults to None
        :type version: str, optional
        :param site: a site specified in the site catalog to on which this transformation resides, defaults to None
        :type site: str, optional
        :param pfn: the physical filename of this transformation (e.g. "/usr/bin/tar"), defaults to None
        :type pfn: str, optional
        :param is_stageable: whether or not this transformation is stageable or installed
        :type type: bool
        :param arch: architecture that this :py:class:`~Pegasus.api.transformation_catalog.Transformation` was compiled for (defined in :py:class:`~Pegasus.api.site_catalog.Arch`), defaults to None
        :type arch: Arch, optional
        :param os_type: name of os that this :py:class:`~Pegasus.api.transformation_catalog.Transformation` was compiled for (defined in :py:class:`~Pegasus.api.site_catalog.OS`), defaults to None
        :type os_type: OS, optional
        :param os_release: release of os that this :py:class:`~Pegasus.api.transformation_catalog.Transformation` was compiled for, defaults to None, defaults to None
        :type os_release: str, optional
        :param os_version: version of os that this :py:class:`~Pegasus.api.transformation_catalog.Transformation` was compiled for, defaults to None, defaults to None
        :type os_version: str, optional
        :param glibc: version of glibc this :py:class:`~Pegasus.api.transformation_catalog.Transformation` was compiled against, defaults to None
        :type glibc: str, optional
        :param container: a container object or name of the container to be used for this transformation, optional
        :type container: Container or str
        :raises TypeError: Container must be of type Container or str
        :raises ValueError: arch must be one of :py:class:`~Pegasus.api.site_catalog.Arch`
        :raises ValueError: os_type must be one of :py:class:`~Pegasus.api.site_catalog.OS`
        :raises ValueError: fields: namespace, name, and field must not contain any ":"s
        """

        for field, value in {
            "name": name,
            "namespace": namespace,
            "version": version,
        }.items():
            if ":" in str(value):
                raise ValueError(
                    "invalid {field}: {value}; {field} must not contain ':'".format(
                        field=field, value=value
                    )
                )

        self.name = name
        self.namespace = namespace
        self.version = version

        self.sites = dict()
        self.requires = set()

        self.hooks = defaultdict(list)
        self.profiles = defaultdict(dict)
        self.metadata = dict()

        # add site if site if given
        if site is not None and pfn is not None and is_stageable is not None:
            self.add_sites(
                TransformationSite(
                    site,
                    pfn,
                    is_stageable,
                    arch=arch,
                    os_type=os_type,
                    os_release=os_release,
                    os_version=os_version,
                    glibc=glibc,
                    container=container,
                )
            )

    def _get_key(self):
        return "{}::{}::{}".format(self.namespace, self.name, self.version)

    @_chained
    def add_sites(self, *transformation_sites):
        """Add one or more :py:class:`~Pegasus.api.transformation_catalog.TransformationSite` to this
        transformation

        :param transformation_sites: the transformation site(s) to be added
        :raises TypeError: argument(s) must be of type :py:class:`~Pegasus.api.transformation_catalog.TransformationSite`
        :raises DuplicateError: a transformation site with the same name as the one you are attempting to add already exists
        :return: self
        """
        for ts in transformation_sites:
            if not isinstance(ts, TransformationSite):
                raise TypeError(
                    "invalid transformation_site: {transformation_site}; transformation_site must be of type TransformationSite".format(
                        transformation_site=ts
                    )
                )

            if ts.name in self.sites:
                raise DuplicateError(
                    "transformation site: {name} has already been added to {transformation}".format(
                        name=ts.name, transformation=self
                    )
                )

            self.sites[ts.name] = ts

    @_chained
    def add_requirement(self, required_transformation, namespace=None, version=None):
        """Add a requirement to this Transformation. Specify the other
        transformation, identified by name, namespace, and version, that this
        transformation depends upon. If a :py:class:`~Pegasus.api.transformation_catalog.Transformation`
        is passed in for *required_transformation*, then namespace and version
        are ignored.

        :param required_transformation: transformation that this transformation requires
        :type required_transformation: str or Transformation
        :raises DuplicateError: this requirement already exists
        :raises ValueError: required_transformation must be of type :py:class:`~Pegasus.api.transformation_catalog.Transformation` or str
        :raises ValueError: namespace, required transformation name, and version cannot contain any ":" characters
        :return: self
        """
        key = ""
        if isinstance(required_transformation, Transformation):
            if required_transformation.namespace:
                key += required_transformation.namespace + "::"

            key += required_transformation.name

            if required_transformation.version:
                key += ":" + required_transformation.version

        elif isinstance(required_transformation, str):
            for field, value in {
                "namespace": namespace,
                "required_transformation": required_transformation,
                "version": version,
            }.items():
                if ":" in str(value):
                    raise ValueError(
                        "invalid {field}: {value}; {field} cannot contain `:` characters".format(
                            field=field, value=value
                        )
                    )

            if namespace:
                key += namespace + "::"

            key += required_transformation

            if version:
                key += ":" + version

        else:
            raise TypeError(
                "invalid required_transformation: {required_transformation}; required_transformation must be of type Transformation or str".format(
                    required_transformation=required_transformation
                )
            )

        if key in self.requires:
            raise DuplicateError(
                "transformation: {key} already added as a required transformation to {tr}".format(
                    key=key, tr=self
                )
            )

        self.requires.add(key)

    def __json__(self):
        return _filter_out_nones(
            {
                "namespace": self.namespace,
                "name": self.name,
                "version": self.version,
                "requires": [req for req in self.requires]
                if len(self.requires) > 0
                else None,
                "sites": [site for _, site in self.sites.items()],
                "profiles": dict(self.profiles) if len(self.profiles) > 0 else None,
                "hooks": {
                    hook_name: [hook for hook in values]
                    for hook_name, values in self.hooks.items()
                }
                if len(self.hooks) > 0
                else None,
                "metadata": self.metadata if len(self.metadata) > 0 else None,
            }
        )

    def __str__(self):
        return "<Transformation {}::{}:{}>".format(
            self.namespace, self.name, self.version
        )

    def __hash__(self):
        return hash(self._get_key())

    def __eq__(self, other):
        if isinstance(other, Transformation):
            return self._get_key() == other._get_key()
        raise ValueError(
            "Transformation cannot be compared with {}".format(type(other))
        )


class TransformationCatalog(Writable):
    """Maintains a list a :py:class:`~Pegasus.api.transformation_catalog.Transformations`, site specific
    transformation information, and a list of containers

    .. code-block:: python

        # Example
        preprocess = (Transformation("preprocess", namespace="pegasus", version="4.0")
                .add_sites(
                    TransformationSite(
                        CONDOR_POOL,
                        PEGASUS_LOCATION,
                        is_stageable=False,
                        arch=Arch.X86_64,
                        os_type=OS.LINUX)
                ))

        findrage = (Transformation("findrange", namespace="pegasus", version="4.0")
                        .add_sites(
                            TransformationSite(
                                CONDOR_POOL,
                                PEGASUS_LOCATION,
                                is_stageable=False,
                                arch=Arch.X86_64,
                                os_type=OS.LINUX)
                        ))

        analyze = (Transformation("analyze", namespace="pegasus", version="4.0")
                        .add_sites(
                            TransformationSite(
                                CONDOR_POOL,
                                PEGASUS_LOCATION,
                                is_stageable=False,
                                arch=Arch.X86_64,
                                os_type=OS.LINUX)
                        ))

        (TransformationCatalog()
            .add_transformations(preprocess, findrage, analyze)
            .write())

    """

    _DEFAULT_FILENAME = "transformations.yml"

    def __init__(self):
        self.transformations = dict()
        self.containers = dict()

    @_chained
    def add_transformations(self, *transformations):
        """Add one or more :py:class:`~Pegasus.api.transformation_catalog.Transformation` to this catalog

        :param transformations: the :py:class:`~Pegasus.api.transformation_catalog.Transformation` to be added
        :raises TypeError: argument(s) must be of type :py:class:`~Pegasus.api.transformation_catalog.Transformation`
        :raises DuplicateError: Transformation already exists in this catalog
        :return: self
        """
        for tr in transformations:
            if not isinstance(tr, Transformation):
                raise TypeError(
                    "invalid transformation: {tr}, transformation(s) must be of type Transformation".format(
                        tr=tr
                    )
                )

            if tr._get_key() in self.transformations:
                raise DuplicateError(
                    "transformation: {key} has already been added to this TransformationCatalog".format(
                        key=tr._get_key()
                    )
                )

            self.transformations[tr._get_key()] = tr

    @_chained
    def add_containers(self, *containers):
        """Add one or more :py:class:`~Pegasus.api.transformation_catalog.Container` to this catalog

        .. code-block:: python

            # Example
            tc.add_containers(
                Container(
                    "centos-pegasus",
                    Container.DOCKER,
                    "docker:///ryan/centos-pegasus:latest",
                    ["/Volumes/Work/lfs1:/shared-data/:ro"]
                )
            )

        :param container: the :py:class:`~Pegasus.api.transformation_catalog.Container` to be added
        :raises TypeError: argument(s) must be of type :py:class:`~Pegasus.api.transformation_catalog.Container`
        :raises DuplicateError: a container with the same name already exists in this catalog
        :return: self

        """
        for c in containers:
            if not isinstance(c, Container):
                raise TypeError(
                    "invalid container: {container}; container must be of type Container".format(
                        container=c
                    )
                )

            if c.name in self.containers:
                raise DuplicateError(
                    "container: {} has already been added to this TransformationCatalog".format(
                        c.name
                    )
                )

            self.containers[c.name] = c

    def __json__(self):
        containers = None
        if len(self.containers) > 0:
            containers = [c for _, c in self.containers.items()]

        return _filter_out_nones(
            OrderedDict(
                [
                    ("pegasus", PEGASUS_VERSION),
                    ("transformations", [t for _, t in self.transformations.items()]),
                    ("containers", containers),
                ]
            )
        )
