from collections import OrderedDict, defaultdict
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional, Union

from .errors import DuplicateError
from .mixins import HookMixin, MetadataMixin, ProfileMixin
from .site_catalog import OS, Arch
from .writable import Writable, _filter_out_nones

from Pegasus.api._utils import _chained, _get_class_enum_member_str, _get_enum_str

PEGASUS_VERSION = "5.0.4"

__all__ = [
    "Container",
    "Transformation",
    "TransformationSite",
    "TransformationCatalog",
]


class _ContainerType(Enum):
    """Container types recognized by Pegasus"""

    DOCKER = "docker"
    SINGULARITY = "singularity"
    SHIFTER = "shifter"


class Container(ProfileMixin):
    """
    Describes a container that can be added to the :py:class:`~Pegasus.api.transformation_catalog.TransformationCatalog` .
    Note that the :code:`checksum` parameter refers to the checksum of the tar file of the image and not the specific
    version of an image (digest/content-addressable identifer in the case of Docker).

    .. code-block:: python

        # Example 1: Docker
        centos_pegasus = Container(
                            "centos-pegasus",
                            Container.DOCKER,
                            "docker:///ryan/centos-pegasus:latest",
                            arguments="--shm-size",
                            mounts=["/Volumes/Work/lfs1:/shared-data/:ro"]
                        )

        # Example 2: Singularity
        fb_nlp = Container(
                    "fb-nlp",
                    Container.SINGULARITY,
                    image="library://papajim/default/fb-nlp",
                    mounts=["/data:/mnt:ro"]
                )

    """

    DOCKER = _ContainerType.DOCKER
    SINGULARITY = _ContainerType.SINGULARITY
    SHIFTER = _ContainerType.SHIFTER

    _SUPPORTED_CHECKSUMS = {"sha256"}

    def __init__(
        self,
        name: str,
        container_type: _ContainerType,
        image: str,
        arguments: Optional[str] = None,
        mounts: Optional[List[str]] = None,
        image_site: Optional[str] = None,
        checksum: Optional[Dict[str, str]] = None,
        metadata: Optional[Dict[str, Union[int, str, float]]] = None,
        bypass_staging: bool = False,
    ):
        """
        :param name: name of this container
        :type name: str
        :param container_type: a container type defined in :py:class:`~Pegasus.api.transformation_catalog.Container`
        :type container_type: _ContainerType
        :param image: image, such as :code:`docker:///rynge/montage:latest`
        :type image: str
        :param arguments: additional cli arguments to be added to the :code:`docker container run` or :code:`singularity exec` commands when starting this container
        :type arguments: Optional[str]
        :param mounts: list of mount strings such as :code:`['/Volumes/Work/lfs1:/shared-data/:ro', ...]`
        :type mounts: Optional[List[str]]
        :param image_site: optional site attribute to tell pegasus which site tar file exists, defaults to None
        :type image_site: Optional[str]
        :param checksum: Dict containing checksums for the tar file of this image. Currently only sha256 is supported. This should be entered as :code:`{"sha256": <value>}`, defaults to None
        :type checksum: Optional[Dict[str, str]]
        :param metadata: Dict containing metadata key, value pairs associated with this container, defaults to None
        :type metadata: Optional[Dict[str, Union[int, str, float]]]
        :param bypass_staging: whether or not to bypass the stage in job for this container, defaults to False
        :type bypass_staging: bool, optional
        :raises ValueError: container_type must be one of :py:class:`~Pegasus.api.transformation_catalog._ContainerType` (:code:`Container.DOCKER` | :code:`Container.SINGULARITY` | :code:`Container.SHIFTER`)
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

        # ensure supported checksum type given
        if checksum and len(checksum) > 0:
            for checksum_type in checksum:
                if checksum_type.lower() not in Container._SUPPORTED_CHECKSUMS:
                    raise ValueError(
                        "invalid checksum: {}, supported checksum types are: {}".format(
                            checksum_type, Container._SUPPORTED_CHECKSUMS
                        )
                    )
        self.checksum = checksum

        # TODO: remove once this is supported
        if metadata:
            raise NotImplementedError(
                "Metadata support for Containers is not yet supported"
            )
        self.metadata = metadata

        self.profiles = defaultdict(OrderedDict)

        # add additional arguments if given (this is not part of the schema
        # and must be added to profiles)
        if arguments:
            self.add_pegasus_profile(container_arguments=arguments)

        self.bypass = None
        if bypass_staging:
            self.bypass = bypass_staging

    def __json__(self):
        return _filter_out_nones(
            OrderedDict(
                [
                    ("name", self.name),
                    ("type", self.container_type),
                    ("image", self.image),
                    ("mounts", self.mounts),
                    ("bypass", self.bypass),
                    ("image.site", self.image_site),
                    ("checksum", self.checksum),
                    ("metadata", self.metadata),
                    (
                        "profiles",
                        OrderedDict(sorted(self.profiles.items(), key=lambda _: _[0]))
                        if len(self.profiles) > 0
                        else None,
                    ),
                ]
            )
        )


class TransformationSite(ProfileMixin, MetadataMixin):
    """Site specific information about a :py:class:`~Pegasus.api.transformation_catalog.Transformation`.
    A :py:class:`~Pegasus.api.transformation_catalog.Transformation` must contain at least one
    transformation site.
    """

    def __init__(
        self,
        name: str,
        pfn: Union[str, Path],
        is_stageable: bool = False,
        bypass_staging: bool = False,
        arch: Optional[Arch] = None,
        os_type: Optional[OS] = None,
        os_release: Optional[str] = None,
        os_version: Optional[str] = None,
        container: Optional[Union[Container, str]] = None,
    ):
        """
        :param name: name of the site at which this :py:class:`~Pegasus.api.transformation_catalog.Transformation` resides
        :type name: str
        :param pfn: physical file name, an absolute path given as a str or Path
        :type pfn: Union[str, Path]
        :param is_stageable: whether or not this transformation is stageable or installed, defaults to False
        :type type: bool, optional
        :param bypass_staging: whether or not to bypass the stage in job of this executable (Note that this only works for transformations where :code:`is_stageable=True`), defaults to False
        :type bypass_staging: bool, optional
        :param arch: architecture that this :py:class:`~Pegasus.api.transformation_catalog.Transformation` was compiled for (defined in :py:class:`~Pegasus.api.site_catalog.Arch`), defaults to None
        :type arch: Optional[Arch], optional
        :param os_type: name of os that this :py:class:`~Pegasus.api.transformation_catalog.Transformation` was compiled for (defined in :py:class:`~Pegasus.api.site_catalog.OS`), defaults to None
        :type os_type: Optional[OS], optional
        :param os_release: release of os that this :py:class:`~Pegasus.api.transformation_catalog.Transformation` was compiled for, defaults to None, defaults to None
        :type os_release: Optional[str], optional
        :param os_version: version of os that this :py:class:`~Pegasus.api.transformation_catalog.Transformation` was compiled for, defaults to None, defaults to None
        :type os_version: Optional[str], optional
        :param container: specify the name of the container or Container object to use, optional
        :type container: Optional[Union[Container, str]], optional
        :raises TypeError: arch must be one of :py:class:`~Pegasus.api.site_catalog.Arch`
        :raises TypeError: os_type must be one of :py:class:`~Pegasus.api.site_catalog.OS`
        :raises ValueError: pfn must be given as an absolute path when :code:`pathlib.Path` is used
        :raises ValueError: :code:`bypass_staging=True` can only be used when :code:`is_stageable=True`
        """

        self.name = name

        if isinstance(pfn, Path):
            if not pfn.is_absolute():
                raise ValueError(
                    "invalid pfn: {}, the given pfn must be an absolute path".format(
                        str(pfn)
                    )
                )

            pfn = str(pfn)

        self.pfn = pfn
        self.transformation_type = "stageable" if is_stageable else "installed"

        self.bypass = None
        if bypass_staging:
            if not is_stageable:
                raise ValueError(
                    "bypass_staging can only be used when is_stageable is set to True"
                )

            self.bypass = bypass_staging

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

        self.profiles = defaultdict(OrderedDict)
        self.metadata = OrderedDict()

    def __json__(self):
        return _filter_out_nones(
            OrderedDict(
                [
                    ("name", self.name),
                    ("pfn", self.pfn),
                    ("type", self.transformation_type),
                    ("bypass", self.bypass),
                    ("arch", self.arch),
                    ("os.type", self.os_type),
                    ("os.release", self.os_release),
                    ("os.version", self.os_version),
                    ("container", self.container),
                    (
                        "profiles",
                        OrderedDict(sorted(self.profiles.items(), key=lambda _: _[0]))
                        if len(self.profiles) > 0
                        else None,
                    ),
                    (
                        "metadata",
                        dict(self.metadata) if len(self.metadata) > 0 else None,
                    ),
                ]
            )
        )


class Transformation(ProfileMixin, HookMixin, MetadataMixin):
    """A transformation, which can be a standalone executable, or one that
    requires other executables.
    """

    _SUPPORTED_CHECKSUMS = {"sha256"}

    def __init__(
        self,
        name: str,
        namespace: Optional[str] = None,
        version: Optional[str] = None,
        site: Optional[str] = None,
        pfn: Optional[Union[str, Path]] = None,
        is_stageable: bool = False,
        bypass_staging: bool = False,
        arch: Optional[Arch] = None,
        os_type: Optional[OS] = None,
        os_release: Optional[str] = None,
        os_version: Optional[str] = None,
        container: Optional[Union[Container, str]] = None,
        checksum: Optional[Dict[str, str]] = None,
    ):
        """
        When a transformation resides on a single site, the
        syntax in Example 1 can be used where the args: site and pfn are
        provided to the constructor. If site and pfn are specified, then
        the args: is_stageable, bypass_staging, arch, os_type, os_release, os_version, and container, are
        applied to the site, else they are ignored. When a transformation resides
        multiple sites, the syntax in Example 2 can be used where multiple
        TransformationSite objects can be added. Note that when specifying a checksum
        such as :code:`{"sha256": <value>}` , this only applies stageable executables.

        .. code-block:: python

            # Example 1: transformation that resides on a single site
            preprocess = Transformation(
                    "preprocess",
                    namespace="pegasus",
                    version="4.0",
                    site="condorpool",
                    pfn="/usr/bin/pegasus-keg",
                    is_stageable=False,
                    bypass_staging=False,
                    arch=Arch.X86_64,
                    os_type=OS.LINUX,
                    container=centos_pegasus
                )

            # Example 2: transformation that resides on multiple sites
            preprocess = Transformation("preprocess", namespace="pegasus", version="4.0")\\
                            .add_sites(
                                TransformationSite(
                                    "condorpool",
                                    "/usr/bin/pegasus-keg",
                                    is_stageable=False,
                                    arch=Arch.X86_64,
                                    os_type=OS.LINUX,
                                    container="centos-pegasus"
                                ),
                                ...
                            )


        :param name: the logical name of the transformation
        :type name: str
        :param namespace: the namespace that this transformation belongs to, defaults to None
        :type namespace: Optional[str]
        :param version: the version of this transformation (e.g. :code:`"1.1"`), defaults to None
        :type version: Optional[str]
        :param site: a :py:class:`~Pegasus.api.site_catalog.Site` specified in the :py:class:`~Pegasus.api.site_catalog.SiteCatalog` on which this transformation resides, defaults to None
        :type site: Optional[str]
        :param pfn: the physical filename of this transformation (e.g. :code:`"/usr/bin/tar"`), defaults to None
        :type pfn: Optional[Union[str, Path]]
        :param is_stageable: whether or not this transformation is stageable or installed, defaults to False
        :type type: bool, optional
        :param bypass_staging: whether or not to bypass the stage in job of this executable (Note that this only works for transformations where :code:`is_stageable=True`), defaults to False
        :type bypass_staging: bool, optional
        :param arch: architecture that this transformation was compiled for (defined in :py:class:`~Pegasus.api.site_catalog.Arch` , e.g :code:`Arch.X86_64`), defaults to None
        :type arch: Optional[Arch]
        :param os_type: name of os that this transformation was compiled for (defined in :py:class:`~Pegasus.api.site_catalog.OS` , e.g. :code:`OS.LINUX`), defaults to None
        :type os_type: Optional[OS]
        :param os_release: release of os that this transformation was compiled for, defaults to None
        :type os_release: Optional[str]
        :param os_version: version of os that this transformation was compiled for, defaults to None
        :type os_version: Optional[str]
        :param container: a :py:class:`~Pegasus.api.transformation_catalog.Container` or name of the container to be used for this transformation, defaults to None
        :type container: Optional[Union[Container, str]]
        :param checksum: Dict containing checksums for this file. Currently only sha256 is given. This should be entered as :code:`{"sha256": <value>}`, defaults to None
        :type checksum: Optional[Dict[str, str]]
        :raises TypeError: container must be of type :py:class:`~Pegasus.api.transformation_catalog.Container` or str
        :raises TypeError: arch must be one of :py:class:`~Pegasus.api.site_catalog.Arch`
        :raises TypeError: os_type must be one of :py:class:`~Pegasus.api.site_catalog.OS`
        :raises ValueError: fields: namespace, name, and field must not contain any :code:`:` (colons)
        :raises ValueError: pfn must be given as an absolute path when :code:`pathlib.Path` is used
        :raises ValueError: :code:`bypass_staging=True` can only be used when :code:`is_stageable=True`
        """

        for field, value in {
            "name": name,
            "namespace": namespace,
            "version": version,
        }.items():
            if ":" in str(value):
                raise ValueError(
                    "invalid {field}: {value}; {field} must not contain ':' (colon) characters".format(
                        field=field, value=value
                    )
                )

        self.name = name
        self.namespace = namespace
        self.version = version

        self.sites = OrderedDict()
        self.requires = set()

        self.hooks = defaultdict(list)
        self.profiles = defaultdict(OrderedDict)
        self.metadata = OrderedDict()

        # add site if site if given
        if site is not None and pfn is not None:
            self.add_sites(
                TransformationSite(
                    site,
                    pfn,
                    is_stageable,
                    bypass_staging=bypass_staging,
                    arch=arch,
                    os_type=os_type,
                    os_release=os_release,
                    os_version=os_version,
                    container=container,
                )
            )

        # ensure supported checksum type given
        if checksum and len(checksum) > 0:
            for checksum_type in checksum:
                if checksum_type.lower() not in Transformation._SUPPORTED_CHECKSUMS:
                    raise ValueError(
                        "invalid checksum: {}, supported checksum types are: {}".format(
                            checksum_type, Transformation._SUPPORTED_CHECKSUMS
                        )
                    )
        self.checksum = checksum

    def _get_key(self):
        return "{}::{}::{}".format(self.namespace, self.name, self.version)

    @_chained
    def add_sites(self, *transformation_sites: TransformationSite):
        """
        add_sites(self, *transformation_sites: TransformationSite)
        Add one or more :py:class:`~Pegasus.api.transformation_catalog.TransformationSite` s to this
        transformation.

        :param transformation_sites: the :py:class:`~Pegasus.api.transformation_catalog.TransformationSite` (s) to be added
        :raises TypeError: argument(s) must be of type :py:class:`~Pegasus.api.transformation_catalog.TransformationSite`
        :raises DuplicateError: a :py:class:`~Pegasus.api.transformation_catalog.TransformationSite` with the same name as the one you are attempting to add already exists
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
    def add_requirement(
        self,
        required_transformation: Union[str, "Transformation"],
        namespace: str = None,
        version: str = None,
    ):
        """
        add_requirement(self, required_transformation: Union[str, Transformation], namespace: str = None, version: str = None)
        Add a requirement to this transformation. Specify the other
        transformation, identified by name, namespace, and version, that this
        transformation depends upon. If a :py:class:`~Pegasus.api.transformation_catalog.Transformation`
        is passed in for :code:`required_transformation`, then namespace and version
        are ignored.

        :param required_transformation: :py:class:`~Pegasus.api.transformation_catalog.Transformation` that this transformation requires
        :type required_transformation: str or Transformation
        :raises DuplicateError: this requirement already exists
        :raises ValueError: :code:`required_transformation` must be of type :py:class:`~Pegasus.api.transformation_catalog.Transformation` or str
        :raises ValueError: namespace, required transformation name, and version cannot contain any :code:`:` (colon) characters
        :raises TypeError: required_transformation must be one of type str or :py:class:`~Pegasus.api.transformation_catalog.Transformation`
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
                        "invalid {field}: {value}; {field} must not contain `:` characters".format(
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
            OrderedDict(
                [
                    ("namespace", self.namespace),
                    ("name", self.name),
                    ("version", self.version),
                    (
                        "requires",
                        [req for req in self.requires]
                        if len(self.requires) > 0
                        else None,
                    ),
                    ("sites", [site for _, site in self.sites.items()]),
                    (
                        "profiles",
                        OrderedDict(sorted(self.profiles.items(), key=lambda _: _[0]))
                        if len(self.profiles) > 0
                        else None,
                    ),
                    (
                        "hooks",
                        OrderedDict(
                            [
                                (hook_name, [hook for hook in values])
                                for hook_name, values in self.hooks.items()
                            ]
                        )
                        if len(self.hooks) > 0
                        else None,
                    ),
                    ("metadata", self.metadata if len(self.metadata) > 0 else None),
                    ("checksum", self.checksum),
                ]
            )
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
    """Maintains a list a :py:class:`~Pegasus.api.transformation_catalog.Transformation` s and
    :py:class:~`Pegasus.api.transformation_catalog.Container` s

    .. code-block:: python

        # Example
        centos_pegasus = Container(
                "centos-pegasus",
                Container.DOCKER,
                "docker:///ryan/centos-pegasus:latest",
                mounts=["/Volumes/Work/lfs1:/shared-data/:ro"]
            )

        preprocess = Transformation(
                        "preprocess",
                        site="condorpool",
                        pfn="/usr/bin/pegasus-keg",
                        is_stageable=False,
                        arch=Arch.X86_64,
                        os_type=OS.LINUX,
                        container=centos_pegasus
                    )

        findrange = Transformation(
                        "findrange",
                        site="condorpool",
                        pfn="/usr/bin/pegasus-keg",
                        is_stageable=False,
                        arch=Arch.X86_64,
                        os_type=OS.LINUX,
                        container=centos_pegasus
                    )

        analyze = Transformation(
                        "analyze",
                        site="condorpool",
                        pfn="/usr/bin/pegasus-keg",
                        is_stageable=False,
                        arch=Arch.X86_64,
                        os_type=OS.LINUX,
                        container=centos_pegasus
                    )

        tc = TransformationCatalog()\\
            .add_containers(centos_pegasus)\\
            .add_transformations(preprocess, findrange, analyze)\\
            .write()

    """

    _DEFAULT_FILENAME = "transformations.yml"

    def __init__(self):
        Writable.__init__(self)

        self.transformations = OrderedDict()
        self.containers = OrderedDict()

    @_chained
    def add_transformations(self, *transformations: Transformation):
        """
        add_transformations(self, *transformations: Transformation)
        Add one or more :py:class:`~Pegasus.api.transformation_catalog.Transformation` s to this catalog

        .. code-block:: python

            # Example
            tc.add_transformations(
                Transformation(
                        "analyze",
                        site="condorpool",
                        pfn="/usr/bin/pegasus-keg",
                        is_stageable=False,
                        arch=Arch.X86_64,
                        os_type=OS.LINUX,
                        container=centos_pegasus
                    )
            )


        :param transformations: the :py:class:`~Pegasus.api.transformation_catalog.Transformation` (s) to be added
        :raises TypeError: argument(s) must be of type :py:class:`~Pegasus.api.transformation_catalog.Transformation`
        :raises DuplicateError: the given :py:class:`~Pegasus.api.transformation_catalog.Transformation` already exists in this catalog
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
    def add_containers(self, *containers: Container):
        """
        add_containers(self, *containers: Container)
        Add one or more :py:class:`~Pegasus.api.transformation_catalog.Container` s to this catalog

        .. code-block:: python

            # Example
            tc.add_containers(
                Container(
                    "centos-pegasus",
                    Container.DOCKER,
                    "docker:///ryan/centos-pegasus:latest",
                    mounts=["/Volumes/Work/lfs1:/shared-data/:ro"]
                )
            )

        :param containers: the :py:class:`~Pegasus.api.transformation_catalog.Container` to be added
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
