from collections import defaultdict
from enum import Enum

from Pegasus.api._utils import _chained, _get_class_enum_member_str, _get_enum_str
from Pegasus.api.errors import DuplicateError
from Pegasus.api.mixins import ProfileMixin
from Pegasus.api.writable import Writable, _filter_out_nones

PEGASUS_VERSION = "5.0"

__all__ = [
    "Arch",
    "OS",
    "Operation",
    "Scheduler",
    "FileServer",
    "Directory",
    "Site",
    "SiteCatalog",
]


class Arch(Enum):
    """Architecture types"""

    X86 = "x86"
    X86_64 = "x86_64"
    PPC = "ppc"
    PPC_64 = "ppc_64"
    PPC64LE = "ppc64le"
    IA64 = "ia64"
    SPARCV7 = "sparcv7"
    SPARCV9 = "sparcv9"
    AMD64 = "amd64"


class OS(Enum):
    """Operating system types"""

    LINUX = "linux"
    SUNOS = "sunos"
    AIX = "aix"
    MACOSX = "macosx"
    WINDOWS = "windows"


class Operation(Enum):
    """Different types of operations supported by a file server"""

    ALL = "all"
    PUT = "put"
    GET = "get"


class _DirectoryType(Enum):
    """Different types of directories supported for a site"""

    #: Describes a scratch file systems. Pegasus will use this to store
    #: intermediate `da`ta between jobs and other temporary files.
    SHAREDSCRATCH = "sharedScratch"

    # TODO: where is this documented? the others were in user guide
    SHAREDSTORAGE = "sharedStorage"

    #: Describes the scratch file systems available locally on a compute node.
    LOCALSCRATCH = "localScratch"

    #: Describes a long term storage file system. This is the directory
    #: Pegasus will stage output files to.
    LOCALSTORAGE = "localStorage"


class _GridType(Enum):
    """Different grid types that can be supported by Pegasus. Mirror the Condor
    grid types. http://research.cs.wisc.edu/htcondor/manual/v7.9/5_3Grid_Universe.html
    """

    GT2 = "gt2"
    GT4 = "gt4"
    GT5 = "gt5"
    CONDOR = "condor"
    CREAM = "cream"
    BATCH = "batch"
    PBS = "pbs"
    LSF = "lsf"
    SGE = "sge"
    NORDUGRID = "nordugrid"
    UNICORE = "unicore"
    EC2 = "ec2"
    DELTACLOUD = "deltacloud"


class Scheduler(Enum):
    """Different scheduler types on the Grid"""

    FORK = "fork"
    PBS = "pbs"
    LSF = "lsf"
    CONDOR = "condor"
    SGE = "sge"
    UNKNOWN = "unknown"


class FileServer(ProfileMixin):
    """Describes the fileserver to access data from outside
    """

    def __init__(self, url, operation_type):
        """
        :param url: url including protocol such as 'scp://obelix.isi.edu/data'
        :type url: str
        :param operation_type: operation type defined in :py:class:`~Pegasus.api.site_catalog.OperationType`
        :type operation_type: OperationType
        :raises ValueError: operation_type must be one defined in :py:class:`~Pegasus.api.site_catalog.OperationType`
        """
        self.url = url

        if not isinstance(operation_type, Operation):
            raise TypeError(
                "invalid operation_type: {operation_type}; operation_type must be one of {enum_str}".format(
                    operation_type=operation_type, enum_str=_get_enum_str(Operation)
                )
            )

        self.operation_type = operation_type.value

        self.profiles = defaultdict(dict)

    def __json__(self):
        return _filter_out_nones(
            {
                "url": self.url,
                "operation": self.operation_type,
                "profiles": dict(self.profiles) if len(self.profiles) > 0 else None,
            }
        )


class Directory:
    """Information about filesystems Pegasus can use for storing temporary and long-term
    files. 
    """

    #: Describes a scratch file systems. Pegasus will use this to store
    #: intermediate data between jobs and other temporary files.
    SHAREDSCRATCH = _DirectoryType.SHAREDSCRATCH

    # TODO: where is this documented? the others were in user guide
    SHAREDSTORAGE = _DirectoryType.SHAREDSTORAGE

    #: Describes the scratch file systems available locally on a compute node.
    LOCALSCRATCH = _DirectoryType.LOCALSCRATCH

    #: Describes a long term storage file system. This is the directory
    #: Pegasus will stage output files to.
    LOCALSTORAGE = _DirectoryType.LOCALSTORAGE

    # the site catalog schema lists freeSize and totalSize as an attribute
    # however this appears to not be used; removing it as a parameter
    # def __init__(self, directory_type, path, free_size=None, total_size=None):
    def __init__(self, directory_type, path):
        """
        :param directory_type: directory type defined in :py:class:`~Pegasus.api.site_catalog.DirectoryType`
        :type directory_type: DirectoryType
        :param path: directory path
        :type path: str
        :raises ValueError: directory_type must be one of :py:class:`~Pegasus.api.site_catalog.DirectoryType`
        """
        if not isinstance(directory_type, _DirectoryType):
            raise TypeError(
                "invalid directory_type: {directory_type}; directory type must be one of {cls_enum_str}".format(
                    directory_type=directory_type,
                    cls_enum_str=_get_class_enum_member_str(Directory, _DirectoryType),
                )
            )

        self.directory_type = directory_type.value

        self.path = path
        # self.free_size = free_size
        # self.total_size = total_size

        self.file_servers = list()

    @_chained
    def add_file_servers(self, *file_servers):
        """Add one or more access methods to this directory
        
        :param file_server: a :py:class:`~Pegasus.api.site_catalog.FileServer`
        :raises ValueError: file_server must be of type :py:class:`~Pegasus.api.site_catalog.FileServer`
        :return: self
        """
        for fs in file_servers:
            if not isinstance(fs, FileServer):
                raise TypeError(
                    "invalid file_server: {file_server}; file_server must be of type FileServer".format(
                        file_server=fs
                    )
                )

            self.file_servers.append(fs)

    def __json__(self):
        return _filter_out_nones(
            {
                "type": self.directory_type,
                "path": self.path,
                "fileServers": [fs for fs in self.file_servers],
                "freeSize": None,
                "totalSize": None,
            }
        )


class SupportedJobs(Enum):
    """Types of jobs in the executable workflow this grid supports"""

    COMPUTE = "compute"
    AUXILLARY = "auxillary"
    TRANSFER = "transfer"
    REGISTER = "register"
    CLEANUP = "cleanup"


class Grid:
    """Each site supports various (usually two) job managers"""

    GT2 = _GridType.GT2
    GT4 = _GridType.GT4
    GT5 = _GridType.GT5
    CONDOR = _GridType.CONDOR
    CREAM = _GridType.CREAM
    BATCH = _GridType.BATCH
    PBS = _GridType.PBS
    LSF = _GridType.LSF
    SGE = _GridType.SGE
    NORDUGRID = _GridType.NORDUGRID
    UNICORE = _GridType.UNICORE
    EC2 = _GridType.EC2
    DELTACLOUD = _GridType.DELTACLOUD

    def __init__(
        self,
        grid_type,
        contact,
        scheduler_type,
        job_type=None,
        free_mem=None,
        total_mem=None,
        max_count=None,
        max_cpu_time=None,
        running_jobs=None,
        jobs_in_queue=None,
        idle_nodes=None,
        total_nodes=None,
    ):
        # TODO: get descriptions for params
        """
        :param grid_type: [description]
        :type grid_type: [type]
        :param contact: [description]
        :type contact: [type]
        :param scheduler_type: [description]
        :type scheduler_type: [type]
        :param job_type: [description], defaults to None
        :type job_type: [type], optional
        :param free_mem: [description], defaults to None
        :type free_mem: [type], optional
        :param total_mem: [description], defaults to None
        :type total_mem: [type], optional
        :param max_count: [description], defaults to None
        :type max_count: [type], optional
        :param max_cpu_time: [description], defaults to None
        :type max_cpu_time: [type], optional
        :param running_jobs: [description], defaults to None
        :type running_jobs: [type], optional
        :param jobs_in_queue: [description], defaults to None
        :type jobs_in_queue: [type], optional
        :param idle_nodes: [description], defaults to None
        :type idle_nodes: [type], optional
        :param total_nodes: [description], defaults to None
        :type total_nodes: [type], optional
        :raises ValueError: 
        :raises ValueError: 
        :raises ValueError:
        """
        if not isinstance(grid_type, _GridType):
            raise TypeError(
                "invalid grid_type: {grid_type}; grid_type must be one of {cls_enum_str}".format(
                    grid_type=grid_type,
                    cls_enum_str=_get_class_enum_member_str(Grid, _GridType),
                )
            )

        self.grid_type = grid_type.value

        self.contact = contact

        if not isinstance(scheduler_type, Scheduler):
            raise TypeError(
                "invalid scheduler_type: {scheduler_type}; scheduler_type must be one of {enum_str}".format(
                    scheduler_type=scheduler_type, enum_str=_get_enum_str(Scheduler)
                )
            )

        self.scheduler_type = scheduler_type.value

        if job_type is not None:
            if not isinstance(job_type, SupportedJobs):
                raise TypeError(
                    "invalid job_type: {job_type}; job_type must be one of {enum_str}".format(
                        job_type=job_type, enum_str=_get_enum_str(SupportedJobs)
                    )
                )
            else:
                self.job_type = job_type.value
        else:
            self.job_type = None

        self.free_mem = free_mem
        self.total_mem = total_mem
        self.max_count = max_count
        self.max_cpu_time = max_cpu_time
        self.running_jobs = running_jobs
        self.jobs_in_queue = jobs_in_queue
        self.idle_nodes = idle_nodes
        self.total_nodes = total_nodes

    def __json__(self):
        return _filter_out_nones(
            {
                "type": self.grid_type,
                "contact": self.contact,
                "scheduler": self.scheduler_type,
                "jobtype": self.job_type,
                "freeMem": self.free_mem,
                "totalMem": self.total_mem,
                "maxCount": self.max_count,
                "maxCPUTime": self.max_cpu_time,
                "runningJobs": self.running_jobs,
                "jobsInQueue": self.jobs_in_queue,
                "idleNodes": self.idle_nodes,
                "totalNodes": self.total_nodes,
            }
        )


class Site(ProfileMixin):
    """A compute resource (which is often a cluster) that we indent to run
    the workflow upon. A site is a homogeneous part of a cluster that has at 
    least a single GRAM gatekeeper with a jobmanager-fork and jobmanager-<scheduler> 
    interface and at least one gridftp server along with a shared file system. 
    The GRAM gatekeeper can be either WS GRAM or Pre-WS GRAM. A site can also 
    be a condor pool or glidein pool with a shared file system.

    .. code-block:: python

        # Example
        (Site(LOCAL, arch=Arch.X86_64, os_type=OS.LINUX, os_release="rhel", os_version="7")
            .add_directories(
                Directory(Directory.SHARED_SCRATCH, shared_scratch_dir)
                    .add_file_servers(FileServer("file://" + shared_scratch_dir, Operation.ALL)),

                Directory(Directory.LOCAL_STORAGE, local_storage_dir)
                    .add_file_servers(FileServer("file://" + local_storage_dir, Operation.ALL))
            ))
    """

    def __init__(
        self,
        name,
        arch=None,
        os_type=None,
        os_release=None,
        os_version=None,
        glibc=None,
    ):
        """
        :param name: name of the site
        :type name: str
        :param arch: the site's architecture, defaults to None
        :type arch: Arch, optional
        :param os_type: the site's operating system, defaults to None
        :type os_type: OS, optional
        :param os_release: the release of the site's operating system, defaults to None
        :type os_release: str, optional
        :param os_version: the version of the site's operating system, defaults to None
        :type os_version: str, optional
        :param glibc: glibc used on the site, defaults to None
        :type glibc: str, optional
        :raises ValueError: arch must be one of :py:class:`~Pegasus.api.site_catalog.Arch`
        :raises ValueError: os_type must be one of :py:class:`~Pegasus.api.site_catalog.OSType`
        """
        self.name = name
        self.directories = list()
        self.grids = list()

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
            self.arch = arch

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
            self.os_type = os_type

        self.os_release = os_release
        self.os_version = os_version
        self.glibc = glibc

        self.profiles = defaultdict(dict)

    @_chained
    def add_directories(self, *directories):
        """Add one or more :py:class:`~Pegasus.api.site_catalog.Directory` to this :py:class:`~Pegasus.api.site_catalog.Site`
        
        :param directory: the :py:class:`~Pegasus.api.site_catalog.Directory` to be added
        :raises TypeError: directory must be of type :py:class:`~Pegasus.api.site_catalog.Directory`
        :return: self
        """
        for d in directories:
            if not isinstance(d, Directory):
                raise TypeError(
                    "invalid directory: {directory}; directory is not of type Directory".format(
                        directory=d
                    )
                )

            self.directories.append(d)

    @_chained
    def add_grids(self, *grids):
        """Add one or more :py:class:`~Pegasus.api.site_catalog.Grid` to this :py:class:`~Pegasus.api.site_catalog.Site`
        
        :param grid: the :py:class:`~Pegasus.api.site_catalog.Grid` to be added
        :raises TypeError: grid must be of type :py:class:`~Pegasus.api.site_catalog.Grid`
        :return: self
        """
        for g in grids:
            if not isinstance(g, Grid):
                raise TypeError(
                    "invalid grid: {grid}; grid must be of type Grid".format(grid=g)
                )

            self.grids.append(g)

    def __json__(self):
        return _filter_out_nones(
            {
                "name": self.name,
                "arch": self.arch,
                "os.type": self.os_type,
                "os.release": self.os_release,
                "os.version": self.os_version,
                "glibc": self.glibc,
                "directories": [d for d in self.directories],
                "grids": [g for g in self.grids] if len(self.grids) > 0 else None,
                "profiles": dict(self.profiles) if len(self.profiles) > 0 else None,
            }
        )


class SiteCatalog(Writable):
    """The SiteCatalog describes the compute resources, or :py:class:`~Pegasus.api.site_catalog.Site` s
    that we intend to run the workflow upon.

    .. code-block:: python

        # Example 
        (SiteCatalog()
            .add_sites(
                Site(LOCAL, arch=Arch.X86_64, os_type=OS.LINUX, os_release="rhel", os_version="7")
                    .add_directories(
                        Directory(Directory.SHARED_SCRATCH, shared_scratch_dir)
                            .add_file_servers(FileServer("file://" + shared_scratch_dir, Operation.ALL)),

                        Directory(Directory.LOCAL_STORAGE, local_storage_dir)
                            .add_file_servers(FileServer("file://" + local_storage_dir, Operation.ALL))
                    ),

                Site(CONDOR_POOL, arch=Arch.X86_64, os_type=OS.LINUX)
                    .add_profile_pegasus(style="condor")
                    .add_profile_condor(universe="vanilla")

            ).write("SiteCatalog.yml"))
            
    """

    def __init__(self):
        self.sites = dict()

    @_chained
    def add_sites(self, *sites):
        """Add one or more sites to this catalog
        
        :param site: the site to be added
        :raises TypeError: site must be of type :py:class:`~Pegasus.api.site_catalog.Site`
        :raises DuplicateError: a site with the same name already exists in this catalog
        :return: self
        """
        for s in sites:
            if not isinstance(s, Site):
                raise TypeError(
                    "invalid site: {site}; site must be of type Site".format(site=s)
                )

            if s.name in self.sites:
                raise DuplicateError(
                    "site with name: {} already exists in this SiteCatalog".format(
                        s.name
                    )
                )

            self.sites[s.name] = s

    def __json__(self):
        return {
            "pegasus": PEGASUS_VERSION,
            "sites": [site for _, site in self.sites.items()],
        }
