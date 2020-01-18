from enum import Enum
from collections import defaultdict

from Pegasus.dax4.mixins import ProfileMixin
from Pegasus.dax4.writable import _filter_out_nones

__all__ = [
    "Arch",
    "OSType",
    "OperationType",
    "DirectoryType",
    "GridType",
    "SchedulerType",
    "JobType",
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
    IA64 = "ia64"
    SPARCV7 = "sparcv7"
    SPARCV9 = "sparcv9"
    AMD64 = "amd64"


class OSType(Enum):
    """Operating system types"""

    LINUX = "linux"
    SUNOS = "sunos"
    AIX = "aix"
    MACOSX = "macosx"
    WINDOWS = "windows"


class OperationType(Enum):
    """Different types of operations supported by a file server"""

    ALL = "all"
    PUT = "put"
    GET = "get"


class DirectoryType(Enum):
    """Different types of directories supported for a site"""

    #: Describes a scratch file systems. Pegasus will use this to store
    #: intermediate data between jobs and other temporary files.
    SHARED_SCRATCH = "sharedScratch"

    # TODO: where is this documented? the others were in user guide
    SHARED_STORAGE = "sharedStorage"

    #: Describes the scratch file systems available locally on a compute node.
    LOCAL_SCRATCH = "localScratch"

    #: Describes a long term storage file system. This is the directory
    #: Pegasus will stage output files to.
    LOCAL_STOARGE = "localStorage"


class GridType(Enum):
    """Different grid types that can be supporte by Pegasus. Mirror the Condor
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


class SchedulerType(Enum):
    """Different scheduler types on the Grid"""

    FORK = "fork"
    PBS = "pbs"
    LSF = "lsf"
    CONDOR = "condor"
    SGE = "sge"
    UNKNOWN = "unknown"


class JobType(Enum):
    """Types of jobs in the executable workflow this grid supports"""

    COMPUTE = "compute"
    AUXILLARY = "auxillary"
    TRANSFER = "transfer"
    REGISTER = "register"
    CLEANUP = "cleanup"


class FileServer(ProfileMixin):
    """Describes the fileserver to access data from outside
    
    .. code-block:: python

        # Example 1
        fs1 = FileServer("scp://obelix.isi.edu/data", OperationType.PUT)

        # Example 2
        fs2 = FileServer("http://obelix.isi.edu/data", OperationType.GET)

    """

    def __init__(self, url, operation_type):
        """
        :param url: url including protocol such as 'scp://obelix.isi.edu/data'
        :type url: str
        :param operation_type: operation type defined in :py:class`~Pegasus.dax4.site_catalog.OperationType`
        :type operation_type: OperationType
        :raises ValueError: operation_type must be one defined in :py:class:`~Pegasus.dax4.site_catalog.OperationType`
        """
        self.url = url

        if not isinstance(operation_type, OperationType):
            raise ValueError("operation_type must be one of OperationType")

        self.operation_type = operation_type.value

        self.profiles = defaultdict(dict)

    def __json__(self):
        return {
            "url": self.url,
            "operation": self.operation_type,
            "profiles": dict(self.profiles) if len(self.profiles) > 0 else None,
        }


class Directory:
    """Information about filesystems Pegasus can use for storing temporary and long-term
    files. 
    
    .. code-block:: python

        # Example
        d = (
            Directory(DirectoryType.SHARED_SCRATCH, "/data")
                .add_file_server(FileServer("scp://obelix.isi.edu/data", OperationType.PUT))
                .add_file_server(FileServer("http://obelix.isi.edu/data", OperationType.GET))
        )

    """

    # the site catalog schema lists freeSize and totalSize as an attribute
    # however this appears to not be used; removing it as a parameter
    # def __init__(self, directory_type, path, free_size=None, total_size=None):
    def __init__(self, directory_type, path):
        """
        :param directory_type: directory type defined in :py:class:`~Pegasus.dax4.site_catalog.DirectoryType`
        :type directory_type: DirectoryType
        :param path: directory path
        :type path: str
        :raises ValueError: directory_type must be one of :py:class:`~Pegasus.dax4.site_catalog.DirectoryType`
        """
        if not isinstance(directory_type, DirectoryType):
            raise ValueError("directory_type must be one of DirectoryType")

        self.directory_type = directory_type.value

        self.path = path
        # self.free_size = free_size
        # self.total_size = total_size

        self.file_servers = list()

    def add_file_server(self, file_server):
        """Add access methods to this directory
        
        :param file_server: a :py:class:`~Pegasus.dax4.site_catalog.FileServer
        :type file_server: FileServer
        :raises ValueError: file_server must be of type :py:class:`~Pegasus.dax4.site_catalog.FileServer`
        :return: self
        """
        if not isinstance(file_server, FileServer):
            raise ValueError("file_server must be of type FileServer")

        self.file_servers.append(file_server)

        return self

    def __json__(self):
        return _filter_out_nones(
            {
                "type": self.directory_type,
                "path": self.path,
                "fileServers": [fs.__json__() for fs in self.file_servers],
                "freeSize": None,
                "totalSize": None,
            }
        )


class SiteCatalog:
    def __init__(self):
        pass

    def add_site(self):
        pass

    def __json__(self):
        pass

