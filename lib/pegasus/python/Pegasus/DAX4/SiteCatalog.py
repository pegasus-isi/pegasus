from enum import Enum


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


class SiteCatalog:
    pass
