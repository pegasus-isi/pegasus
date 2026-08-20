"""
Installed-layout path helpers shared by the Pegasus CLI tools.
"""

import shutil
import sysconfig
from pathlib import Path


def get_bin_dir():
    """
    Get the directory where the Pegasus console scripts are installed.

    This is the venv or system bin/ directory.
    """
    pegasus_exe = shutil.which("pegasus")
    if pegasus_exe:
        return str(Path(pegasus_exe).resolve().parent)

    return sysconfig.get_path("scripts")
