"""
Installed-layout path helpers shared by the Pegasus CLI tools.
"""

import sysconfig
from importlib.metadata import PackageNotFoundError, distribution
from pathlib import Path


def get_bin_dir():
    """
    Get the directory where this installation's console scripts live.

    Read from the pegasus-wms RECORD rather than PATH, so an unactivated venv
    never picks up another install's `pegasus` (e.g. the .deb's /usr/bin/pegasus).
    sysconfig is only a fallback: it is wrong for --user and some conda installs.
    """
    try:
        files = distribution("pegasus-wms").files or []
    except PackageNotFoundError:
        files = []
    for f in files:
        if f.name == "pegasus" and f.parent.name == "bin":
            script = Path(f.locate()).resolve()
            if script.exists():
                return str(script.parent)

    return sysconfig.get_path("scripts")
