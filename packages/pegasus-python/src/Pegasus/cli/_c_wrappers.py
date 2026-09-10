"""
C binary wrappers for editable installs.

In a normal pip install, pegasus-kickstart/cluster/keg are installed
directly to <venv>/bin/ by CMake via the wheel scripts section — no Python
wrapper is needed and these functions are not registered as console_scripts.

This module is retained as a fallback for editable installs where a developer
manually compiled binaries and placed them at the old location
(src/Pegasus/data/bin/) before migrating to the CMake build.
"""

import os
import shutil
import sys
from pathlib import Path


def _exec_c_tool(name: str) -> None:
    # Primary: binary is on PATH (installed by CMake to <venv>/bin/)
    on_path = shutil.which(name)
    if on_path:
        os.execv(on_path, [on_path] + sys.argv[1:])

    # Fallback: old editable-install location (src/Pegasus/data/bin/)
    import Pegasus

    binary = Path(Pegasus.__file__).parent / "data" / "bin" / name
    if binary.exists():
        os.execv(str(binary), [str(binary)] + sys.argv[1:])

    print(
        f"pegasus: {name}: C binary not found.\n"
        f"Re-run 'pip install .' to compile C tools.",
        file=sys.stderr,
    )
    sys.exit(1)


def kickstart() -> None:
    _exec_c_tool("pegasus-kickstart")


def cluster() -> None:
    _exec_c_tool("pegasus-cluster")


def keg() -> None:
    _exec_c_tool("pegasus-keg")
