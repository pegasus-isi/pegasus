#!/usr/bin/env python3

"""
Pegasus utility for checking that prereqs are installed
"""

import os
import sys

# PEGASUS_PYTHONPATH is set by the pegasus-python-wrapper script
peg_path = os.environ.get("PEGASUS_PYTHONPATH")
if peg_path:
    for p in reversed(peg_path.split(":")):
        if p not in sys.path:
            sys.path.insert(0, p)

success = True

if not sys.version_info >= (3, 6):
    sys.stderr.write("Pegasus requires Python 3.6 or above\n")
    sys.exit(1)

try:
    import yaml  # noqa
except Exception:
    sys.stderr.write("Pegasus requires the Python3 YAML module to be installed.\n")
    sys.stderr.write(
        "Please install it via your OS package manager, via pip, or activate a venv which includes the module.\n"
    )
    success = False

try:
    import git  # noqa
except Exception:
    sys.stderr.write("Pegasus requires the Python3 GitPython module to be installed\n")
    sys.stderr.write(
        "Please install it via your OS package manager, via pip, or activate a venv which includes the module.\n"
    )
    success = False

try:
    import dataclasses  # noqa
except Exception:
    sys.stderr.write(
        "Pegasus requires the Python3 dataclasses module to be installed\n"
    )
    sys.stderr.write(
        "Please install it via your OS package manager, via pip, or activate a venv which includes the module.\n"
    )
    success = False

if not success:
    sys.stderr.write(f"The PYTHONPATH is {sys.path}\n\n")
    sys.exit(1)
