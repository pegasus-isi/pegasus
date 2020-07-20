#!/usr/bin/env python3

"""
Pegasus utility for checking that prereqs are installed
"""

import sys

if not sys.version_info >= (3, 5):
    sys.stderr.write("Pegasus requires Python 3.5 or above\n")
    sys.exit(1)

try:
    import yaml  # noqa
except Exception:
    sys.stderr.write("Pegasus requires the Python3 YAML module to be installed\n")
    sys.exit(1)

