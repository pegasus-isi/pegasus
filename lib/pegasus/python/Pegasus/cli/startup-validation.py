#!/usr/bin/python3

import sys

if not sys.version_info >= (3, 5):
    sys.stderr.write('Pegasus requires Python 3.5 or above\n')
    sys.exit(1)

try:
    import yaml
except:
    sys.stderr.write('Pegasus requires the Python3 YAML module to be installed\n')
    sys.exit(1)

try:
    import OpenSSL
except:
    sys.stderr.write('Pegasus requires the Python3 PyOpenSSL module to be installed\n')
    sys.exit(1)

