##
#  Copyright 2007-2010 University Of Southern California
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
##

# Revision : $Revision: 2012 $

# Import few Python modules

import os
import sys
import logging

# Get logger object
logger = logging.getLogger()
# Set default level to WARNING
logger.setLevel(logging.WARNING)

# Format our log messages the way we want
cl = logging.StreamHandler()

# Don't add funcName to the formatter for Python versions < 2.5
if sys.version_info < (2, 5):
    formatter = logging.Formatter("%(asctime)s:%(filename)s:%(lineno)d: %(levelname)s: %(message)s")
else:
    formatter = logging.Formatter("%(asctime)s:%(filename)s:%(funcName)s:%(lineno)d: %(levelname)s: %(message)s")
cl.setFormatter(formatter)
logger.addHandler(cl)

# Figure out where our lib/Python directory is located, and put that in our module search path

# First let's set out PEGASUS_HOME
pegasus_home = os.path.normpath(os.path.join(os.path.dirname(sys.argv[0]), ".."))
os.environ["PEGASUS_HOME"] = pegasus_home
lib_path = os.path.join(pegasus_home, "lib/python")

# Check if this is a directory
if os.path.isdir(lib_path):
    # Insert this directory in our search path
    os.sys.path.insert(0, lib_path)
else:
    # Cannot figure out what to do!
    logger.critical("cannot find Pegasus's Python library directory... exiting!")
    sys.exit(1)
