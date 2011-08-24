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

