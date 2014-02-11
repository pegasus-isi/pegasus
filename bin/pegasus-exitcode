#!/usr/bin/env python
#
# Copyright 2010 University Of Southern California
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


# This program parses job outputs looking for failures. If failures are found,
# it prints a message and exits with a non-zero exit code. If no failures are
# found, it exits with 0.
#
# This program also renames the .out and .err file to .out.XXX and .err.XXX
# where XXX is a sequence number. This sequence number is incremented each time
# the program is run with the same filename.out argument.
import sys
import os
import subprocess

# Use pegasus-config to find our lib path
bin_dir = os.path.normpath(os.path.join(os.path.dirname(sys.argv[0])))
pegasus_config = os.path.join(bin_dir, "pegasus-config") + " --noeoln --python"
lib_dir = subprocess.Popen(pegasus_config, stdout=subprocess.PIPE, shell=True).communicate()[0]
sys.path.insert(0, lib_dir)

from Pegasus import exitcode

exitcode.main(sys.argv[1:])

