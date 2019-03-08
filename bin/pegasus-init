#!/usr/bin/env python
import sys
import os
import subprocess

# Use pegasus-config to find our lib path
bin_dir = os.path.normpath(os.path.join(os.path.dirname(sys.argv[0])))
pegasus_config = os.path.join(bin_dir, "pegasus-config") + " --python-dump"
exec(subprocess.Popen(pegasus_config, stdout=subprocess.PIPE, shell=True).communicate()[0])

# Insert this directory in our search path
os.sys.path.insert(0, pegasus_python_dir)
os.sys.path.insert(0, pegasus_python_externals_dir)

from Pegasus.init import main

main(pegasus_share_dir)

