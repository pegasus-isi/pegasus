#!/usr/bin/env python
import sys
import os
import subprocess

# only expand PYTHONPATH if it not already set
if 'PEGASUS_PYTHONPATH_SET' not in os.environ:
    bin_dir = os.path.normpath(os.path.join(os.path.dirname(sys.argv[0])))
    pegasus_config = os.path.abspath(os.path.join(bin_dir, "pegasus-config"))
    lib_dir = subprocess.Popen([pegasus_config,"--noeoln","--python"], stdout=subprocess.PIPE, shell=False).communicate()[0]
    lib_ext_dir = subprocess.Popen([pegasus_config,"--noeoln","--python-externals"], stdout=subprocess.PIPE, shell=False).communicate()[0]

    sys.path.insert(0, lib_ext_dir)
    sys.path.insert(0, lib_dir)


from Pegasus import s3

s3.main()
