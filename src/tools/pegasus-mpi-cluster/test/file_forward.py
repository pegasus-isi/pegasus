#!/usr/bin/env python3

import os
import sys

for fname in sys.argv[1:]:
    dname = os.path.dirname(fname)
    if not os.path.isdir(dname):
        os.makedirs(dname)
    f = open(fname, "w")
    f.write("This is file %s\n" % fname)
    f.close()

