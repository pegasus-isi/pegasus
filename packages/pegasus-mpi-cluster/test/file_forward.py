#!/usr/bin/env python3

import os
import sys
from pathlib import Path

for fname in sys.argv[1:]:
    dname = str(Path(fname).parent)
    if not Path(dname).is_dir():
        os.makedirs(dname)
    f = open(fname, "w")
    f.write("This is file %s\n" % fname)
    f.close()
