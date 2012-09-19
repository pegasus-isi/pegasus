#!/usr/bin/env python

import os
import sys

for var in sys.argv[1:]:
    fd = os.getenv(var)
    if fd is None:
        raise Exception("%s not in environment" % var)
    fd = int(fd)
    os.write(fd, "Variable %s is fd %d\n" % (var,fd))
    os.close(fd)

