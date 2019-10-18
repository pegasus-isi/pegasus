#!/usr/bin/env python3

import os
import sys

for var in sys.argv[1:]:
    fd = os.getenv(var)
    if fd is None:
        raise Exception("%s not in environment" % var)
    fd = int(fd)
    os.write(fd, bytes("Variable %s is fd %d\n" % (var,fd), 'utf-8'))
    os.close(fd)

