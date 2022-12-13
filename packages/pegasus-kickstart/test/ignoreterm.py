from __future__ import print_function

import signal
import time
import sys

signal.signal(signal.SIGTERM, signal.SIG_IGN)

if len(sys.argv) != 2:
    raise Exception("Specify timeout")

sleeptime = int(sys.argv[1])

print("Sleeping for %d seconds" % sleeptime)
time.sleep(int(sleeptime))

