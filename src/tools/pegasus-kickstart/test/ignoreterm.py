import signal
import time

signal.signal(signal.SIGTERM, signal.SIG_IGN)

print "Sleeping for 5 seconds"
time.sleep(5)

