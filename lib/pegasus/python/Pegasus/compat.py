"""Python 2/3 compatibility helpers."""

import sys

PY3 = sys.version_info[0] >= 3

if PY3:

    import configparser
    import queue
    import urllib.parse as urllib

else:

    import ConfigParser as configparser
    import Queue as queue
    import urllib as urllib

# flake8: noqa
