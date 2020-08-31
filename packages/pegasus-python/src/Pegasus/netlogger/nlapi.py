## Copyright (c) 2004, The Regents of the University of California, through
## Lawrence Berkeley National Laboratory (subject to receipt of any required
## approvals from the U.S. Dept. of Energy).  All rights reserved.

"""
NetLogger instrumentation API for Python

Write NetLogger log messages. Most users of this API will
use the Log class, which is a little like a 'Logger' object in the
Python logging API.

Utility functions include functions to get and set the Grid Job ID.
"""
__author__ = "Dan Gunter"
__created__ = "1 April 2004"
__rcsid__ = "$Id: nlapi.py 27037 2011-02-04 20:16:27Z dang $"

import calendar
import datetime
import os
import socket
import sys
import time
import urllib.parse

from Pegasus.netlogger.nldate import utcFormatISO
from Pegasus.netlogger.util import uuid1

#
## Exceptions
#


class ParseException(Exception):
    pass


class FormatException(Exception):
    pass


#
## Constants
#

# Environment variable to store GID
GID_ENV = "NETLOGGER_GUID"  # new
# OLD: GID_ENV = 'NL_GID'

# Environment variable to store destination
NLDEST_ENV = "NL_DEST"

# Environment variable for level file
CFG_ENV = "NL_CFG"

FIELD_SEP = " "
REC_SEP = "\n"
EOR = "\n"
KEYVAL_SEP = "="

# Port
DEFAULT_PORT = 14380

# Level
class Level:
    NOLOG = 0
    FATAL = 1
    ERROR = 2
    WARN = 3
    WARNING = 3
    INFO = 4
    DEBUG = 5
    DEBUG1 = 6
    DEBUG2 = 7
    DEBUG3 = 8
    TRACE = DEBUG1
    ALL = -1

    names = {
        NOLOG: "NOLOG",
        FATAL: "Fatal",
        ERROR: "Error",
        WARN: "Warn",
        INFO: "Info",
        DEBUG: "Debug",
        TRACE: "Trace",
        DEBUG2: "Debug2",
        DEBUG3: "Debug3",
    }

    @staticmethod
    def getName(level):
        return Level.names.get(level, "User")

    @staticmethod
    def getLevel(name):
        if name.isupper() and hasattr(Level, name):
            return getattr(Level, name)
        raise ValueError("no such level name: %s" % name)


DATE_FMT = "%04d-%02d-%02dT%02d:%02d:%02d"

# Required fields
TS_FIELD = "ts"
EVENT_FIELD = "event"
# Other conventions
LEVEL_FIELD = "level"
STATUS_FIELD = "status"
MESSAGE_FIELD = "msg"
HASH_FIELD = "nlhash"

#
## Utility functions
#


def quotestr(v):
    """Quote a string value to be output."""
    if not v:
        v = '""'
    elif " " in v or "\t" in v or '"' in v or "=" in v:
        v = '"%s"' % v.replace(r'"', r"\"")
    return v


def getGuid(create=True, env=GID_ENV):
    """Return a GUID.
    If 'create' is True (the default), and if none is found
    in the environment then create one.
    """
    gid = os.environ.get(env, None)
    if gid is None:
        if create:
            gid = uuid1()
    return gid


# Call this if you want to set a GID manually
def setGuid(id, env=GID_ENV):
    """Replace current guid in the environment with provided value.
    Return old value, or None if there was no old value.

    Note: may cause memory leak on FreeBSD and MacOS. See system docs.
    """
    old_gid = os.environ.get(env, None)
    os.environ[env] = id
    return old_gid


def clearGuid(env=GID_ENV):
    """Unset guid"""
    old_gid = os.environ.get(env, None)
    if old_gid:
        del os.environ[env]
    return old_gid


_g_hostip = None


def getHost():
    global _g_hostip
    if _g_hostip is not None:
        return _g_hostip
    try:
        ip = socket.gethostbyname(socket.getfqdn())
    except Exception:
        ip = "127.0.0.1"
    _g_hostip = ip
    return ip


def getProg():
    import sys

    return sys.argv[0]


def getDest():
    return os.environ.get(NLDEST_ENV, None)


class LevelConfig:
    """Set logging level from a configuration file.
    The format of the file is trivial: an integer log level.
    """

    DEFAULT = Level.INFO

    def __init__(self, filename):
        self._f = filename
        self._level = None

    def getLevel(self):
        if self._level is None:
            try:
                self._level = self.DEFAULT
                f = file(self._f)
                line = f.readline()
                i = int(line.strip())
                self._level = i
            except OSError:
                pass
            except ValueError:
                pass
        return self._level


if os.getenv(CFG_ENV) is not None:
    g_level_cfg = LevelConfig(os.getenv(CFG_ENV))
else:
    g_level_cfg = None


class Log:
    """NetLogger log class.

    Name=value pairs for the log are passed as keyword arguments.
    This is mostly good, but one drawback is that a period '.' in the
    name is confusing to python. As a work-around, use '__' to mean '.',
    e.g. if you want the result to be "foo.id=bar", then do::
        log.write(.., foo__id='bar')
    Similarly, a leading '__' will be stripped (e.g. to avoid stepping
    on keywords like 'class')

    If you instantiate this class without a 'logfile', it will act
    as a formatter, returning a string.

    To disable filtering of messages on level, add 'level=Level.ALL'
    """

    class OpenError(Exception):
        pass

    def __init__(
        self,
        logfile=None,
        flush=False,
        prefix=None,
        level=Level.INFO,
        newline=True,
        guid=True,
        pretty=False,
        float_time=False,
        meta={},
    ):
        """Constructor."""
        self._logfile = None
        self._float_time = float_time
        self._pretty = pretty
        self._newline = newline
        self._flush = [None, self.flush][flush]
        self.setPrefix(prefix)
        self._meta = {}
        if meta:
            self._meta[None] = meta
        if isinstance(logfile, str):
            try:
                self._logfile = urlfile(logfile)
            except (socket.gaierror, OSError) as E:
                raise self.OpenError(E)
        else:
            self._logfile = logfile
        if g_level_cfg is None:
            self._level = level
        else:
            self._level = g_level_cfg.getLevel()
        if guid is True:
            guid = getGuid(create=False)
            if guid:
                _m = self._meta.get(None, {})
                _m["guid"] = guid
                self._meta[None] = _m
        elif isinstance(guid, str):
            _m = self._meta.get(None, {})
            _m["guid"] = guid
            self._meta[None] = _m

    def setLevel(self, level):
        """Set highest level of messages that WILL be logged.
        Messages below this level (that is, less severe,
        higher numbers) will be dropped.

        For example::
          log.setLevel(Level.WARN)
          log.error('argh',{}) # logged
          log.info('whatever',{}) # dropped!
        """
        self._level = level

    def setPrefix(self, prefix):
        if prefix is None:
            self._pfx = ""
        elif prefix.endswith("."):
            self._pfx = prefix
        else:
            self._pfx = prefix + "."

    def debugging(self):
        """Return whether the level >= debug."""
        return self._level >= Level.DEBUG

    def flush(self):
        """Flush output object."""
        if self._logfile:
            self._logfile.flush()

    def write(self, event="event", ts=None, level=Level.INFO, **kw):
        """Write a NetLogger string.
        If there is a logfile, returns None
        Otherwise, returns a string that would have been written.
        """
        if self._level != Level.ALL and level > self._level:
            if self._logfile:
                return None
            else:
                return ""
        if not ts:
            ts = time.time()
        buf = self.format(self._pfx + event, ts, level, kw)
        if self._logfile is None:
            return buf
        self._logfile.write(buf)
        if self._flush:
            self.flush()

    __call__ = write

    def error(self, event="", **kwargs):
        return self.write(event, level=Level.ERROR, **kwargs)

    def warn(self, event="", **kwargs):
        return self.write(event, level=Level.WARN, **kwargs)

    def info(self, event="", **kwargs):
        return self.write(event, level=Level.INFO, **kwargs)

    def debug(self, event="", **kwargs):
        return self.write(event, level=Level.DEBUG, **kwargs)

    def _append(self, fields, kw):
        for k, v in kw.items():
            if k.startswith("__"):
                k = k[2:]
            k = k.replace("__", ".")
            if isinstance(v, str):
                v = quotestr(v)
                fields.append("{}={}".format(k, v))
            elif isinstance(v, float):
                fields.append("{}={:f}".format(k, v))
            elif isinstance(v, int):
                fields.append("%s=%d" % (k, v))
            else:
                s = str(v)
                if " " in s or "\t" in s:
                    s = '"%s"' % s
                fields.append("{}={}".format(k, s))

    def format(self, event, ts, level, kw):
        if not self._pretty:
            # Regular BP formatting
            if isinstance(ts, str):
                fields = ["ts=" + ts, "event=" + event]
            elif isinstance(ts, datetime.datetime):
                if self._float_time:
                    tsfloat = calendar.timegm(ts.utctimetuple()) + ts.microsecond / 1e6
                    fields = ["ts=%.6f" % tsfloat, "event=" + event]
                else:
                    tsstr = "%s.%06dZ" % (
                        DATE_FMT % ts.utctimetuple()[0:6],
                        ts.microsecond,
                    )
                    fields = ["ts=" + tsstr, "event=" + event]
            elif self._float_time:
                fields = ["ts=%.6f" % ts, "event=" + event]
            else:
                fields = ["ts=" + utcFormatISO(ts), "event=" + event]
            if level is not None:
                if isinstance(level, int):
                    fields.append("level=" + Level.getName(level))
                else:
                    fields.append("level=%s" % level)
            if kw:
                self._append(fields, kw)
            if event in self._meta:
                self._append(fields, self._meta[event])
            if None in self._meta:
                self._append(fields, self._meta[None])
            buf = FIELD_SEP.join(fields)
        else:
            # "Pretty" BP formatting
            if not isinstance(ts, str):
                ts = utcFormatISO(ts)
            if isinstance(level, int):
                level = Level.getName(level).upper()
            # print traceback later
            if "traceback" in kw:
                tbstr = kw["traceback"]
                del kw["traceback"]
            else:
                tbstr = None
            if "msg" in kw:
                msg = kw["msg"]
                del kw["msg"]
            else:
                msg = None
            remainder = ",".join(
                ["{}={}".format(key, value) for key, value in kw.items()]
            )
            if msg:
                buf = "{} {:<6} {} | {}. {}".format(ts, level, event, msg, remainder)
            else:
                buf = "{} {:<6} {} | {}".format(ts, level, event, remainder)
            # add traceback
            if tbstr:
                buf += "\n" + tbstr
        if self._newline:
            return buf + REC_SEP
        else:
            return buf

    def setMeta(self, event=None, **kw):
        self._meta[event] = kw

    def close(self):
        self.flush()

    def __del__(self):
        if not hasattr(self, "closed"):
            self.close()
        self.closed = True

    def __str__(self):
        if self._logfile:
            return str(self._logfile)
        else:
            return repr(self)


# set up urlparse to recognize x-netlog schemes
for scheme in "x-netlog", "x-netlog-udp":
    urllib.parse.uses_netloc.append(scheme)
    try:
        urllib.parse.uses_query.append(scheme)
    except AttributeError:
        pass


def urlfile(url):
    """urlfile(url:str) -> file

    Open a NetLogger URL and return a write-only file object.
    """
    # print "url='%s'" % url
    # Split URL
    scheme, netloc, path, params, query, frag = urllib.parse.urlparse(url)
    # Put query parts into a dictionary for easy access later
    query_data = {}
    if query:
        query_parts = query.split("&")
        for flag in query_parts:
            name, value = flag.split("=")
            query_data[name] = value
    # Create file object
    if scheme == "file" or scheme == "" or scheme is None:
        # File
        if path == "-":
            fileobj = sys.stdout
        elif path == "&":
            fileobj = sys.stderr
        else:
            if "append" in query_data:
                is_append = boolparse(query_data["append"])
                open_flag = "aw"[is_append]
            else:
                open_flag = "a"
            fileobj = file(path, open_flag)
    elif scheme.startswith("x-netlog"):
        # TCP or UDP socket
        if netloc.find(":") == -1:
            addr = (netloc, DEFAULT_PORT)
        else:
            host, port_str = netloc.split(":")
            addr = (host, int(port_str))
        if scheme == "x-netlog":
            # TCP Socket
            sock = socket.socket()
        elif scheme == "x-netlog-udp":
            # UDP Socket
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        else:
            raise ValueError(
                "Unknown URL scheme '%s', "
                "must be empty, 'file' or 'x-netlog[-udp]'" % scheme
            )
        # print "connect to address %s" % addr
        sock.connect(addr)
        fileobj = sock.makefile("w")
    else:
        raise ValueError(
            "Unknown URL scheme '%s', "
            "must be empty, 'file' or 'x-netlog[-udp]'" % scheme
        )
    return fileobj


def urltype(url):
    """urltype(url:str) -> 'file' | 'tcp' | None

    Return a canonical string representing the type of URL,
    or None if the type is unknown
    """
    scheme = urllib.parse.urlparse(url)[0]
    if scheme == "file" or scheme == "" or scheme is None:
        return "file"
    elif scheme == "x-netlog":
        return "tcp"
    else:
        return None


# Get host

_g_hostip = None


def get_host():
    global _g_hostip
    if _g_hostip is not None:
        return _g_hostip
    try:
        ip = socket.gethostbyname(socket.getfqdn())
    except Exception:
        ip = "127.0.0.1"
    _g_hostip = ip
    return ip
