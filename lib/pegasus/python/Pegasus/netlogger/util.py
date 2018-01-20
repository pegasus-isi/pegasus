"""
Utility functions for NetLogger modules and command-line programs
"""
__rcsid__ = "$Id: util.py 27069 2011-02-08 20:09:10Z dang $"
__author__ = "Dan Gunter (dkgunter (at) lbl.gov)"

from asyncore import compact_traceback
from copy import copy
import glob
import imp
import logging
try:
    from hashlib import md5
    md5_new = md5
except ImportError:
    import md5
    md5_new = md5.new
from optparse import OptionParser, Option, OptionValueError, make_option
import os
import Queue
import re
import signal
import sys
import tempfile
import time
import traceback
#
import Pegasus.netlogger
from Pegasus.netlogger import configobj

## Globals

NL_HOME = "NETLOGGER_HOME"

EOF_EVENT = "netlogger.EOF"

MAGICDATE_EXAMPLES = ', '.join(["%s" % s for s in (
     '<N> weeks|days|hours|minutes|seconds time ago',
     'Today',
     'Now',
     'Tomorrow',
     'Yesterday',
     '4th [[Jan] 2003]',
     'Jan 4th 2003',
     'mm/dd/yyyy (first)',
     'dd/mm/yyyy (second)',
     'yyyy-mm-dd',
     'yyyymmdd',
     'next Tuesday',
     'last Tuesday')])

DATA_DIR = os.path.join(os.path.dirname(Pegasus.netlogger.__file__), 'data')

## Exceptions

class ConfigError(Exception):
    """Use this exception for configuration-time error handling.
    """
    pass

class DBConnectError(Exception):
    """Used for failed database connections.
    """
    pass

## Classes

class ScriptOptionParser(OptionParser):
    standard_option_list = (
        make_option('-q', '--quiet',
                    action='store_true', dest='quiet',
                    help="only report errors"),                
        make_option('-v', '--verbosity', default=0,
                    action='count', dest='verbosity',
                    help="show verbose status information, "
                    "repeat for even more detail"),
        )

class CountingQueue(Queue.Queue):
    """Wrapper around Queue that counts how many items
    were processed, for accounting purposes.
    """
    def __init__(self, type_, *args):
        Queue.Queue.__init__(self, *args)
        self.n = 0
        if type_ == 'get':
            self._gets, self._puts = True, False
        else:
            self._gets, self._puts = False, True
            
    def get(self, *args, **kwargs):
        if self._gets:
            self.n += 1
        return Queue.Queue.get(self, *args, **kwargs)

    def put(self, *args, **kwargs):
        if self._puts:
            self.n += 1
        return Queue.Queue.put(self, *args, **kwargs)
        
    def getNumProcessed(self):
        return self.n

class FIFODict:
    """A container that is limited to a fixed size.
    When that size is exceeded, every new item inserted
    results in an item removed (in FIFO order).
    Any 'old' items do not replace newer ones.
    The interface is specialized for adding an item and checking
    that it is new at the same time.
    """
    class CircularBuffer:
        """Specialized circular buffer
        """
        def __init__(self, size):
            self._data = [ ]
            self._size = size
            self._p = 0

        def getput(self, item):
            """Put item in buffer.
            If the buffer is full, return the oldest element,
            otherwise return None.
            """
            if len(self._data) == self._size:
                oldest = self._data[self._p]
                self._data[self._p] = item
                self._p = (self._p + 1) % self._size
            else:
                oldest = None
                self._data.append(item)
            return oldest

    def __init__(self, size):
        self._q = self.CircularBuffer(size)
        self._data = { }

    def add(self, key):
        """Add a key.
        If it is new, return True otherwise False.
        """
        if key in self._data:
            return False
        self._data[key] = 1
        removed = self._q.getput(key)
        if removed is not None:
            del self._data[removed]
        return True

def traceback():
    """Traceback as a string with no newlines."""
    return str(compact_traceback())

def parse_nvp(args):
    d = { }
    for arg in args:
        try:
            name, value = arg.split('=')
        except ValueError:
            pass
        d[name] = value
    return d

def tzstr():
    return "%s%02d:%02d" % (('+','-')[time.timezone > 0],
                            time.timezone / 3600 ,
                            (time.timezone - int(time.timezone/3600)*3600)/60)

def parseDatetime(d, utc=False):
    """Parse a datetime object, or anything that formats itself
    with isoformat(), to number of seconds since epoch.
    """
    from Pegasus.netlogger.parsers.base import parseDate
    if d is None:
        raise ValueError("date is None")
    iso = d.isoformat()
    # add 'midnight' time if none given
    if 'T' not in iso:
        iso += 'T00:00:00'
    # only append timezone if none is there already        
    if not iso.endswith('Z') and not re.match('.*[+-]\d\d:\d\d$', iso):
        if utc:
            iso += 'Z'
        else:
            iso += tzstr()
    return parseDate(iso)

class ProgressMeter:
    """A simple textual progress meter.
    """
    REPORT_INTERVAL = 1000
    
    def __init__(self, ofile, units="lines"):
        self.ofile = ofile
        self.units = units
        self._counter = 0
        self.reset(0)

    def reset(self, n):
        self.t0 = time.time()
        self.last_report = n
        
    def advance(self, num=0, inc=1):
        if num == 0:
            self._counter += inc
            num = self._counter
        if num - self.last_report >= self.REPORT_INTERVAL:
            n = num - self.last_report
            dt = time.time() - self.t0
            rate = n / dt
            if rate < 1:
                dig = 2
            elif rate < 10:
                dig = 1
            else:
                dig = 0
            fmt = "[ %%5d ] %%s, rate = %%.%df %%s/sec     \r" % dig
            self.ofile.write(fmt % (num, self.units, rate, self.units))
            self.ofile.flush()
            self.reset(num)
        
class NullProgressMeter:
    """Substitute for ProgressMeter when you don't want anything to
    actually be printed.
    """
    def __init__(self, ofile=None, units=None):
        return

    def reset(self, n):
        pass
    
    def advance(self, num=0):
        pass

def mostRecentFile(dir, file_pattern, after_time=None):
    """Search 'dir' for all files matching glob 'file_pattern',
    and return the mostRecent one(s). If 'after_time' is given,
    it should be a number of seconds since the epoch UTC; no files
    will be returned if none is on or after this time.

    Returns a list of the full paths to file(s), or an empty list.
    More than one file may be returned, in the case that they have
    the same modification time.

    """
    if not os.path.isdir(dir):
        return [ ]
    search_path = os.path.join(dir, file_pattern)
    # make a sortable list of filenames and modification times
    timed_files = [(os.stat_result(os.stat(fname)).st_mtime, fname) 
                   for fname in glob.glob(search_path)]
    # if the list is empty, stop
    if not timed_files:
        return [ ]
    # reverse sort so most-recent is first
    timed_files.sort(reverse=True)
    most_recent_time = timed_files[0][0]
    # return nothing if the most recent time is not
    # after the cutoff
    if after_time is not None and most_recent_time < int(after_time):
        return [ ]
    # start with most recent, then append all 'ties'
    result = [ timed_files[0][1] ]
    i = 1
    try:
        while timed_files[i][0] == most_recent_time:
            result.append(timed_files[i][1])
            i += 1
    except IndexError:
        pass # ran off end of list. all ties (!)
    # return all 'most recent' files
    return result


def daemonize(log=None, root_log=None, close_fds=True):
    """Make current process into a daemon.
    """
    # Do a double-fork so that the daemon process is completely
    # detached from its parent (it becomes a child of init).
    # For details the classic text is: 
    # W. Richard Stevens, "Advanced Programming in the UNIX Environment"
    log.info("daemonize.start")
    log.debug("daemonize.fork1")
    try: 
        pid = os.fork() 
        if pid > 0:
            # parent: exit
            sys.exit(0) 
    except OSError as err: 
        log.exc( "fork.1.failed", err)
        sys.exit(1)
    log.debug("daemonize.fork2")
    # child: do second fork
    try: 
        pid = os.fork() 
        if pid > 0:
            # parent: exit
            sys.exit(0) 
    except OSError as err: 
        log.exc("daemonize.fork2.failed", err)
        sys.exit(1)
    # child: decouple from parent environment
    log.debug("daemonize.chdir_slash")
    os.chdir("/")
    try:
        os.setsid() 
    except OSError:
        pass
    os.umask(0)
    if close_fds:
        # Remove log handlers that write to stdout or stderr.
        # Construct list of other log handlers' file descriptors.
        no_close = [ ] # list of fd's to keep open
        if root_log and len(root_log.handlers) > 0:
            console = (sys.stderr.fileno(), sys.stdout.fileno())
            for handler in root_log.handlers[:]:
                fd = handler.stream.fileno()
                #print "handler: %s, fileno=%d, console=%s" % (handler, fd, console)
                if fd in console:
                    log.removeHandler(handler)
                else:
                    no_close.append(fd)
        # Close all fd's except those that belong to non-console
        # log handlers, just discovered above.
        log.info("daemonize.close_fds", ignore=','.join(no_close))
        for fd in xrange(1024):
            if fd not in no_close:
                try:
                    os.close(fd)
                except OSError:
                    pass
    # redirect stdin, stdout, stderr to /dev/null
    log.info("daemonize.redirect")
    try:
        devnull = os.devnull
    except AttributeError:
        devnull = "/dev/null"
    os.open(devnull, os.O_RDWR)
    try:
        os.dup2(0, 1)
        os.dup2(0, 2)
    except OSError:
        pass
    log.info("daemonize.end")
    
def _getNumberedFiles(path):
    result = [ ]
    for filename in glob.glob(path + ".*"):
        try:
            name, ext = filename.rsplit('.',1)
            n = int(ext)
            result.append((n,filename))
        except (IndexError, ValueError):
            pass
    return result

def getNextNumberedFile(path, mode="w", strip=False, open_file=True):
    if strip: # take off .<num> extension first
        path = path.rsplit('.', 1)[0]
    numbered = _getNumberedFiles(path)
    if numbered:
        numbered.sort(reverse=True)
        next_num = numbered[0][0] + 1
    else:
        next_num = 1
    next_file = "%s.%d" % (path, next_num)
    if open_file:
        return file(next_file, mode)
    else:
        return next_file

def getAllNumberedFiles(path):
    nf = _getNumberedFiles(path)
    return [x[1] for x in nf]

def getLowestNumberedFile(path, mode="r"):
    numbered = _getNumberedFiles(path)
    if numbered:
        numbered.sort()
        result = file(numbered[0][1], mode)
    else:
        result = None
    return result

class ThrottleTimer:
    """Given a ratio of time that the program should be running
    in a given time-slice, and assuming that the program is running
    continuously between calls to throttle(), periodically sleep so
    that the program is running for roughly that proportion of time.

    For example, if run_ratio is 0.1 then calling throttle() in a 
    loop will cause it to sleep 90% of the time:
       tt = ThrottleTimer(0.1)
       ...
       tt.start()
       while True:
           do_something()
           tt.throttle() # sleeps here

    """
    def __init__(self, run_ratio, min_sleep_sec=0.1):
        """Create timer.

        'run_ratio' is the desired ratio of the time between calls to the
          time sleeping in ths timer.
        'min_sleep_sec' is the mininum size of the argument to time.sleep(),
          before throttle() will actually call it. This attempts to minimize
          the inaccuracy encountered with very small sleep times.
        """
        self.sleep_ratio = (1/run_ratio - 1)
        self.t0 = time.time()
        self.min_sleep_sec = min_sleep_sec

    def start(self):
        """Start the timer.
        """
        self.t0 = time.time()

    def throttle(self):
        """Sleep for an appropriate time.

        If that time would be less than 'min_sleep_sec' (see constructor),
        don't actually perform the sleep. Therefore, it should be safe
        to call this in a (relatively) tight loop.
        """
        t1 = time.time()
        sleep_sec = (t1 - self.t0) * self.sleep_ratio
        if sleep_sec >= self.min_sleep_sec:
            time.sleep(sleep_sec)
            self.t0 = time.time()
        
class NullThrottleTimer(ThrottleTimer):
    """Null class for ThrottleTimer. Cleans up calling code."""
    def __init__(self, run_ratio=None, min_sleep_sec=None):
        ThrottleTimer.__init__(self, 1)
    def start(self): 
        return
    def throttle(self): 
        return

class NullFile:
    """Null-object pattern for 'file' class.
    """
    def __init__(self, name='(null)', mode='r', buffering=None):
        self.name = name
        self.mode = mode
        self.encoding = None
        self.newlines = None
        self.softspace = 0
    def close(self):
        pass
    def closed(self):
        return True
    def fileno(self):
        return -1
    def flush(self):
        pass
    def isatty(self):
        return False
    def next(self):
        raise StopIteration()
    def read(self, n):
        return ''
    def readline(self):
        return ''
    def readlines(self):
        return [ ]
    def seek(self, pos):
        pass
    def tell(self):
        return 0
    def write(self, data):
        return None
    def writelines(self, seq):
        return None
    def xreadlines(self):
        return self

def rm_rf(d):
    """Remove directories and their contents, recursively.
    """
    for path in (os.path.join(d,f) for f in os.listdir(d)):
        if os.path.isdir(path):
            rm_rf(path)
        else:
            os.unlink(path)
    os.rmdir(d)


class IncConfigObj(configobj.ConfigObj):
    """Recognize and process '#@include <file>' directives
    transparently. Do not deal with recursive references, i.e.
    ignore directives inside included files.
    """
    def __init__(self, infile, **kw):
        """Take same arguments as ConfigObj, but in the case of a file
        object or filename, process #@include statements in the file.
        """   

        if not(isinstance(infile,str) or hasattr(infile,'read')):
            # not a file: stop
            configobj.ConfigObj.__init__(self, infile, **kw)
            return
        # open file
        if hasattr(infile, 'read'):
            f = infile
            f.seek(0) # rewind to start of file
        else:
            f = file(infile)
        dir_ = os.path.dirname(f.name)
        # Create list of lines that includes the included files
        lines = [ ]
        file_lines = [ ] # tuple: (filename, linenum)
        i = 0
        for line in f:
            # look for include directive
            s = line.strip()
            m = re.match("@include (\"(.*)\"|\'(.*)\'|(\S+))", s)
            if m:
                # This line is an @include.
                # Pick out the group that matched.
                inc_path = filter(None, m.groups()[1:])[0]
                # open the corresponding file
                if not inc_path[0] == '/':
                    inc_path = os.path.join(dir_, inc_path)
                try:
                    inc_file = file(inc_path)
                except IOError:
                    raise IOError("Cannot read file '%s' "
                                  "included from '%s'" % (inc_path, f.name))
                # add contents of file to list of lines
                j = 0
                for line in inc_file:
                    j += 1
                    file_lines.append((inc_file.name, j))
                    lines.append(line)
            else:
                # This is a regular old line
                i += 1
                file_lines.append((f.name, i))
                lines.append(line)
        # Call superclass with list of lines we built
        try:
            configobj.ConfigObj.__init__(self, lines, **kw)
        except configobj.ParseError as E:
            # Report correct file and line on parse error
            m = re.search('line "(\d+)"', str(E))
            if m is None:
                raise
            else:
                #print file_lines
                n = int(m.group(1)) - 1
                filename, lineno = file_lines[n]
                msg = "Invalid line %s in %s: \"%s\"" % (lineno, filename, lines[n].strip())
                raise configobj.ParseError(msg)

    def getHasLoggingSection(self):
        """Return True if configuration had a [logging] section,
        False otherwise.
        """
        return self._has_logging_section

    def _setHasLoggingSection(self, value):
        """Set whether configuration has a [logging] section.
        """
        self._has_logging_section = value

                    
def handleSignals(*siglist):
    """Set up signal handlers.

    Input is a list of pairs of a function, and then a list of signals
    that should trigger that action, e.g.:
       handleSignals( (myfun1, (signal.SIGUSR1, signal.SIGUSR2)),
                      (myfun2, (signal.SIGTERM)) )
    """
    for action, signals in siglist:
        for signame in signals:
            if hasattr(signal, signame):
                signo = getattr(signal, signame)
                signal.signal(signo, action)
       

_TPAT = re.compile("(\d+)\s*([mhds]|minutes?|hours?|days?|seconds?)?$")
_TFAC = { None : 1, 's': 1, 'm':60, 'h': 60*60, 'd': 60*60*24, 
          'seconds': 1, 'minutes':60, 'hours': 60*60, 'days': 60*60*24 ,
          'second': 1, 'minute':60, 'hour': 60*60, 'day': 60*60*24 }
def timeToSec(s):
    """Convert time period to a number of seconds.
    Raise ValueError for invalid time, otherwise
    return a number of seconds.
    """
    s = s.lower()
    m = _TPAT.match(s)
    if m is None:
        raise ValueError("invalid time")
    g = m.groups()
    num = int(g[0])
    factor = _TFAC[g[1]]    
    return num * factor

def check_timeperiod(option, opt, value):
    try:
        return timeToSec(value)
    except ValueError:
        raise OptionValueError(
            "option %s: invalid time period value: %r" % (opt, value))

_BPAT = re.compile("(\d+)\s*(\S+)")
_BFAC = { None : 1, 'b': 1, 'bytes':1, 
          'kb':1024, 'kilobytes':1024,
          'mb':1024*1024, 'megabytes':1024*1024 }
def sizeToBytes(s):
    """Convert a size to a number of bytes
    Return number of bytes or raise ValueError if not parseable.
    """    
    bytes = None
    m = re.match("(\d+)\s*(\S+)", s.lower())
    if not m:
        raise ValueError("Not of form: <num> <units>")
    value, units = m.groups()
    if units not in _BFAC:
        raise ValueError("Unrecognized units for '%s'" % s)
    return int(value) * _BFAC[units]

class ScriptOption(Option):
    TYPES = Option.TYPES + ("timeperiod",)
    TYPE_CHECKER = copy(Option.TYPE_CHECKER)
    TYPE_CHECKER["timeperiod"] = check_timeperiod

def noop(*args, **kwargs):
    """Handy no-operation function.
    """
    pass

def as_bool(x):
    """Convert value (possibly a string) into a boolean.
    """
    if x is True or x is False:
        return x
    if isinstance(x, int):
        return bool(x)
    retval = None
    if isinstance(x, str):
        retval = {
        'yes': True, 'no': False,
        'on': True, 'off': False,
        '1': True, '0': False,
        'true': True, 'false': False,
        }.get(x.lower(), None)
    if retval is None:
        raise ValueError("Cannot convert to bool: %s" % x)
    return retval

def as_list(value, sep=" "):
    """Convert value (possibly a string) into a list.

    Raises ValueError if it's not convert-able.
    """
    retval = None
    if isinstance(value,list) or isinstance(value,tuple):
        retval = value
    elif isinstance(value,str):
        if not value:
            retval = [ ]
        else:
            retval = value.split(sep)
    if retval is None:
        raise ValueError("Cannot convert to list: %s" % value)
    return retval
    
def is_stdout(fname):
    return fname == sys.stdout.name

def parseParams(opt):
    """Parse a set of name=value parameters in the input value.

    Return list of (name,value) pairs.
    Raise ValueError if a parameter is badly formatted.
    """
    params = [ ]
    for nameval in opt:
        try:
            name, val = nameval.split('=')
        except ValueError:
            raise ValueError("Bad name=value format for '%s'" % nameval)
        params.append((name,val))
    return params

def getProgFromFile(f):
    """Get program name from __file__.
    """
    if f.endswith(".py"):
        f = f[:-3]
    return os.path.basename(f)

# Python 2.4-friendly uuid generator
try:
    import uuid
    def uuid1():
        return str(uuid.uuid1())
except ImportError:
    # From: http://aspn.activestate.com/ASPN/Cookbook/Python/Recipe/213761
    import time, random, md5
    def uuid1():
      t = int( time.time() * 1000 )
      r = int( random.random()*100000000000000000 )
      try:
        a = socket.gethostbyname( socket.gethostname() )
      except:
        # if we can't get a network address, just imagine one
        a = random.random()*100000000000000000
      data = str(t)+' '+str(r)+' '+str(a)
      data = md5.md5(data).hexdigest()
      return "%s-%s-%s-%s-%s" % (data[0:8], data[8:12], data[12:16],
                                 data[16:20], data[20:32])

"""
Word-wrap utility functions.
Examples:
   # wrap at 70 cols
   print wrap(dewrap(blob_of_text), 70)
"""

def dewrap(text):
    "Take newlines out of text and normalize whitespace."
    return ' '.join(text.split())

def wrap(text, n, leader=""):
    """Word-wrap text at 'n' columns.
    The 'leader' will be inserted before each new line of text
    after the first.
    """
    if len(text) <= n:
        return text
    else:
        spc = _find_space(text, n)
        if spc < 0:
            return text
        else:
            return text[:spc] + '\n' + leader + \
                   wrap(text[spc+1:].lstrip(), n, leader=leader)

def _find_space(text, maxpos):
    "Find rightmost whitespace position, or -1 if none."
    p = -1
    for ws in (' ', '-'):
        p = max(p, text.rfind(ws, 0, maxpos))
    return p

def process_kvp(option, all={}, _bool={}, type="AMQP"):
    """Process a name=value option.

    Parameters:
    
        - option (str): "name=value" option string
    
    Returns: (key,value)
    
    Raises:  ValueError for bad format or unknown option.
    """
    parts = option.split('=', 1)
    if len(parts) != 2:
        raise ValueError("argument '%s' not in form name=value" % option)
    key, value = parts
    if all and (key not in all):
        raise ValueError("unknown %s option '%s'." % (type, key))
    if key in _bool:
        value = as_bool(value)
    return key, value

def hash_event(e):
    """Generate and return a probabilistically unique hash code
    for event dictionary, 'e'.

    Returns: String (hexdigest) representation of the hash value.
    
    """
    return md5(str(e)).hexdigest()

def stringize(v):
    if isinstance(v,str):
        result = v
    elif isinstance(v, float):
        result = "%f" % v
    elif isinstance(v, int):
        result = "%d" % v
    else:
        result = str(v)
    return result
