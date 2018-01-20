"""
NetLogger interactions with the Python logging module.
"""
__rcsid__ = "$Id: logutil.py 772 2008-05-23 22:59:22Z dang $"

import logging
import logging.handlers
import optparse
import os
import sys
import time
import traceback
import types
#
from Pegasus.netlogger import nlapi
from Pegasus.netlogger.nlapi import Level
from Pegasus.netlogger.version import *

# extra logging levels
TRACE = logging.DEBUG -1

# Top-level qualified name for netlogger
PROJECT_NAMESPACE = "netlogger"

# Global holder of the "correct" NetLogger class
# to use when instantiating new loggers
_logger_class = None

def setLoggerClass(clazz):
    """Set the class used by NetLogger logging
     """
    global _logger_class
    _logger_class = clazz

# consistent with new naming style
set_logger_class = setLoggerClass

def get_logger(filename):
    """
    Return a NetLogger logger with qualified name based on the provied
    filename.  This method is indended to be called by scripts and
    modules by passing in their own __file__ as filename after already
    having initialized the logging module via the NL OptionParser or
    some equivalent action.

    If the logger name starts with a '.', it will be taken as-is, with
    the leading '.' stripped.
    Otherwise, the logger will be rooted at PROJECT_NAMESPACE.

    Parameters:
    filename - The full filename of the NL script or module requesting
    a logger, i.e. __file__
    """
    if filename == "":
        qname = ""
    elif filename[0] == '.':
        qname = filename
    else:
        qname = '.'.join(_modlist(filename))
    return _logger(qname)

def get_root_logger():
    """Return root for all NetLogger loggers.
    """
    return _logger('')# logging.getLogger(PROJECT_NAMESPACE)

def _logger(qualname):
    """
    Return a logger based on the provided qualified name
    Prepend PROJECT_NAMESPACE if not already there, unless
    qualified name starts with a '.'.
    """
    # Mess with qualified name
    if not qualname:
        qualname = PROJECT_NAMESPACE
    elif qualname[0] == '.':
        qualname = qualname[1:]
    elif not qualname.startswith(PROJECT_NAMESPACE):
        qualname = PROJECT_NAMESPACE + '.' + qualname
    # Swap in "my" logger class, create logger, swap back out
    orig_class = logging.getLoggerClass()
    logging.setLoggerClass(_logger_class)
    logger = logging.getLogger(qualname)
    logging.setLoggerClass(orig_class)
    # Return "my" new logger instance
    return logger

def _modlist(path):
    """
    Return a list of module names based on the path provided.  The
    expected path list will be rooted at either "netlogger" or
    "scripts" so won't contain either as one of the module names.  Any
    tailing python extension is also trimmed.
    """
    if path == '/':
        return []
    head, tail = os.path.split(path)
    # ignore python extensions
    if tail.endswith(".py") or tail.endswith(".pyc"):
        tail = os.path.splitext(tail)[0]
    # stop if at top of source tree
    if tail in ('netlogger', 'scripts'):
        return []
    # stop if at root of path
    if head == '' or head == '.':
        return [tail]
    # otherwise continue
    return _modlist(head) + [tail]


class DoesLogging:
    """Mix-in class that creates the attribute 'log', setting its qualified
    name to the name of the module and class.
    """
    def __init__(self, name=None):
        if name is None:
            if self.__module__ != '__main__':
                name = "%s.%s" % (self.__module__, self.__class__.__name__)
            else:
                name = self.__class__.__name__
        self.log = _logger(name)
        # cache whether log is debug or higher in a flag to
        # lower overhead of debugging statements
        self._dbg = self.log.isEnabledFor(logging.DEBUG)
        self._trace = self.log.isEnabledFor(TRACE)

class BPLogger(logging.Logger):
    """Logger class that writes Best-Practices formatted logs.

    Usage:
        The arguments are not exactly the same as for the Logger in
        the logging module. Instead they consist of an event name
        and keywords that are name=value pairs for the event contents.

        An exception to this is the exc() or exception() method,
        which takes an Exception instance as its second argument
        in addition to the event name.

    Example:
        log.info("my.event", value=88.7, units="seconds")
        # output
        # ts=2009-07-24T20:18:04.775650Z event=netlogger.my.event level=INFO units=seconds value=88.700000

    """
    def __init__(self, qualname):
        self._qualname = qualname
        self._format = nlapi.Log(newline=False, level=nlapi.Level.ALL)
        logging.Logger.__init__(self, qualname)

    def set_meta(self, **kw):
        """Set metadata to be logged with every event, e.g.
        an identifier or host name.
        """
        self._format.setMeta(None, **kw)

    def log(self, level, nl_level, event, exc_info=None, **kwargs):
        ts = time.time()
        if self._qualname:
            event = self._qualname + '.' + event
        # replace '__' with '.'
        kwargs = dict([(key.replace('__', '.'), value)
                       for key, value in kwargs.iteritems()])
        # format as BP
        msg = self._format(event, ts, nl_level, **kwargs)
        logging.Logger.log(self, level, msg, exc_info=exc_info)

    def trace(self, event, **kwargs):
        if self.isEnabledFor(TRACE):
            self.log(TRACE, Level.TRACE, event, **kwargs)

    def debug(self, event, **kwargs):
        if self.isEnabledFor(logging.DEBUG):
            self.log(logging.DEBUG, Level.DEBUG, event, **kwargs)

    def info(self, event, **kwargs):
        self.log(logging.INFO, Level.INFO, event,  **kwargs)

    def warning(self, event, **kwargs):
        self.log(logging.WARN, Level.WARN, event,  **kwargs)
    warn = warning

    def error(self, event, **kwargs):
        self.log(logging.ERROR, Level.ERROR, event,  **kwargs)

    def critical(self, event, **kwargs):
        self.log(logging.CRITICAL, Level.FATAL, event,  **kwargs)

    def exception(self, event, err, **kwargs):
        estr = traceback.format_exc()
        estr = ' | '.join([e.strip() for e in estr.split('\n')])
        self.log(logging.ERROR, Level.ERROR, event, msg=str(err),
                 status=-1, traceback=estr, **kwargs)
    exc = exception

class BPSysLogger(BPLogger):
    """This is a hack that prepends a header to the
    output of BPLogger in order to work-around some bug with the
    Python SysLogHandler and Ubuntu rsylog that otherwise splits
    out the first section of the timestamp as part of the header.
    """
    header = "netlogger" # header prefix
    
    def __init__(self, qualname):
        BPLogger.__init__(self, qualname)
        self._orig_format = self._format
        self._format = self.syslog_format
        self._hdr = self.header + ": "

    def set_meta(self, **kw):
        """See set_meta() in superclass.

        Repeated here because superclass method accesses a protected
        attribute that was modified in the constructor.
        """
        self._orig_format.setMeta(None, **kw)

    def flush(self):
        self._orig_format.flush()

    def syslog_format(self, *arg, **kw):
        return self._hdr + self._orig_format(*arg, **kw)

###############################################
## Set BPLogger as default logging class
###############################################
setLoggerClass(BPLogger)

class PrettyBPLogger(BPLogger):
    """Logger class that writes 'pretty' Best-Practices formatted logs.
    This is a variation on BP format. Stack traces logged with the
    method exc() or exception() will be in their original form.

    Usage:
        See Usage notes for BPLogger.

    Example:
        log.info("my.event", value=88.7, units="seconds")
        # output
        # 2009-07-24T20:18:04.716913Z INFO  netlogger.my.event - units=seconds,value=88.7

    """
    def __init__(self, qualname):
        BPLogger.__init__(self, qualname)
        self._format = nlapi.Log(newline=False, level=nlapi.Level.ALL,
                                 pretty=True)

    def exception(self, event, err, **kwargs):
        tbstr = traceback.format_exc()
        self.log(logging.ERROR, Level.ERROR, event, traceback=tbstr, **kwargs)
    exc = exception

class RawBPLogger(logging.Logger):
    """Logger class that does not modify the message, just leaves
    it as a 'raw' dictionary. This is useful for network communication
    that is just going to pickle the event anyways.
    """
    def log(self, level, nl_level, event, exc_info=None, **kwargs):
        ts = time.time()
        if self._qualname:
            event = self._qualname + '.' + event
        # replace '__' with '.'
        kwargs = dict([(key.replace('__', '.'), value)
                       for key, value in kwargs.iteritems()])
        # build msg dictionary
        msg = { 'event': event, 'ts': ts, 'level' : nl_level }
        msg.update(kwargs)
        # 'write' out
        logging.Logger.log(self, level, msg, exc_info=exc_info)

class FakeBPLogger(logging.Logger):
    def __init__(self, qualname):
        logging.Logger.__init__(self, qualname)

    def set_meta(self, **kw):
        pass
            
    def log(self, level, nl_level, event, **kwargs):
        pass

    def trace(self, event, **kwargs):
        pass

    def debug(self, event, **kwargs):
        pass

    def info(self, event, **kwargs):
        pass

    def warning(self, event, **kwargs):
        pass
    warn = warning

    def error(self, event, **kwargs):
        pass

    def critical(self, event, **kwargs):
        pass

    def exception(self, event, err, **kwargs):
        pass
    exc = exception


def profile(func):
    """ decorator for start,end event function profiling with netlogger.
    """
    if os.getenv('NETLOGGER_ON', False) in (
        'off','0','no','false','',False):    
        return func    
    if not isinstance(func, types.FunctionType):
        return func
    
    if func.__module__ == '__main__':
        f = func.__globals__['__file__'] or 'unknown'
        event = '%s' %os.path.splitext(os.path.basename(f))[0]
        log = _logger('script')
        log.set_meta(file=f, pid=os.getpid(), ppid=os.getppid(), gpid=os.getgid())
    else:
        event = '%s' %func.__name__
        log = _logger('%s' %func.__module__)
        log.set_meta(pid=os.getpid(), ppid=os.getppid(), gpid=os.getgid())

    def nl_profile_func(*args, **kw):
        log.debug("%s.start" %event)
        try:
            v = func(*args, **kw)
        except:
            log.error("%s.end" %event)
            raise
        log.debug("%s.end" %event)
        return v

    return nl_profile_func

def profile_result(func):
    """ decorator for start,end event function profiling with netlogger.
    return value is logged as result.
    """
    if os.getenv('NETLOGGER_ON', False) in (
        'off','0','no','false','',False):    
        return func
    if not isinstance(func, types.FunctionType):
        return func
    
    if func.__module__ == '__main__':
        f = func.__globals__['__file__'] or 'unknown'
        event = '%s' %os.path.splitext(os.path.basename(f))[0]
        log = _logger('script')
        log.set_meta(file=f, pid=os.getpid(), ppid=os.getppid(), gpid=os.getgid())
    else:
        event = '%s' %func.__name__
        log = _logger('%s' %func.__module__)
        log.set_meta(pid=os.getpid(), ppid=os.getppid(), gpid=os.getgid())

    def nl_profile_func(*args, **kw):
        log.debug("%s.start" %event)
        try:
            v = func(*args, **kw)
        except:
            log.error("%s.end" %event)
            raise
        log.debug("%s.end" %event, result=v)
        return v

    return nl_profile_func

class Profiler(type):
    """ metaclass that will wrap all user defined methods with start and end event logs.
    Currently wrapping only instancemethod type.
    
    Variables:
        profiler_skip_methods: list of methods profiler will skip 
        profile_skip_all: profiler will not wrap any methods
        _log: Logging object to use with class
    
    Usage:
        class MyClass:
            __metaclass__ = Profiler
            profiler_skip_methods = ['__init__', 'getsecret']
            profiler_skip_all = False
    """
    profiler_skip_methods = ['__init__']
    profiler_skip_all = False

    @staticmethod
    def __profile_method(func):
        """ decorator for start,end event method profiling with netlogger
        skips any classmethod or staticmethod types.
        """
        if not isinstance(func, types.FunctionType):
            return func
        
        event = '%s' %func.__name__
        def nl_profile_method(self, *args, **kw):
            self._log.debug("%s.start" %event)
            try:
                v = func(self, *args, **kw)
            except:
                self._log.error("%s.end" %event)
                raise
            self._log.debug("%s.end" %event)
            return v
    
        return nl_profile_method  
    
    def __new__(cls, classname, bases, classdict):
        if os.getenv('NETLOGGER_ON', False) in (
            'off','0','no','false','',False):
            setLoggerClass(FakeBPLogger)
            classdict['_log'] = _logger('%s.%s' % (
                    classdict['__module__'],classname))
            return type.__new__(cls,classname,bases,classdict)
        
        classdict['_log'] = log = _logger(
            '%s.%s' %(classdict['__module__'],classname))
        log.set_meta(pid=os.getpid(), ppid=os.getppid(), gpid=os.getgid())
        keys = []
        if not classdict.get('profiler_skip_all',cls.profiler_skip_all):
            keys = [k for k in classdict.keys() if isinstance(classdict[k], types.FunctionType) and k not in 
                          classdict.get('profiler_skip_methods',cls.profiler_skip_methods)]

        for k in keys:
            classdict[k] = cls.__profile_method(classdict[k])

        return type.__new__(cls,classname,bases,classdict)


class MethodProfiler(Profiler):
    """ metaclass that will wrap all user defined methods with start and end event logs.
    Currently wrapping only instancemethod type.
    """
    profiler_skip_all = False

class BasicProfiler(Profiler):
    """ metaclass does not wrap methods with 'start' and 'end' tags, to do that use 'Profiler'.  
    Useful for classes where one only wants to do 'precision' logging.
    """
    profiler_skip_all = True    
    
    
class OptionParser(optparse.OptionParser):
    """Set logging 'tude for scripts.

    Usage:
       parser = NLOptionParser(..., can_be_daemon=True/False)
       # add rest of options to 'parser'...
       # This next line sets up logging as a side-effect
       parser.parse_args()
       # rest of program ..

    *******************************************************
    | Pseudo-code description of logic to determine which |
    | types of logs to produce, and where to send them    |
    *******************************************************
    Variables:
        D - daemon mode [True | False]
        L - log file [Missing | Empty | filename]
    Logic:
      if (D) then
          case (L = Missing)
            error!
          case (L = Empty)
            error!
          case (L = filename)
            stderr -> filename
            BP logs -> filename
      else
          case (L = Missing)
            stderr -> stderr
            Pretty logs -> stderr
          case (L = Empty)
            stderr -> stderr
            BP logs -> stderr
          case (L = filename)
            stderr -> stderr
            BP logs -> filename

    *******************************************************
    | Pseudo-code description of logic for verbosity      |
    *******************************************************
    Variables:
        V - verbosity [0 .. N]
        Q - quiet [True | False]
    Logic:
        if (Q) then
            case (V > 0)
                error!
            else
                set verbosity -> OFF
        else
            case V = 0
                set verbosity -> WARN
            case V = 1
                set verbosity -> INFO
            case V = 2
                set verbosity -> DEBUG
            case V >= 3
                set verbosity -> TRACE

    """
    # Attribute (option parser 'dest') names
    DEST_LOG = "log_file"
    DEST_VERBOSE = "verbose"
    DEST_QUIET = "quiet"
    DEST_DAEMON = "daemon"
    DEST_ROT = "log_rotate"

    # Option names, by attribute
    OPTIONS = { DEST_LOG : ('-L', '--log'),
                DEST_VERBOSE : ('-v', '--verbose'),
                DEST_QUIET : ('-q', '--quiet'),
                DEST_DAEMON : (None, '--daemon'),
                DEST_ROT : ('-R', '--logrotate'),
                }

    # Verbosity (number of -v's) to logging level
    VBMAP = (logging.WARN, logging.INFO, logging.DEBUG, TRACE)

    def __init__(self, can_be_daemon=False, **kwargs):
        """Add logging-related command-line options
        to an option parser.

        Parameters:
            can_be_daemon - if True, add an option for daemonizing
            kwargs - additional keywords for OptionParser.
                     The 'version' argument will override the default
                     version
        """
        if 'version' not in kwargs:
            version_str = "%%prog, NetLogger Toolkit version: %s\n  %s" % (
                NL_VERSION, NL_CREATE_DATE)
            version_str += "\n\n" + NL_COPYRIGHT
            kwargs['version'] = version_str
        optparse.OptionParser.__init__(self, **kwargs)
        self._dmn = can_be_daemon

    def _add_options(self):
        group = optparse.OptionGroup(self, "Logging options")
        if self._dmn:
            self.add_option(self.OPTIONS[self.DEST_DAEMON][1],
                            action='store_true',
                            dest=self.DEST_DAEMON,
                            default=False,
                            help="run in daemon mode")
            logfile_default = "required"
        else:
            logfile_default = "default=stderr"
        group.add_option(self.OPTIONS[self.DEST_LOG][0],
                         self.OPTIONS[self.DEST_LOG][1],
                         default=None,
                         action='store',
                         dest=self.DEST_LOG,
                         metavar='FILE',
                         help="write logs to FILE (%s)" % logfile_default)
        group.add_option(self.OPTIONS[self.DEST_ROT][0],
                         self.OPTIONS[self.DEST_ROT][1],
                         default=None,
                         action='store',
                         dest=self.DEST_ROT,
                         metavar='TIME',
                         help="rotate logs at an interval (<N>d or <N>h or <N>m)")
        group.add_option(self.OPTIONS[self.DEST_VERBOSE][0],
                         self.OPTIONS[self.DEST_VERBOSE][1],
                         action="count",
                         default=0,
                         dest=self.DEST_VERBOSE,
                         help="more verbose logging")
        group.add_option(self.OPTIONS[self.DEST_QUIET][0],
                         self.OPTIONS[self.DEST_QUIET][1],
                         action="store_true",
                         default=False,
                         dest=self.DEST_QUIET,
                         help="quiet mode, no logging")
        self.add_option_group(group)

    def check_required (self, opt):
        """Simplify checks for required values.
        The default value for a required option must be None.
        The easiest way to achieve this is not to provide a default.
        Call error() if the required option is not present.
        """
        option = self.get_option(opt)
        # Assumes the option's 'default' is set to None!
        if getattr(self.values, option.dest) is None:
            self.error("%s option not supplied" % option)

    def parse_args(self, args=None):
        """Process command-line options.

        Parameters:
            args - same as OptionParser.parse_args
        Return:
            True if all went well, False if not
        Post-conditions:
            If the return was True, logging levels and handlers
            are properly set for qualified name 'netlogger'.
            Otherwise, an error will be reported via the
            'error' method of the parser passed to the constructor.
        """
        if args is None:
            args = sys.argv[1:]
        self._add_options()
        options, pargs = optparse.OptionParser.parse_args(self, args)
        # Where and in what format to write logs
        if self._dmn:
            is_daemon = getattr(options, self.DEST_DAEMON)
        else:
            is_daemon = False
        logfile = getattr(options, self.DEST_LOG, None)
        logrot = getattr(options, self.DEST_ROT, None)
        if ((not logfile) or logfile == '-') and logrot:
            self.error("Log rotation requires a logfile")
        if logrot:
            if len(logrot) < 1:
                self.error("Bad log rotation interval, too short")
            tm_unit = logrot[-1].lower()
            if tm_unit not in ('h', 'm', 'd'):
                self.error("Bad log rotation unit '%s' "
                           "not in m,h,d" % tm_unit)
            try:
                tm_interval = int(logrot[:-1])
            except ValueError:
                self.error("Log rotation value '%s' must be an integer" %
                           logrot[:-1])
            do_logrot = True
            _tfrh = logging.handlers.TimedRotatingFileHandler
        else:
            do_logrot = False
        log = logging.getLogger(PROJECT_NAMESPACE)
        # Set handler and logger class
        handler = None
        if is_daemon:
            if logfile is None or logfile == '' or logfile == '-': # missing/empty
                self.error("log file is required in daemon mode")
                return # defensive
            else:
                # stderr and BP logs -> logfile
                setLoggerClass(BPLogger)
                logfile = logfile.strip()
                try:
                    if do_logrot:
                        handler = _tfrh(logfile, when=tm_unit, interval=tm_interval)
                    else:
                        handler = logging.FileHandler(logfile)
                except IOError as err:
                    self.error("Cannot open log file '%s': %s" % (logfile, err))
                sys.stderr = handler.stream
                handler.setFormatter(logging.Formatter("%(message)s"))
        else:
            if logfile is None or logfile == '': # missing
                # Pretty-BP logs -> stderr
                setLoggerClass(PrettyBPLogger)
                handler = logging.StreamHandler()
            elif logfile.strip() == '-': # empty
                # BP logs -> stderr
                setLoggerClass(BPLogger)
                handler = logging.StreamHandler()
            else:
                # BP logs -> logfile
                logfile = logfile.strip()
                setLoggerClass(BPLogger)
                try:
                    if do_logrot:
                        handler = _tfrh(logfile, when=tm_unit, interval=tm_interval)
                    else:
                        handler = logging.FileHandler(logfile)
                except IOError as err:
                    self.error("Cannot open log file '%s': %s" % (logfile, err))
                handler.setFormatter(logging.Formatter("%(message)s"))
        if handler:
            log.addHandler(handler)
        # Verbosity level
        quiet = getattr(options, self.DEST_QUIET, False)
        #delattr(options, self.DEST_QUIET)
        vb = getattr(options, self.DEST_VERBOSE, 0)
        #delattr(options, self.DEST_VERBOSE)
        if quiet and (vb > 0):
            self.error("quiet and verbosity options conflict")
            return # defensive
        if quiet:
            log.setLevel(logging.CRITICAL + 1)
        else:
            log.setLevel(self.VBMAP[min(vb, len(self.VBMAP) - 1)])
        # Return remaining options and args to caller
        return options, pargs
