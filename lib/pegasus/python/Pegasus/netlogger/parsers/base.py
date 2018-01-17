"""
Common code for NetLogger parsers
"""
__author__ = 'Dan Gunter <dkgunter@lbl.gov>'
__rcsid__ = '$Id: base.py 28287 2011-08-18 03:42:53Z dang $'

import calendar 
from Pegasus.netlogger.configobj import ConfigObj, Section
import glob
import imp
from itertools import starmap
import os
from Queue import Queue, Empty
import re
from select import select
import sys
import time
#
try:
    from pyparsing import Word, alphanums, CharsNotIn, ZeroOrMore
    from pyparsing import Group, Literal
    from pyparsing import StringEnd, White, QuotedString, ParseException
    from pyparsing import Each, OneOrMore, Optional, oneOf
    HAVE_PYPARSING = True
except ImportError:
    HAVE_PYPARSING = False
#
from Pegasus.netlogger import nldate
from Pegasus.netlogger.nllog import DoesLogging
from Pegasus.netlogger import nlapi
from Pegasus.netlogger.nlapi import Log, Level
from Pegasus.netlogger.nlapi import TS_FIELD, EVENT_FIELD, HASH_FIELD
from Pegasus.netlogger.parsers import nlreadline
from Pegasus.netlogger.util import hash_event

# Special result code for parsers to return
# when they 'on purpose' skip a line
LINE_SKIPPED = 999

try:
    from hashlib import md5
    md5_new = md5
except ImportError:
    import md5
    md5_new = md5.new

def getGuid(*strings):
    m5 = md5_new()
    for s in strings:
        m5.update(s)
    t = m5.hexdigest()
    guid = "%s-%s-%s-%s-%s" % (
        t[:8], t[8:12], t[12:16], t[16:20], t[20:])
    return guid

def autoParseValue(vstr):
    try:
        value = int(vstr)
    except ValueError:
        try:
            value = float(vstr)
        except ValueError:
            value = vstr
    return value

def parse_ts(ts):
    "Parse a netlogger timestamp"
    # until 2033, a '1' in the first place means a float
    if ts[0] == '1':
        return float(ts)
    ts, subs = ts.split('.')
    subs = float('.' + subs[:-1])
    return calendar.timegm(time.strptime(ts, r'%Y-%m-%dT%H:%M:%S')) + subs 

parseDate = parse_ts

def tolerateBlanksAndComments(line=None, error=None, linenum=0):
    """Callback function fitting the signature of the callback
    expected by NLBaseParser.parseStream() that re-raises the error unless
    the line is empty or starts with a hash character, in which
    case it does nothing.
    """
    if len(line) <= 1 or line[0] == '#':
        pass
    else:
        raise(error)



# Parse BP log lines with Regular Expressions

class BPError(ValueError):
    """
    Exception class to indicate violations from the logging best practices
    standard.
    """
    def __init__(self, lineno, msg):
        """Create error object.

        Arguments:
          lineno - The line number on which the error occured.
          msg    - The error message.
        """
        self.lineno = lineno
        self.msg = msg

    def __str__(self):
        return "Parser error on line %i: %s" % (self.lineno, self.msg)

class BPValidationError(BPError):
    def __init__(self, msg):
        self.msg = msg
    def __str__(self):
        return "Validation error: " + str(self.msg)

class BPParseError(BPError):
    def __init__(self, msg):
        self.msg = msg
    def __str__(self):
        return "Parse error: " + str(self.msg)

"""
Regular expression that captures names and data
"""
BP_EXPR = re.compile(r"""
(?:
    \s*                        # leading whitespace
    ([0-9a-zA-Z_.\-]+)         # Name
    =
    (?:                        # Value:
      ([^"\s]+) |              # a) simple value
      "((?:[^"] | (?<=\\)")*)" # b) quoted string
    )
    \s*
)
""", flags = re.X)

"""
For validation, regular expression that captures all valid characters,
so by simply comparing with string length we can tell if there are invalid
characters in the string.
"""
BP_EXPR_WS = re.compile(r"""
(?:
    (\s*)                      # leading whitespace
    ([0-9a-zA-Z_.\-]+)         # Name
    =
    (?:                        # Value:
      ([^"\s]+) |              # a) simple value
      (")((?:[^"] | (?<=\\)")*)(") # b) quoted string
    )
    (\s*)
)""", flags=re.X)
         
def _bp_extract(s, as_dict=True, validate=False):
    """Parse BP log line and extract key=value pairs.

    If validate is False, this will skip over many types of "junk" data.
    The only escape sequence recognized is a backslash-escaped double-quote,
    within a quoted value.

    Args:
      s - Input line
      as_dict - If True, return a dictionary, otherwise, a list of tuples.
                Note: when OrderedDict (Python 3.1+, 2.7+) becomes common,
                this will go away.
      validate - If True, see if there are any 'extra' characters in the string.
                 If there are, raise a BPValidationError.
    Raises:
      BPParseError - If a token doesn't contain a recognizable name=value pair,
      BPValidationError - If (optional) validation fails.
    Returns:
      Dictionary or list of tuples (see `as_dict` arg)
    """
    if as_dict:
        result = { }
    else:
        result = [ ]
    if not validate:
        for n, v, vq in BP_EXPR.findall(s):
            # check input
            if not n:
                raise BPParseError("Bad key: '{0}'".format(n))
            # add to result
            if vq:
                v = vq.replace('\\"', '"')
            if as_dict:
                result[n] = v
            else:
                result.append((n,v))
    else:
        valid_data_len = 0
        for ws1, n, v, q1, vq, q2, ws2 in BP_EXPR_WS.findall(s):
            #print(",".join(["<"+x+">" for x in (ws1, n, v, q1, vq, q2, ws2)]))
            # check input
            if not n:
                raise BPParseError("Bad key: '{0}'".format(n))
            valid_data_len += sum(map(len, (ws1, n, v, q1, vq, q2, ws2))) + 1 # 1 for '='
            # add to result
            if vq:
                v = vq.replace('\\"', '"')
            if as_dict:
                result[n] = v
            else:
                result.append((n,v))
        # check overall input
        junk_chars = len(s) - valid_data_len
        if junk_chars != 0:
            raise BPValidationError("{0:d} junk chars in '{1}'".format(junk_chars, s))
    return result


class ProcessInterface:
    """Process interface for a parser
    """
    def process(self, line):
        """Subclasses must override this method to return a list
        of dictionaries or formatted log strings (with newlines).

        If there is an error with the format, they should
        raise a ValueError, KeyError, or this module's ParseError.
        If nothing is yet ready, return an empty list or tuple.
        To cause the caller to stop parsing this log, i.e. nothing will
        ever be ready, return None.
        """
        pass


class BaseParser(ProcessInterface, DoesLogging):
    """Base class for all other Parser classes in the parser modules.

    Uses iterator protocol to return one result at a time; where a
    result is a Best Practices log line (a string).
    Calls read(file) on the subclass to actually get and parse data.
    Each read() can return multiple events,
    or multiple read()s can return one event, transparently
    to the caller who only sees one log line per iteration.
    If 'unparsed_file' is not None, write all lines that returned an 
    error, or the constant LINE_SKIPPED, to this file.

    Parameters:
      - add_hash {yes,no,no*}: To each output event, add a new field,
           '_hash', which is a probabilistically unique (MD5) hash of all
           the other fields in the event.
    """
    def __init__(self, input_file, fullname='unknown', 
                 unparsed_file=None, parse_date=True,
                 add_hash='no',
                 **kw):
        """Initialize base parser.

        Parameters:

            input_file - File object (must support readline)

            fullname - For logging, the fully qualified name
                  for the logger (matches 'qualname' in the logging config).

            unparsed_file - File object to place records that caused
                  a parse exception

            parse_date - Whether to parse the ISO date to a number
                         or represent it as a string.

            **kw - Remaining keyword, value pairs are appended to each
                  line of the log. If the same keyword is in a
                  parsed result, the newer value takes precedence.
                  The exception to this is if the parser returns a string
                  instead of a dictionary, e.g. the 'bp' parser:
                  to avoid O(N*M) behavior where N is the number of
                  the keywords and M is the length of the output string,
                  duplicates are not checked.
        """
        if not input_file:
            raise ValueError("input file cannot be empty")
        DoesLogging.__init__(self, fullname)
        # common parameters
        self._add_hash = self.boolParam(add_hash)
        # rest of parameters
        self._infile = nlreadline.BufferedReadline(input_file)
        if hasattr(input_file, 'fileno'):
            self._fake_file = False
            self._infile_rlist = (input_file.fileno(),) # used in read_line
        else:
            # not a real file
            self._fake_file = True
            self._infile_rlist = ()
        try:
            self._offs = self._infile.tell()
        except IOError:
            self._offs = 0
        self._prev_len, self._saved_len = 0, 0
        self._saved = [ ]
        self._name = fullname
        self._ufile = unparsed_file
        self._header_values = { }
        self._parser = NLSimpleParser(parse_date=parse_date)
        # Constant to add to each record
        self._const_nvp = { }
        # add GUID in env, if present
        guid = nlapi.getGuid(create=False)
        if guid:
            self._const_nvp['guid'] = guid
        # add user-provided values (can override guid)
        self._const_nvp.update(kw)
        # cache string-valued version, will be empty string if kw == {}
        self._const_nvp_str = ' '.join(["%s=%s" % (k,v) 
                                        for k,v in self._const_nvp.items()])
        self.parse_date = parse_date

    def close(self):
        if self._infile:
            self._infile.close()

    def getFilename(self):
        if self._infile:
            return self._infile.name
        else:
            return ""

    def getOffset(self):
        """Return the offset of the last entirely parsed line.

        In the case of a single line that returned multiple items,
        all of which haven't yet been yet consumed, return the offset at
        the start of this line. This avoids dropping events at the
        expense of possible duplicates.

        It is best to call flush() first to avoid this issue entirely.

        """
        return self._offs

    def setOffset(self, offs):
        """Explicitly set offset.

        This is not normally necessary, as the next() function will
        advance self._offs every time all the resulting items from
        the associated input line have been returned.
        """
        self._infile.seek(offs)
        self._offs = offs

    def getParameters(self):
        """Subclasses should override this method to return
        a dictionary, with all basic types, representing any additional
        state that needs to be saved and restored.
        """
        return { }

    def setParameters(self, param):
        """Subclasses should override this method to update their
        state with the contents of the arg 'param', a dictionary.
        """
        pass

    def setHeaderValues(self, value_dict):
        """Set a dictionary of header keyword, value pairs.
        """
        self._header_values = value_dict

    def getHeaderValue(self, key):
        """Get value from group named 'key', or None.
        """
        return self._header_values.get(key, None)

    def __iter__(self):
        return self

    def next(self):
        """
        Return one saved or new item.

        Get new item(s) by reading and parsing the file.
        Return None if no result, so caller can
        count how many lines were processed and thus
        do fair sharing across multiple inputs
        """
        self.updateOffset()
        # get an item to return
        if self._saved:
            # multiple items were returned before, so just return one
            item = self._saved.pop()
            # if saved is now empty, then we have processed
            # all the items from the last readline, so
            # advance offset by its (saved) length
            if not self._saved:
                self._prev_len = self._saved_len 
        else:
            line = self._read_line()
            # stop if line is empty
            if line == '':
                raise StopIteration
            item = line.strip()
            # main processing for the module
            try:
                result = self.process(item)
            except (ValueError, KeyError) as E:
                if self._ufile:
                    self._ufile.write(line)
                else:
                    self.log.warn("unparsed.event", 
                                  value=line.strip(), msg=E)
                result = False
            # A skipped line means that it will never
            # be parsed, but there was no error.
            # Like an error, it is written to the unparsed-events
            # file (if that exists).
            if result == LINE_SKIPPED:
                if self._ufile:
                    self._ufile.write(line)
                result = False
            if not result:
                self._offs += len(line)
                if result is None:
                    raise StopIteration("EOF")
                item = None # return this to caller
            else:
                item = result[0]
                if len(result) == 1:
                    # advance offset by this on next call
                    self._prev_len = len(line)
                else:
                    # don't advance offset until all results are returned
                    self._saved = list(result[1:])
                    self._saved.reverse() # so we can pop()
                    self._saved_len = len(line)
        # return the item
        if item is None:
            return None
        else:
            return self._result(item)

    def _read_line(self):
        """Read one line.
        """
        if self._trace:
            self.log.trace("readline.start")
        if self._fake_file or select(self._infile_rlist, (), (), 0.1)[0]:
            line = self._infile.readline()
        else:
            line = ''
        if self._trace:
            self.log.trace("readline.end", n=len(line))
        return line

    def updateOffset(self):
        """Advance offset by length previously parsed input.
        """
        self._offs += self._prev_len
        self._prev_len = 0 # do not add this to offset again

    def _result(self, item):
        """Make item into a returnable result.
        Normalize 'level' and add constant attributes.
        Also run any postprocessing steps, such as adding a hash.
        """
        # Parse if a string
        if isinstance(item, str):
            item = self._parser.parseLine(item)
        # Normalize the 'level' value
        if 'level' in item:
            level = item['level']
            if hasattr(level, 'upper'):
                lvlname = item['level'].upper()
                item['level'] = Level.getLevel(lvlname)
        # Add constant key, value pairs: do a copy and 
        # reverse update so new values override old ones.
        if self._const_nvp:
            _tmp = self._const_nvp.copy()
            _tmp.update(item)
            item = _tmp
        # Do post-processing.
        if self._add_hash:
            item[HASH_FIELD] = hash_event(item)
        # Done
        return item

    def flush(self):
        """Return a list of all saved items, i.e., of all items 
        that were parsed but not returned yet, and clear this list.
        """
        result = [ ]
        # pending items, returned by parser
        for item in self._saved:
            result.append(self._result(item))
        self._saved = [ ]
        self._offs += self._saved_len
        self._saved_len, self._prev_len = 0, 0
        return result

    def done(self):
        """Close internal state in parser.
        Useful if the parser is waiting for something, but hits EOF.
        """
        # items waiting in parser
        for item in self.finalize():
            self._saved.append(item)

    def finalize(self):
        """Any events to return at end of parse.
        Default is an empty list.
        """
        return ()

    def boolParam(self, s):
        """Convert a possibly-string-valued boolean parameter, from
        the configuration file, into a proper boolean value.
        """
        if isinstance(s, bool):
            return s
        if isinstance(s, str):
            sl = s.lower()
            if sl in ('yes', 'on', 'true', '1'):
                return True
            else:
                return False
        return bool(s)

    def __str__(self):
        return "%s(%s)" % (self._name, self._infile)

        
class NLBaseParser(BaseParser):
    def __init__(self, input_file=None, err_cb=None, **kw):
        """Create a NetLogger parser, that implements the BaseParser
        interface as well as its own API for parsing individual lines
        and streams with user-supplied callbacks.

        Arguments:
          input_file - File to be parsed
          err_cb - Optional callback on errors. Signature:
                     err_cb(line=<string with newline>, error=<Exception>,
                            linenum=<integer line number>)
                   If set to a function, the function is called.
                   If set to False, errors are completely ignored.
                   If set to True, errors are appended to err_list.
                   If None (the default) errors are propagated to the caller.
                           These errors are of type BPError.
        """
        self.err_cb = err_cb
        self.err_list = [ ]
        if input_file is None:
            input_file = NullFile()
        BaseParser.__init__(self, input_file, fullname='NLParser',  **kw)

    def parseLine(self, line):
        """Return a dictionary corresponding to the name,value pairs
        in the input 'line'.
        Raises ValueError if the format is incorrect.
        """
        pass

    def parseStream(self):
        """Parse input stream, calling parseLine() for each line.

        Return:
           generator function, yielding the result of parseLine() for
           each line in the input stream
        """
        for line_num, line in enumerate(self._infile):
            try:
                d = self.parseLine(line)
                yield d
            except ValueError as E:
                if self.err_cb is False:
                    pass
                else:
                    bpe = BPError(line_num, E)
                    if self.err_cb is True:
                        self.err_list.append(bpe)
                    elif self.err_cb is None:
                        raise bpe
                    else:
                        self.err_cb(line=line, error=bpe, linenum=line_num)

class NLSimpleParser(DoesLogging):
    """Simple, fast, not-too-flexible NL parser.

    Does *not* inherit from NLBaseParser.
    This is important to allow the BaseParser to itself parse 
    netlogger input.
    """
    def __init__(self, verify=False, parse_date=True, **kw):
        DoesLogging.__init__(self)
        self.verify, self.parse_date = verify, parse_date

    def parseLine(self, line):
        """Parse a BP-formatted line.
        For lines which are completely whitespace, raise BPParseError
        """
        try:
            fields = _bp_extract(line, validate=self.verify)
        except BPError as err:
            raise BPParseError("BP parse error: " + str(err))
        # higher-level verification
        for key in TS_FIELD, EVENT_FIELD:
            if key not in fields:
                raise BPParseError("missing required key '{0}'".format(key))
        # Pre-process date, if requested
        if self.parse_date:
            fields[TS_FIELD] = parse_ts(fields[TS_FIELD])
        # Done.
        return fields

class NLFastParser(NLSimpleParser, NLBaseParser):
    """NetLogger parser that does inherit from NLBaseParser
    Simpler, faster, less flexible NL parser.

    * Optionally does some error-checking, but can't tell why it is wrong.
    Note: error-checking takes 50% or so longer.
    * Uses regular expressions instead of pyparsing.
    * Observed speedups on order of 50x (YMMV).
    """
    def __init__(self, *args, **kw):
        # strip out kw for simple parser
        simple_parser_kw = { }
        for key, value in kw.items():
            if key in ('verify', 'parse_date', 'strip_quotes'):
                simple_parser_kw[key] = value
        for key in simple_parser_kw.keys():
            if key != "parse_date": #shared
                del kw[key]
        NLSimpleParser.__init__(self, **simple_parser_kw)
        NLBaseParser.__init__(self, *args, **kw)

    # implementation of the BaseParser API
    def process(self, line):
        return (self.parseLine(line),)

if HAVE_PYPARSING:
    class NLPyParser(NLBaseParser):
        """pyparsing--based implementation of the NLBaseParser
        """
        notSpace = CharsNotIn(" \n")
        eq = Literal('=').suppress()
        value = (QuotedString('"', escChar=chr(92), unquoteResults=False) \
                     ^ OneOrMore(notSpace))
        ts = Group(Literal('ts') + eq + value)
        event = Group(Literal('event') + eq + value)
        name = ~oneOf("ts event") + Word(alphanums +'-_.')
        nv = ZeroOrMore(Group(name + eq + value))
        nvp = Each([ts, event, nv]) + White('\n').suppress() + StringEnd()

        def parseLine(self, line):
            try:
                rlist = self.nvp.parseString(line).asList()
            except ParseException as E:
                raise ValueError(E)
            result = {}
            for a in rlist:
                if self.parse_date and a[0] == 'ts':
                    result[a[0]] = parse_ts(a[1])
                else:
                    result[a[0]] = a[1]
            return result

    # implementation of the BaseParser API
    def process(self, line):
        return (self.parseLine(line),)

else:
    class NLPyParser:
        BADNESS = """
Can't use the NLPyParser class because pyparsing is not installed. 
You can use NLFastParser instead, run 'easy_install pyparsing', or 
install from http://pyparsing.wikispaces.com/ .
"""
        def __init__(self, *args, **kw):
            raise NotImplementedError(self.BADNESS)

class NullFile:
    def __init__(self, *args):
        return
    def read(self, n): return ''
    def readline(self): return ''
    def seek(self, n, mode): pass
    def tell(self): return 0

def getTimezone(t=None):
    """Return current timezone as UTC offset,
    formatted as [+/-]HH:MM
    """
    hr, min, sign = nldate.getLocaltimeOffsetParts(t)
    return "%s%02d:%02d" % (sign, hr, min) 
