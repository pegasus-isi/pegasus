from __future__ import absolute_import
import calendar
from . import magicdate
import re
import time

ISO_DATE_PARTS = re.compile( "(\d\d\d\d)(?:-(\d\d)(?:-(\d\d)(?:T(\d\d)(?::(\d\d)(?::(\d\d)(?:\.(\d+))?)?)?)?)?)?(Z|[+-]\d\d:\d\d)?" )
ISO_DATE_ZEROES = (None, '01', '01', '00', '00', '00', '0')

NUMBER_DATE = re.compile("(\d+)(?:\.(\d+))?")

# Date format constants
UNKNOWN = "Unknown"
ISO8601 = "ISO8601 date"
ENGLISH = "Natural English language date"
SECONDS = "Seconds since UNIX epoch"

DATE_FMT = "%04d-%02d-%02dT%02d:%02d:%02d"

class DateFormatError(Exception):
    pass

def getLocaltimeOffsetSec(t=None):
    """Return current localtime offset at time 't' (default=now)
    from UTC as a number of seconds.
    """
    if t is None:
        t = time.time()
    # this doesn't handle DST properly
    #offs_sec = time.mktime(time.localtime(t)) - time.mktime(time.gmtime(t))
    # this does:
    offs_sec = calendar.timegm(time.localtime(t)) - time.mktime(time.localtime(t))
    return offs_sec

def getLocaltimeOffsetParts(t=None):
    """Return current localtime offset at time 't' (default=now) 
    from GMT as a triple: 
       hours, minutes, sign (1 for positive or 0, -1 for negative)
    """
    offs_sec = getLocaltimeOffsetSec(t)
    offs_hr = int(offs_sec / 3600)
    offs_min = int((offs_sec - (offs_hr * 3600)) / 60)
    sign = (1, -1)[offs_hr < 0]
    return abs(offs_hr), abs(offs_min), sign

def getLocaltimeISO(t):
    """Get an ISO8601 string for the local timezone at time 't', where 
    t is either seconds since the epoch as a float, or a tuple like the one
    returned by time.localtime(), or a string date

    Return the string, which may be [+-]HH:MM or 'Z'.
    """
    # If 't' is a list, tuple or string, first convert to seconds
    if isinstance(t, list) or isinstance(t, tuple) or isinstance(t,str):
        # Convert a string to a list
        if isinstance(t,str):
            parts = splitISODate(t)
        else:
            # If it's a tuple or list, just assign
            parts = t
        # From the tuple or list, calculate time offset for the
        # same year, month, day, and hour.
        tm = (int(parts[0]), int(parts[1]), int(parts[2]),
              int(parts[3]), 0, 0, 0, 1, -1)
        t = time.mktime(tm)
    # Given the time, figure out the timezone offset
    hr, min, sign = getLocaltimeOffsetParts(t)
    # Format the offset as an ISO string
    if hr == 0 and min == 0:
        s = 'Z'
    else:
        s = "%s%02d:%02d" % (('','+', '-')[sign], hr, min)
    return s

def splitISODate(s):
    """Split an ISO date into parts like those returned by localtime()

    Raises a DateFormatError if the date couldn't be split.

    Returns the list of parts.
    """
    m = ISO_DATE_PARTS.match(s)
    if not m:
        raise DateFormatError("Not a partial ISO date: %s" % s)
    parts = list(m.groups())
    # Fill in missing parts with 'zero'
    for i, part in enumerate(parts[:-1]):
        if part is None:
            parts[i] = ISO_DATE_ZEROES[i]
    return parts

def completeISO(s, is_gmt=False, set_gmt=False):
    """Make a partial ISO8601 date into a full ISO8601 date.

    If 'gmt' is True, assume timezone is GMT when not given.
    Otherwise, assume localtime.
    """
    parts = splitISODate(s)
    # add timezone
    iso_str = None
    if parts[7] is None:
        if is_gmt and set_gmt:
            tz = 'Z'
        elif not is_gmt and not set_gmt:
            tz = getLocaltimeISO(parts)
        else:
            # adjust time GMT to localtime or localtime to GMT
            # easiest at this point to just format string from time
            p = list(map(int, parts[:-1] + [0, -1]))
            if set_gmt:
                t = time.mktime(p)
                iso_str = utcFormatISO(t)
            else:
                t = calendar.timegm(p)
                iso_str = localtimeFormatISO(t)
    else:
        # explicit timezone overrides
        tz = parts[7]
    if iso_str is None:
        iso_str = '-'.join(parts[:3]) + 'T' + ':'.join(parts[3:6]) + '.' + parts[6] + tz
    return iso_str

def parseISO(s):
    """Parse ISO8601 (string) date into a floating point seconds since epoch UTC.

    The string must be an ISO8601 date of the form
          YYYY-MM-DDTHH:MM:SS[.fff...](Z|[+-]dd:dd)

    If something doesn't parse, a DateFormatError will be raised.

    The return value is floating point seconds since the UNIX
    epoch (January 1, 1970 at midnight UTC).
    """
    # if it's too short
    if len(s) < 7:
        raise DateFormatError("Date '%s' is too short" % s)
    # UTC timezone?
    if s[-1] == 'Z':
        tz_offs, tz_len = 0, 1
    # explicit +/-nn:nn timezone?
    elif s[-3] == ':':
        tz_offs = int(s[-6] + '1') * (int(s[-5:-3])*3600 + int(s[-2:])*60)
        tz_len = 6
    # otherwise
    else:
        raise DateFormatError("Date '%s' is missing timezone" % s)
    # split into components
    cal, clock = s.split('T')
    year, month, day = cal.split('-')
    hr, minute, sec = clock[:-tz_len].split(':')
    # handle fractional seconds
    frac = 0
    point = sec.find('.')
    if point != -1:
        frac = float(sec[point+1:]) / pow(10, len(sec) - point - 1)
        sec = sec[:point]
    # use calendar to get seconds since epoch
    args = list(map(int, (year, month, day, hr, minute, sec))) + [0,1,-1]
    return calendar.timegm(args) + frac - tz_offs # adjust to GMT

def makeISO(value, is_gmt=False, set_gmt=False):
    """If value is a tuple, assume it is the one returned by time.gmtime() or time.localtime()
    Otherwise, assume value is an English language description (for partial ISO
    strings, use completeISO() instead).

    Return an ISO8601 string, with timezone set to GMT or localtime.
    """
    tz_str = 'Z' # assume GMT
    if isinstance(value,tuple) or isinstance(value,list):
        fmt = ("%04d","-%02d","-%02d","T%02d",":%02d", ":%02d")
        s = ''.join([f % v for f,v in zip(fmt,value)])
        if not gmt:
            tz_str = getLocaltimeISO(value)
        iso = s + tz_str
    else:
        try:
            d = magicdate.magicdate(value)
        except Exception as E:
            raise ValueError("magicdate cannot parse '%s'" % value)
        partial_iso = d.isoformat()
        iso = completeISO(partial_iso, is_gmt=is_gmt, set_gmt=set_gmt)
    return iso

def guess(s, parse=True, is_gmt=False, set_gmt=False,
             try_iso=True, try_num=True, try_en=True):
    """Guess the format, and optionally parse, the input string.

    If 'is_gmt' is True, assume timezone is GMT when not given.
    Otherwise, assume localtime.
    If 'set_gmt' is True then set the timezone to GMT, otherwise
    set it to localtime.

    The answer is a pair containing the guessed format and, if the 'parse'
    flag was given, the parsed value as seconds since the epoch, otherwise None.

    The format is a constant defined in this module:
      UNKNOWN - Cannot guess the format (associated value is None)
      ISO8601 - This is a prefix of the ISO8601 format accepted by completeISO()
      ENGLISH - This is an natural English-language format accepted by makeISO()
      SECONDS - This is seconds since the UNIX epoch (Midnight on 1970/1/1).
    """
    if not(s):
        return UNKNOWN, None
    sec = None
    s = s.strip()
    # try ISO8601
    if try_iso:
        m =  ISO_DATE_PARTS.match(s)
        if m and m.start() == 0 and m.end() == len(s):
            if parse:
                if s[-1] == 'Z':
                    # explicit timezone overrides option
                    is_gmt = True
                iso_s = completeISO(s, is_gmt=is_gmt, set_gmt=set_gmt)
                sec = parseISO(iso_s)
            return ISO8601, sec
    # try number
    if try_num:
        m = NUMBER_DATE.match(s)
        if m and m.start() == 0 and m.end() == len(s):
            if parse:
                sec = float(s)
            return SECONDS, sec
    # try natural language
    if try_en:
        try:
            d = magicdate.magicdate(s)
        except Exception as E:
            d = None
        if d is not None:
            if parse:
                partial_iso = d.isoformat()
                iso = completeISO(partial_iso, is_gmt=False, set_gmt=set_gmt)
                sec = parseISO(iso)
            return ENGLISH, sec
    # default: unknown
    return UNKNOWN, None

def utcFormatISO(sec):
    """Format 'sec' seconds since the epoch as a UTC ISO8601 date,
    in the UTC (GMT) timezone.
    Use the format: "YYYY-MM-DDThh:mm:ss.fffffffZ"
    """
    tm = time.gmtime(sec)
    usec = int((sec - int(sec)) * 1e6)
    iso_date = "%s.%06dZ" % (DATE_FMT % tm[0:6], usec)
    return iso_date

def localtimeFormatISO(sec):
    """Format 'sec' seconds since the epoch as a ISO8601 date,
    in the local timezone.
    Use the format: "YYYY-MM-DDThh:mm:ss.fffffff[+-]HH:MM"
    """
    tm = time.localtime(sec)
    usec = int((sec - int(sec)) * 1000000)
    hr, min, sign = getLocaltimeOffsetParts(sec)
    iso_date = "%s.%06d%s%02d:%02d" % (DATE_FMT % tm[0:6], usec,
               ('-','+')[sign==1], hr, min)
    return iso_date

# Syslog-style dates (always in localtime)
SYSLOG_DATE_RE = re.compile("\s*(...)\s+(...)\s+(\d\d?) " + 
                            "(\d\d):(\d\d):(\d\d)\s+(\d\d\d\d)\s*")
MONTHS = {'Jan':1, 'Feb':2, 'Mar':3, 'Apr':4,  'May':5,  'Jun': 6,
          'Jul':7, 'Aug':8, 'Sep':9, 'Oct':10, 'Nov':11, 'Dec':12}
def parseSyslogDate(date):
    """Parse syslog-style date to seconds UTC.
    Expected format: Fri Oct 24 04:18:36 2008
    """
    m = SYSLOG_DATE_RE.match(date)
    if m is None:
        raise ValueError("bad syslog date '%s'" % date)
    g = m.groups()
    month = MONTHS[g[1]]
    day, hh, mm, ss, year = list(map(int, g[2:]))        
    sec = time.mktime((year, month, day, hh, mm, ss, 0, 0, -1))
    return sec
