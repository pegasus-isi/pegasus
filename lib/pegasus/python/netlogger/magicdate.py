import re
import datetime
import calendar
from optparse import Option, OptionValueError
from copy import copy


def check_magicdate(option, opt, value):
    try:
        return magicdate(value)
    except:
        raise OptionValueError(
            "option %s: invalid date value: %r" % (opt, value))


class MagicDateOption(Option):
    TYPES = Option.TYPES + ("magicdate",)
    TYPE_CHECKER = copy(Option.TYPE_CHECKER)
    TYPE_CHECKER["magicdate"] = check_magicdate



res = [
        # x time ago
        (re.compile(
            r'''^
                ((?P<weeks>\d+) \s weeks?)?
                [^\d]*
                ((?P<days>\d+) \s days?)?
                [^\d]*
                ((?P<hours>\d+) \s hours?)?
                [^\d]*
                ((?P<minutes>\d+) \s minutes?)?
                [^\d]*
                ((?P<seconds>\d+) \s seconds?)?
                \s
                ago
            ''',
            (re.VERBOSE | re.IGNORECASE)),
        lambda m: datetime.datetime.today() - datetime.timedelta(
                days=int(m.group('days') or 0),
                seconds=int(m.group('seconds') or 0),
                minutes=int(m.group('minutes') or 0),
                hours=int(m.group('hours') or 0),
                weeks=int(m.group('weeks') or 0))),

        # Today
        (re.compile(
            r'''^
                tod                             # Today
            ''',
            (re.VERBOSE | re.IGNORECASE)),
        lambda m: datetime.date.today()),

        # Now (special case, returns datetime.datetime
        (re.compile(
            r'''^
                now                             # Now
            ''',
            (re.VERBOSE | re.IGNORECASE)),
        lambda m: datetime.datetime.now()),
              
        # Tomorrow
        (re.compile(
            r'''^
                tom                             # Tomorrow
            ''',
            (re.VERBOSE | re.IGNORECASE)),
        lambda m: datetime.date.today() + datetime.timedelta(days=1)),
                    
        # Yesterday
        (re.compile(
            r'''^
                yes                             # Yesterday
            ''',
            (re.VERBOSE | re.IGNORECASE)),
        lambda m: datetime.date.today() - datetime.timedelta(days=1)),
                    
        # 4th
        (re.compile(
            r'''^
                (?P<day>\d{1,2})                # 4
                (?:st|nd|rd|th)?                # optional suffix
                $                               # EOL
            ''',
            (re.VERBOSE | re.IGNORECASE)),
        lambda m: datetime.date.today().replace(
            day=int(m.group('day')))),
                    
        # 4th Jan
        (re.compile(
            r'''^
                (?P<day>\d{1,2})                # 4
                (?:st|nd|rd|th)?                # optional suffix
                \s+                             # whitespace
                (?P<month>\w+)                  # Jan
                $                               # EOL
            ''',
            (re.VERBOSE | re.IGNORECASE)),
        lambda m: datetime.date.today().replace(
            day=int(m.group('day')),
            month=_parseMonth(m.group('month')))),
                    
        # 4th Jan 2003
        (re.compile(
            r'''^
                (?P<day>\d{1,2})                # 4
                (?:st|nd|rd|th)?                # optional suffix
                \s+                             # whitespace
                (?P<month>\w+)                  # Jan
                ,?                              # optional comma
                \s+                             # whitespace
                (?P<year>\d{4})                 # 2003
                $                               # EOL
            ''',
            (re.VERBOSE | re.IGNORECASE)),
        lambda m: datetime.date(
            year=int(m.group('year')),
            month=_parseMonth(m.group('month')),
            day=int(m.group('day')))),
                    
        # Jan 4th
        (re.compile(
            r'''^
                (?P<month>\w+)                  # Jan
                \s+                             # whitespace
                (?P<day>\d{1,2})                # 4
                (?:st|nd|rd|th)?                # optional suffix
                $                               # EOL
            ''',
            (re.VERBOSE | re.IGNORECASE)),
        lambda m: datetime.date.today().replace(
            day=int(m.group('day')),
            month=_parseMonth(m.group('month')))),
                    
        # Jan 4th 2003
        (re.compile(
            r'''^
                (?P<month>\w+)                  # Jan
                \s+                             # whitespace
                (?P<day>\d{1,2})                # 4
                (?:st|nd|rd|th)?                # optional suffix
                ,?                              # optional comma
                \s+                             # whitespace
                (?P<year>\d{4})                 # 2003
                $                               # EOL
            ''',
            (re.VERBOSE | re.IGNORECASE)),
        lambda m: datetime.date(
            year=int(m.group('year')),
            month=_parseMonth(m.group('month')),
            day=int(m.group('day')))),

        # mm/dd/yyyy (American style, default in case of doubt)
        (re.compile(
            r'''^
                (?P<month>0?[1-9]|10|11|12)     # m or mm
                /                               #
                (?P<day>0?[1-9]|[12]\d|30|31)   # d or dd
                /                               #
                (?P<year>\d{4})                 # yyyy
                $                               # EOL
            ''',
            (re.VERBOSE | re.IGNORECASE)),
        lambda m: datetime.date(
            year=int(m.group('year')),
            month=int(m.group('month')),
            day=int(m.group('day')))),

        # dd/mm/yyyy (European style)
        (re.compile(
            r'''^
                (?P<day>0?[1-9]|[12]\d|30|31)   # d or dd
                /                               #
                (?P<month>0?[1-9]|10|11|12)     # m or mm
                /                               #
                (?P<year>\d{4})                 # yyyy
                $                               # EOL
            ''',
            (re.VERBOSE | re.IGNORECASE)),
        lambda m: datetime.date(
            year=int(m.group('year')),
            month=int(m.group('month')),
            day=int(m.group('day')))),

        # yyyy-mm-dd (ISO style)
        (re.compile(
            r'''^
                (?P<year>\d{4})                 # yyyy
                -                               #
                (?P<month>0?[1-9]|10|11|12)     # m or mm
                -                               #
                (?P<day>0?[1-9]|[12]\d|30|31)   # d or dd
                $                               # EOL
            ''',
            (re.VERBOSE | re.IGNORECASE)),
        lambda m: datetime.date(
            year=int(m.group('year')),
            month=int(m.group('month')),
            day=int(m.group('day')))),

        # yyyymmdd
        (re.compile(
            r'''^
                (?P<year>\d{4})                 # yyyy
                (?P<month>0?[1-9]|10|11|12)     # m or mm
                (?P<day>0?[1-9]|[12]\d|30|31)   # d or dd
                $                               # EOL
            ''',
            (re.VERBOSE | re.IGNORECASE)),
        lambda m: datetime.date(
            year=int(m.group('year')),
            month=int(m.group('month')),
            day=int(m.group('day')))),

        # next Tuesday
        (re.compile(
            r'''^
                next                            # next
                \s+                             # whitespace
                (?P<weekday>\w+)                # Tuesday
                $                               # EOL
            ''',
            (re.VERBOSE | re.IGNORECASE)),
        lambda m: _nextWeekday(_parseWeekday(m.group('weekday')))),

        # last Tuesday
        (re.compile(
            r'''^
                (last                           # last
                \s+)?                           # whitespace
                (?P<weekday>\w+)                # Tuesday
                $                               # EOL
            ''',
            (re.VERBOSE | re.IGNORECASE)),
        lambda m: _lastWeekday(_parseWeekday(m.group('weekday')))),
       ]


def _parseMonth(input):
    months = "January February March April May June July August September October November December".split(' ')
    for i, month in enumerate(months):
        p = re.compile(input, re.IGNORECASE)
        if p.match(month): return i+1
    else:
        raise Exception


def _parseWeekday(input):
    days = "Monday Tuesday Wednesday Thursday Friday Saturday Sunday".split(' ')
    for i, day in enumerate(days):
        p = re.compile(input, re.IGNORECASE)
        if p.match(day): return i
    else:
        raise Exception


def _nextWeekday(weekday):
    day = datetime.date.today() + datetime.timedelta(days=1)
    while calendar.weekday(*day.timetuple()[:3]) != weekday:
        day = day + datetime.timedelta(days=1)

    return day


def _lastWeekday(weekday):
    day = datetime.date.today() - datetime.timedelta(days=1)
    while calendar.weekday(*day.timetuple()[:3]) != weekday:
        day = day - datetime.timedelta(days=1)

    return day


def magicdate(input):
    for r, f in res:
        m = r.match(input.strip())
        if m:
            return f(m)
