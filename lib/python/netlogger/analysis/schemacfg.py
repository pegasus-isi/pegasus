"""
Read and handle simple event "schemas" encoded
in a Python configuration file.

Format::

  [<event-name>]
  <field> = <type-name>
  ..

The <type-name> is the target type for the field value.
Accepted values for <type-name> are:
  int(eger)
  float
  date - datetime object

Sample usage::

  from <thismodule> import SchemaParser, Schema
  parser = SchemaParser(files=("myschemafile.cfg",))
  schema = parser.get_schema()
  for e in event_list:
      print "before: %s" % e
      schema.event(e)
      print "after: %s" % e


"""
__author__ = "Dan Gunter <dkgunter@lbl.gov>"
__rcsid__ = "$Id: schemacfg.py 26536 2010-10-01 03:05:32Z dang $"

# System imports
from datetime import datetime
import ConfigParser
import re
# Local imports
from netlogger import nldate
from netlogger import nlapi
from netlogger import nllog
from netlogger.parsers.base import parseDate

def convert_to_date(x):
    """Convert value to date.

    Exceptions:
      - ValueError: if string date is invalid.
    """
    result = x
    if isinstance(x,float):
        result = datetime.utcfromtimestamp(x)
    elif isinstance(x,str):
        sec = parseDate(x)
        result = datetime.utcfromtimestamp(sec)
    return result

def identity(x):
    return x

class SchemaParser(nllog.DoesLogging):
    _TYPEFN = {
        'int' : int,
        'integer' : int,
        'float' : float,
        'date' : convert_to_date,
        'str' : None, # no-op
        'string' : None, # no-op
        }
    def __init__(self, files=[], **read_kw):
        """Constructor.

        Args:
          - files (str[]): File objects or names
              passed to read(), if present.
              
        Kwargs:
          - **read_kw: Keywords passed through to read() function

        Exceptions:
          If `files` is non-empty, then will raise exceptions
          just like read().
          
        """
        nllog.DoesLogging.__init__(self)
        self._parser = ConfigParser.RawConfigParser()
        self._mapping = { }
        for f in files:
            self.read(f, **read_kw)
            
    def read(self, str_or_file):
        """Read and parse the data.

        Args:
          - str_or_file (str|file): A string or file-like
              object, which must implement readline(). If it is a
              string attempt to open the file with that name.

        Exceptions:
          - IOError: If a file is specified but can't be opened
          - ValueError: Bad type specification
        """
        if hasattr(str_or_file, "readline"):
            fileobj = str_or_file
        else:
            fileobj = open(str(str_or_file), 'r')
        self._parser.readfp(fileobj)
        name_expr = re.compile("^[0-9a-zA-Z._-]+$")
        msg = "must be 1 or more of alphanumeric, dash, underline or dot"
        for sect in self._parser.sections():
            # check that section name is legal
            m = name_expr.match(sect)
            if m is None:
                raise ValueError("Event name [%s]: %s" % (sect, msg))
            type_map = { }
            for name, value in self._parser.items(sect):
                # check that name is legal
                m = name_expr.match(name)
                if m is None:
                    raise ValueError("Field name '%s': %s" % (name, msg))
                try:
                    fn = self._TYPEFN[value]
                    # make "None" a cheap no-op
                    if fn is not None:
                        type_map[name] = fn
                except KeyError:
                    raise ValueError("Unknown type in '%s=%s' "
                                     "in section [%s]" % (
                                         name, value, sect))
            self._mapping[sect] = type_map

    def get_schema(self):
        """Get the schema so far.

        Returns:
          - Schema: The schema as an object.
        """
        return Schema(self._mapping)

class Schema(nllog.DoesLogging):
    """Thin wrapper around a mapping that specifies functions
    for converting field values for a given event type.

    Attributes:
       - mapping: the original mapping
    """
    def __init__(self, mapping):
        """Constructor.

        Args:
          - mapping (dict): Layout of dictionary is
             { event-name : { field-name : function, .. }, .. }

        """
        nllog.DoesLogging.__init__(self)
        self.mapping = mapping

    def event(self, event):
        """Modify input event dictionary, in place,
        parsing types as specified by the schema.

        Args:
          - event (dict): NetLogger event dictionary

        Exceptions:
          - ValueError: If the event doesn't have required fields,
              or there is an error parsing one of the values.
        """
        try:
            event_name = event[nlapi.EVENT_FIELD]
        except KeyError:
            raise ValueError("Bad event, missing required field '%s'" %
                             nlapi.EVENT_FIELD)
        # Look for type map for this event
        if self.mapping.has_key(event_name):
            type_map = self.mapping[event_name]
            for key in type_map:
                # If present, apply conversion
                if key in event:
                    if self._trace:
                        self.log.trace("convert.start", key=key)
                    try:
                        event[key] = type_map[key](event[key])                        
                    except ValueError, err:
                        if self._trace:
                            self.log.trace("convert.end", key=key, status=-1, msg=err)
                        raise ValueError("parsing '%s': %s" % (key, err))
                    if self._trace:
                        self.log.trace("convert.end", key=key, status=0)
