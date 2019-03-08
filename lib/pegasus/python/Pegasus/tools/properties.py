"""
properties.py

This module reads Java properties for the GriPhyN Virtual Data
System. It allows for command-line based overrides of properties
using Java's -Dk=v syntax in Python by removing initial
definitions from sys.argv during the module
initialization. Therefore, it is recommended to use this module
before parsing other command-line arguments.
"""
from __future__ import print_function

##
#  Copyright 2007-2010 University Of Southern California
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
##

# Revision : $Revision: 2012 $

import os
import re
import sys
import time
import logging
import tempfile
import pwd

# Regular expressions
re_remove_escapes = re.compile(r"\\(.)")
re_parse_property = re.compile(r"([^:= \t]+)\s*[:=]?\s*(.*)")
re_find_subs = re.compile(r"(\$\{([A-Za-z0-9._]+)\})")

re_inline_comments = re.compile("#(.*)$") # not using it right now...

system = {} # System properties
initial = {} # Command-line properties

logger = logging.getLogger(__name__)

# Assemble system properties
system["file.separator"] = ','
if "JAVA_HOME" in os.environ:
    system["java.home"] = os.environ["JAVA_HOME"]
if "CLASSPATH" in os.environ:
    system["java.class.path"] = os.environ["CLASSPATH"]
if "TMP" in os.environ:
    system["java.io.tmpdir"] = os.environ["TMP"]
else:
    system["java.io.tmpdir"] = tempfile.gettempdir()
system["os.name"] = os.uname()[0]
system["os.version"] = os.uname()[2]
system["os.arch"] = os.uname()[4]
system["user.dir"] = os.getcwd()
if "HOME" in os.environ:
    system["user.home"] = os.environ["HOME"]
else:
    system["user.home"] = pwd.getpwuid(os.geteuid())[5]
if "LANG" in os.environ:
    system["user.language"] = os.environ["LANG"]
else:
    system["user.language"] = "en"
if "USER" in os.environ:
    system["user.name"] = os.environ["USER"]
elif "LOGNAME" in os.environ:
    system["user.name"] = os.environ["LOGNAME"]
else:
    system["user.name"] = pwd.getpwuid(os.geteuid())[0]
# Can be undefined
if "TZ" in os.environ:
    system["user.timezone"] = os.environ["TZ"]
# Useful, but not required
if "PEGASUS_HOME" in os.environ:
    system["pegasus.home"] = os.environ["PEGASUS_HOME"]

# Assemble command-line properties

if len(sys.argv) > 0:
    # First parameter is program name, just skip it, and remove it for now...
    program_name = sys.argv.pop(0)

    while len(sys.argv) > 0 and sys.argv[0][:2] == "-D":
        my_arg = sys.argv.pop(0)
        if my_arg == "-D":
            # k, v must be in next parameter
            if len(sys.argv) > 0:
                # Make sure we have another parameter
                my_arg = sys.argv.pop(0)
            else:
                # No, let's put the "-D" back and leave this loop
                sys.argv.insert(0, my_arg)
                break
        else:
            # remove -D from this parameter before split
            my_arg = my_arg[2:]
    
        try:
            k, v = my_arg.split("=", 1)
        except:
            logger.warn("cannot parse command-line option %s... continuing..." % (my_arg))
            k = ""
        if len(k):
            if k == "pegasus.properties" or k == "pegasus.user.properties":
                logger.warn("%s is no longer supported, ignoring, please use --conf!" % (k))
            else:
                logger.debug("parsed property %s..." % (my_arg))
                initial[k] = v
            #print "key:value = %s:%s" % (k, v)

    # Re-insert program_name
    sys.argv.insert(0, program_name)

# Merge the two, with command-line taking precedence over environmental variables
system.update(initial)

def parse_properties(my_file, hashref={}):
    """
    This functions parses properties from a file
    """
    # my_file is the filename or file-like of the property file to read
    # hashref contains more properties for substitution (optional)
    # global system contains yet more properties for substitution
    # returns a map of properties, possibly empty
    my_result = {}
    my_save = ''

    if isinstance(my_file, str):
        my_file = open(my_file, 'r')

    logger.debug("# parsing properties in %s..." % (my_file))

    for line in my_file:
        line = line.strip(" \t") # Remove leading and trailing spaces, tabs
        if line.startswith('!') or line.startswith('#'):
            # Skip comments
            continue
        line = line.rstrip("\n\r") # Remove new lines, if any
        # line = re_inline_comments.sub('', line) # Remove inline comments using regular expressions
        line = line.split('#')[0] # Remove inline comments
        line = re_remove_escapes.sub(r"\1", line) # replace Java properties escaped special characters #!=:
        line = line.strip() # Remove all starting and trailing whitespaces
        # Skip empty lines
        if len(line) == 0:
            continue

        if line[-1] == '\\':
            # Continuation line
            line = line[:-1]
            my_save += line
        else:
            # Regular line
            if my_save != "":
                # Append current line to previous line(s) and process
                my_save+= line
                line = my_save
                my_save = ""
            logger.trace("#Property being parsed is # %s" % (line))

            # Try to parse property
            my_res = re_parse_property.search(line)
            if my_res:
                # Parse successful
                k = my_res.group(1)
                v = my_res.group(2)
                logger.trace("#Property being stored is # %s ==> %s" % (k, v))

                # Substitutions
                subs = re_find_subs.search(v)
                while subs:
                    if subs.group(1) in hashref:
                        my_newval = hashref[subs.group(1)]
                    elif subs.group(1) in system:
                        my_newval = system[subs.group(1)]
                    else:
                        my_newval = ''

                    # Make substitution
                    new_v = v[:subs.start(1)]
                    new_v += my_newval
                    new_v += v[subs.end(1):]
                    v = new_v
                    # Search again, and loop
                    subs = re_find_subs.search(v)

                # Insert key, value into my_result
                my_result[k] = v
            else:
                logger.fatal("Illegal content in %s: %s" % (my_file, line))
                sys.exit(1)

    my_file.close()
    return my_result

class Properties:

    def __init__(self, props=None):
        # Initialize class variables
        self.m_config = props or {}


    def __delitem__(self, key):
        """
        Delete a property from the underlying dictionary.

        :param key:
        :return:
        """
        del self.m_config[key]

    def new(self, config_file=None, rundir_propfile=None):
        """
        Initialize instance variable, processing the appropriate
        properties file. config_file is the properties file passed via
        the --conf command-line option, it has the highest
        priority. rundir_propfile is the properties file in the run
        directory (specified in the braindump.txt file with the
        properties tag. It has the second highest priority. If those
        are not specified, we try to lost $(HOME)/.pegasusrc, as a
        last resort.
        """
        my_config = {}
        my_already_loaded = False

        # First, try config_file, highest priority
        if config_file is not None:
            if os.path.isfile(config_file) and os.access(config_file, os.R_OK):
                logger.debug("processing properties file %s..." % (config_file))
                my_config.update(parse_properties(config_file))
                my_already_loaded = True
            else:
                logger.warn("cannot access properties file %s... continuing..." % (config_file))

        # Second, try rundir_propfile
        if not my_already_loaded and rundir_propfile is not None:
            if os.path.isfile(rundir_propfile) and os.access(rundir_propfile, os.R_OK):
                logger.debug("processing properties file %s... " % (rundir_propfile))
                my_config.update(parse_properties(rundir_propfile))
                my_already_loaded = True
            else:
                logger.warn("cannot access properties file %s... continuing..." % (rundir_propfile))

        # Last chance, look for $(HOME)/.pegasusrc
        if not my_already_loaded:
            if "user.home" in system:
                my_user_propfile = os.path.join(system["user.home"], ".pegasusrc")
                if os.path.isfile(my_user_propfile) and os.access(my_user_propfile, os.R_OK):
                    logger.debug("processing properties file %s... " % (my_user_propfile))
                    my_config.update(parse_properties(my_user_propfile))
                    my_already_loaded = True
                else:
                    # No need to complain about this
                    pass

        if not my_already_loaded:
            logger.warn("no properties file parsed whatsoever!")

        # Keep ordering of config before initial so that the -D CLI
        # properties can override any other properties
        self.m_config = my_config
        self.m_config.update(initial)

    def property(self, key, val=None):
        """
        Get and set a property
        Param: key is the property name to access
        Param: val is not None is the value to set the key
        Return: in get mode, the current value (None if not found)
        Return: in set mode, the old value (None if it didn't exist before)
        """
        my_old_val = None

        if key in self.m_config:
            my_old_val = self.m_config[key]
        if val is not None:
            self.m_config[key] = val

        return my_old_val

    def remove(self, key):
        """
        Removes a property matching the key
        :param key:
        :return: the old value if it exists
        """
        value = None
        if key in self.m_config.keys():
            value = self.m_config[key]
            del self.m_config[key]
        return value

    def keyset(self, predicate=None):
        """
        Finds a subset of keys that matches a predicate
        Param: predicate is what we match against
        Return: set of keys that match the predicate, or all keys if no predicate
        """
        if predicate is None:
            return self.m_config.keys()

        my_set = []
        for my_key in self.m_config.keys():
            if re.match(predicate, my_key):
                my_set.append(my_key)

        return my_set

    def propertyset(self, prefix, remove=False):
        """
        Finds a subset of keys that match a prefix
        Param: prefix to compare keys against
        Param: remove if True, remove prefix
        Return: Dictionary containing the matching results
        """
        my_result = {}

        for my_key in self.m_config.keys():
            # Check if it begins with prefix
            if my_key.startswith(prefix):
                if remove:
                    # Remove prefix from my_key
                    my_newkey = my_key[len(prefix):]
                else:
                    # Keep my_key as it is
                    my_newkey = my_key
                if len(my_newkey) > 0:
                    # Only copy if my_newkey is not empty
                    my_result[my_newkey] = self.m_config[my_key]

        return my_result

    def dump(self, fn='-'):
        """
        Prints the key set in property format
        Param: fn is the name of the file to print to, defaults to stdout
        Return: number of things printed, None if error
        """
        my_count = 0

        if fn == '-':
            my_file = sys.stdout
        else:
            try:
                my_file = open(fn, 'w')
            except:
                logger.warn("error opening %s !" % (fn))
                return None

        # Add header
        my_file.write("# generated %s\n" % (time.asctime()))
        for my_key in self.m_config:
            # Write entry
            my_file.write("%s=%s\n" % (my_key, self.m_config[my_key]))
            my_count = my_count + 1

        # Close file if not stdout
        if fn != '-':
            my_file.close()

        return my_count

    def __str__(self):
        """
        Returns string representation
        :return:
        """
        return self.m_config.__str__()
    
if __name__ == "__main__":
    a = Properties()
    a.new()
    print("testing finished!")
