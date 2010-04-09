"""
properties.py

This module reads Java properties for the GriPhyN Virtual Data
System. It allows for command-line based overrides of properties
using Java's -Dk=v syntax in Python by removing initial
definitions from sys.argv during the module
initialization. Therefore, it is recommended to use this module
before parsing other command-line arguments.
"""

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

# Constants
PARSE_NONE = 0
PARSE_ALL = 1

# Regular expressions
re_remove_escapes = re.compile(r"\\(.)")
re_parse_property = re.compile(r"([^:= \t]+)\s*[:=]?\s*(.*)")
re_find_subs = re.compile(r"(\$\{([A-Za-z0-9._]+)\})")

re_inline_comments = re.compile("#(.*)$") # not using it right now...

system = {} # System properties
initial = {} # Command-line properties

# Get logger object (initialized elsewhere)
logger = logging.getLogger()

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

# First parameter is program name, just skip it
sys.argv.pop(0)
while len(sys.argv) > 0 and sys.argv[0][:2] == "-D":
    my_arg = sys.argv.pop(0)
    if my_arg == "-D":
	# k, v must be in next parameter
	my_arg = sys.argv.pop(0)
    else:
	# remove -D from this parameter before split
	my_arg = my_arg[2:]
    
    k, v = my_arg.split("=", 1)
    if len(k):
	initial[k.lower()] = v
	#print "key:value = %s:%s" % (k, v)

# Merge the two, with command-line taking precedence over environmental variables
system.update(initial)

def parse_properties(fn, hashref={}):
    """
    This functions parses properties from a file
    """
    # fn is the filename of the property file to read
    # hashref contains more properties for substitution (optional)
    # global system contains yet more properties for substitution
    # returns a map of properties, possibly empty
    my_result = {}
    my_save = ''

    try:
	my_file = open(fn, 'r')
    except:
	# Error opening file
	logger.warn("Could not open %s!" % (fn))
	sys.exit(1)

    logger.debug("# parsing properties in %s..." % (fn))

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
	    logger.debug("#Property being parsed is # %s" % (line))

	    # Try to parse property
	    my_res = re_parse_property.search(line)
	    if my_res:
		# Parse successful
		k = my_res.group(1)
		v = my_res.group(2)
		logger.debug("#Property being stored is # %s ==> %s" % (k, v))

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
		my_result[k.lower()] = v
	    else:
		logger.fatal("Illegal content in %s: %s" % (fn, line))
		sys.exit(1)

    my_file.close()
    return my_result

class Properties:

    m_config = {}
    m_flags = None

    def __init__(self):
	# Nothing to do here
	pass

    def new(self, flags=PARSE_ALL, hashref=None):
	"""
	Initialize instance variable
	Param: flags limits files to parse
	Param: hashref key value property list of least priority
	"""
	my_config = {}
	my_pegasushome = None

	if hashref is not None:
	    my_config = hashref.copy()

	my_flag = 0

	if flags == PARSE_ALL:
	    if "PEGASUS_HOME" in os.environ:
		if os.path.isdir(os.environ["PEGASUS_HOME"]):
		    my_pegasushome = os.environ["PEGASUS_HOME"]
		    my_config["pegasus.home"] = os.environ["PEGASUS_HOME"]
		else:
		    logger.warn("PEGASUS_HOME does not point to a(n accessible) directory!")
	    elif "VDT_LOCATION" in os.environ:
		my_tmp = os.path.join(os.environ["VDT_LOCATION"], "pegasus")
		if os.path.isdir(my_tmp):
		    my_pegasushome = my_tmp
		    my_config["pegasus.home"] = my_tmp
		else:
		    logger.warn("%s does not point to a(n accessible) directory!" % (my_tmp))
	    else:
		# Print message and exit
		logger.fatal("Your environmental variable PEGASUS_HOME is not set!")
		sys.exit(1)

	    # system properties go first
	    if "pegasus.properties" in system:
		# Overwrite for system property location from CLI interface
		my_sys = system["pegasus.properties"]
		if os.path.isfile(my_sys) and os.access(my_sys, os.R_OK):
		    my_config.update(parse_properties(my_sys))
		else:
		    my_flag = my_flag + 1
	    elif "pegasus.properties" in my_config:
		# Overwrite for system property location from hashref property
		my_sys = my_config["pegasus.properties"]
		if os.path.isfile(my_sys) and os.access(my_sys, os.R_OK):
		    my_config.update(parse_properties(my_sys))
		else:
		    my_flag = my_flag + 1
	    elif my_pegasushome is not None:
		# Default system property location
		my_sys = os.path.join(my_pegasushome, "etc", "properties")
		if os.path.isfile(my_sys) and os.access(my_sys, os.R_OK):
		    my_config.update(parse_properties(my_sys))
		else:
		    my_flag = my_flag + 1
	    else:
		my_flag = my_flag + 1

	    # User properties go last
	    if "pegasus.user.properties" in system:
		# Overwrite for user property location from CLI interface
		my_usr = system["pegasus.user.properties"]
		if os.path.isfile(my_usr) and os.access(my_usr, os.R_OK):
		    my_config.update(parse_properties(my_usr))
		else:
		    my_flag = my_flag + 1
	    elif "pegasus.user.properties" in my_config:
		# Overwrite for user property location from hashref property
		my_usr = my_config["pegasus.user.properties"]
		if os.path.isfile(my_usr) and os.access(my_usr, os.R_OK):
		    my_config.update(parse_properties(my_usr))
		else:
		    my_flag = my_flag + 1
	    elif "HOME" in os.environ:
		# Default user property location
		my_usr2 = os.path.join(os.environ["HOME"], ".pegasusrc")
		if os.path.isfile(my_usr2) and os.access(my_usr2, os.R_OK):
		    my_config.update(parse_properties(my_usr2))
		else:
		    my_flag = my_flag + 1
	    else:
		my_flag = my_flag + 1

	if my_flag == 1 and flags == PARSE_ALL:
	    logger.debug("Unable to load any properties at all")

	# Keep ordering of config before initial so that CLI
	# properties can override any other properties
	self.m_config = my_config
	self.m_config.update(initial)
	self.m_flags = flags

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
		my.set.append(my_key)

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
		    my_result[my_newkey] = self.m_config[key]

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
    
if __name__ == "__main__":
    a = Properties()
    a.new(PARSE_ALL)
    print "testing finished!"
