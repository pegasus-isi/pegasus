#!/usr/bin/env python

"""
Pegasus utility functions for pasing a kickstart output file and return wanted information

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

# Import Python modules

from xml.parsers import expat
import sys
import re

# Regular expressions used in the kickstart parser
re_parse_clustered_props = re.compile(r'(\S+)\s*=\s*([^",]+)')
re_parse_quoted_clustered_props = re.compile(r'(\S+)\s*=\s*"([^"]+)"')

class Parser:
    """
    This class is used to parse a kickstart output file, and return
    requested information.
    """

    _kickstart_output_file = None
    _parsing_arguments = False
    _parsing_main_job = False
    _parsing_machine = False
    _parsing_cwd = False
    _arguments = ""
    _keys = {}
    _ks_elements = {}
    _cwd = ""
    _fh = None
    _open_error = False

    def __init__(self, filename):
	"""
	This function initializes the Parser class with the kickstart
	output file that should be parsed.
	"""
	self._kickstart_output_file = filename
        self._open_error = False

    def open(self):
	"""
	This function opens a kickstart output file.
	"""
	try:
	    self._fh = open(self._kickstart_output_file)
	except:
	    # Error opening file
	    self._fh = None
            self._open_error = True
	    return False

	# Open succeeded
        self._open_error = False
	return True

    def close(self):
	"""
	This function closes the kickstart output file.
	"""
	try:
	    self._fh.close()
	except:
	    return False

	return True

    def read_record(self):
	"""
	This function reads an invocation record from the kickstart
	output file. We also look for the struct at the end of a file
        containing multiple records. It returns a string containing
	the record, or None if it is not found.
	"""
	buffer = ""

	# First, we find the beginning <invocation xmlns....
	while True:
	    line = self._fh.readline()
	    if line == '':
		# End of file, record not found
		return None
	    if line.find("<invocation") != -1:
		break
	    if line.find("[struct") != -1:
		break

	# Found something!
	if line.find("<invocation") >= 0:
	    # Found invocation record
	    start = line.find("<invocation")
	    buffer = line[start:]
	    end = buffer.find("</invocation>")

	    # Check if we have everything in a single line
	    if end >= 0:
		end = end + len("</invocation>")
		return buffer[:end]
	elif line.find("[struct") >= 0:
	    # Found line with cluster jobs summary
	    start = line.find("[struct")
	    buffer = line[start:]
	    end = buffer.find("]")

	    if end >= 0:
		end = end + len("]")
		return buffer[:end]

	    # clustered struct should be a single line!
	    return None
	else:
	    return None

	# Ok, now continue reading the file until we get a full record
	while True:
	    line = self._fh.readline()
	    if line == '':
		# End of file, record not found
		return None
	    buffer = buffer + line
	    if buffer.find("</invocation>") > 0:
		break

	# Now, we got it, let's make sure
	end = buffer.find("</invocation>")
	if end == -1:
	    return None

	end = end + len("</invocation>")
	return buffer[:end]

    def is_invocation_record(self, buffer=''):
	"""
	Returns True if buffer contains an invocation record.
	"""
	if buffer.find("<invocation") == -1:
	    return False
	return True

    def is_clustered_record(self, buffer=''):
	"""
	Returns True if buffer contains a clustered record.
	"""
	if buffer.find("[struct") == -1:
	    return False
	return True

    def start_element(self, name, attrs):
	"""
	Function called by the parser every time a new element starts
	"""
	# Keep track if we are parsing the main job element
	if name == "mainjob":
	    self._parsing_main_job = True
	if name == "machine":
	    self._parsing_machine = True

	if name == "argument-vector" and name in self._ks_elements:
	    # Start parsing arguments
	    self._parsing_arguments = True
	elif name == "cwd" and name in self._ks_elements:
	    # Start parsing cwd
	    self._parsing_cwd = True
	elif name == "file" and name in self._ks_elements:
	    if self._parsing_main_job == True:
		# Special case for name inside the mainjob element (will change this later)
		for my_element in self._ks_elements[name]:
		    if my_element in attrs:
			self._keys[my_element] = attrs[my_element]
	elif name == "ram" and name in self._ks_elements:
	    if self._parsing_machine == True:
		# Special case for ram inside the machine element (will change this later)
		for my_element in self._ks_elements[name]:
		    if my_element in attrs:
			self._keys[my_element] = attrs[my_element]
	elif name == "uname" and name in self._ks_elements:
	    if self._parsing_machine == True:
		# Special case for uname inside the machine element (will change this later)
		for my_element in self._ks_elements[name]:
		    if my_element in attrs:
			self._keys[my_element] = attrs[my_element]
	else:
	    # For all other elements, check if we want them
	    if name in self._ks_elements:
		for my_element in self._ks_elements[name]:
		    if my_element in attrs:
			self._keys[my_element] = attrs[my_element]
			
    def end_element(self, name):
	"""
	Function called by the parser whenever we reach the end of an element
	"""
	# Stop parsing argement-vector and cwd if we reached the end of those elements
	if name == "argument-vector":
	    self._parsing_arguments = False
	elif name == "cwd":
	    self._parsing_cwd = False
	elif name == "mainjob":
	    self._parsing_main_job = False
	elif name == "machine":
	    self._parsing_machine = False

    def char_data(self, data=''):
	"""
	Function called by the parser whenever there's character data in an element
	"""
	# Return if nothing of interest
	data = data.strip()
	if data == "":
	    return

	# Capture cwd
	if self._parsing_cwd == True:
	    self._cwd = data

	# Concatenate arguments
	if self._parsing_arguments == True:
	    if self._arguments == "":
		self._arguments = data
	    else:
		self._arguments = self._arguments + " " + data

    def parse_invocation_record(self, buffer=''):
	"""
	Parses the xml record in buffer, returning the desired keys.
	"""
	self._keys = {}

	# Check if we have an invocation record
	if self.is_invocation_record(buffer) == False:
	    return self._keys

	# Add invocation key to our response
	self._keys["invocation"] = True

	# Prepend XML header
	buffer = '<?xml version="1.0" encoding="ISO-8859-1"?>\n' + buffer

	# Create parser
	my_parser = expat.ParserCreate()
	my_parser.StartElementHandler = self.start_element
	my_parser.EndElementHandler = self.end_element
	my_parser.CharacterDataHandler = self.char_data

	# Parse everything!
	output = my_parser.Parse(buffer)

	# Add cwd and arguments to keys
	if "cwd" in self._ks_elements:
	    self._keys["cwd"] = self._cwd

	if "argument-vector" in self._ks_elements:
	    self._keys["argument-vector"] = self._arguments

	return self._keys

    def parse_clustered_record(self, buffer=''):
	"""
	Parses the clustered record in buffer, returning all found keys
	"""
	self._keys = {}

	# Check if we have an invocation record
	if self.is_clustered_record(buffer) == False:
	    return self._keys

	# Add clustered key to our response
	self._keys["clustered"] = True

	# Parse all quoted properties
	for my_key, my_val in re_parse_quoted_clustered_props.findall(buffer):
	    self._keys[my_key] = my_val

	# And add unquoted properties as well
	for my_key, my_val in re_parse_clustered_props.findall(buffer):
	    self._keys[my_key] = my_val

	return self._keys

    def parse(self, keys_dict):
	"""
	This function parses the kickstart output file, looking for
        the keys specified in the keys_dict variable. It returns a
	list of dictionaries containing the found keys. Look at the
        parse_stampede function for details about how to pass keys
	using the keys_dict structure. The function will return an
	empty list if no records are found or if an error happens.
	"""
	my_reply = []

	# Place keys_dict in the _ks_elements
	self._ks_elements = keys_dict

	# Try to open the file
	if self.open() == False:
	    return my_reply

	# Read first record
	my_buffer = self.read_record()
	
	# Loop while we still have record to read
	while my_buffer is not None:
	    if self.is_invocation_record(my_buffer) == True:
		# We have an invocation record, parse it!
		my_reply.append(self.parse_invocation_record(my_buffer))
	    elif self.is_clustered_record(my_buffer) == True:
		# We have a clustered record, parse it!
		my_reply.append(self.parse_clustered_record(my_buffer))
	    else:
		# We have something else, this shouldn't happen!
		# Just skip it
		pass

	    # Read next record
	    my_buffer = self.read_record()

	# Lastly, close the file
	self.close()

	return my_reply

    def parse_stampede(self):
	"""
	This function works similarly to the parse function above,
	but does not require a keys_dict parameter as it uses a
	built-in list of keys speficically used in the Stampede
	schema.
	"""

	stampede_elements = {"invocation": ["hostname", "resource", "user", "hostaddr", "transformation"],
			     "mainjob": ["duration", "start"],
			     "ram": ["total"],
			     "uname": ["system", "release", "machine"],
			     "file": ["name"],
			     "regular": ["exitcode"],
			     "argument-vector": [],
			     "cwd": []}

	return self.parse(stampede_elements)

if __name__ == "__main__":

    # Let's run a test!
    print "Testing kickstart output file parsing..."

    # Make sure we have an argument
    if len(sys.argv) < 2:
	print "For testing, please give a kickstart output filename!"
	sys.exit(1)

    # Create parser class
    p = Parser(sys.argv[1])

    # Parse file according to the Stampede schema
    output = p.parse_stampede()

    # Print output
    for record in output:
	print record
