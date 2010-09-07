#!/usr/bin/env python
#
# Copyright 2010 University Of Southern California
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import sys
import os
from optparse import OptionParser

__author__ = "Gideon Juve <juve@usc.edu>"

def cleanup(input, stat=False, verbose=False):
	"""
	Read 'input' line by line and delete the files specified. Skip blank lines
	and lines beginning with '#'. If 'stat' is True, then stat the files before
	they are deleted and report the total number of bytes deleted.
	"""
	bytes = 0
	dir = os.getcwd()
	for f in input.readlines():
		f = f.strip()
		if len(f) == 0 or f.startswith("#"):
			continue
		path = f
		if verbose:
			print 'cleanup: removing "%s"' % f
		if not os.path.isabs(f):
			path = os.path.join(dir,f)
		if os.path.isfile(path):
			if stat:
				info = os.stat(path)
				bytes += info.st_size
				if verbose: 
					print 'cleanup: "%s" was %d bytes' % (f, info.st_size)
			os.remove(path)
		elif os.path.isdir(path):
			os.removedirs(path)
		elif verbose:
			print 'cleanup: "%s" not found' % f
	if stat:
		print 'cleanup: deleted %d bytes' % bytes

def main():
	usage = "Usage: %prog [options]"
	epilog = """Deletes a list of files. Each file to delete
should be on a separate line in the input. Absolute paths are allowed,
otherwise files are assumed to be relative to the current directory. 
Directories are allowed, but must be empty. Blank lines and lines 
beginning with the '#' character are skipped. Input is taken from
STDIN by default, but a file can be specified using -f/--file."""
	parser = OptionParser(usage, epilog=epilog)
	
	parser.add_option("-s", "--stat", action="store_true", default=False,
		dest="stat", help="Print total size of deleted files.")
	parser.add_option("-f", "--file", action="store", dest="file",
		help="Read list of files from FILE [default: stdin]")
	parser.add_option("-v", action="store_true", default=False,
		dest="verbose", help="Print verbose messages")
		
	(options, args) = parser.parse_args()
	
	if len(args) > 0:
		parser.error("Unrecognized argument")

	if options.file:
		input = open(options.file)
	else:
		input = sys.stdin	
	
	cleanup(input, options.stat, options.verbose)

	if options.file:
		input.close()

if __name__ == '__main__':
	main()
