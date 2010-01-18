#!/usr/bin/env python
#
#  Copyright 2009 University Of Southern California
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

__author__ = "Gideon Juve <juve@usc.edu>"
__all__ = ["Analysis"]
__version__ = "1.0"

import sys, os, re

class Analysis:
	def __init__(self):
		pass

	def print_stats(self):
		pass

	def process_datafile(self):
		pass

	def is_datafile(self, file):
		return True

	def process_file(self, file):
		if self.is_datafile(file):
			self.process_datafile(file)
	
	def process_dir(self, dir):
		for file in os.listdir(dir):
			path = os.path.join(dir, file)
			if os.path.isdir(path):
				self.process_dir(path)
			elif os.path.isfile(path):
				self.process_file(path)

	def process_arg(self, arg):
		if arg == '-h' or arg == '-help' or arg == '--help':
			print "Usage: %s [PATH...] [< LIST_OF_PATHS]" % sys.argv[0]
			sys.exit(1)
		elif os.path.isdir(arg):
			self.process_dir(arg)
		elif os.path.isfile(arg):
			self.process_file(arg)
		else:
			print "Unrecognized argument: %s" % arg
			sys.exit(1)

	def analyze(self):
		if len(sys.argv) == 1:
			for arg in sys.stdin.readlines():
				self.process_arg(arg[:-1])
		else:
			for arg in sys.argv[1:]:
				self.process_arg(arg)
		self.print_stats()
