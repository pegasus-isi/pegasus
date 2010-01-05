#!/usr/bin/env python
#
#  Copyright 2010 University Of Southern California
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

"""This module contains utilities for computing summary statistics"""

__author__ = "Gideon Juve <juve@usc.edu>"
__all__ = []
__version__ = "1.0"

from math import sqrt

class Variable:
	def __init__(self):
		self.n = 0
		self.max = 0
		self.min = 1e100
		self.mean = 0
		self.M2 = 0
		self.sum = 0

	def update(self, x):
		self.min = min(self.min, x)
		self.max = max(self.max, x)
		self.n += 1
		delta = x - self.mean
		self.mean = self.mean + (delta/self.n)
		self.M2 = self.M2 + (delta*(x-self.mean))
		self.sum += x

	def __str__(self):
		if self.n <= 1:
			stddev = 0.0
		else:
			stddev = sqrt(self.M2/(self.n - 1))
		return "%d,%f,%f,%f,%f,%f" % (self.n, self.min, self.max, self.mean, stddev, self.sum)