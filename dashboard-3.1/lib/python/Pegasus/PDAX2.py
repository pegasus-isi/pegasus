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

"""API for generating Pegasus PDAXes

The classes in this module can be used to generate PDAXes that can be
read by Pegasus.

The official PDAX schema is here: http://pegasus.isi.edu/schema/pdax-2.0.xsd
"""

__author__ = "Gideon Juve <juve@usc.edu>"
__all__ = ["PDAX","Partition"]
__version__ = "2.0"

import datetime, pwd, os
from cStringIO import StringIO

SCHEMA_NAMESPACE = u"http://pegasus.isi.edu/schema/PDAX"
SCHEMA_LOCATION = u"http://pegasus.isi.edu/schema/pdax-2.0.xsd"
SCHEMA_VERSION = u"2.0"


class Partition:
	"""Partition(name,index,id)
	
	A Partition represents one DAX in the PDAX. You should create a 
	Partition for each DAX you want to include in your PDAX.
	
	Jobs and job-dependencies are not supported at this time as they will
	be ignored by Pegasus.
	"""
	
	def __init__(self,name,index,id):
		"""The name and index are used by Pegasus to determine the name of 
		the DAX file for this partition. For example, if name is 
		"test_workflow" and index is "1" then Pegasus will assume the DAX 
		name is "test_workflow_1.dax".
		
		The partition ID is used by Pegasus to generate working directories 
		and scripts. Pegasus prefixes the ID value with 'P' to generate the 
		working directory name and DAG file name. The ID is also used as the
		name of the submit file and associated log files for the Condor job 
		that runs the dag.
		
		Arguments:
			name: The partition name
			index: The partition index
			id:  The partition ID
		"""
		self._name = name
		self._index = index
		self._id = id
		
	def addJob(self, job):
		"""Raises NotImplementedError"""
		raise NotImplementedError
		
	def addDependency(self, parent, child):
		"""Raises NotImplementedError"""
		raise NotImplementedError
		
	def getName(self):
		return self._name
		
	def setName(self, name):
		self._name = name
		
	def getIndex(self):
		return self._index
		
	def setIndex(self, index):
		self._index = index
		
	def getID(self):
		return self._id
		
	def setID(self, id):
		self._id = id
		
	def toXML(self, level=0, indent='\t'):
		"""Return an XML representation of this partition
		
		Arguments:
			level: The indentation level
			indent: The character(s) to use for indentation
		"""
		xml = StringIO()
		indentation = u''.join(indent for x in range(0,level))
		
		xml.write(indentation)
		xml.write(u'<partition name="%s" index="%s" id="%s"/>' % 
				(self._name, self._index, self._id))
		
		result = xml.getvalue()
		xml.close()
		return result
		

class PDAX:
	"""PDAX(name[,index][,count])
	
	A PDAX is a partitioned DAX. Basically, its a DAX that refers to other
	DAXes. Pegasus turns PDAXes into PDAGs that can be used to plan and
	execute a series of DAXes. In other words, the PDAX is a DAX of DAXes.
	"""
	
	class Dependency:
		"""A dependency in the PDAX
		
		This class is used internally. You shouldn't use it in your code.
		See PDAX.addDependency(...)
		"""
		
		def __init__(self, child):
			self._child = child
			self._parents = []
			
		def addParent(self, parent):
			self._parents.append(parent)
		
		def toXML(self, level=0, indent='\t'):
			xml = StringIO()
			indentation = u''.join(indent for x in range(0,level))

			xml.write(indentation)
			xml.write(u'<child ref="%s">\n' % self._child.getID())
			for parent in self._parents:
				xml.write(indentation)
				xml.write(indent)
				xml.write(u'<parent ref="%s"/>\n' % parent.getID())
			xml.write(indentation)
			xml.write(u'</child>')
			
			result = xml.getvalue()
			xml.close()
			return result
	
	def __init__(self, name, index=0, count=1):
		"""
		Arguments:
			name: The name of the PDAX
			index: The index of this PDAX (this should be 0)
			count: The total number of PDAXes (this should be 1)
		"""
		self._name = name
		self._index = index
		self._count = count
		self._lookup = {}
		self._dependencies = []
		self._partitions = []
		
	def addPartition(self, partition):
		"""Add a partition to this PDAX"""
		self._partitions.append(partition)
	
	def addDependency(self, parent, child):
		"""Add a dependency to this PDAX
		
		Arguments:
			parent: The parent Partition
			child: The child Partition
		"""
		if child not in self._lookup:
			dep = PDAX.Dependency(child)
			self._lookup[child] = dep
			self._dependencies.append(dep)
		self._lookup[child].addParent(parent)
		
	def writeXML(self, out, indent='\t'):
		"""Write the PDAX as XML to a stream"""
		# Preamble
		out.write(u'<?xml version="1.0" encoding="UTF-8"?>\n')
		
		# Metadata
		out.write(u'<!-- generated: %s -->\n' % datetime.datetime.now())
		out.write(u'<!-- generated by: %s -->\n' % pwd.getpwuid(os.getuid())[0])
		out.write(u'<!-- generator: python -->\n')
		
		# Open tag
		out.write(u'<pdag xmlns="%s" ' % SCHEMA_NAMESPACE)
		out.write(u'xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" ')
		out.write(u'xsi:schemaLocation="%s %s" ' % (SCHEMA_NAMESPACE, SCHEMA_LOCATION))
		out.write(u'name="%s" index="%s" count="%s" version="%s">\n' % 
				(self._name, self._index, self._count, SCHEMA_VERSION))

		# Partitions
		for partition in self._partitions:
			out.write(partition.toXML(level=1,indent=indent))
			out.write(u'\n')

		# Dependencies
		for dep in self._dependencies:
			out.write(dep.toXML(level=1,indent=indent))
			out.write(u'\n')

		# Close tag
		out.write(u'</pdag>\n')
		
if __name__ == '__main__':
	"""An example of using the PDAX API"""

	# Create a PDAX object
	pdax = PDAX('blackdiamond')

	# Add 20 partitions linked together in a chain
	last = None
	for i in range(0,20):
		part = Partition(name='blackdiamond',index=i,id='ID%02d' % i)
		pdax.addPartition(part)
		if last is not None: pdax.addDependency(last, part)
		last = part

	# Write the PDAX to stdout
	import sys
	pdax.writeXML(sys.stdout)