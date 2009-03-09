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

"""API for generating Pegasus DAXes

The classes in this module can be used to generate DAXes that can be
read by Pegasus.

The official DAX schema is here: http://pegasus.isi.edu/schema/dax-2.1.xsd
"""

__author__ = "Gideon Juve <juve@usc.edu>"
__all__ = ["DAX","Filename","Profile","Job","Namespace","LFN"]
__version__ = "2.1"

import datetime, pwd, os
from cStringIO import StringIO

SCHEMA_NAMESPACE = u"http://pegasus.isi.edu/schema/DAX"
SCHEMA_LOCATION = u"http://pegasus.isi.edu/schema/dax-2.1.xsd"
SCHEMA_VERSION = u"2.1"


class Namespace:
	"""Namespace values recognized by Pegasus. You can use these, or just
	pass your own value when creating a Profile object (see Profile).
	"""
	
	PEGASUS = u'pegasus'
	CONDOR = u'condor'
	DAGMAN = u'dagman'
	ENV = u'env'
	HINTS = u'hints'
	GLOBUS = u'globus'
	SELECTOR = u'selector'


class LFN:
	"""Logical file name attributes. These include:
	
	Linkage Attributes:
		NONE, INPUT, OUTPUT, INOUT
	Type Attributes:
	 	TYPE_DATA, TYPE_EXECUTABLE, TYPE_PATTERN
	Transfer Attributes:
		XFER_NOT, XFER_OPTIONAL, XFER_MANDATORY
	"""

	# Linkage
	NONE = u'none'
	INPUT = u'input'
	OUTPUT = u'output'
	INOUT = u'inout'

	# File type
	TYPE_DATA = u'data'
	TYPE_EXECUTABLE = u'executable'
	TYPE_PATTERN = u'pattern'

	# Transfer
	XFER_NOT = u'false'
	XFER_OPTIONAL = u'optional'
	XFER_MANDATORY = u'true'


class Filename:
	"""Filename(filename[,link][,register][,transfer][,optional][,type])
	
	A logical file name.
	
	Examples:
		input = Filename('input.txt',link=LFN.INPUT,transfer=True)
		intermediate = Filename('intermediate.txt',link=LFN.OUTPUT)
		result = Filename('result.txt',link=LFN.OUTPUT,register=True,transfer=True)
		opt = Filename('optional.txt',link=LFN.OUTPUT,optional=True)
		binary = Filename('bin/binary',link=LFN.INPUT,type=LFN.TYPE_EXECUTABLE,transfer=True)
	"""
	
	def __init__(self, filename, type=LFN.TYPE_DATA, link=LFN.NONE, 
				register=False, transfer=LFN.XFER_NOT, optional=None, varname=None):
		"""
		All arguments specify the workflow-level behavior of this Filename. Job-level
		behavior can be defined when adding the Filename to a Job's uses. If the
		properties are not overridden at the job-level, then the workflow-level
		values are used as defaults.
		
		If this LFN is to be used as a job's stdin/stdout/stderr then the value
		of link is ignored when generating the <std*> tags.
		
		Arguments:
			filename: The name of the file (required)
			type: The file type (see LFN)
			link: Is this file a workflow-level input/output/both? (see LFN)
			register: The default value for register (True/False)
			transfer: The default value for transfer (see LFN, or True/False)
			optional: The default value for optional (True/False)
			type: The file type (see LFN)
			varname: Only used for stdio files
		"""
		if filename is None:
			raise ValueError, 'filename required'
		self._filename = filename
		self._link = link
		self._register = register
		self._transfer = transfer
		self._optional = optional
		self._type = type
		self._varname = varname
		
	def getFilename(self):
		return self._filename
	def setFilename(self, filename):
		self._filename = filename
	def getType(self):
		return self._type
	def setType(self, type):
		self._type = type
	def getLink(self):
		return self._link
	def setLink(self, link):
		self._link = link	
	def getRegister(self): 
		return self._register
	def setRegister(self, register):
		self._register = register
	def getTransfer(self):
		return self._transfer
	def setTransfer(self, transfer):
		self._transfer = transfer
	def getOptional(self):
		return self._optional
	def setOptional(self, optional):
		self._optional = optional
	def getVarname(self):
		return self._varname
	def setVarname(self, varname):
		self._varname = varname
		
	def __str__(self):
		"""Returns argument-style version of the filename XML tag"""
		return self.toArgumentXML()

	def toArgumentXML(self):
		"""Returns an XML representation of this file as a short filename 
		tag for use in job arguments"""
		return u'<filename file="%s"/>' % (self._filename)
	
	def toFilenameXML(self):
		"""Returns an XML representation of this file as a filename tag"""
		xml = StringIO()

		xml.write(u'<filename file="%s"' % self._filename)
		if self._link is not None:
			xml.write(u' link="%s"' % self._link)
		if self._optional is not None:
			if isinstance(self._optional, bool):
				xml.write(u' optional="%s"' % str(self._optional).lower())
			else:
				xml.write(u' optional="%s"' % self._optional)
		xml.write(u'/>')

		result = xml.getvalue()
		xml.close()
		return result
		
	def toStdioXML(self, tag):
		"""Returns an XML representation of this file as a stdin/out/err tag"""
		xml = StringIO()
		xml.write(u'<%s file="%s"' % (tag, self._filename))
		if self._varname is not None:
			xml.write(u' varname="%s"' % self._varname)
		if tag is 'stdin':
			xml.write(u' link="input"') # stdin is always input
		else:
			xml.write(u' link="output"') # stdout/stderr are always output
		xml.write(u'/>')
		
		result = xml.getvalue()
		xml.close()
		return result
	

class Profile:
	"""Profile(namespace,key,value[,origin])
	
	A Profile captures scheduler-, system-, and environment-specific 
	parameters in a uniform fashion. Each profile declaration assigns a value
	to a key within a namespace. The origin records what entity is responsible
	for setting the profile and is optional.
	
	Examples:
		path = Profile(Namespace.ENV,'PATH','/bin')
		vanilla = Profile(Namespace.CONDOR,'universe','vanilla')
		path = Profile(namespace='env',key='PATH',value='/bin')
		path = Profile('env','PATH','/bin')
	"""
	
	def __init__(self, namespace, key, value, origin=None):
		"""
		Arguments:
			namespace: The namespace of the profile (see Namespace) 
			key: The key name. Can be anything that responds to str().
			value: The value for the profile. Can be anything that responds to str().
			origin: The entity responsible for setting this profile (optional)
		"""
		self._namespace = namespace
		self._key = key
		self._value = value
		self._origin = origin

	def toXML(self):
		"""Return an XML representation of this profile"""
		xml = StringIO()
		xml.write(u'<profile namespace="%s" key="%s"' % (self._namespace, self._key))
		if self._origin is not None:
			xml.write(u' origin="%s"' % self._origin)
		xml.write(u'>')
		xml.write(unicode(self._value))
		xml.write(u'</profile>')
		result = xml.getvalue()
		xml.close()
		return result
		
	def __str__(self):
		return u'%s:%s = %s' % (self._namespace, self._key, self._value)


class Job:
	"""Job(name[,id][,namespace][,version][,dv_name][,dv_namespace][,dv_version][,level][,compound])
	
	This class defines the specifics of a job to run in an abstract manner.
	All filename references still refer to logical files. All references
	transformations also refer to logical transformations, though
	physical location hints can be passed through profiles.
	
	Examples:
		sleep = Job(id="ID0001",name="sleep")
		jbsim = Job(id="ID0002",name="jbsim",namespace="cybershake",version="2.1")
		merge = Job(name="merge",level=2) 
		
	Several arguments can be added at the same time:
		input = Filename(...)
		output = Filename(...)
		job.addArguments("-i",input,"-o",output)
	
	Profiles are added similarly:
		job.addProfile(Profile(Namespace.ENV,key='PATH',value='/bin'))
		
	Adding file uses is simple, and you can override global Filename attributes:
		job.addUses(input,LFN.INPUT)
		job.addUses(output,LFN.OUTPUT,transfer=True,register=True)
	"""
	
	class Use:
		"""Use(file[,link][,register][,transfer][,optional][,temporaryHint])

		Use of a logical file name. Used for referencing LFNs in the DAX.

		Note: This class is used internally. You shouldn't need to use it in
		your code. You should use Job.addUses(...).
		"""

		def __init__(self, file, link=None, register=None, transfer=None, 
					optional=None, temporaryHint=None):
			if file is None:
				raise ValueError, 'file required'
			self._file = file
			self._link = link
			self._optional = optional
			self._register = register
			self._transfer = transfer
			self._temporaryHint = temporaryHint

		def toXML(self):
			xml = StringIO()

			if self._link is None: link = self._file.getLink()
			else: link = self._link
			if self._optional is None: optional = self._file.getOptional()
			else: optional = self._optional
			if self._register is None: register = self._file.getRegister()
			else: register = self._register
			if self._transfer is None: transfer = self._file.getTransfer()
			else: transfer = self._transfer
			type = self._file.getType()
			temporaryHint = self._temporaryHint
			
			xml.write(u'<uses file="%s"' % self._file.getFilename())
			if temporaryHint is not None:
				if isinstance(temporaryHint, bool):
					xml.write(u' temporaryHint="%s"' % unicode(temporaryHint).lower())
				else:
					xml.write(u' temporaryHint="%s"' % temporaryHint)
			if link is not None:
				xml.write(u' link="%s"' % link)
			if optional is not None:
				if isinstance(optional, bool):
					xml.write(u' optional="%s"' % unicode(optional).lower())
				else:
					xml.write(u' optional="%s"' % optional)
			if register is not None:
				if isinstance(register, bool):
					xml.write(u' register="%s"' % unicode(register).lower())
				else:
					xml.write(u' register="%s"' % register)
			if transfer is not None:
				if isinstance(transfer, bool):
					xml.write(u' transfer="%s"' % unicode(transfer).lower())
				else:
					xml.write(u' transfer="%s"' % transfer)
			if type is not None:
				xml.write(u' type="%s"' % type)
			xml.write(u'/>')

			result = xml.getvalue()
			xml.close()
			return result
			
	def __init__(self, name, id=None, namespace=None, version=None,
				dv_name=None, dv_namespace=None, dv_version=None,
				level=None, compound=None):
		"""The ID for each job should be unique in the DAX. If it is None, then
		it will be automatically generated when the job is added to the DAX.
		As far as I can tell this ID is only used for uniqueness during
		planning, and is otherwise ignored. For example, when Condor is running
		the job there doesn't seem to be a way to use this ID to trace the
		running job back to its entry in the DAX.
		
		The name, namespace, and version should match what you have in your
		transformation catalog. For example, if namespace="foo" name="bar" 
		and version="1.0", then the transformation catalog should have an
		entry for "foo::bar:1.0".
		
		Level is the level in the workflow. So if you have a workflow with
		three jobs--A, B, and C--and you have dependencies between A->B and
		B->C, then A is level 1, B is level 2, and C is level 3. You don't
		need to specify this because Pegasus calculates it automatically.
		
		I have no idea what 'compound' does, or what the 'dv_' stuff does.
		
		Arguments:
			name: The transformation name (required)
			id: A unique identifier for the job (autogenerated if None)
			namespace: The namespace of the transformation
			version: The transformation version
			dv_name: ?
			dv_namespace: ?
			dv_version: ?
			level: The level of the job in the workflow
			compound: ?
		"""
		if name is None:
			raise ValueError, 'name required'
		self._name = name
		self._namespace = namespace
		self._version = version
		self._id = id
		self._dv_namespace = dv_namespace
		self._dv_name = dv_name
		self._dv_version = dv_version
		self._level = level
		self._compound = compound
		
		self._arguments = []
		self._profiles = []
		self._uses = []

		self._stdout = None
		self._stderr = None
		self._stdin = None

	
	def addArguments(self, *arguments):
		"""Add several arguments to the job"""
		self._arguments.extend(arguments)

	def addArgument(self, arg):
		"""Add an argument to the job"""
		self.addArguments(arg)

	def addProfile(self,profile):
		"""Add a profile to the job"""
		self._profiles.append(profile)

	def addUses(self, file, link=None, register=None, transfer=None, 
				optional=None, temporaryHint=None):
		"""Add a logical filename that the job uses.
		
		Optional arguments to this method specify job-level attributes of
		the 'uses' tag in the DAX. If not specified, these values default
		to those specified when creating the Filename object.
		
		I don't know what 'temporaryHint' does.
		
		Arguments:
			file: A Filename object representing the logical file name
			link: Is this file a job input, output or both (See LFN)
			register: Should this file be registered in RLS? (True/False)
			transfer: Should this file be transferred? (True/False or See LFN)
			optional: Is this file optional, or should its absence be an error?
			temporaryHint: ?
		"""
		use = Job.Use(file,link,register,transfer,optional)
		self._uses.append(use)

	def setStdout(self, filename):
		"""Redirect stdout to a file"""
		self._stdout = filename

	def setStderr(self, filename):
		"""Redirect stderr to a file"""
		self._stderr = filename

	def setStdin(self, filename):
		"""Redirect stdin from a file"""
		self._stdin = filename

	def setID(self, id):
		"""Set the ID of this job"""
		self._id = id
		
	def getID(self):
		"""Return the job ID"""
		return self._id
		
	def setNamespace(self, namespace):
		"""Set the transformation namespace for this job"""
		self._namespace = namespace
		
	def getNamespace(self):
		"""Get the transformation namespace for this job"""
		return self._namespace
		
	def setName(self, name):
		"""Set the transformation name of this job"""
		self._name = name
		
	def getName(self):
		"""Get the transformation name of this job"""
		return self._name
		
	def setVersion(self, version):
		"""Set the version of the transformation"""
		self._version = version
		
	def getVersion(self):
		"""Get the version of the transformation"""
		return self._version
		
	def toXML(self,level=0,indent=u'\t'):
		"""Return an XML representation of this job
		
		Arguments:
			level: The level of indentation
			indent: The indentation string
		"""
		xml = StringIO()
		indentation = u''.join(indent for x in range(0,level))
		
		# Open tag
		xml.write(indentation)
		xml.write(u'<job id="%s"' % self._id)
		if self._namespace is not None: xml.write(u' namespace="%s"' % self._namespace)
		xml.write(u' name="%s"' % self._name)
		if self._version is not None: xml.write(u' version="%s"' % self._version)
		if self._dv_namespace is not None: xml.write(u' dv-namespace="%s"' % self._dv_namespace)
		if self._dv_name is not None: xml.write(u' dv-name="%s"' % self._dv_name)
		if self._dv_version is not None: xml.write(u' dv-version="%s"' % self._dv_version)
		if self._level is not None: xml.write(u' level="%s"' % self._level)
		if self._compound is not None: xml.write(u' compound="%s"' % self._compound)
		xml.write(u'>\n')

		# Arguments
		if len(self._arguments) > 0:
			xml.write(indentation)
			xml.write(indent)
			xml.write(u'<argument>')
			xml.write(u' '.join(unicode(x) for x in self._arguments))
			xml.write(u'</argument>\n')

		# Profiles
		if len(self._profiles) > 0:
			for pro in self._profiles:
				xml.write(indentation)
				xml.write(indent)
				xml.write(u'%s\n' % pro.toXML())
		
		# Stdin/xml/err
		if self._stdin is not None:
			xml.write(indentation)
			xml.write(indent)
			xml.write(self._stdin.toStdioXML('stdin'))
			xml.write(u'\n')
		if self._stdout is not None:
			xml.write(indentation)
			xml.write(indent)
			xml.write(self._stdout.toStdioXML('stdout'))
			xml.write(u'\n')
		if self._stderr is not None:
			xml.write(indentation)
			xml.write(indent)
			xml.write(self._stderr.toStdioXML('stderr'))
			xml.write(u'\n')

		# Uses
		if len(self._uses) > 0:
			for use in self._uses:
				xml.write(indentation)
				xml.write(indent)
				xml.write(use.toXML())
				xml.write(u'\n')
				
		# Close tag
		xml.write(indentation)
		xml.write(u'</job>')
		
		result = xml.getvalue()
		xml.close()
		return result


class DAX:
	"""DAX(name[,count][,index])
	
	Representation of a directed acyclic graph in XML (DAX).
	
	Examples:
		dax = DAX('diamond')
		part5 = DAX('partition_5',count=10,index=5)
		
	Adding jobs:
		a = Job(...)
		dax.addJob(a)
		
	Adding parent-child control-flow dependency:
		dax.addDependency(a,b)
		dax.addDependency(a,c)
		dax.addDependency(b,d)
		dax.addDependency(c,d)
		
	Adding Filenames (this is not required to produce a valid DAX):
		input = Filename(...)
		dax.addFilename(input)
		
	Writing a DAX out to a file:
		f = open('diamond.dax','w')
		dax.writeXML(f)
		f.close()
	"""
	
	class Dependency:
		"""A control-flow dependency between a child and its parents"""
		def __init__(self,child):
			self._child = child
			self._parents = []

		def addParent(self, parent):
			self._parents.append(parent)

		def toXML(self, level=0, indent=u'\t'):
			xml = StringIO()
			indentation = ''.join([indent for x in range(0,level)])
			
			xml.write(indentation)
			xml.write(u'<child ref="%s"/>\n' % self._child.getID())
			for parent in self._parents:
				xml.write(indentation)
				xml.write(indent)
				xml.write(u'<parent ref="%s"/>\n' % parent.getID())
			xml.write(indentation)
			xml.write(u'</child>')
			
			result = xml.getvalue()
			xml.close()
			return result

	def __init__(self, name, count=1, index=0):
		"""
		Arguments:
			name: The name of the workflow
			count: Total number of DAXes that will be created
			index: Zero-based index of this DAX
		"""
		self._name = name
		self._count = count
		self._index = index
		
		# This is used to generate unique ID numbers
		self._sequence = 1
		
		self._jobs = []
		self._filenames = []
		self._lookup = {} # A lookup table for dependencies
		self._dependencies = []

	def getName(self):
		return self._name

	def setName(self,name):
		self._name = name

	def getCount(self):
		return self._count

	def setCount(self,count):
		self._count = count

	def getIndex(self):
		return self._index

	def setIndex(self,index):
		self._index = index

	def addJob(self,job):
		"""Add a job to the list of jobs in the DAX"""
		# Add an auto-generated ID if the job doesn't have one
		if job.getID() is None:
			job.setID("ID%07d" % self._sequence)
			self._sequence += 1
		self._jobs.append(job)
		
	def addFilename(self, filename):
		"""Add a filename"""
		self._filenames.append(filename)
		
	def addDependency(self, parent, child):
		"""Add a control flow dependency"""
		if not child in self._lookup:
			dep = DAX.Dependency(child)
			self._lookup[child] = dep
			self._dependencies.append(dep)
		self._lookup[child].addParent(parent)

	def writeXML(self, out, indent='\t'):
		"""Write the DAX as XML to a stream"""
		
		# Preamble
		out.write(u'<?xml version="1.0" encoding="UTF-8"?>\n')
		
		# Metadata
		out.write(u'<!-- generated: %s -->\n' % datetime.datetime.now())
		out.write(u'<!-- generated by: %s -->\n' % pwd.getpwuid(os.getuid())[0])
		out.write(u'<!-- generator: python -->\n')
		
		# Open tag
		out.write(u'<adag xmlns="%s" ' % SCHEMA_NAMESPACE)
		out.write(u'xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" ')
		out.write(u'xsi:schemaLocation="%s %s" ' % (SCHEMA_NAMESPACE, SCHEMA_LOCATION))
		out.write(u'version="%s" ' % SCHEMA_VERSION)
		out.write(u'count="%d" index="%d" name="%s" ' % (self._count, self._index, self._name))
		out.write(u'jobCount="%d" fileCount="%d" childCount="%d">\n' % (len(self._jobs), len(self._filenames), len(self._dependencies)))

		# Files
		out.write(u'\n%s<!-- part 1: list of all referenced files (may be empty) -->\n' % indent)
		for filename in self._filenames:
			out.write(indent)
			out.write(filename.toFilenameXML())
			out.write('\n')
		
		# Jobs
		out.write(u'\n%s<!-- part 2: definition of all jobs (at least one) -->\n' % indent)
		for job in self._jobs:
			out.write(job.toXML(level=1,indent=indent))
			out.write(u'\n')
		
		# Dependencies
		out.write(u'\n%s<!-- part 3: list of control-flow dependencies (may be empty) -->\n' % indent)
		for dep in self._dependencies:
			out.write(dep.toXML(level=1,indent=indent))
			out.write(u'\n')
		
		# Close tag
		out.write(u'</adag>\n')

if __name__ == '__main__':
	"""An example of using the DAX API"""

	# Create a DAX
	diamond = DAX("diamond")

	# Create some logical file names
	a = Filename("f.a",link=LFN.INPUT,transfer=True)
	b1 = Filename("f.b1",link=LFN.OUTPUT,transfer=True)
	b2 = Filename("f.b2",link=LFN.OUTPUT,transfer=True)
	c1 = Filename("f.c1",link=LFN.OUTPUT,transfer=True)
	c2 = Filename("f.c2",link=LFN.OUTPUT,transfer=True)
	d = Filename("f.d",link=LFN.OUTPUT,transfer=True,register=True)

	# Add the filenames to the DAX (this is not strictly required)
	diamond.addFilename(a)
	diamond.addFilename(d)

	# Add a preprocess job
	preprocess = Job(namespace="diamond",name="preprocess",version="2.0")
	preprocess.addArguments("-a preprocess","-T60","-i",a,"-o",b1,b2)
	preprocess.addUses(a,link=LFN.INPUT)
	preprocess.addUses(b1,link=LFN.OUTPUT)
	preprocess.addUses(b2,link=LFN.OUTPUT)
	diamond.addJob(preprocess)

	# Add left Findrange job
	frl = Job(namespace="diamond",name="findrange",version="2.0")
	frl.addArguments("-a findrange","-T60","-i",b1,"-o",c1)
	frl.addUses(b1,link=LFN.INPUT)
	frl.addUses(c1,link=LFN.OUTPUT)
	diamond.addJob(frl)

	# Add right Findrange job
	frr = Job(namespace="diamond",name="findrange",version="2.0")
	frr.addArguments("-a findrange","-T60","-i",b2,"-o",c2)
	frr.addUses(b2,link=LFN.INPUT)
	frr.addUses(c2,link=LFN.OUTPUT)
	diamond.addJob(frr)

	# Add Analyze job
	analyze = Job(namespace="diamond",name="analyze",version="2.0")
	analyze.addArguments("-a analyze","-T60","-i",c1,c2,"-o",d)
	analyze.addUses(c1,link=LFN.INPUT)
	analyze.addUses(c2,link=LFN.INPUT)
	analyze.addUses(d,link=LFN.OUTPUT)
	diamond.addJob(analyze)

	# Add control-flow dependencies
	diamond.addDependency(parent=preprocess, child=frl)
	diamond.addDependency(parent=preprocess, child=frr)
	diamond.addDependency(parent=frl, child=analyze)
	diamond.addDependency(parent=frr, child=analyze)

	# Write the DAX to stdout
	import sys
	diamond.writeXML(sys.stdout)
