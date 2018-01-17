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
from __future__ import print_function

__author__ = "Gideon Juve <juve@usc.edu>"
__all__ = ["DAX","Filename","Profile","Job","Namespace","LFN",
	"parse","parseString"]
__version__ = "2.1"

import datetime, pwd, os
from cStringIO import StringIO
import xml.sax
import xml.sax.handler
import shlex

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
	"""Filename(filename[,type][,link][,register][,transfer][,optional][,varname])
	
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
			raise ValueError('filename required')
		self.filename = filename
		self.link = link
		self.register = register
		self.transfer = transfer
		self.optional = optional
		self.type = type
		self.varname = varname
		
	def getFilename(self):
		return self.filename
	def setFilename(self, filename):
		self.filename = filename
	def getType(self):
		return self.type
	def setType(self, type):
		self.type = type
	def getLink(self):
		return self.link
	def setLink(self, link):
		self.link = link	
	def getRegister(self): 
		return self.register
	def setRegister(self, register):
		self.register = register
	def getTransfer(self):
		return self.transfer
	def setTransfer(self, transfer):
		self.transfer = transfer
	def getOptional(self):
		return self.optional
	def setOptional(self, optional):
		self.optional = optional
	def getVarname(self):
		return self.varname
	def setVarname(self, varname):
		self.varname = varname
		
	def __str__(self):
		"""Returns argument-style version of the filename XML tag"""
		return self.toArgumentXML()

	def toArgumentXML(self):
		"""Returns an XML representation of this file as a short filename 
		tag for use in job arguments"""
		return u'<filename file="%s"/>' % (self.filename)
	
	def toFilenameXML(self):
		"""Returns an XML representation of this file as a filename tag"""
		xml = StringIO()

		xml.write(u'<filename file="%s"' % self.filename)
		if self.link is not None:
			xml.write(u' link="%s"' % self.link)
		if self.optional is not None:
			if isinstance(self.optional, bool):
				xml.write(u' optional="%s"' % str(self.optional).lower())
			else:
				xml.write(u' optional="%s"' % self.optional)
		xml.write(u'/>')

		result = xml.getvalue()
		xml.close()
		return result
		
	def toStdioXML(self, tag):
		"""Returns an XML representation of this file as a stdin/out/err tag"""
		xml = StringIO()
		xml.write(u'<%s file="%s"' % (tag, self.filename))
		if self.varname is not None:
			xml.write(u' varname="%s"' % self.varname)
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
		self.namespace = namespace
		self.key = key
		self.value = value
		self.origin = origin

	def toXML(self):
		"""Return an XML representation of this profile"""
		xml = StringIO()
		xml.write(u'<profile namespace="%s" key="%s"' % (self.namespace, self.key))
		if self.origin is not None:
			xml.write(u' origin="%s"' % self.origin)
		xml.write(u'>')
		xml.write(unicode(self.value))
		xml.write(u'</profile>')
		result = xml.getvalue()
		xml.close()
		return result
		
	def __str__(self):
		return u'%s:%s = %s' % (self.namespace, self.key, self.value)


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
				raise ValueError('file required')
			self.file = file
			self.link = link
			self.optional = optional
			self.register = register
			self.transfer = transfer
			self.temporaryHint = temporaryHint

		def toXML(self):
			xml = StringIO()

			if self.link is None: link = self.file.getLink()
			else: link = self.link
			if self.optional is None: optional = self.file.getOptional()
			else: optional = self.optional
			if self.register is None: register = self.file.getRegister()
			else: register = self.register
			if self.transfer is None: transfer = self.file.getTransfer()
			else: transfer = self.transfer
			type = self.file.getType()
			temporaryHint = self.temporaryHint
			
			xml.write(u'<uses file="%s"' % self.file.getFilename())
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
			raise ValueError('name required')
		self.name = name
		self.namespace = namespace
		self.version = version
		self.id = id
		self.dv_namespace = dv_namespace
		self.dv_name = dv_name
		self.dv_version = dv_version
		self.level = level
		self.compound = compound
		
		self.arguments = []
		self.profiles = []
		self.uses = []

		self.stdout = None
		self.stderr = None
		self.stdin = None

	
	def addArguments(self, *arguments):
		"""Add several arguments to the job"""
		self.arguments.extend(arguments)

	def addArgument(self, arg):
		"""Add an argument to the job"""
		self.addArguments(arg)

	def addProfile(self,profile):
		"""Add a profile to the job"""
		self.profiles.append(profile)

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
		self.uses.append(use)

	def setStdout(self, filename):
		"""Redirect stdout to a file"""
		self.stdout = filename

	def setStderr(self, filename):
		"""Redirect stderr to a file"""
		self.stderr = filename

	def setStdin(self, filename):
		"""Redirect stdin from a file"""
		self.stdin = filename

	def setID(self, id):
		"""Set the ID of this job"""
		self.id = id
		
	def getID(self):
		"""Return the job ID"""
		return self.id
		
	def setNamespace(self, namespace):
		"""Set the transformation namespace for this job"""
		self.namespace = namespace
		
	def getNamespace(self):
		"""Get the transformation namespace for this job"""
		return self.namespace
		
	def setName(self, name):
		"""Set the transformation name of this job"""
		self.name = name
		
	def getName(self):
		"""Get the transformation name of this job"""
		return self.name
		
	def setVersion(self, version):
		"""Set the version of the transformation"""
		self.version = version
		
	def getVersion(self):
		"""Get the version of the transformation"""
		return self.version
		
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
		xml.write(u'<job id="%s"' % self.id)
		if self.namespace is not None: xml.write(u' namespace="%s"' % self.namespace)
		xml.write(u' name="%s"' % self.name)
		if self.version is not None: xml.write(u' version="%s"' % self.version)
		if self.dv_namespace is not None: xml.write(u' dv-namespace="%s"' % self.dv_namespace)
		if self.dv_name is not None: xml.write(u' dv-name="%s"' % self.dv_name)
		if self.dv_version is not None: xml.write(u' dv-version="%s"' % self.dv_version)
		if self.level is not None: xml.write(u' level="%s"' % self.level)
		if self.compound is not None: xml.write(u' compound="%s"' % self.compound)
		xml.write(u'>\n')

		# Arguments
		if len(self.arguments) > 0:
			xml.write(indentation)
			xml.write(indent)
			xml.write(u'<argument>')
			xml.write(u' '.join(unicode(x) for x in self.arguments))
			xml.write(u'</argument>\n')

		# Profiles
		if len(self.profiles) > 0:
			for pro in self.profiles:
				xml.write(indentation)
				xml.write(indent)
				xml.write(u'%s\n' % pro.toXML())
		
		# Stdin/xml/err
		if self.stdin is not None:
			xml.write(indentation)
			xml.write(indent)
			xml.write(self.stdin.toStdioXML('stdin'))
			xml.write(u'\n')
		if self.stdout is not None:
			xml.write(indentation)
			xml.write(indent)
			xml.write(self.stdout.toStdioXML('stdout'))
			xml.write(u'\n')
		if self.stderr is not None:
			xml.write(indentation)
			xml.write(indent)
			xml.write(self.stderr.toStdioXML('stderr'))
			xml.write(u'\n')

		# Uses
		if len(self.uses) > 0:
			for use in self.uses:
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
			self.child = child
			self.parents = []

		def addParent(self, parent):
			self.parents.append(parent)

		def toXML(self, level=0, indent=u'\t'):
			xml = StringIO()
			indentation = ''.join([indent for x in range(0,level)])
			
			xml.write(indentation)
			xml.write(u'<child ref="%s">\n' % self.child.getID())
			for parent in self.parents:
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
		self.name = name
		self.count = count
		self.index = index
		
		# This is used to generate unique ID numbers
		self.sequence = 1
		
		self.jobs = []
		self.filenames = []
		self.lookup = {} # A lookup table for dependencies
		self.dependencies = []

	def getName(self):
		return self.name

	def setName(self,name):
		self.name = name

	def getCount(self):
		return self.count

	def setCount(self,count):
		self.count = count

	def getIndex(self):
		return self.index

	def setIndex(self,index):
		self.index = index

	def addJob(self,job):
		"""Add a job to the list of jobs in the DAX"""
		# Add an auto-generated ID if the job doesn't have one
		if job.getID() is None:
			job.setID("ID%07d" % self.sequence)
			self.sequence += 1
		self.jobs.append(job)
		
	def addFilename(self, filename):
		"""Add a filename"""
		self.filenames.append(filename)
		
	def addDependency(self, parent, child):
		"""Add a control flow dependency"""
		if not child in self.lookup:
			dep = DAX.Dependency(child)
			self.lookup[child] = dep
			self.dependencies.append(dep)
		self.lookup[child].addParent(parent)

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
		out.write(u'count="%d" index="%d" name="%s" ' % (self.count, self.index, self.name))
		out.write(u'jobCount="%d" fileCount="%d" childCount="%d">\n' % (len(self.jobs), len(self.filenames), len(self.dependencies)))

		# Files
		out.write(u'\n%s<!-- part 1: list of all referenced files (may be empty) -->\n' % indent)
		for filename in self.filenames:
			out.write(indent)
			out.write(filename.toFilenameXML())
			out.write('\n')
		
		# Jobs
		out.write(u'\n%s<!-- part 2: definition of all jobs (at least one) -->\n' % indent)
		for job in self.jobs:
			out.write(job.toXML(level=1,indent=indent))
			out.write(u'\n')
		
		# Dependencies
		out.write(u'\n%s<!-- part 3: list of control-flow dependencies (may be empty) -->\n' % indent)
		for dep in self.dependencies:
			out.write(dep.toXML(level=1,indent=indent))
			out.write(u'\n')
		
		# Close tag
		out.write(u'</adag>\n')


class DAXHandler(xml.sax.handler.ContentHandler):
	"""
	This is a DAX parser
	"""
	def __init__(self):
		self.dax = None
		self.jobmap = {}
		self.filemap = {}
		self.lastJob = None
		self.lastChild = None
		self.lastArguments = None
		self.lastProfile = None
		
	def startElement(self, name, attrs):
		if name == "adag":
			name = attrs.get("name")
			count = int(attrs.get("count","1"))
			index = int(attrs.get("index","0"))
			self.dax = DAX(name,count,index)
		elif name == "filename":
			if self.lastJob is None:
				file = attrs.get("file")
				link = attrs.get("link")
				optional = attrs.get("optional")
				f = Filename(file, type=None, link=link, register=None, 
					transfer=None, optional=optional)
				self.dax.addFilename(f)
				self.filemap[name] = f
			else:
				name = attrs.get("file")
				if name in self.filemap:
					f = self.filemap[name]
				else:
					f = Filename(name)
					self.filemap[name] = f
				if self.lastProfile is None:
					self.lastArguments.append(f)
				else:
					self.lastProfile.value = f
		elif name == "job":
			id = attrs.get("id")
			namespace = attrs.get("namespace")
			name = attrs.get("name")
			version = attrs.get("version")
			dv_namespace = attrs.get("dv-namespace")
			dv_name = attrs.get("dv-name")
			dv_version = attrs.get("dv-version")
			level = attrs.get("level")
			compound = attrs.get("compound")
			job = Job(id=id, namespace=namespace, name=name, version=version,
				dv_namespace=dv_namespace, dv_name=dv_name, dv_version=dv_version,
				level=level, compound=compound)
			self.dax.addJob(job)
			self.lastJob = job
			self.jobmap[job.getID()] = job
		elif name == "argument":
			self.lastArguments = []
		elif name == "profile":
			namespace = attrs.get("namespace")
			key = attrs.get("key")
			self.lastProfile = Profile(namespace,key,"")
			self.lastJob.addProfile(self.lastProfile)
		elif name in ["stdin","stdout","stderr"]:
			file = attrs.get("file")
			link = attrs.get("link")
			varname = attrs.get("varname")
			if file in self.filemap:
				f = self.filemap[file]
			else:
				f = Filename(file,link=link)
				self.filemap[file] = f
			if varname is not None:
				if f.varname is None:
					f.varname = varname
			if name == "stdin":
				self.lastJob.setStdin(f)
			elif name == "stdout":
				self.lastJob.setStdout(f)
			else:
				self.lastJob.setStderr(f)
		elif name == "uses":
			file = attrs.get("file")
			link = attrs.get("link")
			register = attrs.get("register")
			transfer = attrs.get("transfer")
			type = attrs.get("type")
			temporaryHint = attrs.get("temporaryHint")
			if file in self.filemap:
				f = self.filemap[file]
				if f.type is None:
					f.type = type
			else:
				f = Filename(file, type=type, link=link,
					register=register, transfer=transfer)
				self.filemap[file] = f
			self.lastJob.addUses(f,link=link,register=register,
				transfer=transfer,temporaryHint=temporaryHint)
		elif name == "child":
			id = attrs.get("ref")
			child = self.jobmap[id]
			self.lastChild = child
		elif name == "parent":
			id = attrs.get("ref")
			parent = self.jobmap[id]
			self.dax.addDependency(parent, self.lastChild)
			
	def characters(self, chars):
		if self.lastArguments is not None:
			self.lastArguments += [unicode(a) for a in shlex.split(chars)]
		elif self.lastProfile is not None:
			self.lastProfile.value += chars
			
	def endElement(self, name):
		if name == "child":
			self.lastChild = None
		elif name == "job":
			self.lastJob = None
		elif name == "argument":
			self.lastJob.addArguments(*self.lastArguments)
			self.lastArguments = None
		elif name == "profile":
			self.lastProfile = None

				
def parse(fname):
	"""
	Parse DAX from a Pegasus DAX file.
	"""
	handler = DAXHandler()
	xml.sax.parse(fname, handler)
	return handler.dax


def parseString(string):
	"""
	Parse DAX from a string
	"""
	handler = DAXHandler()
	xml.sax.parseString(string, handler)
	return handler.dax


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
	
	out = StringIO()
	diamond.writeXML(out)
	foo1 = out.getvalue()
	out.close()
	
	diamond = parseString(foo1)
	
	out = StringIO()
	diamond.writeXML(out)
	foo2 = out.getvalue()
	out.close()
	
	print(foo1)
	print(foo2)
	
