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

"""API for generating Pegasus DAXes

The classes in this module can be used to generate DAXes that can be
read by Pegasus.

The official DAX schema is here: http://pegasus.isi.edu/schema/dax-3.2.xsd
"""

__author__ = "Gideon Juve <juve@usc.edu>"
__all__ = ["ADAG","DAX","DAG","Namespace","Arch","Link","When",
		   "Transfer","OS","File","Executable","Metadata","PFN",
		   "Profile","Transformation","Job","parse","parseString"]
__version__ = "3.2"

import datetime, pwd, os, sys
from cStringIO import StringIO
import xml.sax
import xml.sax.handler
import shlex

SCHEMA_NAMESPACE = u"http://pegasus.isi.edu/schema/DAX"
SCHEMA_LOCATION = u"http://pegasus.isi.edu/schema/dax-3.2.xsd"
SCHEMA_VERSION = u"3.2"

def escape(text):
	"""Escape special characters in XML"""
	o = ""
	for c in text:
		if   c == '"':	o += "&quot;"
		elif c == "'":	o += "&apos;"
		elif c == "<":	o += "&lt;"
		elif c == ">":	o += "&gt;"
		elif c == "&":	o += "&amp;"
		else: o += c
	return unicode(o)
	
class Element:
	def __init__(self, name, attrs=[]):
		self.name = name
		self.attrs = []
		for attr,value in attrs:
			if value is not None:
				if type(value) == bool:
					value = unicode(value).lower()
				attr = unicode(attr.replace('__',':'))
				self.attrs.append((attr,value))
		self.children = []
		self.flat = False
		
	def element(self, element):
		self.children.append(element)
		return element
		
	def text(self, value):
		self.children.append(escape(value))
		return self
		
	def comment(self, message):
		self.children.append(u"<!- %s -->" % escape(message))
		
	def flatten(self):
		self.flat = True
		return self
		
	def toXML(self, stream=sys.stdout, level=0, flatten=False):
		flat = self.flat or flatten
		
		stream.write(u'<%s' % self.name)
		
		for attr,value in self.attrs:
			value = escape(value)
			stream.write(u' %s="%s"' % (attr, value))
		
		if len(self.children) == 0:
			stream.write(u'/>')
		else:
			stream.write(u'>')
			if not flat:
				stream.write(u'\n')
			for child in self.children:
				if not flat:
					stream.write(u'\t'*(level+1))
				if type(child) == unicode:
					stream.write(child)
				else:
					child.toXML(stream, level+1, flat)
				if not flat:
					stream.write(u'\n')
			if not flat:
				stream.write(u'\t'*level)
			stream.write(u'</%s>' % self.name)
			
class Namespace:
	"""
	Namespace values recognized by Pegasus. See Executable, 
	Transformation, and Job.
	"""
	PEGASUS = u'pegasus'
	CONDOR = u'condor'
	DAGMAN = u'dagman'
	ENV = u'env'
	HINTS = u'hints'
	GLOBUS = u'globus'
	SELECTOR = u'selector'
	STAT = u'stat'

class Arch:
	"""
	Architecture types. See Executable.
	"""
	X86 = u'x86'
	X86_64 = u'x86_64'
	PPC = u'ppc'
	PPC_64 = u'ppc_64'
	IA64 = u'ia64'
	SPARCV7 = u'sparcv7'
	SPARCV9 = u'sparcv9'
	AMD64 = u'amd64'

class Link:
	"""
	Linkage attributes. See File, Executable and uses().
	"""
	NONE = u'none'
	INPUT = u'input'
	OUTPUT = u'output'
	INOUT = u'inout'

class Transfer:
	"""
	Transfer types for uses. See Executable, File.
	"""
	FALSE = u'false'
	OPTIONAL = u'optional'
	TRUE = u'true'

class OS:
	"""
	OS types. See Executable.
	"""
	LINUX = u'linux'
	SUNOS = u'sunos'
	AIX = u'aix'
	MACOS = u'macos'
	WINDOWS = u'windows'

class When:
	"""
	Job states for notifications. See Job/DAX/DAG.invoke().
	"""
	NEVER = u'never'
	START = u'start'
	ON_ERROR = u'on_error'
	ON_SUCCESS = u'on_success'
	AT_END = u'at_end'
	ALL = u'all'
	
class CatalogType:
	"""Base class for File and Executable"""
	
	def __init__(self, name):
		"""
		All arguments specify the workflow-level behavior of this File. Job-level
		behavior can be defined when adding the File to a Job's uses. If the
		properties are not overridden at the job-level, then the workflow-level
		values are used as defaults.
		
		If this LFN is to be used as a job's stdin/stdout/stderr then the value
		of link is ignored when generating the <std*> tags.
		
		Arguments:
			name: The name of the file (required)
		"""
		if name is None:
			raise ValueError, 'name required'
		self.name = name
		self.profiles = []
		self.metadata = []
		self.pfns = []
		
	def __unicode__(self):
		return unicode(self.name)
		
	def __str__(self):
		return str(self.name)
		
	def addProfile(self, profile):
		"""Add a profile to this replica"""
		if isinstance(profile, Profile):
			self.profiles.append(profile)
		elif isinstance(profile, tuple):
			self.profiles.append(Profile(*profile))
		else:
			raise Exception("Invalid argument")
		
	def addMetadata(self, meta):
		"""Add some metadata to this replica"""
		if isinstance(meta, Metadata):
			self.metadata.append(meta)
		elif isinstance(meta, tuple):
			self.metadata.append(Metadata(*meta))
		else:
			raise Exception("Invalid argument")
		
	def addPFN(self, pfn):
		"""Add a PFN object to this replica"""
		if isinstance(pfn, PFN):
			self.pfns.append(pfn)
		elif isinstance(pfn, tuple):
			self.pfns.append(PFN(*pfn))
		else:
			raise Exception("Invalid argument")
			
	def innerXML(self, element):
		for p in self.profiles:
			element.element(p.toXML())
		for m in self.metadata:
			element.element(m.toXML())
		for p in self.pfns:
			element.element(p.toXML())

class File(CatalogType):
	"""File(name)
	
	A file entry for the DAX-level replica catalog, or a reference to a logical file
	used by the workflow.
	
	Examples:
		input = File('input.txt')
		
	Example use in job:
		input = File('input.txt')
		output = File('output.txt')
		job = Job(name="compute")
		job.uses(input, link=Link.INPUT, transfer=True)
		job.uses(output, link=Link.OUTPUT, transfer=True, register=True)
	"""
	def __init__(self, name):
		"""
		All arguments specify the workflow-level behavior of this File. Job-level
		behavior can be defined when adding the File to a Job's uses. If the
		properties are not overridden at the job-level, then the workflow-level
		values are used as defaults.
		
		If this LFN is to be used as a job's stdin/stdout/stderr then the value
		of link is ignored when generating the <std*> tags.
		
		Arguments:
			name: The name of the file (required)
		"""
		CatalogType.__init__(self, name)
		
	def toArgumentXML(self):
		"""Returns an XML representation of this file as a short filename 
		tag for use in job arguments"""
		return Element('file',[('name',self.name)])
		
	def toStdioXML(self, tag):
		"""Returns an XML representation of this file as a stdin/out/err tag"""
		if tag is 'stdin':
			link = "input" # stdin is always input
		else:
			link = "output" # stdout/stderr are always output
		
		return Element(tag, [
			('name',self.name),
			('link',link)
		])
		
	def toXML(self):
		e = Element('file',[('name',self.name)])
		self.innerXML(e)
		return e
	
class Executable(CatalogType):
	"""Executable(name[,namespace][,version][,arch][,os][,osrelease][,osversion][,glibc])
				
	An entry for an executable in the DAX-level replica catalog.
	
	Examples:
		grep = Executable("grep")
		grep = Executable(namespace="os",name="grep",version="2.3")
		grep = Executable(namespace="os",name="grep",version="2.3",arch=Arch.X86)
		grep = Executable(namespace="os",name="grep",version="2.3",arch=Arch.X86,os=OS.LINUX)
	"""
	def __init__(self, name, namespace=None, version=None, arch=None, os=None, 
				 osrelease=None, osversion=None, glibc=None):
		"""
		Arguments:
			name: Logical name of executable
			namespace: Executable namespace
			version: Executable version
			arch: Architecture that this exe was compiled for
			os: Name of os that this exe was compiled for
			osrelease: Release of os that this exe was compiled for
			osversion: Version of os that this exe was compiled for
			glibc: Version of glibc this exe was compiled against
		"""
		CatalogType.__init__(self, name)
		self.namespace = namespace
		self.version = version
		self.arch = arch
		self.os = os
		self.osrelease = osrelease
		self.osversion = osversion
		self.glibc = glibc
		
	def toXML(self):
		"""Returns an XML representation of this file as a filename tag"""
		e = Element('executable', [
			('name',self.name),
			('namespace',self.namespace),
			('version',self.version),
			('arch',self.arch),
			('os',self.os),
			('osrelease',self.osrelease),
			('osversion',self.osversion),
			('glibc',self.glibc)
		])
		self.innerXML(e)
		return e
	
class Metadata:
	"""Metadata(key,type,value)
	
	A way to add metadata to File and Executable objects. This is
	useful if you want to annotate the DAX with things like file
	sizes, application-specific attributes, etc.
	
	There is currently no restriction on the type.
	
	Examples:
		s = Metadata('size','int','12')
		a = Metadata('algorithm','string','plav')
	"""
	def __init__(self, key, type, value):
		"""
		Arguments:
			key: The key name of the item
			type: The type of the value (e.g. string, int, float)
			value: The value of the item
		"""
		self.key = key
		self.type = type
		self.value = value
		
	def toXML(self):
		m = Element('metadata', [
			('key',self.key),
			('type',self.type)
		])
		m.text(self.value).flatten()
		return m
		
class PFN:
	"""PFN(url[,site])
	
	A physical file name. Used to provide URLs for files and executables
	in the DAX-level replica catalog.
	
	PFNs can be added to File and Executable.
	
	Examples:
		PFN('http://site.com/path/to/file.txt','site')
		PFN('http://site.com/path/to/file.txt',site='site')
		PFN('http://site.com/path/to/file.txt')
	"""
	def __init__(self, url, site="local"):
		"""
		Arguments:
			url: The url of the file.
			site: The name of the site. [default: local]
		"""
		self.url = url
		self.site = site
		self.profiles = []
	
	def addProfile(self, *profile):
		"""Add a profile to this PFN"""
		self.profiles.extend(profile)
		
	def toXML(self):
		pfn = Element('pfn', [
			('url',self.url),
			('site',self.site)
		])
		for p in self.profiles:
			pfn.element(p.toXML())
		return pfn

class Profile:
	"""Profile(namespace,key,value)
	
	A Profile captures scheduler-, system-, and environment-specific 
	parameters in a uniform fashion. Each profile declaration assigns a value
	to a key within a namespace.
	
	Profiles can be added to Job, DAX, DAG, File, Executable, and PFN.
	
	Examples:
		path = Profile(Namespace.ENV,'PATH','/bin')
		vanilla = Profile(Namespace.CONDOR,'universe','vanilla')
		path = Profile(namespace='env',key='PATH',value='/bin')
		path = Profile('env','PATH','/bin')
	"""
	
	def __init__(self, namespace, key, value):
		"""
		Arguments:
			namespace: The namespace of the profile (see Namespace) 
			key: The key name. Can be anything that responds to str().
			value: The value for the profile. Can be anything that responds to str().
		"""
		self.namespace = namespace
		self.key = key
		self.value = value
		
	def toXML(self):
		"""Return an XML element for this profile"""
		p = Element("profile", [
			('namespace', self.namespace),
			('key', self.key)
		])
		p.text(self.value).flatten()
		return p
		
class Use:
	"""Use(file[,link][,register][,transfer][,optional])

	Use of a logical file name. Used for referencing files in the DAX.

	Note: This class is used internally. You shouldn't need to use it in
	your code. You should use the uses(...) method of the object you
	are accessing.
	"""

	def __init__(self, file, link=None, register=None, transfer=None, 
				optional=None):
		if file is None:
			raise ValueError, 'file required'
		if isinstance(file, CatalogType):
			self.name = file.name
		else:
			self.name = file
		self.file = file
		self.link = link
		self.optional = optional
		self.register = register
		self.transfer = transfer
		
	def toXML(self):
		if isinstance(self.file, Executable):
			namespace = self.file.namespace
			version = self.file.version
			executable = True
		else:
			namespace = None
			version = None
			executable = None
			
		return Element('uses', [
			('namespace',namespace),
			('name',self.file.name),
			('version',version),
			('optional',self.optional),
			('register',self.register),
			('transfer',self.transfer),
			('executable',executable)
		])

class Transformation:
	"""Transformation((name|executable)[,namespace][,version])
	
	A logical transformation. This is basically defining one or more
	entries in the transformation catalog. You can think of it like a macro
	for adding <uses> to your jobs. You can define a transformation that
	uses several files and/or executables, and refer to it when creating
	a job. If you do, then all of the uses defined for that transformation
	will be copied to the job during planning.
	
	This code:
		in = File("input.txt")
		exe = Executable("exe")
		t = Transformation(namespace="foo", name="bar", version="baz")
		t.uses(in)
		t.uses(exe)
		j = Job(t)
		
	is equivalent to:
		in = File("input.txt")
		exe = Executable("exe")
		j = Job(namespace="foo", name="bar", version="baz")
		j.uses(in)
		j.uses(exe)
	
	Examples:
		Transformation(name='mDiff')
		Transformation(namespace='montage',name='mDiff')
		Transformation(namespace='montage',name='mDiff',version='3.0')
		
	Using one executable:
		mProjectPP = Executable(namespace="montage",name="mProjectPP",version="3.0")
		x_mProjectPP = Transformation(mProjectPP)
		
	Using several executables:
		mDiff = Executable(namespace="montage",name="mProjectPP",version="3.0")
		mFitplane = Executable(namespace="montage",name="mFitplane",version="3.0")
		mDiffFit = Executable(namespace="montage",name="mDiffFit",version="3.0")
		x_mDiffFit = Transformation(mDiffFit)
		x_mDiffFit.uses(mDiff)
		x_mDiffFit.uses(mFitplane)
		
	Config files too:
		conf = File("jbsim.conf")
		jbsim = Executable(namespace="scec",name="jbsim")
		x_jbsim = Transformation(jbsim)
		x_jbsim.uses(conf)
	"""
	def __init__(self,name,namespace=None,version=None):
		"""
		The name argument can be either a string or an Executable object.
		If it is an Executable object, then the Transformation inherits
		its name, namespace and version from the Executable, and the 
		Transformation is set to use the Executable with link=input,
		transfer=true, and register=False.
		
		Arguments:
			name: The name of the transformation
			namespace: The namespace of the xform (optional)
			version: The version of the xform (optional)
		"""
		self.name = None
		self.namespace = None
		self.version = None
		self.used_files = []
		if isinstance(name, Executable):
			self.name = name.name
			self.namespace = name.namespace
			self.version = name.version
			self.uses(name, link=Link.INPUT, transfer=True, register=False)
		else:
			self.name = name
		if namespace: self.namespace = namespace
		if version: self.version = version
		
	def addUses(self, *args, **kwargs):
		"""Alias for uses()"""
		self.uses(*args, **kwargs)
		
	def uses(self, file, link=None, register=None, transfer=None, 
			 optional=None):
		"""Add a file or executable that the transformation uses.
		
		Optional arguments to this method specify job-level attributes of
		the 'uses' tag in the DAX. If not specified, these values default
		to those specified when creating the File or Executable object.
		
		Arguments:
			file: A File or Executable object representing the logical file
			link: Is this file a job input, output or both
			register: Should this file be registered in RLS? (True/False)
			transfer: Should this file be transferred? (True/False or See LFN)
			optional: Is this file optional, or should its absence be an error?
		"""
		use = Use(file,link,register,transfer,optional)
		self.used_files.append(use)
		
	def toXML(self):
		"""Return an XML representation of this transformation"""
		e = Element('transformation', [
			('namespace',self.namespace),
			('name',self.name),
			('version',self.version)
		])
		for u in self.used_files:
			e.element(u.toXML())
		return e
		
class AbstractJob:
	"""The base class for Job, DAX, and DAG"""
	def __init__(self, name, id=None, node_label=None):
		if name is None:
			raise ValueError, 'name required'
		self.name = name
		self.id = id
		self.node_label = node_label
		
		self.arguments = []
		self.profiles = []
		self.used_files = []
		self.notifications = []

		self.stdout = None
		self.stderr = None
		self.stdin = None
	
	def addArguments(self, *arguments):
		"""Add one or more arguments to the job"""
		self.arguments.extend(arguments)

	def addProfile(self, profile):
		"""Add a profile to the job"""
		if isinstance(profile,Profile):
			self.profiles.append(profile)
		elif isinstance(profile,tuple):
			self.profiles.append(Profile(*profile))
		else:
			raise Exception("Invalid argument")
			
	def addUses(self, *args, **kwargs):
		"""Alias for uses() to maintain backward-compatibility"""
		self.uses(*args, **kwargs)

	def uses(self, file, link=None, register=None, transfer=None, 
			 optional=None):
		"""Add a logical filename that the job uses.
		
		Optional arguments to this method specify job-level attributes of
		the 'uses' tag in the DAX. If not specified, these values default
		to those specified when creating the File or Executable object.
		
		Arguments:
			file: A Filename object representing the logical file name
			link: Is this file a job input, output or both (See LFN)
			register: Should this file be registered in RLS? (True/False)
			transfer: Should this file be transferred? (True/False or See LFN)
			optional: Is this file optional, or should its absence be an error?
		"""
		use = Use(file,link,register,transfer,optional)
		self.used_files.append(use)

	def setStdout(self, filename):
		"""Redirect stdout to a file"""
		self.stdout = filename

	def setStderr(self, filename):
		"""Redirect stderr to a file"""
		self.stderr = filename

	def setStdin(self, filename):
		"""Redirect stdin from a file"""
		self.stdin = filename
		
	def notify(self, when, what):
		"""Alias for invoke(when,what)"""
		self.invoke(when, what)

	def invoke(self, when, what):
		"""
		Invoke executable 'what' when job reaches status 'when'. The value of 
		'what' should be a command that can be executed on the submit host.
	
		The list of valid values for 'when' is:
		
		WHEN		MEANING
		==========	=======================================================
		never		never invoke
		start		invoke just before job gets submitted.
		on_error	invoke after job finishes with failure (exitcode != 0).
		on_success	invoke after job finishes with success (exitcode == 0).
		at_end		invoke after job finishes, regardless of exit status.
		all			like start and at_end combined.
		
		Examples:
			job.invoke('at_end','/usr/bin/mail -s "job done" juve@usc.edu')
			job.invoke('on_error','/usr/bin/update_db -failure')
		"""
		self.notifications.append((when, what))
		
	def innerXML(self, element):
		"""Return an XML representation of this job"""
		# Arguments
		if len(self.arguments) > 0:
			args = Element('argument').flatten()
			element.element(args)
			first=True
			for x in self.arguments:
				if not first:
					args.text(' ')
				first=False
				if x.__class__ == File:
					args.element(x.toArgumentXML())
				else:
					args.text(x)

		# Profiles
		if len(self.profiles) > 0:
			for pro in self.profiles:
				element.element(pro.toXML())
		
		# Stdin/xml/err
		if self.stdin is not None:
			element.element(self.stdin.toStdioXML('stdin'))
		if self.stdout is not None:
			element.element(self.stdout.toStdioXML('stdout'))
		if self.stderr is not None:
			element.element(self.stderr.toStdioXML('stderr'))

		# Uses
		for use in self.used_files:
			element.element(use.toXML())
				
		# Notifications
		for when,what in self.notifications:
			n = Element('invoke',[('when',when)]).text(what).flatten()
			element.element(n)

class Job(AbstractJob):
	"""Job((name|transformation)[,id][,namespace][,version][,node_label])
	
	This class defines the specifics of a job to run in an abstract manner.
	All filename references still refer to logical files. All references
	transformations also refer to logical transformations, though
	physical location hints can be passed through profiles.
	
	Examples:
		sleep = Job(id="ID0001",name="sleep")
		jbsim = Job(id="ID0002",name="jbsim",namespace="cybershake",version="2.1")
		merge = Job("jbsim")
		
	You can create a Job based on a Transformation:
		mDiff_xform = Transformation("mDiff", ...)
		mDiff_job = Job(mDiff)
		
	Several arguments can be added at the same time:
		input = File(...)
		output = File(...)
		job.addArguments("-i",input,"-o",output)
	
	Profiles are added similarly:
		job.addProfile(Profile(Namespace.ENV,key='PATH',value='/bin'))
		
	Adding file uses is simple, and you can override global File attributes:
		job.uses(input,Link.INPUT)
		job.uses(output,Link.OUTPUT,transfer=True,register=True)
	"""
	def __init__(self, name, id=None, namespace=None, version=None, node_label=None):
		"""The ID for each job should be unique in the DAX. If it is None, then
		it will be automatically generated when the job is added to the DAX.
		
		The name, namespace, and version should match what you have in your
		transformation catalog. For example, if namespace="foo" name="bar" 
		and version="1.0", then the transformation catalog should have an
		entry for "foo::bar:1.0".
		
		The name argument can be either a string, or a Transformation object. If
		it is a Transformation object, then the job will inherit the name, namespace,
		and version from the Transformation.
		
		Arguments:
			name: The transformation name (required)
			id: A unique identifier for the job (autogenerated if None)
			namespace: The namespace of the transformation
			version: The transformation version
			node_label: The label for this job to use in graphing
		"""
		self.namespace = None
		self.version = None
		if isinstance(name, Transformation):
			t_name = name.name
			self.namespace = name.namespace
			self.version = name.version
		else:
			t_name = name
		AbstractJob.__init__(self, name=t_name, id=id, node_label=node_label)
		if namespace: self.namespace = namespace
		if version: self.version = version
		
	def toXML(self):
		e = Element('job',[
			('id',self.id),
			('namespace',self.namespace),
			('name',self.name),
			('version',self.version),
			('node-label',self.node_label)
		])
		self.innerXML(e)
		return e
		
class DAX(AbstractJob):
	"""DAX(file[,id][,node_label])
	
	This job represents a sub-DAX that will be planned and executed by
	the workflow.
	
	Examples:
		daxjob1 = DAX("foo.dax")
		
		daxfile = File("foo.dax")
		daxjob2 = DAX(daxfile)
	"""
	def __init__(self, file, id=None, node_label=None):
		"""
		
		The name argument can be either a string, or a File object. If
		it is a File object, then this job will inherit its name from the 
		File and the File will be added in a <uses> with transfer=True,
		register=False, and link=input.
		
		Arguments:
			file: The logical name of the DAX file or the DAX File object
			id: The id of the DAX job [default: autogenerated]
			node_label: The label for this job to use in graphing
		"""
		if isinstance(file, File):
			self.file = file
			t_name = file.name
		else:
			self.file = File(name=file)
			t_name = file
		AbstractJob.__init__(self, name=t_name, id=id, node_label=node_label)
		
	def toXML(self):
		"""Return an XML representation of this job"""
		e = Element('dax', [
			('id',self.id),
			('file',self.name),
			('node-label',self.node_label)
		])
		self.innerXML(e)
		return e
	
class DAG(AbstractJob):
	"""DAG(file[,id][,node_label])
	
	This job represents a sub-DAG that will be executed by this
	workflow.
	
	Examples:
		dagjob1 = DAG(file="foo.dag")
		
		dagfile = File("foo.dag")
		dagjob2 = DAG(dagfile)
	"""
	def __init__(self, file, id=None, node_label=None):
		"""
		The name argument can be either a string, or a File object. If
		it is a File object, then this job will inherit its name from the 
		File and the File will be added in a <uses> with transfer=True,
		register=False, and link=input.
		
		Arguments:
			file: The logical name of the DAG file, or the DAG File object
			id: The ID of the DAG job [default: autogenerated]
			node_label: The label for this job to use in graphing
		"""
		if isinstance(file, File):
			self.file = file
			t_name = file.name
		else:
			self.file = File(name=file)
			t_name = file
		AbstractJob.__init__(self, name=t_name, id=id, node_label=node_label)
			
	def toXML(self):
		"""Return an XML representation of this DAG"""
		e = Element('dag', [
			('id',self.id),
			('file',self.name),
			('node-label',self.node_label)
		])
		self.innerXML(e)
		return e

class Dependency:
	"""A control-flow dependency between a child and its parents"""
	def __init__(self,child):
		self.child = child
		self.parents = []

	def addParent(self, parent, edge_label=None):
		self.parents.append([parent,edge_label])

	def toXML(self):
		"""Generate an XML representation of this"""
		e = Element('child',[('ref',self.child.id)])
		for parent, edge_label in self.parents:
			p = Element('parent',[
				('ref',parent.id),
				('edge-label',edge_label)
			])
			e.element(p)
		return e
		
class ADAG:
	"""ADAG(name[,count][,index])
	
	Representation of a directed acyclic graph in XML (DAX).
	
	Examples:
		dax = ADAG('diamond')
		part5 = ADAG('partition_5',count=10,index=5)
		
	Adding jobs:
		a = Job(...)
		dax.addJob(a)
		
	Adding parent-child control-flow dependency:
		dax.addDependency(a,b)
		dax.addDependency(a,c)
		dax.addDependency(b,d)
		dax.addDependency(c,d)
		
	Adding Files (this is not required to produce a valid DAX):
		input = File(...)
		dax.addFile(input)
		
	Adding Executables (not required):
		exe = Executable(...)
		dax.addExecutable(exe)
		
	Adding Transformations (not required):
		xform = Transformation(...)
		dax.addTransformation(xform)
		
	Writing a DAX out to a file:
		f = open('diamond.dax','w')
		dax.writeXML(f)
		f.close()
	"""
	def __init__(self, name, count=None, index=None):
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
		self.files = []
		self.executables = []
		self.lookup = {} # A lookup table for dependencies
		self.dependencies = []
		self.transformations = []

	def addJob(self, job):
		"""Add a job to the list of jobs in the DAX"""
		# Add an auto-generated ID if the job doesn't have one
		if job.id is None:
			job.id = "ID%07d" % self.sequence
			self.sequence += 1
		self.jobs.append(job)
		
	def addDAX(self, dax):
		self.addJob(dax)
		
	def addDAG(self, dag):
		"""Add a sub-DAG"""
		self.addJob(dag)
		
	def addADAG(self, adag):
		"""Add a recursive adag"""
		self.addJob(adag)
		
	def addFile(self, file):
		"""Add a file"""
		self.files.append(file)
		
	def addExecutable(self, executable):
		self.executables.append(executable)
		
	def addTransformation(self, transformation):
		self.transformations.append(transformation)
		
	def addDependency(self, parent, child, edge_label=None):
		"""Add a control flow dependency
		Arguments:
			parent: The parent job/dax/dag
			child: The child job/dax/dag
			edge_label: A label for the edge (optional)
		"""
		if not child in self.lookup:
			dep = Dependency(child)
			self.lookup[child] = dep
			self.dependencies.append(dep)
		self.lookup[child].addParent(parent,edge_label)

	def writeXML(self, out):
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
		out.write(u'name="%s"' % self.name)
		if self.count: out.write(u' count="%d"' % self.count)
		if self.index: out.write(u' index="%d"' % self.index)
		out.write(u'>\n')

		# Files and executables
		out.write(u'\n\t<!-- part 1: Replica catalog (may be empty) -->\n')
		for f in self.files:
			out.write(u'\t')
			f.toXML().toXML(stream=out,level=1)
			out.write(u'\n')
		for e in self.executables:
			out.write(u'\t')
			e.toXML().toXML(stream=out,level=1)
			out.write(u'\n')
			
		# Transformations
		out.write(u'\n\t<!-- part 2: Transformation catalog (may be empty) -->\n')
		for t in self.transformations:
			out.write(u'\t')
			t.toXML().toXML(stream=out,level=1)
			out.write(u'\n')
		
		# Jobs
		out.write(u'\n\t<!-- part 3: Definition of all jobs/dags/daxes (at least one) -->\n')
		for job in self.jobs:
			out.write(u'\t')
			job.toXML().toXML(stream=out,level=1)
			out.write(u'\n')
		
		# Dependencies
		out.write(u'\n\t<!-- part 4: List of control-flow dependencies (may be empty) -->\n')
		for dep in self.dependencies:
			out.write(u'\t')
			dep.toXML().toXML(stream=out,level=1)
			out.write(u'\n')
		
		# Close tag
		out.write(u'</adag>\n')


class DAXHandler(xml.sax.handler.ContentHandler):
	"""
	This is a DAX parser
	"""
	def __init__(self):
		self.elements = []
		self.adag = None
		self.jobmap = {}
		self.filemap = {}
		self.lastJob = None
		self.lastChild = None
		self.lastArgument = None
		self.lastProfile = None
		self.lastMetadata = None
		self.lastPFN = None
		self.lastFile = None
		self.lastInvoke = None
		self.lastTransformation = None
		
	def startElement(self, element, attrs):
		self.elements.insert(0, element)
		parent = None
		if len(self.elements) > 1:
			parent = self.elements[1]
		
		if element == "adag":
			name = attrs.get("name")
			count = attrs.get("count")
			index = attrs.get("index")
			self.adag = ADAG(name,count,index)
		elif element == "file":
			name = attrs.get("name")
			
			if name in self.filemap:
				f = self.filemap[name]
			else:
				f = File(name=name)
				self.filemap[name] = f
					
			if parent == 'adag':
				self.adag.addFile(f)
			elif parent == 'argument':
				self.lastArgument.append(f)
			else:
				raise Exception("Adding file to %s" % parent)
			self.lastFile = f
		elif element == "executable":
			name = attrs.get("name")
			namespace = attrs.get("namespace")
			version  = attrs.get("version")
			arch = attrs.get("arch")
			os  = attrs.get("os")
			osrelease = attrs.get("osrelease")
			osversion = attrs.get("osversion")
			glibc = attrs.get("glibc")
			e = Executable(name=name, namespace=namespace, version=version,
				arch=arch, os=os, osrelease=osrelease, osversion=osversion,
				glibc=glibc)
			self.filemap[name] = e
			self.adag.addExecutable(e)
			self.lastFile = e
		elif element == "transformation":
			namespace = attrs.get("namespace")
			name = attrs.get("name")
			version = attrs.get("version")
			t = Transformation(name=name, namespace=namespace, version=version)
			self.lastTransformation = t
			self.adag.addTransformation(t)
		elif element in ["job","dag","dax"]:
			id = attrs.get("id")
			namespace = attrs.get("namespace")
			if element == 'job':
				name = attrs.get("name")
			else:
				name = attrs.get("file")
			version = attrs.get("version")
			node_label = attrs.get("node-label")
			if element == "job":
				job = Job(id=id, namespace=namespace, name=name, version=version,
						node_label=node_label)
				self.adag.addJob(job)
			elif element == "dag":
				job = DAG(file=name, id=id, node_label=node_label)
				self.adag.addDAG(job)
			else:
				job = DAX(file=name, id=id, node_label=node_label)
				self.adag.addDAX(job)
			self.jobmap[id] = job
			self.lastJob = job
		elif element == "argument":
			self.lastArgument = []
		elif element == "profile":
			namespace = attrs.get("namespace")
			key = attrs.get("key")
			p = Profile(namespace,key,"")
			if parent == 'job':
				self.lastJob.addProfile(p)
			elif parent in ['file','executable']:
				self.lastFile.addProfile(p)
			elif parent == 'pfn':
				self.lastPFN.addProfile(p)
			else:
				raise Exception("Adding profile to %s" % parent)
			self.lastProfile = p
		elif element == "metadata":
			key = attrs.get("key")
			type = attrs.get("type")
			meta = Metadata(key=key,type=type,value="")
			if parent in ["file","executable"]:
				self.lastFile.addMetadata(meta)
			elif parent == "transformation":
				self.lastTransformation.addMetadata(meta)
			elif parent == "job":
				self.lastJob.addMetadata(meta)
			else:
				raise Exception("Adding metadata to %s" % parent)
			self.lastMetadata = meta
		elif element == "pfn":
			url = attrs.get("url")
			site = attrs.get("site")
			pfn = PFN(url=url, site=site)
			if parent in ["file","executable"]:
				self.lastFile.addPFN(pfn)
			else:
				raise Exception("Adding PFN to %s" % parent)
			self.lastPFN = pfn
		elif element in ["stdin","stdout","stderr"]:
			name = attrs.get("name")
			f = File(name)
			if element == "stdin":
				self.lastJob.setStdin(f)
			elif element == "stdout":
				self.lastJob.setStdout(f)
			else:
				self.lastJob.setStderr(f)
		elif element == "uses":
			name = attrs.get("name")
			link = attrs.get("link")
			optional = attrs.get("optional")
			register = attrs.get("register")
			transfer = attrs.get("transfer")
			namespace = attrs.get("namespace")
			version = attrs.get("version")
			executable = bool(attrs.get("executable",False))
			
			if name in self.filemap:
				f = self.filemap[name]
			elif executable:
				f = Executable(name, namespace=namespace, version=version, 
					link=link, register=register, transfer=transfer)
				self.filemap[name] = f
			else:
				f = File(name, link=link, register=register, 
					transfer=transfer)
				self.filemap[name] = f
				
			if parent in ['job','dax','dag']:
				self.lastJob.uses(f, link=link, register=register,
					transfer=transfer, optional=optional)
			elif parent == 'transformation':
				self.lastTransformation.uses(f, link=link, register=register,
					transfer=transfer, optional=optional)
			else:
				raise Exception("Adding uses to %s" % parent)
		elif element == "invoke":
			self.lastInvoke = [attrs.get("when"), ""]
		elif element == "child":
			ref = attrs.get("ref")
			self.lastChild = self.jobmap[ref]
		elif element == "parent":
			ref = attrs.get("ref")
			edge_label = attrs.get("edge-label")
			p = self.jobmap[ref]
			self.adag.addDependency(p, self.lastChild, edge_label)
		else:
			raise Exception("Unrecognized element %s" % name)
			
	def characters(self, chars):
		parent = self.elements[0]
		
		if parent == "argument":
			self.lastArgument += [unicode(a) for a in shlex.split(chars)]
		elif parent == "profile":
			self.lastProfile.value += chars
		elif parent == "metadata":
			self.lastMetadata.value += chars
		elif parent == "invoke":
			self.lastInvoke[1] += chars
			
	def endElement(self, element):
		self.elements = self.elements[1:]
		
		if element == "child":
			self.lastChild = None
		elif element in ["job","dax","dag"]:
			self.lastJob = None
		elif element == "argument":
			self.lastJob.addArguments(*self.lastArgument)
			self.lastArgument = None
		elif element == "profile":
			self.lastProfile = None
		elif element == "metadata":
			self.lastMetadata = None
		elif element == "pfn":
			self.lastPFN = None
		elif element == "invoke":
			self.lastJob.invoke(*self.lastInvoke)
			self.lastInvoke = None
		elif element == "transformation":
			self.lastTransformation = None
	
	
def parse(fname):
	"""
	Parse DAX from a Pegasus DAX file.
	"""
	handler = DAXHandler()
	xml.sax.parse(fname, handler)
	return handler.adag


def parseString(string):
	"""
	Parse DAX from a string
	"""
	handler = DAXHandler()
	xml.sax.parseString(string, handler)
	return handler.adag


def test():
	"""An example of using the DAX API"""

	# Create a DAX
	diamond = ADAG("diamond")

	# Create some logical file names
	a = File("f.a")
	b1 = File("f.b1")
	b2 = File("f.b2")
	c1 = File("f.c1")
	c2 = File("f.c2")
	d = File("f.d")
	
	# Add a bunch of stuff to test functionality
	a.addProfile(Profile(namespace="test",key="foo",value="baz"))
	a.addMetadata(Metadata(key="size",type="int",value="1024"))
	a_url = PFN(url="http://site.com/f.a",site="site.com")
	a_url.addProfile(Profile(namespace="test",key="bar",value="baz"))
	a.addPFN(a_url)

	# Add the filenames to the DAX (this is not strictly required)
	diamond.addFile(a)
	diamond.addFile(d)
	
	# Add executables
	e_preprocess = Executable(namespace="diamond", name="preprocess", version="2.0")
	e_findrange = Executable(namespace="diamond", name="findrange", version="2.0")
	e_analyze = Executable(namespace="diamond", name="analyze", version="2.0")
	
	# Add a bunch of stuff to test functionality
	e_preprocess.addProfile(Profile(namespace="test",key="pre",value="process"))
	e_preprocess.addProfile(("test","pre","process"))
	e_preprocess.addMetadata(Metadata(key="os",type="string",value="LINUX"))
	e_preprocess.addMetadata(("size","int","1024"))
	e_preprocess.addPFN(PFN(url="http://site.com/preprocess",site="site.com"))
	e_preprocess.addPFN(("http://www.google.com","google"))
	
	diamond.addExecutable(e_preprocess)
	diamond.addExecutable(e_findrange)
	diamond.addExecutable(e_analyze)
	
	# Add transformation (long form)
	t_preprocess = Transformation(namespace="diamond",name="preprocess",version="2.0")
	t_preprocess.uses(e_preprocess)
	t_preprocess.uses(a,link=Link.INPUT,transfer=True)
	diamond.addTransformation(t_preprocess)
	
	# Add transformation (short form)
	t_findrange = Transformation(e_findrange)
	t_analyze = Transformation(e_analyze)
	
	diamond.addTransformation(t_findrange)
	diamond.addTransformation(t_analyze)

	# Add a preprocess job
	preprocess = Job(t_preprocess,node_label="foobar")
	preprocess.addArguments("-a preprocess","-T60","-i",a,"-o",b1,b2)
	preprocess.uses(a,link=Link.INPUT, transfer=True)
	preprocess.uses(b1,link=Link.OUTPUT, transfer=True)
	preprocess.uses(b2,link=Link.OUTPUT, transfer=True)
	diamond.addJob(preprocess)

	# Add left Findrange job
	frl = Job(t_findrange)
	frl.addArguments("-a findrange","-T60","-i",b1,"-o",c1)
	frl.uses(b1,link=Link.INPUT, transfer=True)
	frl.uses(c1,link=Link.OUTPUT, transfer=True)
	diamond.addJob(frl)

	# Add right Findrange job
	frr = Job(namespace="diamond",name="findrange",version="2.0")
	frr.addArguments("-a findrange","-T60","-i",b2,"-o",c2)
	frr.uses(b2,link=Link.INPUT, transfer=True)
	frr.uses(c2,link=Link.OUTPUT, transfer=True)
	diamond.addJob(frr)

	# Add Analyze job
	analyze = Job(namespace="diamond",name="analyze",version="2.0")
	analyze.addArguments("-a analyze","-T60","-i",c1,c2,"-o",d)
	analyze.uses(c1,link=Link.INPUT, transfer=True)
	analyze.uses(c2,link=Link.INPUT, transfer=True)
	analyze.uses(d,link=Link.OUTPUT, transfer=True, register=True)
	diamond.addJob(analyze)
	
	# A DAG
	dagfile = File("pre.dag")
	predag = DAG(dagfile,node_label="predag")
	predag.invoke('at_end','/bin/echo "foo bar"')
	diamond.addDAG(predag)
	
	# A DAX
	daxfile = File("post.xml")
	postdax = DAX(daxfile,node_label="postdax")
	postdax.invoke('at_end',"/bin/echo 'yay'")
	diamond.addDAX(postdax)

	# Add control-flow dependencies
	diamond.addDependency(parent=predag, child=preprocess)
	diamond.addDependency(parent=preprocess, child=frl, edge_label='foobar')
	diamond.addDependency(parent=preprocess, child=frr)
	diamond.addDependency(parent=frl, child=analyze)
	diamond.addDependency(parent=frr, child=analyze)
	diamond.addDependency(parent=analyze, child=postdax)
	
	postdax.setStdin(File("postdax.in"))
	postdax.setStdout(File("postdax.out"))
	postdax.setStderr(File("postdax.err"))

	# Write the DAX to stdout
	out = StringIO()
	diamond.writeXML(out)
	foo1 = out.getvalue()
	out.close()
	
	print foo1
	
	diamond = parseString(foo1)
	
	out = StringIO()
	diamond.writeXML(out)
	foo2 = out.getvalue()
	out.close()
	
	print foo2
	
def diamond():
	# Create a DAX
	diamond = ADAG("diamond")
	
	# Add input file to the DAX-level replica catalog
	a = File("f.a")
	a.addPFN(PFN("gsiftp://site.com/inputs/f.a","site"))
	diamond.addFile(a)
	
	# Add executables to the DAX-level replica catalog
	e_preprocess = Executable(namespace="diamond", name="preprocess", version="4.0", os="linux", arch="x86_64")
	e_preprocess.addPFN(PFN("gsiftp://site.com/bin/preprocess","site"))
	diamond.addExecutable(e_preprocess)
	
	e_findrange = Executable(namespace="diamond", name="findrange", version="4.0", os="linux", arch="x86_64")
	e_findrange.addPFN(PFN("gsiftp://site.com/bin/findrange","site"))
	diamond.addExecutable(e_findrange)
	
	e_analyze = Executable(namespace="diamond", name="analyze", version="4.0", os="linux", arch="x86_64")
	e_analyze.addPFN(PFN("gsiftp://site.com/bin/analyze","site"))
	diamond.addExecutable(e_analyze)
	
	# Add transformations to the DAX-level transformation catalog
	t_preprocess = Transformation(e_preprocess)
	diamond.addTransformation(t_preprocess)
	
	t_findrange = Transformation(e_findrange)
	diamond.addTransformation(t_findrange)
	
	t_analyze = Transformation(e_analyze)
	diamond.addTransformation(t_analyze)

	# Add a preprocess job
	preprocess = Job(t_preprocess)
	b1 = File("f.b1")
	b2 = File("f.b2")
	preprocess.addArguments("-a preprocess","-T60","-i",a,"-o",b1,b2)
	preprocess.uses(a, link=Link.INPUT, transfer=True)
	preprocess.uses(b1, link=Link.OUTPUT, transfer=True)
	preprocess.uses(b2, link=Link.OUTPUT, transfer=True)
	diamond.addJob(preprocess)

	# Add left Findrange job
	frl = Job(t_findrange)
	c1 = File("f.c1")
	frl.addArguments("-a findrange","-T60","-i",b1,"-o",c1)
	frl.uses(b1, link=Link.INPUT, transfer=True)
	frl.uses(c1, link=Link.OUTPUT, transfer=True)
	diamond.addJob(frl)

	# Add right Findrange job
	frr = Job(t_findrange)
	c2 = File("f.c2")
	frr.addArguments("-a findrange","-T60","-i",b2,"-o",c2)
	frr.uses(b2, link=Link.INPUT, transfer=True)
	frr.uses(c2, link=Link.OUTPUT, transfer=True)
	diamond.addJob(frr)

	# Add Analyze job
	analyze = Job(t_analyze)
	d = File("f.d")
	analyze.addArguments("-a analyze","-T60","-i",c1,c2,"-o",d)
	analyze.uses(c1, link=Link.INPUT, transfer=True)
	analyze.uses(c2, link=Link.INPUT, transfer=True)
	analyze.uses(d, link=Link.OUTPUT, transfer=True, register=True)
	diamond.addJob(analyze)

	# Add control-flow dependencies
	diamond.addDependency(parent=preprocess, child=frl)
	diamond.addDependency(parent=preprocess, child=frr)
	diamond.addDependency(parent=frl, child=analyze)
	diamond.addDependency(parent=frr, child=analyze)

	# Write the DAX to stdout
	import sys
	diamond.writeXML(sys.stdout)
	
if __name__ == '__main__':
	test()
	diamond()
