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

Here is an example showing how to create the diamond DAX using this API:

# Create ADAG object
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
preprocess.uses(a, link=Link.INPUT)
preprocess.uses(b1, link=Link.OUTPUT, transfer=True)
preprocess.uses(b2, link=Link.OUTPUT, transfer=True)
diamond.addJob(preprocess)

# Add left Findrange job
frl = Job(t_findrange)
c1 = File("f.c1")
frl.addArguments("-a findrange","-T60","-i",b1,"-o",c1)
frl.uses(b1, link=Link.INPUT)
frl.uses(c1, link=Link.OUTPUT, transfer=True)
diamond.addJob(frl)

# Add right Findrange job
frr = Job(t_findrange)
c2 = File("f.c2")
frr.addArguments("-a findrange","-T60","-i",b2,"-o",c2)
frr.uses(b2, link=Link.INPUT)
frr.uses(c2, link=Link.OUTPUT, transfer=True)
diamond.addJob(frr)

# Add Analyze job
analyze = Job(t_analyze)
d = File("f.d")
analyze.addArguments("-a analyze","-T60","-i",c1,c2,"-o",d)
analyze.uses(c1, link=Link.INPUT)
analyze.uses(c2, link=Link.INPUT)
analyze.uses(d, link=Link.OUTPUT, transfer=True, register=True)
diamond.addJob(analyze)

# Add control-flow dependencies
diamond.depends(parent=preprocess, child=frl)
diamond.depends(parent=preprocess, child=frr)
diamond.depends(parent=frl, child=analyze)
diamond.depends(parent=frr, child=analyze)

# Write the DAX to stdout
import sys
diamond.writeXML(sys.stdout)

# Write the DAX to a file
f = open("diamond.dax","w")
diamond.writeXML(f)
f.close()
"""

__author__ = "Gideon Juve <juve@usc.edu>"
__version__ = "3.2"
__all__ = [
	'DuplicateError',
	'NotFoundError',
	'Metadata',
	'Profile',
	'PFN',
	'Namespace',
	'Arch',
	'Link',
	'Transfer',
	'OS',
	'When',
	'File',
	'Executable',
	'Use',
	'Transformation',
	'Invoke',
	'Job',
	'DAX',
	'DAG',
	'ADAG',
	'Dependency',
	'parse',
	'parseString'
]

import datetime, pwd, os, sys
from cStringIO import StringIO
import codecs
import xml.sax
import xml.sax.handler
import shlex

SCHEMA_NAMESPACE = u"http://pegasus.isi.edu/schema/DAX"
SCHEMA_LOCATION = u"http://pegasus.isi.edu/schema/dax-3.2.xsd"
SCHEMA_VERSION = u"3.2"

class DAX3Error(Exception): pass
class DuplicateError(DAX3Error): pass
class NotFoundError(DAX3Error): pass

class Element:
	"""Representation of an XML element for formatting output"""
	
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
	
	def __escape(self, text):
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
	
	def element(self, element):
		self.children.append(element)
		return element
		
	def text(self, value):
		self.children.append(self.__escape(value))
		return self
		
	def comment(self, message):
		self.children.append(u"<!- %s -->" % self.__escape(message))
		
	def flatten(self):
		self.flat = True
		return self
		
	def __str__(self):
		s = StringIO()
		self.toXML(s)
		x = s.getvalue()
		s.close()
		return x
		
	def toXML(self, stream=sys.stdout, level=0, flatten=False):
		flat = self.flat or flatten
		
		stream.write(u'<%s' % self.name)
		
		for attr,value in self.attrs:
			value = self.__escape(value)
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


class ProfileMixin:
	def addProfile(self, profile):
		"""Add a profile to this object"""
		if self.hasProfile(profile):
			raise DuplicateError("Duplicate profile", profile)
		self.profiles.add(profile)
		
	def hasProfile(self, profile):
		"""Does this object have profile?"""
		return profile in self.profiles
		
	def removeProfile(self, profile):
		"""Remove profile from this object"""
		if not self.hasProfile(profile):
			raise NotFoundError("Profile not found", profile)
		self.profiles.remove(profile)
		
	def clearProfiles(self):
		"""Remove all profiles from this object"""
		self.profiles.clear()
		
	def profile(self, namespace, key, value):
		"""Declarative profile addition"""
		self.addProfile(Profile(namespace, key, value))


class MetadataMixin:
	def addMetadata(self, metadata):
		"""Add metadata to this object"""
		if self.hasMetadata(metadata):
			raise DuplicateError("Duplicate Metadata", metadata)
		self.metadata.add(metadata)
		
	def removeMetadata(self, metadata):
		"""Remove meta from this object"""
		if not self.hasMetadata(metadata):
			raise NotFoundError("Metadata not found", metadata)
		self.metadata.remove(metadata)
		
	def hasMetadata(self, metadata):
		"""Does this object have metadata?"""
		return metadata in self.metadata
		
	def clearMetadata(self):
		"""Remove all metadata from this object"""
		self.metadata.clear()
		
	def metadata(self, key, type, value):
		"""Declarative metadata addition"""
		self.addMetadata(Metadata(key, type, value))


class PFNMixin:
	def addPFN(self, pfn):
		"""Add a PFN to this object"""
		if self.hasPFN(pfn):
			raise DuplicateError("Duplicate PFN", pfn)
		self.pfns.add(pfn)
		
	def removePFN(self, pfn):
		"""Remove PFN from this object"""
		if not self.hasPFN(pfn):
			raise NotFoundError("PFN not found", pfn)
		self.pfns.remove(pfn)
		
	def hasPFN(self, pfn):
		"""Does this object have pfn?"""
		return pfn in self.pfns
		
	def clearPFNs(self):
		"""Remove all PFNs from this object"""
		self.pfns.clear()
		
	def PFN(self, url, site=None):
		"""Declarative PFN addition"""
		self.addPFN(PFN(url,site))


class CatalogType(ProfileMixin, MetadataMixin, PFNMixin):
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
		if not name:
			raise ValueError, 'name required'
		self.name = name
		self.profiles = set()
		self.metadata = set()
		self.pfns = set()
			
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
		
	def __unicode__(self):
		return unicode(self.name)
		
	def __repr__(self):
		return str(self.name)
	
	def __hash__(self):
		return hash(self.name)
		
	def __eq__(self, other):
		return isinstance(other, File) and self.name == other.name
	
	def toArgumentXML(self):
		"""Returns an XML representation of this File with no inner elements"""
		return Element('file',[('name',self.name)])
		
	def toStdioXML(self, tag):
		"""Returns an XML representation of this file as a stdin/out/err tag"""
		if tag is 'stdin':
			link = "input" # stdin is always input
		elif tag in ['stdout','stderr']:
			link = "output" # stdout/stderr are always output
		else:
			raise ValueError("invalid tag",tag,"should be one of stdin, stdout, stderr")
		
		return Element(tag, [
			('name',self.name),
			('link',link)
		])
		
	def toXML(self):
		"""Return the XML representation of this File with inner elements"""
		e = self.toArgumentXML()
		self.innerXML(e)
		return e
	
class Executable(CatalogType):
	"""Executable(name[,namespace][,version][,arch][,os][,osrelease][,osversion][,glibc][,installed])
				
	An entry for an executable in the DAX-level replica catalog.
	
	Examples:
		grep = Executable("grep")
		grep = Executable(namespace="os",name="grep",version="2.3")
		grep = Executable(namespace="os",name="grep",version="2.3",arch=Arch.X86)
		grep = Executable(namespace="os",name="grep",version="2.3",arch=Arch.X86,os=OS.LINUX)
	"""
	def __init__(self, name, namespace=None, version=None, arch=None, os=None, 
				 osrelease=None, osversion=None, glibc=None, installed=None):
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
			installed: Is the executable installed (true), or stageable (false)
		"""
		CatalogType.__init__(self, name)
		self.namespace = namespace
		self.version = version
		self.arch = arch
		self.os = os
		self.osrelease = osrelease
		self.osversion = osversion
		self.glibc = glibc
		self.installed = installed
		
	def __repr__(self):
		return "%s::%s:%s" % (self.namespace, self.name, self.version)
		
	def __hash__(self):
		return hash((self.name,
			self.namespace,
			self.version,
			self.arch,
			self.os,
			self.osrelease,
			self.osversion,
			self.glibc,
			self.installed))
		
	def __eq__(self, other):
		if isinstance(other, Executable):
			return self.name == other.name and \
				self.namespace == other.namespace and \
				self.version == other.version and \
				self.arch == other.arch and \
				self.os == other.os and \
				self.osrelease == other.osrelease and \
				self.osversion == other.osversion and \
				self.glibc == other.glibc and \
				self.installed == other.installed
		return False
		
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
			('glibc',self.glibc),
			('installed',self.installed)
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
		if not key:
			raise ValueError("Invalid key", key)
		if not type:
			raise ValueError("Invalid type", type)
		if not value:
			raise ValueError("Invalid value", value)
		self.key = key
		self.type = type
		self.value = value
		
	def __repr__(self):
		return "%s %s = %s" % (self.type, self.key, self.value)
		
	def __hash__(self):
		return hash(self.key)
		
	def __eq__(self, other):
		return isinstance(other, Metadata) and self.key == other.key
		
	def toXML(self):
		m = Element('metadata', [
			('key',self.key),
			('type',self.type)
		])
		m.text(self.value).flatten()
		return m
		
class PFN(ProfileMixin):
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
		if not url:
			raise ValueError("Invalid url", url)
		if not site:
			raise ValueError("Invalid site", site)
		self.url = url
		self.site = site
		self.profiles = set()
		
	def __repr__(self):
		return str(self.url)
		
	def __hash__(self):
		return hash((self.url, self.site))
		
	def __eq__(self, other):
		return isinstance(other, PFN) and \
			self.url == other.url and \
			self.site == other.site
		
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
		
	def __repr__(self):
		return "%s::%s = %s" % (self.namespace, self.key, self.value)
		
	def __hash__(self):
		return hash((self.namespace, self.key))
		
	def __eq__(self, other):
		return isinstance(other, Profile) and \
			self.namespace == other.namespace and \
		 	self.key == other.key
		
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
				optional=None, namespace=None, version=None, executable=None):
		if not file:
			raise ValueError('Invalid file', file)
		
		if isinstance(file, CatalogType):
			self.name = file.name
		else:
			self.name = file
		
		self.link = link
		self.optional = optional
		self.register = register
		self.transfer = transfer
		
		self.namespace = None
		self.version = None
		self.executable = None
		
		if isinstance(file, Executable):
			self.namespace = file.namespace
			self.version = file.version
			self.executable = True
			
		if namespace is not None:
			self.namespace = namespace
		if version is not None:
			self.version = str(version)
		if executable is not None:
			self.executable = executable
			
	def __repr__(self):
		return "uses %s::%s:%s" % (self.namespace, self.name, self.version)
		
	def __hash__(self):
		return hash((self.namespace, self.name, self.version))
		
	def __eq__(self, other):
		if isinstance(other, Use):
			return self.namespace == other.namespace and \
				self.name == other.name and \
				self.version == other.version
		
	def toXML(self):
		return Element('uses', [
			('namespace',self.namespace),
			('name',self.name),
			('version',self.version),
			('link',self.link),
			('register',self.register),
			('transfer',self.transfer),
			('optional',self.optional),
			('executable',self.executable)
		])


class UseMixin:
	def addUse(self, use):
		"""Add Use to this object"""
		if self.hasUse(use):
			raise DuplicateError("Duplicate Use", use)
		self.used.add(use)
		
	def removeUse(self, use):
		"""Remove use from this object"""
		if not self.hasUse(use):
			raise NotFoundError("No such Use", use)
		self.used.remove(use)
	
	def hasUse(self, use):
		"""Test to see if this object has use"""
		return use in self.used
		
	def clearUses(self):
		"""Remove all uses from this object"""
		self.used.clear()
		
	def uses(self, name=None, link=None, register=None, transfer=None, 
			 optional=None, namespace=None, version=None, executable=None):
		"""Add a file/executable that the object uses.
		
		Optional arguments to this method specify job-level attributes of
		the 'uses' tag in the DAX. If not specified, these values default
		to those specified when creating the File or Executable object.
		
		Arguments:
			name: A Filename object representing the logical file name or an Executable object
			link: Is this file a job input, output or both (See LFN) (optional)
			register: Should this file be registered in RLS? (True/False) (optional)
			transfer: Should this file be transferred? (True/False or See LFN) (optional)
			optional: Is this file optional, or should its absence be an error? (optional)
			namespace: Namespace of executable (optional)
			version: version of executable (optional)
			executable: Is file an executable? (True/False) (optional)
		"""
		use = Use(name,link,register,transfer,optional,namespace,version,executable)
		self.addUse(use)


class Transformation(UseMixin):
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
		self.used = set()
		if isinstance(name, Executable):
			self.name = name.name
			self.namespace = name.namespace
			self.version = name.version
		else:
			self.name = name
		if namespace: self.namespace = namespace
		if version: self.version = version
		
	def __repr__(self):
		return "%s::%s:%s" % (self.namespace, self.name, self.version)
		
	def __hash__(self):
		return hash((self.namespace,self.name,self.version))
		
	def __eq__(self, other):
		if isinstance(other, Transformation):
			return self.namespace == other.namespace and \
				self.name == other.name and \
				self.version == other.version
		
	def toXML(self):
		"""Return an XML representation of this transformation"""
		e = Element('transformation', [
			('namespace',self.namespace),
			('name',self.name),
			('version',self.version)
		])
		for u in self.used:
			e.element(u.toXML())
		return e
		
class Invoke:
	def __init__(self, when, what):
		if not when:
			raise ValueError("invalid when",when)
		if not what:
			raise ValueError("invalid what",what)
		self.when = when
		self.what = what
		
	def __repr__(self):
		return "invoke %s %s" % (self.when, self.what)
		
	def __hash__(self):
		return hash((self.when, self.what))
		
	def __eq__(self, other):
		if isinstance(other, Invoke):
			return self.when == other.when and self.what == other.what
		return False
		
	def toXML(self):
		e = Element('invoke',[('when',self.when)])
		e.text(self.what)
		e.flatten()
		return e
		
class InvokeMixin:
	
	def addInvoke(self, invoke):
		"""Add invoke to this object"""
		if self.hasInvoke(invoke):
			raise DuplicateError("Duplicate Invoke", invoke)
		self.invocations.add(invoke)
		
	def hasInvoke(self, invoke):
		"""Test to see if this object has invoke"""
		return invoke in self.invocations
		
	def removeInvoke(self, invoke):
		"""Remove invoke from this object"""
		if not self.hasInvoke(invoke):
			raise NotFoundError("Invoke not found", invoke)
		self.invocations.remove(invoke)
		
	def clearInvokes(self):
		"""Remove all Invoke objects"""
		self.invocations.clear()
		
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
			obj.invoke('at_end','/usr/bin/mail -s "job done" juve@usc.edu')
			obj.invoke('on_error','/usr/bin/update_db -failure')
		"""
		self.addInvoke(Invoke(when, what))
		
class AbstractJob(ProfileMixin,UseMixin,InvokeMixin):
	"""The base class for Job, DAX, and DAG"""
	def __init__(self, id=None, node_label=None):
		self.id = id
		self.node_label = node_label
		
		self.arguments = []
		self.profiles = set()
		self.used = set()
		self.invocations = set()

		self.stdout = None
		self.stderr = None
		self.stdin = None
	
	def addArguments(self, *arguments):
		"""Add one or more arguments to the job"""
		for arg in arguments:
			if not (isinstance(arg, File) or isinstance(arg, str) or isinstance(arg, unicode)):
				raise ValueError("Invalid argument", arg)
		for arg in arguments:
			if len(self.arguments) > 0:
				self.arguments.append(' ')
			self.arguments.append(arg)
			
	def clearArguments(self):
		"""Remove all arguments from this job"""
		self.arguments = []
		
	def getArguments(self):
		"""Get the arguments of this job"""
		args = []
		for a in self.arguments:
			if isinstance(a, File):
				args.append(unicode(a.toArgumentXML()))
			else:
				args.append(a)
		return ''.join(args)
			
	def setStdout(self, filename):
		"""Redirect stdout to a file"""
		if isinstance(filename,File):
			self.stdout = filename
		else:
			self.stdout = File(filename)
			
	def clearStdout(self):
		"""Remove stdout file"""
		self.stdout = None

	def setStderr(self, filename):
		"""Redirect stderr to a file"""
		if isinstance(filename,File):
			self.stderr = filename
		else:
			self.stderr = File(filename)
			
	def clearStderr(self):
		"""Remove stderr file"""
		self.stderr = None

	def setStdin(self, filename):
		"""Redirect stdin from a file"""
		if isinstance(filename,File):
			self.stdin = filename
		else:
			self.stdin = File(filename)
			
	def clearStdin(self):
		"""Remove stdin file"""
		self.stdin = None
		
	def innerXML(self, element):
		"""Return an XML representation of this job"""
		# Arguments
		if len(self.arguments) > 0:
			args = Element('argument').flatten()
			for x in self.arguments:
				if isinstance(x, File):
					args.element(x.toArgumentXML())
				else:
					args.text(x)
			element.element(args)

		# Profiles
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
		for use in self.used:
			element.element(use.toXML())
				
		# Invocations
		for inv in self.invocations:
			element.element(inv.toXML())

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
			name: The transformation name or Transformation object (required)
			id: A unique identifier for the job (optional)
			namespace: The namespace of the transformation (optional)
			version: The transformation version (optional)
			node_label: The label for this job to use in graphing (optional)
		"""
		self.namespace = None
		self.version = None
		if isinstance(name, Transformation):
			self.name = name.name
			self.namespace = name.namespace
			self.version = name.version
		else:
			self.name = name
		if not name:
			raise ValueError("Invalid name",name)
		AbstractJob.__init__(self, id=id, node_label=node_label)
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
		elif isinstance(file, str) or isinstance(file, unicode):
			self.file = File(name=file)
		else:
			raise ValueError("invalid file",file)
		AbstractJob.__init__(self, id=id, node_label=node_label)
		
	def toXML(self):
		"""Return an XML representation of this job"""
		e = Element('dax', [
			('id',self.id),
			('file',self.file.name),
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
		elif isinstance(file, str) or isinstance(file, unicode):
			self.file = File(name=file)
		else:
			raise ValueError("Invalid file", file)
		AbstractJob.__init__(self, id=id, node_label=node_label)
			
	def toXML(self):
		"""Return an XML representation of this DAG"""
		e = Element('dag', [
			('id',self.id),
			('file',self.file.name),
			('node-label',self.node_label)
		])
		self.innerXML(e)
		return e

class Dependency:
	"""A dependency between two nodes in the ADAG"""
	def __init__(self, parent, child, edge_label=None):
		if isinstance(parent, AbstractJob):
			if not parent.id:
				raise ValueError("Parent job has no id", parent)
			self.parent = parent.id
		elif parent:
			self.parent = parent
		else:
			raise ValueError("Invalid parent", parent)
		if isinstance(child, AbstractJob):
			if not child.id:
				raise ValueError("Child job has no id", child)
			self.child = child.id
		elif child:
			self.child = child
		else:
			raise ValueError("Invalid child", child)
		if self.parent == self.child:
			raise ValueError("No self edges allowed",(self.parent,self.child))
		self.edge_label = edge_label
		
	def __repr__(self):
		return "%s -> %s (%s)" % (self.parent, self.child, self.edge_label)
		
	def __hash__(self):
		return hash((self.parent,self.child))
		
	def __eq__(self, other):
		"""Equal dependencies have the same parent and child"""
		if isinstance(other, Dependency):
			return self.parent == other.parent and self.child == other.child
		return False

		
class ADAG(InvokeMixin):
	"""ADAG(name[,count][,index])
	
	Representation of a directed acyclic graph in XML (DAX).
	
	Examples:
		dax = ADAG('diamond')
	or, if you want to use the old style count/index partitioning stuff:
		part5 = ADAG('partition_5',count=10,index=5)
		
	Adding jobs:
		a = Job(...)
		dax.addJob(a)
		
	Adding parent-child control-flow dependency:
		dax.addDependency(Dependency(parent=a,child=b))
		dax.addDependency(Dependency(parent=a,child=c))
		dax.addDependency(Dependency(parent=b,child=d))
		dax.addDependency(Dependency(parent=c,child=d))	
	or:
		dax.depends(child=b, parent=a)
		
	Adding Files (not required if you have a replica catalog):
		input = File(...)
		dax.addFile(input)
		
	Adding Executables (not required if you have a transformation catalog):
		exe = Executable(...)
		dax.addExecutable(exe)
		
	Adding Transformations (not required if you have a transformation catalog):
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
		if not name:
			raise ValueError("Invalid ADAG name", name)
		self.name = name
		self.count = count
		self.index = index
		
		# This is used to generate unique ID numbers
		self.sequence = 1
		
		self.jobs = {}
		self.files = set()
		self.executables = set()
		self.dependencies = set()
		self.transformations = set()
		self.invocations = set()
		
	def nextJobID(self):
		"""Get an autogenerated ID for the next job"""
		next = None
		while not next or next in self.jobs:
			next = "ID%07d" % self.sequence
			self.sequence += 1
		return next
		
	def getJob(self, jobid):
		"""Get a Job/DAG/DAX"""
		if not jobid in self.jobs:
			raise NotFoundError("Job not found",jobid)
		return self.jobs[jobid]

	def addJob(self, job):
		"""Add a job to this ADAG"""
		# Add an auto-generated ID if the job doesn't have one
		if job.id is None:
			job.id = self.nextJobID()
		if self.hasJob(job):
			raise DuplicateError("Duplicate job",job)
		self.jobs[job.id] = job
		
	def hasJob(self, job):
		"""Test to see if job is in this ADAG
		The job parameter can be an object or a job ID
		"""
		if isinstance(job, AbstractJob):
			return job.id in self.jobs
		else:
			return job in self.jobs
		
	def removeJob(self, job):
		"""Remove job from this ADAG"""
		if not self.hasJob(job):
			raise NotFoundError("Job not found", job)
		if isinstance(job, AbstractJob):
			del self.jobs[job.id]
		else:
			del self.jobs[job]
			
	def clearJobs(self):
		"""Remove all jobs"""
		self.jobs = {}
		
	def addDAX(self, dax):
		"""Add a sub-DAX (synonym for addJob)"""
		if not isinstance(dax, DAX):
			raise ValueError("Not a DAX", dax)
		self.addJob(dax)
		
	def addDAG(self, dag):
		"""Add a sub-DAG (synonym for addJob)"""
		if not isinstance(dag, DAG):
			raise ValueError("Not a DAG", dag)
		self.addJob(dag)
		
	def addFile(self, file):
		"""Add a file to the DAX"""
		if not isinstance(file, File):
			raise ValueError("Invalid File",file)
		if self.hasFile(file):
			raise DuplicateError("Duplicate file", file)
		self.files.add(file)
		
	def hasFile(self, file):
		"""Check to see if file is in this ADAG"""
		return file in self.files
		
	def removeFile(self, file):
		"""Remove file from this ADAG"""
		if not self.hasFile(file):
			raise NotFoundError("File not found", file)
		self.files.remove(file)
		
	def clearFiles(self):
		"""Remove all files"""
		self.files.clear()
		
	def addExecutable(self, executable):
		"""Add an executable to this ADAG"""
		if self.hasExecutable(executable):
			raise DuplicateError("Duplicate executable",executable)
		self.executables.add(executable)
		
	def hasExecutable(self, executable):
		"""Check if executable is in this ADAG"""
		return executable in self.executables
		
	def removeExecutable(self, executable):
		"""Remove executable from this ADAG"""
		if not self.hasExecutable(executable):
			raise NotFoundError("Executable not found",executable)
		self.executables.remove(executable)
		
	def clearExecutables(self):
		"""Remove all executables"""
		self.executables.clear()
		
	def addTransformation(self, transformation):
		"""Add a transformation to this ADAG"""
		if self.hasTransformation(transformation):
			raise DuplicateError("Duplicate tranformation",transformation)
		self.transformations.add(transformation)
		
	def hasTransformation(self, transformation):
		"""Check to see if transformation is in this ADAG"""
		return transformation in self.transformations
		
	def removeTransformation(self, transformation):
		"""Remove transformation from this ADAG"""
		if not self.hasTransformation(transformation):
			raise NotFoundError("Transformation not found",transformation)
		self.transformations.remove(transformation)
		
	def clearTransformations(self):
		"""Remove all transformations"""
		self.transformations.clear()
		
	def depends(self, child, parent, edge_label=None):
		"""Add a dependency to the workflow
		Arguments:
			child: The child job/dax/dag or id
			parent: The parent job/dax/dag or id
			edge_label: A label for the edge (optional)
		"""
		d = Dependency(parent, child, edge_label)
		self.addDependency(d)
	
	def addDependency(self, dep):
		"""Add a dependency to the workflow
		
		The old way to call this method is no longer valid. Please change:
			adag.addDependency(parent="ID01", child="ID02", edge_label="E01")
		to be:
			adag.addDependency(Dependency(parent="ID01", child="ID02", edge_label="E01"))
		or:
			adag.depends(parent="ID01", child="ID02", edge_label="E01")
		
		"""
		if self.hasDependency(dep):
			raise DuplicateError("Duplicate dependency", dep)
		# Check the jobs
		if dep.parent not in self.jobs:
			raise NotFoundError("Parent not found", dep.parent)
		if dep.child not in self.jobs:
			raise NotFoundError("Child not found", dep.child)
		self.dependencies.add(dep)
		
	def hasDependency(self, dep):
		"""Check to see if dependency exists"""
		return dep in self.dependencies
		
	def removeDependency(self, dep):
		"""Remove dependency from workflow"""
		if not self.hasDependency(dep):
			raise NotFoundError("Dependency not found",dep)
		self.dependencies.remove(dep)
		
	def clearDependencies(self):
		"""Remove all dependencies"""
		self.dependencies.clear()
		
	def toXML(self):
		"""Get the XML string for this ADAG
		This is primarily intended for testing. If you have a large ADAG
		you should use writeXML instead.
		"""
		s = StringIO()
		self.writeXML(s)
		xml = s.getvalue()
		s.close()
		return xml

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
		
		# Invocations
		out.write(u'\n\t<!-- part 0: Workflow-level notifications (may be empty) -->\n')
		for i in self.invocations:
			out.write(u'\t')
			i.toXML().toXML(stream=out,level=1)
			out.write(u'\n')

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
		keys = self.jobs.keys()
		keys.sort()
		for job_id in keys:
			job = self.jobs[job_id]
			out.write(u'\t')
			job.toXML().toXML(stream=out,level=1)
			out.write(u'\n')
		
		# Dependencies
		out.write(u'\n\t<!-- part 4: List of control-flow dependencies (may be empty) -->\n')
		
		# Since we store dependencies as tuples, but we need to print them as nested elements
		# we first build a map of all the children that maps child -> [(parent,label),...]
		children = {}
		for dep in self.dependencies:
			if not dep.child in children:
				children[dep.child] = []
			children[dep.child].append((dep.parent, dep.edge_label))
		
		# Now output all the xml in sorted order by child, then parent
		keys = children.keys()
		keys.sort()
		for child in keys:
			out.write(u'\t')
			c = Element("child",[("ref",child)])
			parents = children[child]
			parents.sort()
			for parent, edge_label in parents:
				p = Element("parent",[
					("ref",parent),
					("edge-label",edge_label)
				])
				c.element(p)
			c.toXML(stream=out,level=1)
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
			installed = attrs.get("installed")
			if installed is not None:
				installed = bool(installed)
			e = Executable(name=name, namespace=namespace, version=version,
				arch=arch, os=os, osrelease=osrelease, osversion=osversion,
				glibc=glibc, installed=installed)
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
			executable = attrs.get("executable")
			if executable is not None:
				executable = bool(executable)
				
			if parent in ['job','dax','dag']:
				self.lastJob.uses(name, link=link, register=register,
					transfer=transfer, optional=optional, namespace=namespace,
					version=version, executable=executable)
			elif parent == 'transformation':
				self.lastTransformation.uses(name, namespace=namespace,
					version=version, executable=executable)
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
			self.adag.depends(parent=p, child=self.lastChild, edge_label=edge_label)
		else:
			raise Exception("Unrecognized element %s" % name)
			
	def characters(self, chars):
		parent = self.elements[0]
		
		if parent == "argument":
			self.lastArgument.append(unicode(chars))
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
			self.lastJob.arguments = self.lastArgument[:]
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


def main():
	"""Simple smoke test"""
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
	preprocess.uses(a, link=Link.INPUT)
	preprocess.uses(b1, link=Link.OUTPUT, transfer=True)
	preprocess.uses(b2, link=Link.OUTPUT, transfer=True)
	diamond.addJob(preprocess)
	
	# Add left Findrange job
	frl = Job(t_findrange)
	c1 = File("f.c1")
	frl.addArguments("-a findrange","-T60","-i",b1,"-o",c1)
	frl.uses(b1, link=Link.INPUT)
	frl.uses(c1, link=Link.OUTPUT, transfer=True)
	diamond.addJob(frl)
	
	# Add right Findrange job
	frr = Job(t_findrange)
	c2 = File("f.c2")
	frr.addArguments("-a findrange","-T60","-i",b2,"-o",c2)
	frr.uses(b2, link=Link.INPUT)
	frr.uses(c2, link=Link.OUTPUT, transfer=True)
	diamond.addJob(frr)
	
	# Add Analyze job
	analyze = Job(t_analyze)
	d = File("f.d")
	analyze.addArguments("-a analyze","-T60","-i",c1,c2,"-o",d)
	analyze.uses(c1, link=Link.INPUT)
	analyze.uses(c2, link=Link.INPUT)
	analyze.uses(d, link=Link.OUTPUT, transfer=True, register=True)
	diamond.addJob(analyze)
	
	# Add dependencies
	diamond.depends(parent=preprocess, child=frl)
	diamond.depends(parent=preprocess, child=frr)
	diamond.depends(parent=frl, child=analyze)
	diamond.depends(parent=frr, child=analyze)
	
	# Get generated diamond dax
	import sys
	diamond.writeXML(sys.stdout)


if __name__ == '__main__':
	main()