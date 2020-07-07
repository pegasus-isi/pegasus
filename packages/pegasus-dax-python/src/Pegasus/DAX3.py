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

"""API for generating Pegasus DAXes.

The classes in this module can be used to generate DAXes that can be
read by Pegasus.

The official DAX schema is here: http://pegasus.isi.edu/schema/

Here is an example showing how to create the diamond DAX using this API:

# Create a DAX
diamond = ADAG("diamond")

# Add some metadata
diamond.metadata("name", "diamond")
diamond.metadata("createdby", "Gideon Juve")

# Add input file to the DAX-level replica catalog
a = File("f.a")
a.addPFN(PFN("gsiftp://site.com/inputs/f.a","site"))
a.metadata("size", "1024")
diamond.addFile(a)

# Add executables to the DAX-level replica catalog
e_preprocess = Executable(namespace="diamond", name="preprocess", version="4.0", os="linux", arch="x86_64")
e_preprocess.metadata("size", "2048")
e_preprocess.addPFN(PFN("gsiftp://site.com/bin/preprocess","site"))
diamond.addExecutable(e_preprocess)

e_findrange = Executable(namespace="diamond", name="findrange", version="4.0", os="linux", arch="x86_64")
e_findrange.addPFN(PFN("gsiftp://site.com/bin/findrange","site"))
diamond.addExecutable(e_findrange)

e_analyze = Executable(namespace="diamond", name="analyze", version="4.0", os="linux", arch="x86_64")
e_analyze.addPFN(PFN("gsiftp://site.com/bin/analyze","site"))
diamond.addExecutable(e_analyze)

# Add a preprocess job
preprocess = Job(e_preprocess)
preprocess.metadata("time", "60")
b1 = File("f.b1")
b2 = File("f.b2")
preprocess.addArguments("-a preprocess","-T60","-i",a,"-o",b1,b2)
preprocess.uses(a, link=Link.INPUT)
preprocess.uses(b1, link=Link.OUTPUT, transfer=True)
preprocess.uses(b2, link=Link.OUTPUT, transfer=True)
diamond.addJob(preprocess)

# Add left Findrange job
frl = Job(e_findrange)
frl.metadata("time", "60")
c1 = File("f.c1")
frl.addArguments("-a findrange","-T60","-i",b1,"-o",c1)
frl.uses(b1, link=Link.INPUT)
frl.uses(c1, link=Link.OUTPUT, transfer=True)
diamond.addJob(frl)

# Add right Findrange job
frr = Job(e_findrange)
frr.metadata("time", "60")
c2 = File("f.c2")
frr.addArguments("-a findrange","-T60","-i",b2,"-o",c2)
frr.uses(b2, link=Link.INPUT)
frr.uses(c2, link=Link.OUTPUT, transfer=True)
diamond.addJob(frr)

# Add Analyze job
analyze = Job(e_analyze)
analyze.metadata("time", "60")
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

# Write the DAX to stdout
import sys
diamond.writeXML(sys.stdout)

# Write the DAX to a file
f = open("diamond.dax","w")
diamond.writeXML(f)
f.close()
"""

__author__ = "Gideon Juve <gideon@isi.edu>, Rafael Ferreira da Silva <rafsilva@isi.edu>"

__version__ = "3.6"

__all__ = [
    "DAX3Error",
    "DuplicateError",
    "NotFoundError",
    "FormatError",
    "ParseError",
    "Element",
    "Namespace",
    "ContainerType",
    "Arch",
    "Link",
    "Transfer",
    "OS",
    "When",
    "Invoke",
    "InvokeMixin",
    "ProfileMixin",
    "MetadataMixin",
    "PFNMixin",
    "CatalogType",
    "File",
    "Executable",
    "Container",
    "Metadata",
    "PFN",
    "Profile",
    "Use",
    "UseMixin",
    "Transformation",
    "AbstractJob",
    "Job",
    "DAX",
    "DAG",
    "Dependency",
    "ADAG",
    "parseString",
    "parse",
]
import codecs
import datetime
import os
import sys
import warnings

import six
from six import StringIO

SCHEMA_NAMESPACE = "http://pegasus.isi.edu/schema/DAX"
SCHEMA_LOCATION = "http://pegasus.isi.edu/schema/dax-3.6.xsd"
SCHEMA_VERSION = "3.6"

warnings.filterwarnings("once", category=DeprecationWarning)
warnings.warn(
    "Pegasus.DAX3 API has been deprecated and will be removed in v5.1.0. "
    "Please use the new API released in v5.0.0.",
    DeprecationWarning,
)


class DAX3Error(Exception):
    pass


class DuplicateError(DAX3Error):
    pass


class NotFoundError(DAX3Error):
    pass


class FormatError(DAX3Error):
    pass


class ParseError(DAX3Error):
    pass


@six.python_2_unicode_compatible
class Element:
    """Representation of an XML element for formatting output"""

    def __init__(self, name, attrs=[]):
        self.name = name
        self.attrs = []
        for attr, value in attrs:
            if value is not None:
                if isinstance(value, bool):
                    value = str(value).lower()
                elif not isinstance(value, six.string_types):
                    value = repr(value)
                attr = attr.replace("__", ":")
                self.attrs.append((attr, value))
        self.children = []
        self.flat = False

    def _escape(self, text):
        """Escape special characters in XML"""
        o = []
        for c in text:
            if c == '"':
                o.append("&quot;")
            elif c == "'":
                o.append("&apos;")
            elif c == "<":
                o.append("&lt;")
            elif c == ">":
                o.append("&gt;")
            elif c == "&":
                o.append("&amp;")
            else:
                o.append(c)
        return "".join(o)

    def element(self, element):
        self.children.append(element)
        return element

    def text(self, value):
        if not isinstance(value, six.string_types):
            value = str(value)
        self.children.append(self._escape(value))
        return self

    def comment(self, message):
        self.children.append("<!-- %s -->" % self._escape(message))

    def flatten(self):
        self.flat = True
        return self

    def __str__(self):
        s = StringIO()
        self.write(s)
        x = s.getvalue()
        s.close()
        return six.text_type(x)

    def write(self, stream=sys.stdout, level=0, flatten=False):
        flat = self.flat or flatten

        stream.write("<%s" % self.name)

        for attr, value in self.attrs:
            value = self._escape(value)
            stream.write(' %s="%s"' % (attr, value))

        if len(self.children) == 0:
            stream.write("/>")
        else:
            stream.write(">")
            if not flat:
                stream.write("\n")
            for child in self.children:
                if not flat:
                    stream.write("\t" * (level + 1))
                if isinstance(child, six.string_types):
                    stream.write(child)
                else:
                    child.write(stream, level + 1, flat)
                if not flat:
                    stream.write("\n")
            if not flat:
                stream.write("\t" * level)
            stream.write("</%s>" % self.name)


class Namespace:
    """
    Namespace values recognized by Pegasus. See Executable,
    Transformation, and Job.
    """

    PEGASUS = "pegasus"
    CONDOR = "condor"
    DAGMAN = "dagman"
    ENV = "env"
    HINTS = "hints"
    GLOBUS = "globus"
    SELECTOR = "selector"
    STAT = "stat"


class Arch:
    """
    Architecture types. See Executable.
    """

    X86 = "x86"
    X86_64 = "x86_64"
    PPC = "ppc"
    PPC_64 = "ppc_64"
    IA64 = "ia64"
    SPARCV7 = "sparcv7"
    SPARCV9 = "sparcv9"
    AMD64 = "amd64"


class Link:
    """
    Linkage attributes. See File, Executable and uses().
    """

    NONE = "none"
    INPUT = "input"
    OUTPUT = "output"
    INOUT = "inout"
    CHECKPOINT = "checkpoint"


class Transfer:
    """
    Transfer types for uses. See Executable, File.
    """

    FALSE = "false"
    OPTIONAL = "optional"
    TRUE = "true"


class OS:
    """
    OS types. See Executable.
    """

    LINUX = "linux"
    SUNOS = "sunos"
    AIX = "aix"
    MACOS = "macos"
    WINDOWS = "windows"


class When:
    """
    Job states for notifications. See Job/DAX/DAG.invoke().
    """

    NEVER = "never"
    START = "start"
    ON_ERROR = "on_error"
    ON_SUCCESS = "on_success"
    AT_END = "at_end"
    ALL = "all"


class ContainerType:
    """
    Container types. See Container.
    """

    DOCKER = "docker"
    SINGULARITY = "singularity"


@six.python_2_unicode_compatible
class Invoke:
    def __init__(self, when, what):
        if not when:
            raise FormatError("invalid when", when)
        if not what:
            raise FormatError("invalid what", what)
        self.when = when
        self.what = what

    def __str__(self):
        return "<Invoke %s %s>" % (self.when, self.what)

    def __hash__(self):
        return hash((self.when, self.what))

    def __eq__(self, other):
        if isinstance(other, Invoke):
            return self.when == other.when and self.what == other.what
        return False

    def toXML(self):
        e = Element("invoke", [("when", self.when)])
        e.text(self.what)
        e.flatten()
        return e


class InvokeMixin:
    def addInvoke(self, invoke):
        """Add invoke to this object"""
        if self.hasInvoke(invoke):
            raise DuplicateError("Duplicate Invoke %s" % invoke)
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

        WHEN        MEANING
        ==========  =======================================================
        never       never invoke
        start       invoke just before job gets submitted.
        on_error    invoke after job finishes with failure (exitcode != 0).
        on_success  invoke after job finishes with success (exitcode == 0).
        at_end      invoke after job finishes, regardless of exit status.
        all         like start and at_end combined.

        Examples:
            obj.invoke('at_end','/usr/bin/mail -s "job done" juve@usc.edu')
            obj.invoke('on_error','/usr/bin/update_db -failure')
        """
        self.addInvoke(Invoke(when, what))


class ProfileMixin:
    def addProfile(self, profile):
        """Add a profile to this object"""
        if self.hasProfile(profile):
            raise DuplicateError("Duplicate profile %s" % profile)
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
            raise DuplicateError("Duplicate Metadata %s" % metadata)
        self._metadata.add(metadata)

    def removeMetadata(self, metadata):
        """Remove meta from this object"""
        if not self.hasMetadata(metadata):
            raise NotFoundError("Metadata not found", metadata)
        self._metadata.remove(metadata)

    def hasMetadata(self, metadata):
        """Does this object have metadata?"""
        return metadata in self._metadata

    def clearMetadata(self):
        """Remove all metadata from this object"""
        self._metadata.clear()

    def metadata(self, key, value):
        """Declarative metadata addition"""
        self.addMetadata(Metadata(key, value))


class PFNMixin:
    def addPFN(self, pfn):
        """Add a PFN to this object"""
        if self.hasPFN(pfn):
            raise DuplicateError("Duplicate PFN %s" % pfn)
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
        self.addPFN(PFN(url, site))


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
            raise FormatError("name required")
        self.name = name
        self.profiles = set()
        self._metadata = set()
        self.pfns = set()

    def innerXML(self, parent):
        for p in self.profiles:
            parent.element(p.toXML())
        for m in self._metadata:
            parent.element(m.toXML())
        for p in self.pfns:
            parent.element(p.toXML())


@six.python_2_unicode_compatible
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

    def __str__(self):
        return "<File %s>" % self.name

    def __hash__(self):
        return hash(self.name)

    def __eq__(self, other):
        return isinstance(other, File) and self.name == other.name

    def toArgumentXML(self):
        """Returns an XML representation of this File with no inner elements"""
        return Element("file", [("name", self.name)])

    def toStdioXML(self, tag):
        """Returns an XML representation of this file as a stdin/out/err tag"""
        if tag == "stdin":
            link = "input"  # stdin is always input
        elif tag in ["stdout", "stderr"]:
            link = "output"  # stdout/stderr are always output
        else:
            raise FormatError(
                "invalid tag", tag, "should be one of stdin, stdout, stderr"
            )

        return Element(tag, [("name", self.name), ("link", link)])

    def toXML(self):
        """Return the XML representation of this File with inner elements"""
        e = self.toArgumentXML()
        self.innerXML(e)
        return e


@six.python_2_unicode_compatible
class Executable(CatalogType, InvokeMixin):
    """Executable(name[,namespace][,version][,arch][,os][,osrelease][,osversion][,glibc][,installed])

    An entry for an executable in the DAX-level transformation catalog.

    Examples:
        grep = Executable("grep")
        grep = Executable(namespace="os",name="grep",version="2.3")
        grep = Executable(namespace="os",name="grep",version="2.3",arch=Arch.X86)
        grep = Executable(namespace="os",name="grep",version="2.3",arch=Arch.X86,os=OS.LINUX)
    """

    def __init__(
        self,
        name,
        namespace=None,
        version=None,
        arch=None,
        os=None,
        osrelease=None,
        osversion=None,
        glibc=None,
        installed=None,
        container=None,
    ):
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
            container: Optional attribute to specify the container to use
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
        self.container = container
        self.invocations = set()

    def __str__(self):
        return "<Executable %s::%s:%s>" % (self.namespace, self.name, self.version)

    def __hash__(self):
        return hash(
            (
                self.name,
                self.namespace,
                self.version,
                self.arch,
                self.os,
                self.osrelease,
                self.osversion,
                self.glibc,
                self.installed,
                self.container,
            )
        )

    def __eq__(self, other):
        if isinstance(other, Executable):
            return (
                self.name == other.name
                and self.namespace == other.namespace
                and self.version == other.version
                and self.arch == other.arch
                and self.os == other.os
                and self.osrelease == other.osrelease
                and self.osversion == other.osversion
                and self.glibc == other.glibc
                and self.installed == other.installed
                and self.container == other.container
            )
        return False

    def toXML(self):
        """Returns an XML representation of this file as a filename tag"""
        e = Element(
            "executable",
            [
                ("name", self.name),
                ("namespace", self.namespace),
                ("version", self.version),
                ("arch", self.arch),
                ("os", self.os),
                ("osrelease", self.osrelease),
                ("osversion", self.osversion),
                ("glibc", self.glibc),
                ("installed", self.installed)
                # containers are not support by the DAX3 schema
            ],
        )
        self.innerXML(e)

        if self.container:
            warnings.warn(
                "The DAX API extensions do not support references for containers."
            )

        # Invocations
        for inv in self.invocations:
            e.element(inv.toXML())

        return e


@six.python_2_unicode_compatible
class Container(ProfileMixin):
    """Container(name,type,image[,image_site])

    An entry for a container in the DAX-level transformation catalog.

    Examples:
        mycontainer = Container("myapp", type="docker", image="docker:///rynge/montage:latest")
    """

    def __init__(self, name, type, image, imagesite=None, dockerfile=None, mount=None):
        """
        Arguments:
            name: Container name
            type: Container type (see ContainerType)
            image: URL to image in a container hub OR URL to an existing container image
            imagesite: optional site attribute to tell pegasus which site tar file exist
            dockerfile: a url to an existing docker file to build container image from scratch
            mount: list of volumes to be mounted
        """
        if not name:
            raise FormatError("Invalid name", name)
        if not type:
            raise FormatError("Invalid container type", type)
        if not image:
            raise FormatError("Invalid image", image)
        self.name = name
        self.type = type
        self.image = image
        self.imagesite = imagesite
        self.dockerfile = dockerfile
        self.mount = mount if mount else []
        self.profiles = set()

    def __str__(self):
        return "<Container %s:%s>" % (self.name, self.type)

    def __hash__(self):
        return hash((self.name, self.type, self.image, self.imagesite, self.dockerfile))

    def __eq__(self, other):
        if isinstance(other, Container):
            return (
                self.name == other.name
                and self.type == other.type
                and self.image == other.image
                and self.imagesite == other.imagesite
                and self.dockerfile == other.dockerfile
            )
        return False


@six.python_2_unicode_compatible
class Metadata:
    """Metadata(key,value)

    A way to add metadata to File and Executable objects. This is
    useful if you want to annotate the DAX with things like file
    sizes, application-specific attributes, etc.

    There is currently no restriction on the type.

    Examples:
        s = Metadata('size','12')
        a = Metadata('algorithm','plav')
    """

    def __init__(self, key, value):
        """
        Arguments:
            key: The key name of the item
            value: The value of the item
        """
        if not key:
            raise FormatError("Invalid key", key)
        if not value:
            raise FormatError("Invalid value", value)
        self.key = key
        self.value = value

    def __str__(self):
        return "<Metadata %s = %s>" % (self.key, self.value)

    def __hash__(self):
        return hash(self.key)

    def __eq__(self, other):
        return isinstance(other, Metadata) and self.key == other.key

    def toXML(self):
        m = Element("metadata", [("key", self.key)])
        m.text(self.value).flatten()
        return m


@six.python_2_unicode_compatible
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

    def __init__(self, url, site=None):
        """
        Arguments:
            url: The url of the file.
            site: The name of the site. [default: local]
        """
        if not url:
            raise FormatError("Invalid url", url)
        if not site:
            raise FormatError("Invalid site", site)
        self.url = url
        self.site = site
        self.profiles = set()

    def __str__(self):
        return "<PFN %s %s>" % (self.site, self.url)

    def __hash__(self):
        return hash((self.url, self.site))

    def __eq__(self, other):
        return (
            isinstance(other, PFN) and self.url == other.url and self.site == other.site
        )

    def toXML(self):
        pfn = Element("pfn", [("url", self.url), ("site", self.site)])
        for p in self.profiles:
            pfn.element(p.toXML())
        return pfn


@six.python_2_unicode_compatible
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

    def __str__(self):
        return "<Profile %s::%s = %s>" % (self.namespace, self.key, self.value)

    def __hash__(self):
        return hash((self.namespace, self.key))

    def __eq__(self, other):
        return (
            isinstance(other, Profile)
            and self.namespace == other.namespace
            and self.key == other.key
        )

    def toXML(self):
        """Return an XML element for this profile"""
        p = Element("profile", [("namespace", self.namespace), ("key", self.key)])
        p.text(self.value).flatten()
        return p


@six.python_2_unicode_compatible
class Use(MetadataMixin):
    """Use(file[,link][,register][,transfer][,optional]
           [,namespace][,version][,executable][,size])

    Use of a logical file name. Used for referencing files in the DAX.

    Attributes:
        file: A string, File or Executable representing the logical file
        link: Is this file a job input, output or both (See LFN) (optional)
        register: Should this file be registered in RLS? (True/False) (optional)
        transfer: Should this file be transferred? (True/False or See LFN) (optional)
        optional: Is this file optional, or should its absence be an error? (optional)
        namespace: Namespace of executable (optional)
        version: version of executable (optional)
        executable: Is file an executable? (True/False) (optional)
        size: The size of the file (optional)

    For Use objects that are added to Transformations, the attributes 'link', 'register',
    'transfer', 'optional' and 'size' are ignored.

    If a File object is passed in as 'file', then the default value for executable
    is 'false'. Similarly, if an Executable object is passed in, then the default
    value for executable is 'true'.
    """

    def __init__(
        self,
        name,
        link=None,
        register=None,
        transfer=None,
        optional=None,
        namespace=None,
        version=None,
        executable=None,
        size=None,
    ):
        if not name:
            raise FormatError("Invalid name", name)

        self.name = name
        self.link = link
        self.optional = optional
        self.register = register
        self.transfer = transfer
        self.namespace = namespace
        self.version = version
        self.executable = executable
        self.size = size

        self._metadata = set()

    def __str__(self):
        return "<Use %s::%s:%s>" % (self.namespace, self.name, self.version)

    def __hash__(self):
        return hash((self.namespace, self.name, self.version))

    def __eq__(self, other):
        if isinstance(other, Use):
            return (
                self.namespace == other.namespace
                and self.name == other.name
                and self.version == other.version
            )

    def toTransformationXML(self):
        e = Element(
            "uses",
            [
                ("namespace", self.namespace),
                ("name", self.name),
                ("version", self.version),
                ("executable", self.executable),
            ],
        )

        for m in self._metadata:
            e.element(m.toXML())

        return e

    def toJobXML(self):
        e = Element(
            "uses",
            [
                ("namespace", self.namespace),
                ("name", self.name),
                ("version", self.version),
                ("link", self.link),
                ("register", self.register),
                ("transfer", self.transfer),
                ("optional", self.optional),
                ("executable", self.executable),
                ("size", self.size),
            ],
        )

        for m in self._metadata:
            e.element(m.toXML())

        return e


class UseMixin:
    def addUse(self, use):
        """Add Use to this object"""
        if self.hasUse(use):
            raise DuplicateError("Duplicate Use %s" % use)
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

    def uses(
        self,
        arg,
        link=None,
        register=None,
        transfer=None,
        optional=None,
        namespace=None,
        version=None,
        executable=None,
        size=None,
    ):

        if isinstance(arg, CatalogType):
            _name = arg.name
        else:
            _name = arg

        _namespace = None
        _version = None
        _executable = None

        if isinstance(arg, Executable):
            _namespace = arg.namespace
            _version = arg.version
            # We only need to set this for jobs
            # the default is True for Transformations
            if isinstance(self, AbstractJob):
                _executable = True

        if isinstance(arg, File):
            # We only need to set this for transformations
            # The default is False for Jobs
            if isinstance(self, Transformation):
                _executable = False

        if namespace is not None:
            _namespace = namespace
        if version is not None:
            _version = str(version)
        if executable is not None:
            _executable = executable

        use = Use(
            _name,
            link,
            register,
            transfer,
            optional,
            _namespace,
            _version,
            _executable,
            size,
        )

        # Copy metadata from File or Executable
        # XXX Maybe we only want this if link!=input
        if isinstance(arg, CatalogType):
            for m in arg._metadata:
                use.addMetadata(m)

        self.addUse(use)


@six.python_2_unicode_compatible
class Transformation(UseMixin, InvokeMixin, MetadataMixin):
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

    def __init__(self, name, namespace=None, version=None):
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
        self.invocations = set()
        self._metadata = set()
        if isinstance(name, Executable):
            self.name = name.name
            self.namespace = name.namespace
            self.version = name.version
        else:
            self.name = name
        if namespace:
            self.namespace = namespace
        if version:
            self.version = version

    def __str__(self):
        return "<Transformation %s::%s:%s>" % (self.namespace, self.name, self.version)

    def __hash__(self):
        return hash((self.namespace, self.name, self.version))

    def __eq__(self, other):
        if isinstance(other, Transformation):
            return (
                self.namespace == other.namespace
                and self.name == other.name
                and self.version == other.version
            )

    def toXML(self):
        """Return an XML representation of this transformation"""
        e = Element(
            "transformation",
            [
                ("namespace", self.namespace),
                ("name", self.name),
                ("version", self.version),
            ],
        )

        # Metadata
        for m in self._metadata:
            e.element(m.toXML())

        # Uses
        def getlink(a):
            if a.link is not None:
                return a.link
            # Python 3 - make sure we return a string
            return ""

        used = list(self.used)
        used.sort(key=getlink)
        for u in used:
            e.element(u.toTransformationXML())

        # Invocations
        for inv in self.invocations:
            e.element(inv.toXML())

        return e


class AbstractJob(ProfileMixin, UseMixin, InvokeMixin, MetadataMixin):
    """The base class for Job, DAX, and DAG"""

    def __init__(self, id=None, node_label=None):
        self.id = id
        self.node_label = node_label

        self.arguments = []
        self.profiles = set()
        self.used = set()
        self.invocations = set()
        self._metadata = set()

        self.stdout = None
        self.stderr = None
        self.stdin = None

    def addArguments(self, *arguments):
        """Add one or more arguments to the job (this will add whitespace)"""
        for arg in arguments:
            if not isinstance(arg, (File, six.string_types)):
                raise FormatError("Invalid argument", arg)
        for arg in arguments:
            if len(self.arguments) > 0:
                self.arguments.append(" ")
            self.arguments.append(arg)

    def addRawArguments(self, *arguments):
        """Add one or more arguments to the job (whitespace will NOT be added)"""
        for arg in arguments:
            if not isinstance(arg, (File, six.string_types)):
                raise FormatError("Invalid argument", arg)
        self.arguments.extend(arguments)

    def clearArguments(self):
        """Remove all arguments from this job"""
        self.arguments = []

    def getArguments(self):
        """Get the arguments of this job"""
        args = []
        for a in self.arguments:
            if isinstance(a, File):
                args.append(six.text_type(a.toArgumentXML()))
            else:
                args.append(a)
        return "".join(args)

    def setStdout(self, filename):
        """Redirect stdout to a file"""
        if isinstance(filename, File):
            self.stdout = filename
        else:
            self.stdout = File(filename)

    def clearStdout(self):
        """Remove stdout file"""
        self.stdout = None

    def setStderr(self, filename):
        """Redirect stderr to a file"""
        if isinstance(filename, File):
            self.stderr = filename
        else:
            self.stderr = File(filename)

    def clearStderr(self):
        """Remove stderr file"""
        self.stderr = None

    def setStdin(self, filename):
        """Redirect stdin from a file"""
        if isinstance(filename, File):
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
            args = Element("argument").flatten()
            for x in self.arguments:
                if isinstance(x, File):
                    args.element(x.toArgumentXML())
                else:
                    args.text(x)
            element.element(args)

        # Metadata
        for m in self._metadata:
            element.element(m.toXML())

        # Profiles
        for pro in self.profiles:
            element.element(pro.toXML())

        # Stdin/xml/err
        if self.stdin is not None:
            element.element(self.stdin.toStdioXML("stdin"))
        if self.stdout is not None:
            element.element(self.stdout.toStdioXML("stdout"))
        if self.stderr is not None:
            element.element(self.stderr.toStdioXML("stderr"))

        # Uses
        def getlink(a):
            if a.link is not None:
                return a.link
            # Python 3 - make sure we return a string
            return ""

        used = list(self.used)
        used.sort(key=getlink)
        for use in used:
            element.element(use.toJobXML())

        # Invocations
        for inv in self.invocations:
            element.element(inv.toXML())


@six.python_2_unicode_compatible
class Job(AbstractJob):
    """Job((name|Executable|Transformation)[,id][,namespace][,version][,node_label])

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
        mDiff_job = Job(mDiff_xform)

    Or an Executable:
        mDiff_exe = Executable("mDiff", ...)
        mDiff_job = Job(mDiff_exe)

    Several arguments can be added at the same time:
        input = File(...)
        output = File(...)
        job.addArguments("-i",input,"-o",output)

    Profiles are added similarly:
        job.addProfile(Profile(Namespace.ENV, key='PATH', value='/bin'))
        job.profile(Namespace.ENV, "PATH", "/bin")

    Adding file uses is simple, and you can override global File attributes:
        job.uses(input, Link.INPUT)
        job.uses(output, Link.OUTPUT, transfer=True, register=True)
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
        if isinstance(name, (Transformation, Executable)):
            self.name = name.name
            self.namespace = name.namespace
            self.version = name.version
        elif isinstance(name, six.string_types):
            self.name = name
        else:
            raise FormatError("Name must be a string, Transformation or Executable")
        if not self.name:
            raise FormatError("Invalid name", self.name)
        AbstractJob.__init__(self, id=id, node_label=node_label)
        if namespace:
            self.namespace = namespace
        if version:
            self.version = version

    def __str__(self):
        return "<Job %s %s::%s:%s>" % (
            self.id,
            self.namespace,
            self.name,
            self.version,
        )

    def toXML(self):
        e = Element(
            "job",
            [
                ("id", self.id),
                ("namespace", self.namespace),
                ("name", self.name),
                ("version", self.version),
                ("node-label", self.node_label),
            ],
        )
        self.innerXML(e)
        return e


@six.python_2_unicode_compatible
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
        elif isinstance(file, six.string_types):
            self.file = File(name=file)
        else:
            raise FormatError("invalid file", file)
        AbstractJob.__init__(self, id=id, node_label=node_label)

    def __str__(self):
        return "<DAX %s %s>" % (self.id, self.file.name)

    def toXML(self):
        """Return an XML representation of this job"""
        e = Element(
            "dax",
            [
                ("id", self.id),
                ("file", self.file.name),
                ("node-label", self.node_label),
            ],
        )
        self.innerXML(e)
        return e


@six.python_2_unicode_compatible
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
        elif isinstance(file, six.string_types):
            self.file = File(name=file)
        else:
            raise FormatError("Invalid file", file)
        AbstractJob.__init__(self, id=id, node_label=node_label)

    def __str__(self):
        return "<DAG %s %s>" % (self.id, self.file.name)

    def toXML(self):
        """Return an XML representation of this DAG"""
        e = Element(
            "dag",
            [
                ("id", self.id),
                ("file", self.file.name),
                ("node-label", self.node_label),
            ],
        )
        self.innerXML(e)
        return e


@six.python_2_unicode_compatible
class Dependency:
    """A dependency between two nodes in the ADAG"""

    def __init__(self, parent, child, edge_label=None):
        if isinstance(parent, AbstractJob):
            if not parent.id:
                raise FormatError("Parent job has no id", parent)
            self.parent = parent.id
        elif parent:
            self.parent = parent
        else:
            raise FormatError("Invalid parent", parent)
        if isinstance(child, AbstractJob):
            if not child.id:
                raise FormatError("Child job has no id", child)
            self.child = child.id
        elif child:
            self.child = child
        else:
            raise FormatError("Invalid child", child)
        if self.parent == self.child:
            raise FormatError("No self edges allowed", (self.parent, self.child))
        self.edge_label = edge_label

    def __str__(self):
        return "<Dependency %s -> %s>" % (self.parent, self.child)

    def __hash__(self):
        return hash((self.parent, self.child))

    def __eq__(self, other):
        """Equal dependencies have the same parent and child"""
        if isinstance(other, Dependency):
            return self.parent == other.parent and self.child == other.child
        return False


@six.python_2_unicode_compatible
class ADAG(InvokeMixin, MetadataMixin):
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

    def __init__(self, name, count=None, index=None, auto=False):
        """
        Arguments:
            name: The name of the workflow
            count: Total number of DAXes that will be created
            index: Zero-based index of this DAX
        """
        if not name:
            raise FormatError("Invalid ADAG name", name)
        self.name = name
        if count:
            count = int(count)
        if index:
            index = int(index)
        self.count = count
        self.index = index
        self._auto = auto if auto is True else False

        # This is used to generate unique ID numbers
        self.sequence = 1

        self.jobs = dict()
        self.files = dict()
        self.executables = set()
        self.dependencies = set()
        self.transformations = set()
        self.invocations = set()
        self._metadata = set()

        # PM-1311 always associate dax.api metadata
        self.metadata("dax.api", "python")

    def __str__(self):
        return "<ADAG %s>" % self.name

    def nextJobID(self):
        """Get an autogenerated ID for the next job"""
        next = None
        while not next or next in self.jobs:
            next = "ID%07d" % self.sequence
            self.sequence += 1
        return next

    def getJob(self, jobid):
        """Get a Job/DAG/DAX"""
        if jobid not in self.jobs:
            raise NotFoundError("Job not found", jobid)
        return self.jobs[jobid]

    def addJob(self, job):
        """Add a job to this ADAG"""
        # Add an auto-generated ID if the job doesn't have one
        if job.id is None:
            job.id = self.nextJobID()
        if self.hasJob(job):
            raise DuplicateError("Duplicate job %s" % job)
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

    def getJobInputFiles(self, jobid):
        """Get the set of input files used by a job"""
        job = self.getJob(jobid)
        input_files = set()

        for use in job.used:
            # this use refers to a file
            if not use.executable:
                if use.link == Link.INPUT:
                    input_files.add(self.getFile(use.name))

        return input_files

    def getJobOutputFiles(self, jobid):
        """Get the set of input files used by a job"""
        job = self.getJob(jobid)
        output_files = set()

        for use in job.used:
            # this use refers to a file
            if not use.executable:
                if use.link == Link.OUTPUT:
                    output_files.add(self.getFile(use.name))

        return output_files

    def addDAX(self, dax):
        """Add a sub-DAX (synonym for addJob)"""
        if not isinstance(dax, DAX):
            raise FormatError("Not a DAX", dax)
        self.addJob(dax)

    def addDAG(self, dag):
        """Add a sub-DAG (synonym for addJob)"""
        if not isinstance(dag, DAG):
            raise FormatError("Not a DAG", dag)
        self.addJob(dag)

    def addFile(self, file):
        """Add a file to the DAX"""
        if not isinstance(file, File):
            raise FormatError("Invalid File", file)
        if self.hasFile(file.name):
            raise DuplicateError("Duplicate file %s" % file)
        self.files[file.name] = file

    def getFile(self, filename):
        """Returns a File which was added to this ADAG"""
        if not self.hasFile(filename):
            raise NotFoundError("File not found", filename)
        return self.files[filename]

    def hasFile(self, filename):
        """Check to see if file is in this ADAG"""
        return filename in self.files

    def removeFile(self, filename):
        """Remove file from this ADAG"""
        if not self.hasFile(filename):
            raise NotFoundError("File not found", filename)
        del self.files[filename]

    def clearFiles(self):
        """Remove all files"""
        self.files.clear()

    def addExecutable(self, executable):
        """Add an executable to this ADAG"""
        if self.hasExecutable(executable):
            raise DuplicateError("Duplicate executable %s" % executable)
        self.executables.add(executable)

    def hasExecutable(self, executable):
        """Check if executable is in this ADAG"""
        return executable in self.executables

    def removeExecutable(self, executable):
        """Remove executable from this ADAG"""
        if not self.hasExecutable(executable):
            raise NotFoundError("Executable not found %s" % executable)
        self.executables.remove(executable)

    def clearExecutables(self):
        """Remove all executables"""
        self.executables.clear()

    def addTransformation(self, transformation):
        """Add a transformation to this ADAG"""
        if self.hasTransformation(transformation):
            raise DuplicateError("Duplicate tranformation %s" % transformation)
        self.transformations.add(transformation)

    def hasTransformation(self, transformation):
        """Check to see if transformation is in this ADAG"""
        return transformation in self.transformations

    def removeTransformation(self, transformation):
        """Remove transformation from this ADAG"""
        if not self.hasTransformation(transformation):
            raise NotFoundError("Transformation not found %s" % transformation)
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
            raise DuplicateError("Duplicate dependency %s" % dep)
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
            raise NotFoundError("Dependency not found", dep)
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

    def writeXMLFile(self, filename):
        """Write the ADAG to an XML file"""
        file = codecs.open(filename, "w", "utf-8")
        self.writeXML(file)
        file.close()

    def _autoDependencies(self):
        """Automatically compute job dependencies based on input/output files used by a job"""
        if self._auto is False:
            return

        mapping = {}

        def addOutput(job, file_obj):
            if file_obj:
                file_obj = file_obj.name

                if file_obj not in mapping:
                    mapping[file_obj] = (set(), set())

                mapping[file_obj][1].add(job)

        # Automatically determine dependencies

        # Traverse each job
        for job_id, job in self.jobs.items():
            file_used = job.used

            # If job produces to stdout, identify it as an output file
            addOutput(job, job.stdout)
            # If job produces to stderr, identify it as an output file
            addOutput(job, job.stderr)

            # If job consumes from stdin, identify it as an input file
            if job.stdin:
                if job.stdin.name not in mapping:
                    mapping[job.stdin.name] = (set(), set())

                mapping[job.stdin.name][0].add(job)

            for f in file_used:
                if f.name not in mapping:
                    mapping[f.name] = (set(), set())

                if f.link == Link.INPUT:
                    mapping[f.name][0].add(job)
                else:
                    mapping[f.name][1].add(job)

        for file_name, io in mapping.items():
            # Go through the mapping and for each file add dependencies between the
            # job producing a file and the jobs consuming the file
            inputs = io[0]

            if len(io[1]) > 0:
                output = io[1].pop()

                for _input in inputs:
                    try:
                        self.depends(parent=output, child=_input)
                    except DuplicateError:
                        pass

    def writeXML(self, out):
        """Write the ADAG as XML to a stream"""
        self._autoDependencies()

        # Preamble
        out.write('<?xml version="1.0" encoding="UTF-8"?>\n')

        out.write("<!-- generated: %s -->\n" % datetime.datetime.now())
        if os.name == "posix":
            import pwd

            username = pwd.getpwuid(os.getuid())[0]
        elif os.name == "nt":
            username = os.getenv("USERNAME", "N/A")
        else:
            username = "N/A"
        out.write("<!-- generated by: %s -->\n" % username)
        out.write("<!-- generator: python -->\n")

        # Open tag
        out.write('<adag xmlns="%s" ' % SCHEMA_NAMESPACE)
        out.write('xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" ')
        out.write('xsi:schemaLocation="%s %s" ' % (SCHEMA_NAMESPACE, SCHEMA_LOCATION))
        out.write('version="%s" ' % SCHEMA_VERSION)
        out.write('name="%s"' % self.name)
        if self.count:
            out.write(' count="%d"' % self.count)
        if self.index:
            out.write(' index="%d"' % self.index)
        out.write(">\n")

        # Metadata
        for m in self._metadata:
            out.write("\t")
            m.toXML().write(stream=out, level=1)
            out.write("\n")

        # Invocations
        for i in self.invocations:
            out.write("\t")
            i.toXML().write(stream=out, level=1)
            out.write("\n")

        # Files
        for fname, f in self.files.items():
            out.write("\t")
            f.toXML().write(stream=out, level=1)
            out.write("\n")

        # Executables
        for e in self.executables:
            out.write("\t")
            e.toXML().write(stream=out, level=1)
            out.write("\n")

        # Transformations
        for t in self.transformations:
            out.write("\t")
            t.toXML().write(stream=out, level=1)
            out.write("\n")

        # Jobs
        keys = self.jobs.keys()
        keys = sorted(keys)
        for job_id in keys:
            job = self.jobs[job_id]
            out.write("\t")
            job.toXML().write(stream=out, level=1)
            out.write("\n")

        # Dependencies
        # Since we store dependencies as tuples, but we need to print them as nested elements
        # we first build a map of all the children that maps child -> [(parent,label),...]
        children = {}
        for dep in self.dependencies:
            if not dep.child in children:
                children[dep.child] = []
            children[dep.child].append((dep.parent, dep.edge_label))

        # Now output all the xml in sorted order by child, then parent
        keys = children.keys()
        keys = sorted(keys)
        for child in keys:
            out.write("\t")
            c = Element("child", [("ref", child)])
            parents = children[child]
            parents = sorted(parents)
            for parent, edge_label in parents:
                p = Element("parent", [("ref", parent), ("edge-label", edge_label)])
                c.element(p)
            c.write(stream=out, level=1)
            out.write("\n")

        # Close tag
        out.write("</adag>\n")


def parseString(string):
    s = StringIO(string)
    return parse(s)


def parse(infile):
    try:
        from xml.etree import cElementTree as etree
    except Exception:
        try:
            from xml.etree import ElementTree as etree
        except Exception:
            try:
                from elementtree import ElementTree as etree
            except Exception:
                raise Exception("Please install elementtree")

    NS = "{http://pegasus.isi.edu/schema/DAX}"

    def QN(tag):
        return NS + tag

    def badattr(e, exc):
        return ParseError(
            "Attribute '%s' is required for element %s" % (exc.args[0], e.tag)
        )

    def parse_invoke(e):
        try:
            return Invoke(when=e.attrib["when"], what=e.text)
        except KeyError as ke:
            raise badattr(e, ke)

    def parse_adag(e):
        try:
            name = e.attrib["name"]
            count = e.get("count", None)
            index = e.get("index", None)
            return ADAG(name=name, count=count, index=index)
        except KeyError as ke:
            raise badattr(e, ke)

    def parse_profile(e):
        try:
            return Profile(
                namespace=e.attrib["namespace"], key=e.attrib["key"], value=e.text
            )
        except KeyError as ke:
            raise badattr(e, ke)

    def parse_metadata(e):
        try:
            return Metadata(key=e.attrib["key"], value=e.text)
        except KeyError as ke:
            raise badattr(e, ke)

    def parse_pfn(e):
        try:
            p = PFN(url=e.attrib["url"], site=e.get("site", None))
        except KeyError as ke:
            raise badattr(e, ke)
        for pr in e.findall(QN("profile")):
            p.addProfile(parse_profile(pr))
        return p

    def parse_catalog(e, f):
        for p in e.findall(QN("profile")):
            f.addProfile(parse_profile(p))
        for m in e.findall(QN("metadata")):
            f.addMetadata(parse_metadata(m))
        for p in e.findall(QN("pfn")):
            f.addPFN(parse_pfn(p))
        return f

    def parse_file(e):
        try:
            f = File(e.attrib["name"])
        except KeyError as ke:
            raise badattr(e, ke)
        return parse_catalog(e, f)

    def parse_executable(e):
        try:
            exe = Executable(
                name=e.attrib["name"],
                namespace=e.get("namespace", None),
                version=e.get("version", None),
                arch=e.get("arch", None),
                os=e.get("os", None),
                osrelease=e.get("osrelease", None),
                osversion=e.get("osversion", None),
                glibc=e.get("glibc", None),
                installed=e.get("installed", None),
            )
        except KeyError as ke:
            raise badattr(e, ke)
        parse_catalog(e, exe)
        for i in e.findall(QN("invoke")):
            exe.addInvoke(parse_invoke(i))
        return exe

    def parse_uses(e):
        try:
            u = Use(
                e.attrib["name"],
                namespace=e.get("namespace", None),
                version=e.get("version", None),
                link=e.get("link", None),
                register=e.get("register", None),
                transfer=e.get("transfer", None),
                optional=e.get("optional", None),
                executable=e.get("executable", None),
            )
        except KeyError as ke:
            raise badattr(e, ke)
        for m in e.findall(QN("metadata")):
            u.addMetadata(parse_metadata(m))
        return u

    def parse_transformation(e):
        try:
            t = Transformation(
                namespace=e.get("namespace", None),
                name=e.attrib["name"],
                version=e.get("version", None),
            )
        except KeyError as ke:
            raise badattr(e, ke)
        for u in e.findall(QN("uses")):
            t.addUse(parse_uses(u))
        for i in e.findall(QN("invoke")):
            t.addInvoke(parse_invoke(i))
        for m in e.findall(QN("metadata")):
            t.addMetadata(parse_metadata(m))
        return t

    def iterelem(e):
        if e.text:
            yield e.text
        for f in e:
            if f.text:
                yield f.text
            yield f
            if f.tail:
                yield f.tail

    def parse_absjob(e, j):
        args = e.find(QN("argument"))
        if args is not None:
            for i in iterelem(args):
                if isinstance(i, six.string_types):
                    j.addRawArguments(i)
                else:
                    j.addRawArguments(File(i.attrib["name"]))

        try:
            s = e.find(QN("stdin"))
            if s is not None:
                j.setStdin(s.attrib["name"])

            s = e.find(QN("stdout"))
            if s is not None:
                j.setStdout(s.attrib["name"])

            s = e.find(QN("stderr"))
            if s is not None:
                j.setStderr(s.attrib["name"])
        except KeyError as ke:
            raise badattr(s, ke)

        for p in e.findall(QN("profile")):
            j.addProfile(parse_profile(p))

        for u in e.findall(QN("uses")):
            j.addUse(parse_uses(u))

        for i in e.findall(QN("invoke")):
            j.addInvoke(parse_invoke(i))

        for m in e.findall(QN("metadata")):
            j.addMetadata(parse_metadata(m))

        return j

    def parse_job(e):
        try:
            j = Job(
                name=e.attrib["name"],
                id=e.attrib["id"],
                namespace=e.get("namespace", None),
                version=e.get("version", None),
                node_label=e.get("node-label", None),
            )
        except KeyError as ke:
            raise badattr(e, ke)
        return parse_absjob(e, j)

    def parse_dax(e):
        try:
            d = DAX(
                file=e.attrib["file"],
                id=e.attrib["id"],
                node_label=e.get("node-label", None),
            )
        except KeyError as ke:
            raise badattr(e, ke)
        return parse_absjob(e, d)

    def parse_dag(e):
        try:
            d = DAG(
                file=e.attrib["file"],
                id=e.attrib["id"],
                node_label=e.get("node-label", None),
            )
        except KeyError as ke:
            raise badattr(e, ke)
        return parse_absjob(e, d)

    def parse_dependencies(e):
        try:
            child = e.attrib["ref"]
        except KeyError as ke:
            raise badattr(e, ke)
        for p in e.findall(QN("parent")):
            try:
                parent = p.attrib["ref"]
                label = p.attrib.get("edge-label", None)
                yield Dependency(parent, child, label)
            except KeyError as ke:
                raise badattr(p, ke)

    # We use iterparse because we don't have to read in the
    # entire document
    iterator = etree.iterparse(infile, events=("start", "end"))
    iterator = iter(iterator)

    # Get the document element (should be <adag>)
    event, root = next(iterator)
    adag = parse_adag(root)

    # This function reads all the children of "node"
    def expand(node):
        event, elem = next(iterator)
        while elem != node:
            event, elem = next(iterator)

        # We clear the document element to prevent
        # the memory usage from growing
        root.clear()

    for ev, elem in iterator:
        if ev == "end":
            continue

        # Read in the entire element and children
        expand(elem)

        if elem.tag == QN("job"):
            j = parse_job(elem)
            adag.addJob(j)
        elif elem.tag == QN("child"):
            for d in parse_dependencies(elem):
                adag.addDependency(d)
        elif elem.tag == QN("file"):
            f = parse_file(elem)
            adag.addFile(f)
        elif elem.tag == QN("executable"):
            e = parse_executable(elem)
            adag.addExecutable(e)
        elif elem.tag == QN("transformation"):
            t = parse_transformation(elem)
            adag.addTransformation(t)
        elif elem.tag == QN("dag"):
            d = parse_dag(elem)
            adag.addJob(d)
        elif elem.tag == QN("dax"):
            d = parse_dax(elem)
            adag.addJob(d)
        elif elem.tag == QN("invoke"):
            adag.addInvoke(parse_invoke(elem))
        elif elem.tag == QN("metadata"):
            adag.addMetadata(parse_metadata(elem))
        else:
            raise ParseError("Unknown tag", elem.tag)

    return adag


def main():
    """Simple smoke test"""
    # Create a DAX
    diamond = ADAG("diamond")

    # Add some metadata
    diamond.metadata("name", "diamond")
    diamond.metadata("createdby", "Gideon Juve")

    # add some invoke condition
    diamond.invoke("on_error", "/usr/bin/update_db -failure")

    # Add input file to the DAX-level replica catalog
    a = File("f.a")
    a.addPFN(PFN("gsiftp://site.com/inputs/f.a", "site"))
    a.metadata("size", "1024")
    diamond.addFile(a)

    # Add executables to the DAX-level replica catalog
    e_preprocess = Executable(
        namespace="diamond", name="preprocess", version="4.0", os="linux", arch="x86_64"
    )
    e_preprocess.metadata("size", "2048")
    e_preprocess.addPFN(PFN("gsiftp://site.com/bin/preprocess", "site"))
    diamond.addExecutable(e_preprocess)

    e_findrange = Executable(
        namespace="diamond", name="findrange", version="4.0", os="linux", arch="x86_64"
    )
    e_findrange.addPFN(PFN("gsiftp://site.com/bin/findrange", "site"))
    diamond.addExecutable(e_findrange)

    e_analyze = Executable(
        namespace="diamond", name="analyze", version="4.0", os="linux", arch="x86_64"
    )
    e_analyze.addPFN(PFN("gsiftp://site.com/bin/analyze", "site"))
    e_analyze.addProfile(Profile(namespace="env", key="APP_HOME", value="/app"))
    diamond.addExecutable(e_analyze)

    # Add a preprocess job
    preprocess = Job(e_preprocess)
    preprocess.metadata("time", "60")
    b1 = File("f.b1")
    b2 = File("f.b2")
    preprocess.addArguments("-a preprocess", "-T60", "-i", a, "-o", b1, b2)
    preprocess.uses(a, link=Link.INPUT)
    preprocess.uses(b1, link=Link.OUTPUT, transfer=True)
    preprocess.uses(b2, link=Link.OUTPUT, transfer=True)
    diamond.addJob(preprocess)

    # Add left Findrange job
    frl = Job(e_findrange)
    frl.metadata("time", "60")
    c1 = File("f.c1")
    frl.addArguments("-a findrange", "-T60", "-i", b1, "-o", c1)
    frl.uses(b1, link=Link.INPUT)
    frl.uses(c1, link=Link.OUTPUT, transfer=True)
    diamond.addJob(frl)

    # Add right Findrange job
    frr = Job(e_findrange)
    frr.metadata("time", "60")
    c2 = File("f.c2")
    frr.addArguments("-a findrange", "-T60", "-i", b2, "-o", c2)
    frr.uses(b2, link=Link.INPUT)
    frr.uses(c2, link=Link.OUTPUT, transfer=True)
    diamond.addJob(frr)

    # Add Analyze job
    analyze = Job(e_analyze)
    analyze.metadata("time", "60")
    d = File("f.d")
    analyze.addArguments("-a analyze", "-T60", "-i", c1, c2, "-o", d)
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


if __name__ == "__main__":
    main()
