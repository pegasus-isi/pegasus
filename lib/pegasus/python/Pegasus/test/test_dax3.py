"""
This runs the unit test suite for the DAX3 module
"""

import unittest
from Pegasus.DAX3 import *
from Pegasus.DAX3 import Element, CatalogType
import sys
import os

DIR = os.path.dirname(__file__)
DIAMOND_DAX = os.path.join(DIR, "diamond.xml")
DAX3TEST_DAX = os.path.join(DIR, "dax3.xml")

class TestElement(unittest.TestCase):
    def testSimple(self):
        x = Element("x")
        self.assertEqual(str(x), '<x/>')

    def testElement(self):
        x = Element("x")
        y = Element("y")
        x.element(y)
        self.assertEqual(str(x), '<x>\n\t<y/>\n</x>')

    def testText(self):
        x = Element("x")
        x.text("y")
        self.assertEqual(str(x), '<x>\n\ty\n</x>')

    def testUnicode(self):
        x = Element("x")
        x.comment(u'\u03a3')
        x.flatten()
        self.assertEqual(unicode(x), u'<x><!-- \u03a3 --></x>')

        x = Element(u'\u03a3')
        self.assertEqual(unicode(x), u'<\u03a3/>')

        x = Element('x', [(u'\u03a3', 'foo')])
        self.assertEqual(unicode(x), u'<x \u03a3="foo"/>')

        x = Element('x', [('foo', u'\u03a3')])
        self.assertEqual(unicode(x), u'<x foo="\u03a3"/>')

        x = Element('x')
        x.text(u'\u03a3')
        x.flatten()
        self.assertEqual(unicode(x), u'<x>\u03a3</x>')

    def testFlatten(self):
        x = Element("x")
        x.text("y")
        x.flatten()
        self.assertEqual(str(x), '<x>y</x>')

    def testComment(self):
        x = Element("x")
        x.comment("test")
        self.assertEqual(str(x), '<x>\n\t<!-- test -->\n</x>')

class TestMetadata(unittest.TestCase):
    def testConstructor(self):
        """Metadata constructor should only allow valid values"""
        m = Metadata("key","value")
        self.assertEqual(m.key, "key")
        self.assertEqual(m.value, "value")
        self.assertRaises(FormatError, Metadata, None, "value")
        self.assertRaises(FormatError, Metadata, "key", None)

    def testEqual(self):
        """Equal Metadata should have the same key"""
        a = Metadata("key","value")
        b = Metadata("key","value1")
        c = Metadata("key1","value")
        self.assertTrue(a == b)
        self.assertFalse(a == c)
        self.assertFalse(b == c)

    def testXML(self):
        """toXML should output properly formatted XML"""
        a = Metadata("key","value")
        self.assertEqual(str(a.toXML()), '<metadata key="key">value</metadata>')

class TestPFN(unittest.TestCase):
    def testConstructor(self):
        """PFN constructor should only allow valid values"""
        a = PFN("url","site")
        self.assertEqual(a.url, "url")
        self.assertEqual(a.site, "site")
        self.assertRaises(FormatError, PFN, None)
        self.assertRaises(FormatError, PFN, "url", None)
        self.assertRaises(FormatError, PFN, "")
        self.assertRaises(FormatError, PFN, "url", "")

    def testEqual(self):
        """Equal PFNs should have the same URL and site"""
        a = PFN("http://abc","a")
        b = PFN("http://abc","a")
        c = PFN("http://abc","b")
        d = PFN("http://cde","a")
        self.assertTrue(a == b)
        self.assertFalse(a == c)
        self.assertFalse(a == d)

    def testProfiles(self):
        """PFNs should handle profile properly"""
        c = PFN("http","a")
        p = Profile("ns","name","value")
        self.assertFalse(c.hasProfile(p))
        c.addProfile(p)
        self.assertRaises(DuplicateError, c.addProfile, p)
        self.assertTrue(c.hasProfile(p))
        c.removeProfile(p)
        self.assertFalse(c.hasProfile(p))
        self.assertRaises(NotFoundError, c.removeProfile, p)
        c.addProfile(p)
        c.clearProfiles()
        self.assertFalse(c.hasProfile(p))

    def testXML(self):
        """toXML should output properly formatted XML"""
        a = PFN("http://abc", "a")
        self.assertEqual(unicode(a.toXML()), '<pfn url="http://abc" site="a"/>')

        a.addProfile(Profile("ns","name","value"))
        self.assertEqual(str(a.toXML()), '<pfn url="http://abc" site="a">\n\t<profile namespace="ns" key="name">value</profile>\n</pfn>')

class TestProfile(unittest.TestCase):
    def testConstructor(self):
        a = Profile("ns","key","value")
        self.assertEqual(a.namespace,"ns")
        self.assertEqual(a.key,"key")
        self.assertEqual(a.value,"value")

    def testEqual(self):
        """Equal profiles should have the same (ns, key)"""
        a = Profile("ns","key","value")
        b = Profile("ns","key","value")
        c = Profile("ns","key","value1")
        d = Profile("ns","key1","value")
        e = Profile("ns1","key","value")
        self.assertTrue(a == b)
        self.assertTrue(a == c)
        self.assertTrue(b == c)
        self.assertFalse(a == d)
        self.assertFalse(a == e)
        self.assertFalse(d == e)

    def testXML(self):
        """toXML should output properly formatted XML"""
        a = Profile("ns","key","value")
        self.assertEqual(str(a.toXML()),'<profile namespace="ns" key="key">value</profile>')

class TestCatalogType(unittest.TestCase):
    def testConstructor(self):
        """Catalog types require a name"""
        t = CatalogType("name")
        self.assertEqual(t.name, "name")
        self.assertRaises(FormatError, CatalogType, None)
        self.assertRaises(FormatError, CatalogType, "")

    def testProfile(self):
        """Should be able to add/remove/has profiles"""
        c = CatalogType("name")
        p = Profile("ns","name","value")
        self.assertFalse(c.hasProfile(p))
        c.addProfile(p)
        self.assertRaises(DuplicateError, c.addProfile, p)
        self.assertTrue(c.hasProfile(p))
        c.removeProfile(p)
        self.assertFalse(c.hasProfile(p))
        self.assertRaises(NotFoundError, c.removeProfile, p)
        c.addProfile(p)
        c.clearProfiles()
        self.assertFalse(c.hasProfile(p))

    def testMetadata(self):
        """Should be able to add/remove/has metadata"""
        c = CatalogType("name")
        p = Metadata("key","value")
        self.assertFalse(c.hasMetadata(p))
        c.addMetadata(p)
        self.assertRaises(DuplicateError, c.addMetadata, p)
        self.assertTrue(c.hasMetadata(p))
        c.removeMetadata(p)
        self.assertFalse(c.hasMetadata(p))
        self.assertRaises(NotFoundError, c.removeMetadata, p)
        c.addMetadata(p)
        c.clearMetadata()
        self.assertFalse(c.hasMetadata(p))

    def testPFN(self):
        "Should be able to add/remove/has PFNs"
        c = CatalogType("name")
        p = PFN("url","site")
        self.assertFalse(c.hasPFN(p))
        c.addPFN(p)
        self.assertRaises(DuplicateError, c.addPFN, p)
        self.assertTrue(c.hasPFN(p))
        c.removePFN(p)
        self.assertFalse(c.hasPFN(p))
        self.assertRaises(NotFoundError, c.removePFN, p)
        c.addPFN(p)
        c.clearPFNs()
        self.assertFalse(c.hasPFN(p))

class TestFile(unittest.TestCase):
    def testEqual(self):
        """Equal files should have the same name"""
        a = File("a")
        b = File("a")
        c = File("b")
        self.assertTrue(a==b)
        self.assertFalse(a==c)

    def testXML(self):
        """toXML should output proper XML with nested elements"""
        c = File("name")
        self.assertEqual(str(c.toXML()), '<file name="name"/>')

        # Profile
        c.addProfile(Profile("ns","key","value"))
        self.assertEqual(str(c.toXML()), '<file name="name">\n\t<profile namespace="ns" key="key">value</profile>\n</file>')
        c.clearProfiles()

        # Metadata
        c.addMetadata(Metadata("key","value"))
        self.assertEqual(str(c.toXML()), '<file name="name">\n\t<metadata key="key">value</metadata>\n</file>')
        c.clearMetadata()

        # PFN
        c.addPFN(PFN("url","site"))
        self.assertEqual(str(c.toXML()), '<file name="name">\n\t<pfn url="url" site="site"/>\n</file>')

    def testArgumentXML(self):
        """toArgumentXML should never include inner elements"""
        c = File("name")
        self.assertEqual(str(c.toArgumentXML()), '<file name="name"/>')
        c.addProfile(Profile("ns","key","value"))
        c.addMetadata(Metadata("key","value"))
        c.addPFN(PFN("url","site"))
        self.assertEqual(str(c.toArgumentXML()), '<file name="name"/>')

    def testStdioXML(self):
        """toStdioXML should return proper xml for the supported stdio tags"""
        f = File("name")
        f.addProfile(Profile("ns","key","value"))
        f.addMetadata(Metadata("key","value"))
        f.addPFN(PFN("url","site"))
        self.assertEqual(str(f.toStdioXML("stdin")), '<stdin name="name" link="input"/>')
        self.assertEqual(str(f.toStdioXML("stdout")), '<stdout name="name" link="output"/>')
        self.assertEqual(str(f.toStdioXML("stderr")), '<stderr name="name" link="output"/>')
        self.assertRaises(FormatError, f.toStdioXML, "other")

class TestExecutable(unittest.TestCase):
    def testEqual(self):
        """Equal Executables have the same namespace,name,version,os,arch,osrelease,osversion,glibc,installed"""    
        a = Executable("grep")
        b = Executable("grep")
        c = Executable(namespace="os",name="grep")
        d = Executable(namespace="os",name="grep",version="2.3")
        e = Executable(namespace="os",name="grep",version="2.3",arch=Arch.X86)
        f = Executable(namespace="os",name="grep",version="2.3",arch=Arch.X86,os=OS.LINUX)
        g = Executable(namespace="os",name="grep",version="2.3",arch=Arch.X86,os=OS.LINUX,osrelease="foo")
        h = Executable(namespace="os",name="grep",version="2.3",arch=Arch.X86,os=OS.LINUX,osrelease="foo",osversion="bar")
        i = Executable(namespace="os",name="grep",version="2.3",arch=Arch.X86,os=OS.LINUX,osrelease="foo",osversion="bar",glibc="2.4")
        j = Executable(namespace="os",name="grep",version="2.3",arch=Arch.X86,os=OS.LINUX,osrelease="foo",osversion="bar",glibc="2.4",installed=True)

        self.assertTrue(a == b)
        self.assertFalse(b == c)
        self.assertFalse(c == d)
        self.assertFalse(b == c)
        self.assertFalse(d == e)
        self.assertFalse(e == f)
        self.assertFalse(f == g)
        self.assertFalse(g == h)
        self.assertFalse(h == i)
        self.assertFalse(i == j)
        for x in [a,b,c,d,e,f,g,h,i,j]:
            self.assertTrue(x == x)

    def testInvoke(self):
        """Transformations should support invoke"""
        c = Executable('myjob')
        p = Invoke("when","what")
        self.assertFalse(c.hasInvoke(p))
        c.addInvoke(p)
        self.assertRaises(DuplicateError, c.addInvoke, p)
        self.assertTrue(c.hasInvoke(p))
        c.removeInvoke(p)
        self.assertFalse(c.hasInvoke(p))
        self.assertRaises(NotFoundError, c.removeInvoke, p)
        c.addInvoke(p)
        c.clearInvokes()
        self.assertFalse(c.hasInvoke(p))
        c.invoke("when","what")
        self.assertTrue(c.hasInvoke(p))

    def testXML(self):
        """toXML should output proper xml"""
        x = Executable(namespace="os",name="grep",version="2.3",arch=Arch.X86,os=OS.LINUX,osrelease="foo",osversion="bar",glibc="2.4",installed=True)
        self.assertEqual(str(x.toXML()), '<executable name="grep" namespace="os" version="2.3" arch="x86" os="linux" osrelease="foo" osversion="bar" glibc="2.4" installed="true"/>')

        x.metadata("key","value")
        self.assertEqual(str(x.toXML()), '<executable name="grep" namespace="os" version="2.3" arch="x86" os="linux" osrelease="foo" osversion="bar" glibc="2.4" installed="true">\n\t<metadata key="key">value</metadata>\n</executable>')
        x.clearMetadata()

        x.invoke("when","what")
        self.assertEqual(str(x.toXML()), '<executable name="grep" namespace="os" version="2.3" arch="x86" os="linux" osrelease="foo" osversion="bar" glibc="2.4" installed="true">\n\t<invoke when="when">what</invoke>\n</executable>')
        x.clearInvokes()


class TestUse(unittest.TestCase):
    def testConstructor(self):
        """Constructor should only allow valid objects"""
        Use("name")
        Use("name", namespace="ns")
        Use("name", version="version")
        Use("name", register=True)
        Use("name", transfer=True)
        Use("name", link="link")
        Use("name", executable=True)
        Use("name", optional=True)
        self.assertRaises(FormatError, Use, None)

    def testEquals(self):
        """Equal uses have the same (namespace, name, version)"""
        a = Use("name", namespace="ns", version="version")
        b = Use("name", namespace="ns", version="version")
        c = Use("name", namespace="ns", version="version1")
        d = Use("name", namespace="ns1", version="version")
        e = Use("name1", namespace="ns", version="version")
        f = Use("name", namespace="ns", version="version", transfer=True)

        self.assertTrue(a == b)
        self.assertFalse(a == c)
        self.assertFalse(a == d)
        self.assertFalse(a == e)
        self.assertTrue(a == f)

    def testMetadata(self):
        """Should be able to add/remove/has metadata"""
        c = Use("name")
        p = Metadata("key","value")
        self.assertFalse(c.hasMetadata(p))
        c.addMetadata(p)
        self.assertRaises(DuplicateError, c.addMetadata, p)
        self.assertTrue(c.hasMetadata(p))
        c.removeMetadata(p)
        self.assertFalse(c.hasMetadata(p))
        self.assertRaises(NotFoundError, c.removeMetadata, p)
        c.addMetadata(p)
        c.clearMetadata()
        self.assertFalse(c.hasMetadata(p))

    def testJobXML(self):
        """Use.toXML should output properly formatted XML"""
        a = Use("name", namespace="ns", version="version")
        self.assertEqual(str(a.toJobXML()), '<uses namespace="ns" name="name" version="version"/>')

        a = Use("name", version="version")
        self.assertEqual(str(a.toJobXML()), '<uses name="name" version="version"/>')

        a = Use("name")
        self.assertEqual(str(a.toJobXML()), '<uses name="name"/>')

        a = Use("name", version="version", transfer=True)
        self.assertEqual(str(a.toJobXML()), '<uses name="name" version="version" transfer="true"/>')

        a = Use("name", version="version", transfer=True, register=False)
        self.assertEqual(str(a.toJobXML()), '<uses name="name" version="version" register="false" transfer="true"/>')

        a = Use("name", link="link", register="true", transfer="true", 
                optional=True, namespace="ns", version="10", executable=True)
        self.assertEqual(str(a.toJobXML()), '<uses namespace="ns" name="name" version="10" link="link" register="true" transfer="true" optional="true" executable="true"/>')

        a = Use("name", link="link", register="true", transfer="true", 
                optional=True, namespace="ns", version="10", executable=True,
                size=1024)
        self.assertEqual(str(a.toJobXML()), '<uses namespace="ns" name="name" version="10" link="link" register="true" transfer="true" optional="true" executable="true" size="1024"/>')

        a = Use("name")
        a.metadata("key","value")
        self.assertEqual(str(a.toJobXML()), '<uses name="name">\n\t<metadata key="key">value</metadata>\n</uses>')

    def testTransformationXML(self):
        """Use.toXML should output properly formatted XML"""
        a = Use("name", namespace="ns", version="version")
        self.assertEqual(str(a.toTransformationXML()), '<uses namespace="ns" name="name" version="version"/>')

        a = Use("name", version="version")
        self.assertEqual(str(a.toTransformationXML()), '<uses name="name" version="version"/>')

        a = Use("name")
        self.assertEqual(str(a.toTransformationXML()), '<uses name="name"/>')

        a = Use("name", version="version", transfer=True)
        self.assertEqual(str(a.toTransformationXML()), '<uses name="name" version="version"/>')

        a = Use("name", version="version", transfer=True, register=False)
        self.assertEqual(str(a.toTransformationXML()), '<uses name="name" version="version"/>')

        a = Use("name", link="link", register="true", transfer="true", 
                optional=True, namespace="ns", version="10", executable=True, size=1024)
        self.assertEqual(str(a.toTransformationXML()), '<uses namespace="ns" name="name" version="10" executable="true"/>')

        a = Use("name")
        a.metadata("key","value")
        self.assertEqual(str(a.toTransformationXML()), '<uses name="name">\n\t<metadata key="key">value</metadata>\n</uses>')


class TestTransformation(unittest.TestCase):
    def testConstructor(self):
        t = Transformation("name","namespace","version")
        self.assertEqual(t.name, "name")
        self.assertEqual(t.namespace, "namespace")
        self.assertEqual(t.version, "version")

    def testExecutable(self):
        e = Executable("name",namespace="ns",version="version")
        t = Transformation(e)
        self.assertEqual(t.name,e.name)
        self.assertEqual(t.namespace,e.namespace)
        self.assertEqual(t.version,e.version)

    def testUse(self):
        """Transformations should allow Use objects"""
        u = Use("name",namespace="namespace",version="version",register=True,transfer=True)
        t = Transformation("xform")
        t.addUse(u)
        self.assertRaises(DuplicateError, t.addUse, u)
        self.assertTrue(t.hasUse(u))
        t.removeUse(u)
        self.assertRaises(NotFoundError, t.removeUse, u)
        self.assertFalse(t.hasUse(u))
        t.addUse(u)
        t.clearUses()
        self.assertFalse(t.hasUse(u))
        t.uses("name",namespace="namespace",version="version",register=True,transfer=True)
        self.assertTrue(t.hasUse(u))

    def testMetadata(self):
        """Should be able to add/remove/has metadata"""
        c = Transformation("xform")
        p = Metadata("key","value")
        self.assertFalse(c.hasMetadata(p))
        c.addMetadata(p)
        self.assertRaises(DuplicateError, c.addMetadata, p)
        self.assertTrue(c.hasMetadata(p))
        c.removeMetadata(p)
        self.assertFalse(c.hasMetadata(p))
        self.assertRaises(NotFoundError, c.removeMetadata, p)
        c.addMetadata(p)
        c.clearMetadata()
        self.assertFalse(c.hasMetadata(p))

    def testInvoke(self):
        """Transformations should support invoke"""
        c = Transformation('myjob')
        p = Invoke("when","what")
        self.assertFalse(c.hasInvoke(p))
        c.addInvoke(p)
        self.assertRaises(DuplicateError, c.addInvoke, p)
        self.assertTrue(c.hasInvoke(p))
        c.removeInvoke(p)
        self.assertFalse(c.hasInvoke(p))
        self.assertRaises(NotFoundError, c.removeInvoke, p)
        c.addInvoke(p)
        c.clearInvokes()
        self.assertFalse(c.hasInvoke(p))
        c.invoke("when","what")
        self.assertTrue(c.hasInvoke(p))

    def testUsesFile(self):
        """uses should accept File as an argument"""
        c = Transformation('myjob')
        c.uses(File("filename"))
        self.assertEqual(str(c.toXML()), '<transformation name="myjob">\n\t<uses name="filename" executable="false"/>\n</transformation>')

    def testUsesExecutable(self):
        """Use should accept Executable as an argument"""
        c = Transformation('myjob')
        e = Executable(name="exe", namespace="ns", version="1.0")

        c.uses(e)
        self.assertEqual(str(c.toXML()), '<transformation name="myjob">\n\t<uses namespace="ns" name="exe" version="1.0"/>\n</transformation>')
        c.clearUses()

        c.uses(e, namespace="alt")
        self.assertEqual(str(c.toXML()), '<transformation name="myjob">\n\t<uses namespace="alt" name="exe" version="1.0"/>\n</transformation>')
        c.clearUses()

        c.uses(e, version="alt")
        self.assertEqual(str(c.toXML()), '<transformation name="myjob">\n\t<uses namespace="ns" name="exe" version="alt"/>\n</transformation>')
        c.clearUses()

        c.uses(e, register=True)
        self.assertEqual(str(c.toXML()), '<transformation name="myjob">\n\t<uses namespace="ns" name="exe" version="1.0"/>\n</transformation>')
        c.clearUses()

    def testXML(self):
        t = Transformation("name","namespace","version")
        self.assertEqual(str(t.toXML()), '<transformation namespace="namespace" name="name" version="version"/>')

        t.uses("name",namespace="ns",version="ver",executable=True)
        self.assertEqual(str(t.toXML()), '<transformation namespace="namespace" name="name" version="version">\n\t<uses namespace="ns" name="name" version="ver" executable="true"/>\n</transformation>')

        t.clearUses()

        t.uses(Executable(name="name",namespace="ns",version="ver"))
        self.assertEqual(str(t.toXML()), '<transformation namespace="namespace" name="name" version="version">\n\t<uses namespace="ns" name="name" version="ver"/>\n</transformation>')

        t.clearUses()

        t.uses(File(name="filename"),link="input", transfer=True, register=True)
        self.assertEqual(str(t.toXML()), '<transformation namespace="namespace" name="name" version="version">\n\t<uses name="filename" executable="false"/>\n</transformation>')

        t.clearUses()

        t.metadata("key","value")
        self.assertEqual(str(t.toXML()), '<transformation namespace="namespace" name="name" version="version">\n\t<metadata key="key">value</metadata>\n</transformation>')
        t.clearMetadata()

        t.invoke("when","what")
        self.assertEqual(str(t.toXML()), '<transformation namespace="namespace" name="name" version="version">\n\t<invoke when="when">what</invoke>\n</transformation>')


class TestInvoke(unittest.TestCase):
    def testConstructor(self):
        """Invoke requires valid when and what"""
        Invoke("when","what")
        self.assertRaises(FormatError, Invoke, "when", None)
        self.assertRaises(FormatError, Invoke, None, "what")
        self.assertRaises(FormatError, Invoke, "", "what")
        self.assertRaises(FormatError, Invoke, "when", "")

    def testEqual(self):
        """Invoke objects are equal when they have the same when and what"""
        a = Invoke("when","what")
        b = Invoke("when","what")
        c = Invoke("when","what1")
        d = Invoke("when1","what")
        e = Invoke("when1","what1")
        self.assertTrue(a == b)
        self.assertFalse(a == c)
        self.assertFalse(a == d)
        self.assertFalse(a == e)


class TestJob(unittest.TestCase):
    def testConstructor(self):
        """Should be able to create a job using n+ns+ver or Transformation"""
        self.assertRaises(FormatError, Job, None)
        self.assertRaises(FormatError, Job, "")
        j = Job('myjob',namespace="ns",version="2",node_label="label")
        self.assertEqual(j.name,'myjob')
        self.assertEqual(j.namespace,'ns')
        self.assertEqual(j.version,'2')
        self.assertEqual(j.node_label,'label')
        j = Job(Transformation('myxform'))
        self.assertEqual(j.name,'myxform')
        j = Job(Transformation('myxform',version="1"),version="2")
        self.assertEqual(j.version,"2")
        j = Job(Transformation('myxform',namespace="ns1"),namespace="ns2")
        self.assertEqual(j.namespace,"ns2")

    def testStd(self):
        """Should be able to set stdin/out/err using File or string"""
        j = Job('myjob')
        j.setStdout(File("stdout"))
        self.assertEqual(j.stdout, File("stdout"))
        j.setStdin(File("stdin"))
        self.assertEqual(j.stdin, File("stdin"))
        j.setStderr(File("stderr"))
        self.assertEqual(j.stderr, File("stderr"))

        j.setStdout("stdout")
        self.assertEqual(j.stdout, File("stdout"))
        j.setStdin("stdin")
        self.assertEqual(j.stdin, File("stdin"))
        j.setStderr("stderr")
        self.assertEqual(j.stderr, File("stderr"))

    def testMetadata(self):
        """Should be able to add/remove/has metadata"""
        c = Job("myjob")
        p = Metadata("key","value")
        self.assertFalse(c.hasMetadata(p))
        c.addMetadata(p)
        self.assertRaises(DuplicateError, c.addMetadata, p)
        self.assertTrue(c.hasMetadata(p))
        c.removeMetadata(p)
        self.assertFalse(c.hasMetadata(p))
        self.assertRaises(NotFoundError, c.removeMetadata, p)
        c.addMetadata(p)
        c.clearMetadata()
        self.assertFalse(c.hasMetadata(p))

    def testProfile(self):
        """Jobs should support profiles"""
        c = Job('myjob')
        p = Profile("ns","name","value")
        self.assertFalse(c.hasProfile(p))
        c.addProfile(p)
        self.assertRaises(DuplicateError, c.addProfile, p)
        self.assertTrue(c.hasProfile(p))
        c.removeProfile(p)
        self.assertFalse(c.hasProfile(p))
        self.assertRaises(NotFoundError, c.removeProfile, p)
        c.addProfile(p)
        c.clearProfiles()
        self.assertFalse(c.hasProfile(p))

    def testUse(self):
        """Jobs should allow Use objects"""
        u = Use("name",namespace="namespace",version="version",register=True,transfer=True)
        t = Job("xform")
        t.addUse(u)
        self.assertRaises(DuplicateError, t.addUse, u)
        self.assertTrue(t.hasUse(u))
        t.removeUse(u)
        self.assertRaises(NotFoundError, t.removeUse, u)
        self.assertFalse(t.hasUse(u))
        t.addUse(u)
        t.clearUses()
        self.assertFalse(t.hasUse(u))
        t.uses("name",namespace="namespace",version="version",register=True,transfer=True)
        self.assertTrue(t.hasUse(u))

    def testArguments(self):
        j = Job('myjob')

        # Regular arguments
        j.addArguments('a','b','c')
        j.addArguments('d',u'e')
        self.assertEqual(j.getArguments(), 'a b c d e')
        j.clearArguments()

        # File arguments
        f = File("name")
        g = File("name2")
        j.addArguments('a',f,'b',g)
        self.assertEqual(j.getArguments(), 'a <file name="name"/> b <file name="name2"/>')
        j.clearArguments()

        # Quoted strings
        j.addArguments('a','"gideon is cool"','b',"'apple bananna'")
        self.assertEqual(j.getArguments(), 'a "gideon is cool" b \'apple bananna\'')
        j.clearArguments()

        # Non-string arguments
        e = Executable("exe")
        self.assertRaises(FormatError, j.addArguments, e)
        self.assertRaises(FormatError, j.addArguments, 1)
        self.assertRaises(FormatError, j.addArguments, 1.0)

    def testInvoke(self):
        """Jobs should support invoke"""
        c = Job('myjob')
        p = Invoke("when","what")
        self.assertFalse(c.hasInvoke(p))
        c.addInvoke(p)
        self.assertRaises(DuplicateError, c.addInvoke, p)
        self.assertTrue(c.hasInvoke(p))
        c.removeInvoke(p)
        self.assertFalse(c.hasInvoke(p))
        self.assertRaises(NotFoundError, c.removeInvoke, p)
        c.addInvoke(p)
        c.clearInvokes()
        self.assertFalse(c.hasInvoke(p))
        c.invoke("when","what")
        self.assertTrue(c.hasInvoke(p))

    def testUsesFile(self):
        """uses should accept File as an argument"""
        c = Job('myjob')
        c.uses(File("filename"))
        self.assertEqual(str(c.toXML()), '<job name="myjob">\n\t<uses name="filename"/>\n</job>')

    def testUsesExecutable(self):
        """Use should accept Executable as an argument"""
        c = Job('myjob')
        e = Executable(name="exe", namespace="ns", version="1.0")

        c.uses(e)
        self.assertEqual(str(c.toXML()), '<job name="myjob">\n\t<uses namespace="ns" name="exe" version="1.0" executable="true"/>\n</job>')
        c.clearUses()

        c.uses(e, namespace="alt")
        self.assertEqual(str(c.toXML()), '<job name="myjob">\n\t<uses namespace="alt" name="exe" version="1.0" executable="true"/>\n</job>')
        c.clearUses()

        c.uses(e, version="alt")
        self.assertEqual(str(c.toXML()), '<job name="myjob">\n\t<uses namespace="ns" name="exe" version="alt" executable="true"/>\n</job>')
        c.clearUses()

        c.uses(e, register=True)
        self.assertEqual(str(c.toXML()), '<job name="myjob">\n\t<uses namespace="ns" name="exe" version="1.0" register="true" executable="true"/>\n</job>')
        c.clearUses()

    def testXML(self):
        # Job element
        j = Job(name="name")
        self.assertEqual(str(j.toXML()), '<job name="name"/>')
        j = Job(name="name", id="id")
        self.assertEqual(str(j.toXML()), '<job id="id" name="name"/>')
        j = Job(name="name", id="id", namespace="ns")
        self.assertEqual(str(j.toXML()), '<job id="id" namespace="ns" name="name"/>')
        j = Job(name="name", id="id", namespace="ns", version="version")
        self.assertEqual(str(j.toXML()), '<job id="id" namespace="ns" name="name" version="version"/>')
        j = Job(name="name", id="id", namespace="ns", version="version", node_label="label")
        self.assertEqual(str(j.toXML()), '<job id="id" namespace="ns" name="name" version="version" node-label="label"/>')

        # Arguments
        j = Job(name="name")
        j.addArguments('a')
        self.assertEqual(str(j.toXML()), '<job name="name">\n\t<argument>a</argument>\n</job>')
        j.clearArguments()

        # File arguments
        j.addArguments(File("file"))
        self.assertEqual(str(j.toXML()), '<job name="name">\n\t<argument><file name="file"/></argument>\n</job>')
        j.clearArguments()

        # Profiles
        j.addProfile(Profile("namespace","key","value"))
        self.assertEqual(str(j.toXML()), '<job name="name">\n\t<profile namespace="namespace" key="key">value</profile>\n</job>')
        j.clearProfiles()

        # Metadata
        j.metadata("key","value")
        self.assertEqual(str(j.toXML()), '<job name="name">\n\t<metadata key="key">value</metadata>\n</job>')
        j.clearMetadata()

        # Stdin/out/err
        j.setStdin(File("stdin"))
        self.assertEqual(str(j.toXML()), '<job name="name">\n\t<stdin name="stdin" link="input"/>\n</job>')
        j.clearStdin()
        j.setStdout(File("stdout"))
        self.assertEqual(str(j.toXML()), '<job name="name">\n\t<stdout name="stdout" link="output"/>\n</job>')
        j.clearStdout()
        j.setStderr(File("stderr"))
        self.assertEqual(str(j.toXML()), '<job name="name">\n\t<stderr name="stderr" link="output"/>\n</job>')
        j.clearStderr()

        # Uses
        j.uses("name")
        self.assertEqual(str(j.toXML()), '<job name="name">\n\t<uses name="name"/>\n</job>')
        j.clearUses()

        # Invocations
        j.invoke("when","what")
        self.assertEqual(str(j.toXML()), '<job name="name">\n\t<invoke when="when">what</invoke>\n</job>')
        j.clearInvokes()

        # Combined
        j = Job(name="name", id="id", namespace="ns", version="version", node_label="label")
        j.addArguments('-a',File("file"))
        j.addProfile(Profile("namespace","key","value"))
        j.setStdin(File("stdin"))
        j.setStdout(File("stdout"))
        j.setStderr(File("stderr"))
        j.uses("name", link="input", transfer=True, register=True)
        j.invoke("when","what")
        self.assertEqual(str(j.toXML()), '''<job id="id" namespace="ns" name="name" version="version" node-label="label">
\t<argument>-a <file name="file"/></argument>
\t<profile namespace="namespace" key="key">value</profile>
\t<stdin name="stdin" link="input"/>
\t<stdout name="stdout" link="output"/>
\t<stderr name="stderr" link="output"/>
\t<uses name="name" link="input" register="true" transfer="true"/>
\t<invoke when="when">what</invoke>
</job>''')

class TestDAG(unittest.TestCase):
    def testConstructor(self):
        DAG("file")
        DAG(File("file"))
        DAG("file",id="10")
        DAG("file",id="10",node_label="dag")
        self.assertRaises(FormatError, DAG, None)
        self.assertRaises(FormatError, DAG, "")

    def testXML(self):
        d = DAG("file")
        self.assertEqual(str(d.toXML()), '<dag file="file"/>')
        d = DAG(File("file"))
        self.assertEqual(str(d.toXML()), '<dag file="file"/>')
        d = DAG("file",id="10")
        self.assertEqual(str(d.toXML()), '<dag id="10" file="file"/>')
        d = DAG("file",node_label="label")
        self.assertEqual(str(d.toXML()), '<dag file="file" node-label="label"/>')

class TestDAX(unittest.TestCase):
    def testConstructor(self):
        DAX("file")
        DAX(File("file"))
        DAX("file",id="10")
        DAX("file",id="10",node_label="dag")
        self.assertRaises(FormatError, DAX, None)
        self.assertRaises(FormatError, DAX, "")

    def testXML(self):
        d = DAX("file")
        self.assertEqual(str(d.toXML()), '<dax file="file"/>')
        d = DAX(File("file"))
        self.assertEqual(str(d.toXML()), '<dax file="file"/>')
        d = DAX("file",id="10")
        self.assertEqual(str(d.toXML()), '<dax id="10" file="file"/>')
        d = DAX("file",node_label="label")
        self.assertEqual(str(d.toXML()), '<dax file="file" node-label="label"/>')

class TestDependency(unittest.TestCase):
    def testConstructor(self):
        """Constuctor should only allow valid dependencies"""
        # IDs are allowed
        Dependency("a","b")

        # Id must be valid
        self.assertRaises(FormatError, Dependency, "a", None)
        self.assertRaises(FormatError, Dependency, None, "b")
        self.assertRaises(FormatError, Dependency, "a", "")
        self.assertRaises(FormatError, Dependency, "", "b")

        # Jobs, DAGs and DAXes are allowed
        a = Job("a",id="ID01")
        b = Job("b",id="ID02")
        Dependency(a,b)

        a = DAG("a",id="ID01")
        b = DAG("b",id="ID02")
        Dependency(a,b)

        a = DAX("a",id="ID01")
        b = DAX("b",id="ID02")
        Dependency(a,b)

        # Job objects must have IDs
        a = Job("a")
        self.assertRaises(FormatError, Dependency, a, "ID01")
        self.assertRaises(FormatError, Dependency, "ID01", a)

        # No self-edges
        a = Job("a", id="ID01")
        self.assertRaises(FormatError, Dependency, a, a)

    def testEquals(self):
        """Equal dependencies have the same parent and child (but not edge label)"""
        a = Dependency("a","b")
        b = Dependency("a","b")
        c = Dependency("a","c")
        d = Dependency("c","b")
        self.assertTrue(a==b)
        self.assertFalse(a==c)
        self.assertFalse(a==d)

class TestADAG(unittest.TestCase):
    def testConstructor(self):
        """Constructor should only allow valid ADAG objects"""
        self.assertRaises(FormatError, ADAG, None)
        self.assertRaises(FormatError, ADAG, "")
        a = ADAG("name",10,1)
        self.assertEqual(a.name,"name")
        self.assertEqual(a.index,1)
        self.assertEqual(a.count,10)

    def testNextJobID(self):
        """nextJobID() should always return a valid job ID"""
        a = ADAG("foo")
        self.assertEqual(a.nextJobID(),"ID0000001")
        self.assertEqual(a.nextJobID(),"ID0000002")
        self.assertEqual(a.nextJobID(),"ID0000003")
        a.addJob(Job("a",id="ID0000004"))
        self.assertEqual(a.nextJobID(),"ID0000005")
        a.addJob(Job("a",id="ID0000006"))
        a.addJob(Job("a",id="ID0000007"))
        a.addJob(Job("a",id="ID0000008"))
        self.assertEqual(a.nextJobID(),"ID0000009")

    def testJobs(self):
        """Should be able to add/remove/test for jobs/dags/daxes"""
        a = ADAG("adag")
        j = Job("job")
        self.assertTrue(j.id is None)
        a.addJob(j)
        self.assertTrue(j.id is not None)
        self.assertTrue(a.hasJob(j))
        self.assertTrue(a.hasJob(j.id))
        a.removeJob(j)
        self.assertFalse(a.hasJob(j))
        self.assertFalse(a.hasJob(j.id))
        a.addJob(j)
        self.assertTrue(a.hasJob(j))
        a.removeJob(j.id)
        self.assertFalse(a.hasJob(j))
        a.addJob(j)
        a.clearJobs()
        self.assertFalse(a.hasJob(j))
        dax = DAX("dax")
        dag = DAG("dag")
        a.addJob(dax)
        a.addJob(dag)
        a.clearJobs()
        self.assertRaises(FormatError, a.addDAX, j)
        self.assertRaises(FormatError, a.addDAG, j)
        a.addDAX(dax)
        a.addDAG(dag)
        a.clearJobs()
        a.addJob(j)
        self.assertEqual(a.getJob(j.id), j)
        self.assertRaises(DuplicateError, a.addJob, j)
        a.clearJobs()
        self.assertRaises(NotFoundError, a.getJob, j)
        self.assertRaises(NotFoundError, a.removeJob, j)

    def testMetadata(self):
        """Should be able to add/remove/has metadata"""
        c = ADAG("name")
        p = Metadata("key","value")
        self.assertFalse(c.hasMetadata(p))
        c.addMetadata(p)
        self.assertRaises(DuplicateError, c.addMetadata, p)
        self.assertTrue(c.hasMetadata(p))
        c.removeMetadata(p)
        self.assertFalse(c.hasMetadata(p))
        self.assertRaises(NotFoundError, c.removeMetadata, p)
        c.addMetadata(p)
        c.clearMetadata()
        self.assertFalse(c.hasMetadata(p))

    def testFiles(self):
        """Should be able to add/remove/test files in ADAG"""
        a = ADAG("adag")
        f = File("file")
        self.assertFalse(a.hasFile(f))
        a.addFile(f)
        self.assertTrue(a.hasFile(f))
        a.removeFile(f)
        self.assertFalse(a.hasFile(f))
        a.addFile(f)
        self.assertTrue(a.hasFile(f))
        a.clearFiles()
        self.assertFalse(a.hasFile(f))
        a.addFile(f)
        self.assertRaises(DuplicateError, a.addFile, f)
        a.clearFiles()
        self.assertRaises(NotFoundError, a.removeFile, f)

    def testExecutables(self):
        """Should be able to add/remove/test executables in ADAG"""
        a = ADAG("adag")
        e = Executable("exe")
        self.assertFalse(a.hasExecutable(e))
        a.addExecutable(e)
        self.assertTrue(a.hasExecutable(e))
        a.removeExecutable(e)
        self.assertFalse(a.hasExecutable(e))
        a.addExecutable(e)
        self.assertTrue(a.hasExecutable(e))
        a.clearExecutables()
        self.assertFalse(a.hasExecutable(e))
        a.addExecutable(e)
        self.assertRaises(DuplicateError, a.addExecutable, e)
        a.clearExecutables()
        self.assertRaises(NotFoundError, a.removeExecutable, e)

    def testTransformations(self):
        """Should be able to add/remove/clear/test transformations in ADAG"""
        a = ADAG("adag")
        t = Transformation("xform")
        self.assertFalse(a.hasTransformation(t))
        a.addTransformation(t)
        self.assertTrue(a.hasTransformation(t))
        a.removeTransformation(t)
        self.assertFalse(a.hasTransformation(t))
        a.addTransformation(t)
        self.assertTrue(a.hasTransformation(t))
        a.clearTransformations()
        self.assertFalse(a.hasTransformation(t))
        a.addTransformation(t)
        self.assertRaises(DuplicateError, a.addTransformation, t)
        a.clearTransformations()
        self.assertRaises(NotFoundError, a.removeTransformation, t)

    def testDependencies(self):
        """Should be able to add/remove/clear/test dependencies in ADAG"""
        a = ADAG("adag")
        x = Job("x", id="ID01")
        y = Job("y", id="ID02")
        t = Dependency(x, y)
        self.assertRaises(NotFoundError, a.addDependency, t)
        a.addJob(x)
        self.assertRaises(NotFoundError, a.addDependency, t)
        a.addJob(y)
        a.addDependency(t)
        self.assertRaises(DuplicateError, a.addDependency, t)
        self.assertTrue(a.hasDependency(t))
        a.removeDependency(t)
        self.assertFalse(a.hasDependency(t))
        a.depends(parent=x, child=y)
        self.assertTrue(a.hasDependency(t))
        a.clearDependencies()
        self.assertFalse(a.hasDependency(t))
        self.assertRaises(NotFoundError, a.removeDependency, t)

    def testInvoke(self):
        """ADAGs should support invoke"""
        c = ADAG('adag')
        p = Invoke("when","what")
        self.assertFalse(c.hasInvoke(p))
        c.addInvoke(p)
        self.assertRaises(DuplicateError, c.addInvoke, p)
        self.assertTrue(c.hasInvoke(p))
        c.removeInvoke(p)
        self.assertFalse(c.hasInvoke(p))
        self.assertRaises(NotFoundError, c.removeInvoke, p)
        c.addInvoke(p)
        c.clearInvokes()
        self.assertFalse(c.hasInvoke(p))
        c.invoke("when","what")
        self.assertTrue(c.hasInvoke(p))

    def testXML(self):
        """ADAGs should output properly-formatted XML"""
        c = ADAG('adag',count=10,index=1)

        self.assertEqualXML(c.toXML(),"""<adag xmlns="http://pegasus.isi.edu/schema/DAX" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://pegasus.isi.edu/schema/DAX http://pegasus.isi.edu/schema/dax-3.6.xsd" version="3.6" name="adag" count="10" index="1">
</adag>""")

        # Metadata
        c.metadata("key","value")
        self.assertEqualXML(c.toXML(),"""<adag xmlns="http://pegasus.isi.edu/schema/DAX" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://pegasus.isi.edu/schema/DAX http://pegasus.isi.edu/schema/dax-3.6.xsd" version="3.6" name="adag" count="10" index="1">
<metadata key="key">value</metadata>
</adag>""")
        c.clearMetadata()

        # Invoke
        c.invoke("when","what")
        self.assertEqualXML(c.toXML(),"""<adag xmlns="http://pegasus.isi.edu/schema/DAX" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://pegasus.isi.edu/schema/DAX http://pegasus.isi.edu/schema/dax-3.6.xsd" version="3.6" name="adag" count="10" index="1">
<invoke when="when">what</invoke>
</adag>""")
        c.clearInvokes()

        # File
        c.addFile(File("file"))
        self.assertEqualXML(c.toXML(),"""<adag xmlns="http://pegasus.isi.edu/schema/DAX" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://pegasus.isi.edu/schema/DAX http://pegasus.isi.edu/schema/dax-3.6.xsd" version="3.6" name="adag" count="10" index="1">
<file name="file"/>
</adag>""")
        c.clearFiles()

        # Executable
        c.addExecutable(Executable("exe"))
        self.assertEqualXML(c.toXML(),"""<adag xmlns="http://pegasus.isi.edu/schema/DAX" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://pegasus.isi.edu/schema/DAX http://pegasus.isi.edu/schema/dax-3.6.xsd" version="3.6" name="adag" count="10" index="1">
<executable name="exe"/>
</adag>""")
        c.clearExecutables()

        # Transformation
        c.addTransformation(Transformation("xform"))
        self.assertEqualXML(c.toXML(),"""<adag xmlns="http://pegasus.isi.edu/schema/DAX" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://pegasus.isi.edu/schema/DAX http://pegasus.isi.edu/schema/dax-3.6.xsd" version="3.6" name="adag" count="10" index="1">
<transformation name="xform"/>
</adag>""")
        c.clearTransformations()

        # Job
        c.addJob(Job("xform",id="ID01"))
        self.assertEqualXML(c.toXML(),"""<adag xmlns="http://pegasus.isi.edu/schema/DAX" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://pegasus.isi.edu/schema/DAX http://pegasus.isi.edu/schema/dax-3.6.xsd" version="3.6" name="adag" count="10" index="1">
<job id="ID01" name="xform"/>
</adag>""")
        c.clearJobs()

        # Dependency
        c.addJob(Job("xform",id="ID01"))
        c.addJob(Job("xform",id="ID02"))
        c.depends("ID02","ID01")
        self.assertEqualXML(c.toXML(),"""<adag xmlns="http://pegasus.isi.edu/schema/DAX" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://pegasus.isi.edu/schema/DAX http://pegasus.isi.edu/schema/dax-3.6.xsd" version="3.6" name="adag" count="10" index="1">
<job id="ID01" name="xform"/>
<job id="ID02" name="xform"/>
<child ref="ID02">
<parent ref="ID01"/>
</child>
</adag>""")

        # All
        c.invoke("when","what")
        c.addFile(File("file"))
        c.addExecutable(Executable("exe"))
        c.addTransformation(Transformation("xform"))
        self.assertEqualXML(c.toXML(),"""<adag xmlns="http://pegasus.isi.edu/schema/DAX" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://pegasus.isi.edu/schema/DAX http://pegasus.isi.edu/schema/dax-3.6.xsd" version="3.6" name="adag" count="10" index="1">
<invoke when="when">what</invoke>
<file name="file"/>
<executable name="exe"/>
<transformation name="xform"/>
<job id="ID01" name="xform"/>
<job id="ID02" name="xform"/>
<child ref="ID02">
<parent ref="ID01"/>
</child>
</adag>""")

    def testWriteFile(self):
        diamond = ADAG("diamond")
        diamond.addJob(Job(u"\u03a3cat"))
        diamond.writeXMLFile("/dev/null")

    def testDiamond(self):
        """Compare generated DAX to reference DAX"""

        # Create a DAX
        diamond = ADAG("diamond")

        # Add input file to the DAX-level replica catalog
        a = File("f.a")
        a.addPFN(PFN("gsiftp://site.com/inputs/f.a","site"))
        diamond.addFile(a)

        # Add a config file for the transformations
        cfg = File("diamond.cfg")
        diamond.addFile(cfg)

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
        t_preprocess.uses(cfg)
        diamond.addTransformation(t_preprocess)

        t_findrange = Transformation(e_findrange)
        t_findrange.uses(cfg)
        diamond.addTransformation(t_findrange)

        t_analyze = Transformation(e_analyze)
        t_analyze.uses(cfg)
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
        left = diamond.toXML()

        # Get reference diamond dax
        right = open(DIAMOND_DAX).read()

        # For this test we sort because we don't really care about minor
        # ordering differences caused by the use of sets
        self.assertEqualXML(left, right, True)

    def simplifyXML(self, a):
        """Split XML into lines and remove comments, whitespace, and preprocessing tags"""
        a = [x.strip() for x in a.split('\n')]
        a = [x for x in a if x and not x.startswith("<!--") and not x.startswith("<?xml")]
        return a

    def assertEqualXML(self, left, right, sort=False):
        """Assert that two xml documents are the same (more or less)"""
        left = self.simplifyXML(left)
        right = self.simplifyXML(right)
        self.assertEqual(len(left),len(right),"XML document length differs")
        if sort:
            left.sort()
            right.sort()
        for l,r in zip(left,right):
            self.assertEqual(l,r,"XML differs:\n%s\n%s" % (l,r))


class TestParse(unittest.TestCase):
    """This doesn't really do a thorough job of testing the parser"""

    def testParse(self):
        """Should be able to parse a file using parse()"""
        adag = parse(DAX3TEST_DAX)

    def testParseString(self):
        """Should be able to parse a string using parseString()"""
        txt = open(DAX3TEST_DAX).read()
        adag = parseString(txt)

# Disabled so that it won't keep breaking the nightly builds
#class TestScale(unittest.TestCase):
#    TESTFILE = "/tmp/test_pegasus_dax3.xml"
#    
#    def testLargeWorkflow(self):
#        """It shouldn't take >10s to build or parse a 20k job workflow"""
#        import time
#        from io import StringIO
#        start = time.time()
#        a = ADAG("bigun")
#        x = Job("xform")
#        a.addJob(x)
#        for i in range(1,20000):
#            j = Job("xform")
#            a.addJob(j)
#            a.depends(j,x)
#            x = j
#        f = open(self.TESTFILE, "w")
#        a.writeXML(f)
#        f.close()
#        end = time.time()
#        elapsed = end - start
#        self.assertTrue(elapsed < 10)
#        
#        a = None
#        
#        # Parse
#        start = time.time()
#        a = parse(self.TESTFILE)
#        end = time.time()
#        elapsed = end - start
#        self.assertTrue(elapsed < 10)
#    
#    def tearDown(self):
#        os.remove(self.TESTFILE)

if __name__ == "__main__":
    unittest.main()

