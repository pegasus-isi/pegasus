import os
import unittest
import tempfile
import zipfile
import shutil

from Pegasus.service.ensembles.bundle import *

class TestBundle(unittest.TestCase):
    def setUp(self):
        self.fd, self.filename = tempfile.mkstemp()
        self.zipfile = zipfile.ZipFile(self.filename, 'w')

    def tearDown(self):
        self.zipfile.close()
        os.remove(self.filename)

    def test_invalid(self):
        self.assertRaises(BundleException, Bundle, "/nosuchfile")

    def test_notzip(self):
        self.assertRaises(BundleException, Bundle, __file__)

    def test_noproperties(self):
        self.zipfile.close()
        self.assertRaises(BundleException, Bundle, self.filename)

    def test_contains(self):
        self.zipfile.writestr(PROPERTIES_NAME, "foo=bar")
        self.zipfile.close()
        bundle = Bundle(self.filename)
        self.assertTrue(bundle.contains(PROPERTIES_NAME))
        self.assertFalse(bundle.contains("someother"))

    def test_invalid_entry(self):
        self.zipfile.writestr(PROPERTIES_NAME, "foo=bar")
        self.zipfile.writestr("/foo", "absolute path should not be allowed")
        self.zipfile.close()
        self.assertRaises(BundleException, Bundle, self.filename)

    def test_invalid_entry2(self):
        self.zipfile.writestr(PROPERTIES_NAME, "foo=bar")
        self.zipfile.writestr("../foo", "relative path should not be allowed")
        self.zipfile.close()
        self.assertRaises(BundleException, Bundle, self.filename)

    def test_properties(self):
        self.zipfile.writestr(PROPERTIES_NAME, "#comment\n\nfoo=bar\nbaz=boo")
        self.zipfile.close()
        bundle = Bundle(self.filename)
        p = bundle.get_properties()
        self.assertEqual(p["foo"], "bar")
        self.assertEqual(p["baz"], "boo")

    def test_verify(self):
        self.zipfile.writestr(PROPERTIES_NAME, "pegasus.dax.file=%s" % __file__)
        self.zipfile.close()
        bundle = Bundle(self.filename)
        bundle.verify()

    def test_no_dax(self):
        self.zipfile.writestr(PROPERTIES_NAME, "foo=bar")
        self.zipfile.close()
        b = Bundle(self.filename)
        self.assertRaises(BundleException, b.verify)

    def test_bad_dax(self):
        self.zipfile.writestr(PROPERTIES_NAME, "pegasus.dax.file=/doesnotexist")
        self.zipfile.close()
        b = Bundle(self.filename)
        self.assertRaises(BundleException, b.verify)

    def test_bad_rc(self):
        self.zipfile.writestr(PROPERTIES_NAME, "pegasus.dax.file=%s\npegasus.catalog.replica.file=/doesnotexits" % __file__)
        self.zipfile.close()
        bundle = Bundle(self.filename)
        self.assertRaises(BundleException, bundle.verify)

    def test_bad_tc(self):
        self.zipfile.writestr(PROPERTIES_NAME, "pegasus.dax.file=%s\npegasus.catalog.transformation.file=/doesnotexits" % __file__)
        self.zipfile.close()
        bundle = Bundle(self.filename)
        self.assertRaises(BundleException, bundle.verify)

    def test_bad_sc(self):
        self.zipfile.writestr(PROPERTIES_NAME, "pegasus.dax.file=%s\npegasus.catalog.site.file=/doesnotexits" % __file__)
        self.zipfile.close()
        bundle = Bundle(self.filename)
        self.assertRaises(BundleException, bundle.verify)

    def test_unpack(self):
        self.zipfile.writestr(PROPERTIES_NAME, "hello=world")
        self.zipfile.close()
        bundle = Bundle(self.filename)
        dirname = tempfile.mkdtemp()
        filename =os.path.join(dirname, PROPERTIES_NAME)
        try:
            bundle.unpack(dirname)
            self.assertTrue(os.path.isfile(filename))
            self.assertEqual(open(filename).read(), "hello=world")
        finally:
            shutil.rmtree(dirname)

