import unittest
import time
import os

from Pegasus.tools import utils

class TestQuoting(unittest.TestCase):
    def testQuote(self):
        "Quoting should escape non-printing characters"
        self.assertEquals("hello%0D%0A%09", utils.quote("hello\r\n\t"))
    
    def testUnquote(self):
        "Unquoting should convert escape sequences back"
        self.assertEquals("hello\r\n\t", utils.unquote("hello%0D%0A%09"))

class TestISODate(unittest.TestCase):
    def setUp(self):
        self.now = 1334714132
    
    def testLocal(self):
        "Long local timestamp"
        self.assertEquals("2012-04-17T18:55:32-0700", utils.isodate(now=self.now))
    
    def testShortLocal(self):
        "Short local timestamp"
        self.assertEquals("20120417T185532-0700", utils.isodate(now=self.now, short=True))
    
    def testUTC(self):
        "Long UTC timestamp"
        self.assertEquals("2012-04-18T01:55:32Z", utils.isodate(now=self.now, utc=True))
    
    def testShortUTC(self):
        "Short UTC timestamp"
        self.assertEquals("20120418T015532Z", utils.isodate(now=self.now, utc=True, short=True))

class TestEpochDate(unittest.TestCase):
    def setUp(self):
        self.now = 1334714132
    
    def testLocal(self):
        "Should be able to get the epoch from a local isodate"
        self.assertEquals(self.now, utils.epochdate(utils.isodate(now=self.now)))
    
    def testShortLocal(self):
        "Should be able to get the epoch from a short local isodate"
        self.assertEquals(self.now, utils.epochdate(utils.isodate(now=self.now, short=True)))
    
    def testUTC(self):
        "Should be able to get the epoch from a UTC isodate"
        self.assertEquals(self.now, utils.epochdate(utils.isodate(now=self.now, utc=True)))
    
    def testShortUTC(self):
        "Should eb able to get the epoch from a short UTC isodate"
        self.assertEquals(self.now, utils.epochdate(utils.isodate(now=self.now, utc=True, short=True)))

class TestFindExec(unittest.TestCase):
    def setUp(self):
        self.test_dir = os.path.abspath(os.path.dirname(__file__))
        self.cwd = os.getcwd()
        os.chdir(self.test_dir)
    
    def tearDown(self):
        os.chdir(self.cwd)
    
    def testSimple(self):
        "Should always be able to find ls given default path"
        self.assertTrue(utils.find_exec('ls') is not None)
    
    def testWorkingDir(self):
        "Should find executables in the current directory"
        self.assertTrue(utils.find_exec('test.sh', True) is not None)
    
    def testNotFound(self):
        "Should not find non-existent executables"
        self.assertTrue(utils.find_exec("doesntexistorshouldnt") is None)
    
    def testAlternates(self):
        "Should find executables on specific paths"
        self.assertTrue(utils.find_exec(program="test.sh", otherdirs=["/doesntexist", self.test_dir]) is not None)

if __name__ == '__main__':
    unittest.main()
