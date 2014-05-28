# coding=utf-8
import unittest
import time
import os

from Pegasus.tools import utils

class TestQuoting(unittest.TestCase):
    def testQuote(self):
        "Quoting should replace non-printing characters with XML character entity references"
        self.assertEquals(utils.quote("hello\r\n\t"), "hello&#13;&#10;&#9;")

        for i in range(0, 0x20):
            self.assertEquals(utils.quote(unichr(i)), "&#%d;" % i)

        for i in range(0x20, 0x7F):
            if not unichr(i) in u"'\"&":
                self.assertEquals(utils.quote(unichr(i)), unichr(i))

        for i in range(0x7F, 0xA0):
            self.assertEquals(utils.quote(unichr(i)), u"&#%d;" % i)

        for i in range(0xA0, 0xFF):
            self.assertEquals(utils.quote(unichr(i)), unichr(i))

        self.assertEquals(utils.quote("&"), "&amp;")
        self.assertEquals(utils.quote("'"), "&apos;")
        self.assertEquals(utils.quote('"'), "&quot;")

        self.assertEquals(utils.quote(u"\u0170\u0171\u2200"), u"\u0170\u0171\u2200")
        self.assertEquals(utils.quote("Hello\nWorld!\n"), "Hello&#10;World!&#10;")
        self.assertEquals(utils.quote("Zoë"), "Zoë")
        self.assertEquals(utils.quote("warning: unused variable ‘Narr’"), "warning: unused variable ‘Narr’")

    def testUnquote(self):
        "Unquoting should convert character entity references back to their Unicode equivalents"
        self.assertEquals(utils.unquote("hello&#13;&#10;&#9;"), "hello\r\n\t")

        for i in range(0, 0x20):
            self.assertEquals(utils.unquote("&#%d;" % i), unichr(i))

        for i in range(0x20, 0x7F):
            if not unichr(i) in u"'\"&":
                self.assertEquals(utils.unquote(unichr(i)), unichr(i))

        for i in range(0x7F, 0xA0):
            self.assertEquals(utils.unquote(u"&#%d;" % i), unichr(i))

        for i in range(0xA0, 0xFF):
            self.assertEquals(utils.unquote(unichr(i)), unichr(i))

        self.assertEquals(utils.unquote("&amp;"), "&")
        self.assertEquals(utils.unquote("&apos;"), "'")
        self.assertEquals(utils.unquote("&quot;"), '"')

        self.assertEquals(utils.unquote(u"\u0170\u0171\u2200"), u"\u0170\u0171\u2200")
        self.assertEquals(utils.unquote("Hello&#10;World!&#10;"), "Hello\nWorld!\n")
        self.assertEquals(utils.unquote("Zoë"), "Zoë")
        self.assertEquals(utils.unquote("warning: unused variable ‘Narr’"), "warning: unused variable ‘Narr’")

        # Check that hex and decimal references work
        self.assertEquals(utils.unquote("&#x41;"), "A")
        self.assertEquals(utils.unquote("&#x0041;"), "A")
        self.assertEquals(utils.unquote("&#65;"), "A")
        self.assertEquals(utils.unquote("&#0065;"), "A")

    def testUnquoteFailures(self):
        "Unquoting bad strings should fail"
        # unterminated
        self.assertRaises(utils.CharRefException, utils.unquote, "&")
        self.assertRaises(utils.CharRefException, utils.unquote, "&foo")
        # empty
        self.assertRaises(utils.CharRefException, utils.unquote, "&;")
        # Unsupported
        self.assertRaises(utils.CharRefException, utils.unquote, "&foo;")
        self.assertRaises(utils.CharRefException, utils.unquote, "&nbsp;")
        # Not string
        self.assertRaises(TypeError, utils.unquote, 1)
        self.assertRaises(TypeError, utils.quote, 1)

    def testOldUnquote(self):
        "Unquoting old strings (those that end with %0A), should work"
        self.assertEquals(utils.unquote("hello%0D%09%0A"), "hello\r\t\n")

    def testStringTypes(self):
        "Quote and unquote should return strings of the same type as what was passed"
        u = u"Hello"
        self.assertTrue(isinstance(utils.quote(u), unicode))
        self.assertTrue(isinstance(utils.unquote(u), unicode))

        b = "Hello"
        self.assertTrue(isinstance(utils.quote(b), str))
        self.assertTrue(isinstance(utils.unquote(b), str))

    def testQuoteInvalidChars(self):
        "Invalid UTF-8 byte strings should not cause quote to fail"
        utils.quote("\x80")  # Invalid 1 Octet Sequence
        utils.quote("\xc3\x28")  # Invalid 2 Octet Sequence
        utils.quote("\xa0\xa1")  # Invalid Sequence Identifier
        utils.quote("\xe2\x82\xa1")  # Valid 3 Octet Sequence
        utils.quote("\xe2\x28\xa1")  # Invalid 3 Octet Sequence (in 2nd Octet)
        utils.quote("\xe2\x82\x28")  # Invalid 3 Octet Sequence (in 3rd Octet)
        utils.quote("\xf0\x90\x8c\xbc")  # Valid 4 Octet Sequence
        utils.quote("\xf0\x28\x8c\xbc")  # Invalid 4 Octet Sequence (in 2nd Octet)
        utils.quote("\xf0\x90\x28\xbc")  # Invalid 4 Octet Sequence (in 3rd Octet)
        utils.quote("\xf0\x28\x8c\x28")  # Invalid 4 Octet Sequence (in 4th Octet)
        utils.quote("\xf8\xa1\xa1\xa1\xa1")  # Valid 5 Octet Sequence (but not Unicode!)
        utils.quote("\xfc\xa1\xa1\xa1\xa1\xa1")  # Valid 6 Octet Sequence (but not Unicode!)

    def testUnquoteInvalidChars(self):
        "Invalid UTF-8 byte strings should not cause unquote to fail"
        utils.unquote("&amp;\x80")  # Invalid 1 Octet Sequence
        utils.unquote("&amp;\xc3\x28")  # Invalid 2 Octet Sequence
        utils.unquote("&amp;\xa0\xa1")  # Invalid Sequence Identifier
        utils.unquote("&amp;\xe2\x82\xa1")  # Valid 3 Octet Sequence
        utils.unquote("&amp;\xe2\x28\xa1")  # Invalid 3 Octet Sequence (in 2nd Octet)
        utils.unquote("&amp;\xe2\x82\x28")  # Invalid 3 Octet Sequence (in 3rd Octet)
        utils.unquote("&amp;\xf0\x90\x8c\xbc")  # Valid 4 Octet Sequence
        utils.unquote("&amp;\xf0\x28\x8c\xbc")  # Invalid 4 Octet Sequence (in 2nd Octet)
        utils.unquote("&amp;\xf0\x90\x28\xbc")  # Invalid 4 Octet Sequence (in 3rd Octet)
        utils.unquote("&amp;\xf0\x28\x8c\x28")  # Invalid 4 Octet Sequence (in 4th Octet)
        utils.unquote("&amp;\xf8\xa1\xa1\xa1\xa1")  # Valid 5 Octet Sequence (but not Unicode!)
        utils.unquote("&amp;\xfc\xa1\xa1\xa1\xa1\xa1")  # Valid 6 Octet Sequence (but not Unicode!)

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
        self.assertTrue(utils.find_exec('simple.sh', True) is not None)
    
    def testNotFound(self):
        "Should not find non-existent executables"
        self.assertTrue(utils.find_exec("doesntexistorshouldnt") is None)
    
    def testAlternates(self):
        "Should find executables on specific paths"
        self.assertTrue(utils.find_exec(program="simple.sh", otherdirs=["/doesntexist", self.test_dir]) is not None)

if __name__ == '__main__':
    unittest.main()
