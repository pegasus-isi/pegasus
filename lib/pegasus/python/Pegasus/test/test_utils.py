# coding=utf-8
import unittest
import time
import os

from Pegasus.tools import utils

class TestQuoting(unittest.TestCase):
    def testQuote(self):
        "Quoting should replace non-printing characters with XML character entity references"
        self.assertEqual(utils.quote("hello\r\n\t"), "hello%0D%0A%09")

        for i in range(0, 0x20):
            self.assertEqual(utils.quote(chr(i)), "%%%02X" % i)

        for i in range(0x20, 0x7F):
            if not chr(i) in "'\"%":
                self.assertEqual(utils.quote(chr(i)), chr(i))

        for i in range(0x7F, 0xFF):
            self.assertEqual(utils.quote(chr(i)), "%%%02X" % i)

        self.assertEqual(utils.quote("%"), "%25")
        self.assertEqual(utils.quote("'"), "%27")
        self.assertEqual(utils.quote('"'), "%22")

        self.assertEqual(utils.quote("Hello\nWorld!\n"), "Hello%0AWorld!%0A")
        self.assertEqual(utils.quote("Zoë"), "Zo%C3%AB")
        self.assertEqual(utils.quote(u"Zoë"), "Zo%C3%AB")
        self.assertEqual(utils.quote(u"Zo\xeb"), "Zo%C3%AB")
        self.assertEqual(utils.quote("warning: unused variable ‘Narr’"), "warning: unused variable %E2%80%98Narr%E2%80%99")
        self.assertEqual(utils.quote(u"warning: unused variable ‘Narr’"), "warning: unused variable %E2%80%98Narr%E2%80%99")
        self.assertEqual(utils.quote(u"warning: unused variable \u2018Narr\u2019"), "warning: unused variable %E2%80%98Narr%E2%80%99")

    def testUnquote(self):
        "Unquoting should convert character entity references back to their Unicode equivalents"
        self.assertEqual(utils.unquote("hello%0D%0A%09"), "hello\r\n\t")

        for i in range(0, 0x20):
            self.assertEqual(utils.unquote("%%%02X" % i), chr(i))

        for i in range(0x20, 0x7F):
            if not chr(i) in "'\"%":
                self.assertEqual(utils.unquote(chr(i)), chr(i))

        for i in range(0x7F, 0xFF):
            self.assertEqual(utils.unquote("%%%02X" % i), chr(i))

        self.assertEqual(utils.unquote("%25"), "%")
        self.assertEqual(utils.unquote("%27"), "'")
        self.assertEqual(utils.unquote("%22"), '"')

        self.assertEqual(utils.unquote("Hello%0AWorld!%0A"), "Hello\nWorld!\n")
        self.assertEqual(utils.unquote("Zo%C3%AB"), "Zoë")
        self.assertEqual(utils.unquote("warning: unused variable %E2%80%98Narr%E2%80%99"), "warning: unused variable ‘Narr’")
        self.assertEqual(utils.unquote("warning: unused variable %E2%80%98Narr%E2%80%99").decode('utf-8'), u"warning: unused variable ‘Narr’")
        self.assertEqual(utils.unquote("warning: unused variable %E2%80%98Narr%E2%80%99").decode('utf-8'), u"warning: unused variable \u2018Narr\u2019")

    def testQuoteInvalidChars(self):
        "Invalid UTF-8 byte strings should not cause quote to fail"
        self.assertEqual(utils.quote("\x80"), "%80")  # Invalid 1 Octet Sequence
        self.assertEqual(utils.quote("\xc3\x28"), "%C3(")  # Invalid 2 Octet Sequence
        self.assertEqual(utils.quote("\xa0\xa1"), "%A0%A1")  # Invalid Sequence Identifier
        self.assertEqual(utils.quote("\xe2\x82\xa1"), "%E2%82%A1")  # Valid 3 Octet Sequence
        self.assertEqual(utils.quote("\xe2\x28\xa1"), "%E2(%A1")  # Invalid 3 Octet Sequence (in 2nd Octet)
        self.assertEqual(utils.quote("\xe2\x82\x28"), "%E2%82(")  # Invalid 3 Octet Sequence (in 3rd Octet)
        self.assertEqual(utils.quote("\xf0\x90\x8c\xbc"), "%F0%90%8C%BC")  # Valid 4 Octet Sequence
        self.assertEqual(utils.quote("\xf0\x28\x8c\xbc"), "%F0(%8C%BC")  # Invalid 4 Octet Sequence (in 2nd Octet)
        self.assertEqual(utils.quote("\xf0\x90\x28\xbc"), "%F0%90(%BC")  # Invalid 4 Octet Sequence (in 3rd Octet)
        self.assertEqual(utils.quote("\xf0\x28\x8c\x28"), "%F0(%8C(")  # Invalid 4 Octet Sequence (in 4th Octet)
        self.assertEqual(utils.quote("\xf8\xa1\xa1\xa1\xa1"), "%F8%A1%A1%A1%A1")  # Valid 5 Octet Sequence (but not Unicode!)
        self.assertEqual(utils.quote("\xfc\xa1\xa1\xa1\xa1\xa1"), "%FC%A1%A1%A1%A1%A1")  # Valid 6 Octet Sequence (but not Unicode!)

    def testUnquoteInvalidChars(self):
        "Invalid UTF-8 byte strings should not cause unquote to fail"
        self.assertEqual(utils.unquote("%80"), "\x80")  # Invalid 1 Octet Sequence
        self.assertEqual(utils.unquote("%C3("), "\xc3\x28")  # Invalid 2 Octet Sequence
        self.assertEqual(utils.unquote("%A0%A1"), "\xa0\xa1")  # Invalid Sequence Identifier
        self.assertEqual(utils.unquote("%E2%82%A1"), "\xe2\x82\xa1")  # Valid 3 Octet Sequence
        self.assertEqual(utils.unquote("%E2(%A1"), "\xe2\x28\xa1")  # Invalid 3 Octet Sequence (in 2nd Octet)
        self.assertEqual(utils.unquote("%E2%82("), "\xe2\x82\x28")  # Invalid 3 Octet Sequence (in 3rd Octet)
        self.assertEqual(utils.unquote("%F0%90%8C%BC"), "\xf0\x90\x8c\xbc")  # Valid 4 Octet Sequence
        self.assertEqual(utils.unquote("%F0(%8C%BC"), "\xf0\x28\x8c\xbc")  # Invalid 4 Octet Sequence (in 2nd Octet)
        self.assertEqual(utils.unquote("%F0%90(%BC"), "\xf0\x90\x28\xbc")  # Invalid 4 Octet Sequence (in 3rd Octet)
        self.assertEqual(utils.unquote("%F0(%8C("), "\xf0\x28\x8c\x28")  # Invalid 4 Octet Sequence (in 4th Octet)
        self.assertEqual(utils.unquote("%F8%A1%A1%A1%A1"), "\xf8\xa1\xa1\xa1\xa1")  # Valid 5 Octet Sequence (but not Unicode!)
        self.assertEqual(utils.unquote("%FC%A1%A1%A1%A1%A1"), "\xfc\xa1\xa1\xa1\xa1\xa1")  # Valid 6 Octet Sequence (but not Unicode!)

    def testQuoteUnquoteUnicode(self):
        "Unicode strings should be utf-8 encoded when passed through quote"
        self.assertEqual(utils.quote(u"Zo\xeb"), "Zo%C3%AB")

    def testUnquoteUnicode(self):
        "Unicode strings should not be mangled when passed through unquote"
        # This is a unicode string with UTF-8 encoding, which is nonsense, but
        # should still work and return a UTF-8 encoded byte string
        self.assertEqual(utils.unquote(u"Zo%C3%AB"), "Zo\xc3\xab")

    def testQuoteUnquoteLatin1(self):
        "A latin-1 encoded string should be unmodified through quote and unquote"
        self.assertEqual("R\xe9sum\xe9",utils.unquote(utils.quote("R\xe9sum\xe9")))
        self.assertEqual(utils.quote("R\xe9sum\xe9"), "R%E9sum%E9")
        self.assertEqual("R\xe9sum\xe9",utils.unquote("R%E9sum%E9"))
        self.assertEqual("R\xe9sum\xe9",utils.unquote(u"R%E9sum%E9"))

class TestISODate(unittest.TestCase):
    def setUp(self):
        self.now = 1334714132

    def testLocal(self):
        "Long local timestamp"
        if time.timezone == 28800:
            self.assertEquals("2012-04-17T18:55:32-0700", utils.isodate(now=self.now))

    def testShortLocal(self):
        "Short local timestamp"
        if time.timezone == 28800:
            self.assertEquals("20120417T185532-0700", utils.isodate(now=self.now, short=True))

    def testUTC(self):
        "Long UTC timestamp"
        self.assertEquals("2012-04-18T01:55:32Z", utils.isodate(now=self.now, utc=True))

    def testShortUTC(self):
        "Short UTC timestamp"
        self.assertEqual("20120418T015532Z", utils.isodate(now=self.now, utc=True, short=True))

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
        self.assertEqual(self.now, utils.epochdate(utils.isodate(now=self.now, utc=True, short=True)))

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
