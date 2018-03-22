from __future__ import print_function
import sys
import imp
import unittest

from Pegasus.cluster import RecordParser, RecordParseException

class TestRecordParser(unittest.TestCase):
    def parse(self, string):
        parser = RecordParser(string)
        return parser.parse()

    def testBad(self):
        self.assertRaises(RecordParseException, self.parse, "")
        self.assertRaises(RecordParseException, self.parse, "[")
        self.assertRaises(RecordParseException, self.parse, "[]")
        self.assertRaises(RecordParseException, self.parse, "[foo]")
        self.assertRaises(RecordParseException, self.parse, "[cluster-summary]")
        self.assertRaises(RecordParseException, self.parse, "[cluster-summary x]")
        self.assertRaises(RecordParseException, self.parse, "[cluster-summary =]")
        self.assertRaises(RecordParseException, self.parse, "[cluster-summary =,]")
        self.assertRaises(RecordParseException, self.parse, "[cluster-summary x=")
        self.assertRaises(RecordParseException, self.parse, "[cluster-summary x=]")
        self.assertRaises(RecordParseException, self.parse, "[cluster-summary x=,1]")
        self.assertRaises(RecordParseException, self.parse, "[cluster-summary x==1]")
        self.assertRaises(RecordParseException, self.parse, '[cluster-summary x="1]')
        self.assertRaises(RecordParseException, self.parse, '[cluster-summary x="1   ]')

    def testOK(self):
        self.assertTrue("x" in self.parse("[cluster-summary x=1]"))
        self.assertTrue("y" in self.parse("[cluster-summary x=1 y=2]"))
        self.assertTrue("y" in self.parse("[cluster-summary x=1, y=2]"))
        self.assertTrue("y" in self.parse("[cluster-summary x=1, y=2,]"))
        self.assertTrue("y" in self.parse("[cluster-summary     x=1,    y=2,      ]"))
        self.assertTrue("y" in self.parse("[cluster-summary x=1, y=2, y=3]"))
        self.assertTrue("alpha/beta" in self.parse("[cluster-summary alpha/beta=zeta]"))
        self.assertTrue("alpha-beta" in self.parse("[cluster-summary alpha-beta=zeta]"))
        self.assertTrue("alpha_beta" in self.parse("[cluster-summary alpha_beta=zeta]"))

    def testValues(self):
        rec = self.parse('[cluster-summary x=1, y="2" z=2, z=3]')
        self.assertTrue("x" in rec)
        self.assertTrue("y" in rec)
        self.assertTrue("z" in rec)

        self.assertEqual(rec["x"], "1")
        self.assertEqual(rec["y"], "2")
        self.assertEqual(rec["z"], "3")

    def testStrings(self):
        rec = self.parse('[cluster-summary foo="bar" baz="bar boo", boo="\'=,- "]')

        self.assertEqual(rec["foo"], "bar")
        self.assertEqual(rec["baz"], "bar boo")
        self.assertEqual(rec["boo"], "'=,- ")

    def _testSpeed(self):
        import time
        start = time.time()
        for i in range(0, 10000):
            self.parse('[cluster-summary foo="bar", baz="bar", boo="bar"]')
        end = time.time()
        print("Elapsed:", (end-start))

if __name__ == '__main__':
    unittest.main()

