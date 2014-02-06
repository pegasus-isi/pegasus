import sys
import imp
import unittest

if len(sys.argv) != 2:
    print "Usage: %s path/to/pegasus-exitcode"
    exit(1)

exitcode = imp.load_source("exitcode", sys.argv[1])

class TestRecordParser(unittest.TestCase):
    def parse(self, string):
        parser = exitcode.RecordParser(string)
        return parser.parse()

    def test_bad(self):
        self.assertRaises(exitcode.RecordParseException, self.parse, "")
        self.assertRaises(exitcode.RecordParseException, self.parse, "[")
        self.assertRaises(exitcode.RecordParseException, self.parse, "[]")
        self.assertRaises(exitcode.RecordParseException, self.parse, "[foo]")
        self.assertRaises(exitcode.RecordParseException, self.parse, "[cluster-summary]")
        self.assertRaises(exitcode.RecordParseException, self.parse, "[cluster-summary x]")
        self.assertRaises(exitcode.RecordParseException, self.parse, "[cluster-summary =]")
        self.assertRaises(exitcode.RecordParseException, self.parse, "[cluster-summary =,]")
        self.assertRaises(exitcode.RecordParseException, self.parse, "[cluster-summary x=")
        self.assertRaises(exitcode.RecordParseException, self.parse, "[cluster-summary x=]")
        self.assertRaises(exitcode.RecordParseException, self.parse, "[cluster-summary x=,1]")
        self.assertRaises(exitcode.RecordParseException, self.parse, "[cluster-summary x==1]")
        self.assertRaises(exitcode.RecordParseException, self.parse, '[cluster-summary x="1]')
        self.assertRaises(exitcode.RecordParseException, self.parse, '[cluster-summary x="1   ]')

    def test_ok(self):
        self.assertTrue("x" in self.parse("[cluster-summary x=1]"))
        self.assertTrue("y" in self.parse("[cluster-summary x=1 y=2]"))
        self.assertTrue("y" in self.parse("[cluster-summary x=1, y=2]"))
        self.assertTrue("y" in self.parse("[cluster-summary x=1, y=2,]"))
        self.assertTrue("y" in self.parse("[cluster-summary     x=1,    y=2,      ]"))
        self.assertTrue("y" in self.parse("[cluster-summary x=1, y=2, y=3]"))
        self.assertTrue("alpha/beta" in self.parse("[cluster-summary alpha/beta=zeta]"))
        self.assertTrue("alpha-beta" in self.parse("[cluster-summary alpha-beta=zeta]"))
        self.assertTrue("alpha_beta" in self.parse("[cluster-summary alpha_beta=zeta]"))

    def test_values(self):
        rec = self.parse('[cluster-summary x=1, y="2" z=2, z=3]')
        self.assertTrue("x" in rec)
        self.assertTrue("y" in rec)
        self.assertTrue("z" in rec)

        self.assertEquals(rec["x"], "1")
        self.assertEquals(rec["y"], "2")
        self.assertEquals(rec["z"], "3")

    def test_strings(self):
        rec = self.parse('[cluster-summary foo="bar" baz="bar boo", boo="\'=,- "]')

        self.assertEquals(rec["foo"], "bar")
        self.assertEquals(rec["baz"], "bar boo")
        self.assertEquals(rec["boo"], "'=,- ")

    def _test_speed(self):
        import time
        start = time.time()
        for i in range(0, 10000):
            self.parse('[cluster-summary foo="bar", baz="bar", boo="bar"]')
        end = time.time()
        print "Elapsed:", (end-start)

unittest.main(argv=sys.argv[0:1], verbosity=2)

