import unittest

from Pegasus import exitcode

class ExitcodeTestCase(unittest.TestCase):
    def test_unquote_message(self):
        self.assertTrue(exitcode.unquote_message(" "), " ")
        self.assertTrue(exitcode.unquote_message("+"), " ")
        self.assertTrue(exitcode.unquote_message("\\+"), "+")
        self.assertTrue(exitcode.unquote_message("\\"), "\\")
        self.assertTrue(exitcode.unquote_message("hello\\"), "hello\\")
        self.assertTrue(exitcode.unquote_message("hello\\n"), "hello\\n")
        self.assertTrue(exitcode.unquote_message("hello+world"), "hello world")
        self.assertTrue(exitcode.unquote_message("hello world"), "hello world")

    def test_unquote_messages(self):
        messages = ["a b","c+d","e\\+f"]
        uqmessages = exitcode.unquote_messages(messages)
        self.assertEquals(uqmessages[0], "a b")
        self.assertEquals(uqmessages[1], "c d")
        self.assertEquals(uqmessages[2], "e+f")

    def test_has_any_failure_messages(self):
        hafm = exitcode.has_any_failure_messages
        self.assertFalse(hafm(["foo"],[]))
        self.assertFalse(hafm(["foo"],["bar"]))
        self.assertFalse(hafm(["foo","bar"], []))
        self.assertTrue(hafm(["foo"], ["oo"]))
        self.assertTrue(hafm(["foo","bar"], ["foo"]))
        self.assertTrue(hafm(["foo","bar"], ["bar"]))
        self.assertFalse(hafm(["foo","bar"], ["baz"]))

        self.assertTrue(hafm(["ERR MSG"], ["ERR+MSG"]))
        self.assertTrue(hafm(["ERR MSG"], ["ERR MSG"]))

    def test_has_all_success_messages(self):
        hasm = exitcode.has_all_success_messages
        self.assertTrue(hasm(["foo"], []))
        self.assertTrue(hasm(["foo"], ["foo"]))
        self.assertTrue(hasm(["bar","foo"], ["foo"]))
        self.assertTrue(hasm(["bar","foo"], ["foo","oo","bar"]))
        self.assertFalse(hasm(["bar"], ["foo"]))
        self.assertFalse(hasm(["bar","baz"], ["foo"]))
        self.assertFalse(hasm(["foo","bar","baz"], ["foo","bar","bop"]))

        self.assertTrue(hasm(["SUCC MSG"], ["SUCC+MSG"]))
        self.assertTrue(hasm(["SUCC MSG"], ["SUCC MSG"]))

    def test_get_errfile(self):
        self.assertTrue(exitcode.get_errfile("hello.out"), "hello.err")
        self.assertTrue(exitcode.get_errfile("hello.out.000"), "hello.err.000")
        self.assertTrue(exitcode.get_errfile("hello.out.001"), "hello.err.001")
        self.assertTrue(exitcode.get_errfile("hello.out.00"), "hello.err.00")
        self.assertTrue(exitcode.get_errfile("hello.out.0"), "hello.err.0")

if __name__ == '__main__':
    unittest.main()

