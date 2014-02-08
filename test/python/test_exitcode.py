import os
import unittest

from Pegasus import exitcode

dirname = os.path.abspath(os.path.dirname(__file__))

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

    def test_exitcode(self):

        def ec(filename):
            path = os.path.join(dirname, "exitcode", filename)
            exitcode.exitcode(path, rename=False)

        ec("ok.out")
        ec("zeromem.out")
        ec("cluster-none.out")
        ec("cluster-ok.out")
        ec("seqexec-ok.out")
        ec("cluster_summary_ok.out")
        ec("cluster_summary_notasks.out")
        ec("cluster_summary_submitted.out")

        self.assertRaises(exitcode.JobFailed, ec, "failed.out")
        self.assertRaises(exitcode.JobFailed, ec, "walltime.out")
        self.assertRaises(exitcode.JobFailed, ec, "zerolen.out")
        self.assertRaises(exitcode.JobFailed, ec, "cluster-error.out")
        self.assertRaises(exitcode.JobFailed, ec, "nonzero.out")
        self.assertRaises(exitcode.JobFailed, ec, "signalled.out")
        self.assertRaises(exitcode.JobFailed, ec, "largecode.out")
        self.assertRaises(exitcode.JobFailed, ec, "cluster_summary_failed.out")
        self.assertRaises(exitcode.JobFailed, ec, "cluster_summary_stat.out")
        self.assertRaises(exitcode.JobFailed, ec, "cluster_summary_missing.out")
        self.assertRaises(exitcode.JobFailed, ec, "cluster_summary_nosucc.out")

    def ec(self, filename, **args):
        exitcode.exitcode(filename, **args)

    def test_rename_noerrfile(self):
        inf = os.path.join(dirname, "exitcode", "ok.out")
        outf = os.path.join(dirname, "exitcode", "ok.out.000")
        self.ec(inf, rename=True)
        exists = os.path.isfile(outf)
        self.assertTrue(exists)
        if exists:
            os.rename(outf, inf)

#function test_failure_message_zero_exit {
#    result=$($bin/pegasus-exitcode --no-rename --failure-message "Job failed" failure_message_zero_exit.out 2>&1)
#    rc=$?
#    if [ $rc -ne 1 ]; then
#        stderr "$result"
#        return 1
#    fi
#}

#function test_success_message_failure_message {
#    result=$($bin/pegasus-exitcode --no-rename -s "Job succeeded" -f "Job failed" success_message_failure_message.out 2>&1)
#    rc=$?
#    if [ $rc -ne 1 ]; then
#        echo "$result" >&2
#        return 1
#    fi
#}

#function test_success_message_missing {
#    result=$($bin/pegasus-exitcode --no-rename -s "Job succeeded" success_message_missing.out 2>&1)
#    rc=$?
#    if [ $rc -ne 1 ]; then
#        echo "$result" >&2
#        return 1
#    fi
#}

#function test_success_message_present {
#    result=$($bin/pegasus-exitcode --no-rename -s "Job succeeded" success_message_zero_exit.out 2>&1)
#    rc=$?
#    if [ $rc -ne 0 ]; then
#        echo "$result" >&2
#        return 1
#    fi
#}

#function test_success_message_nonzero_exit {
#    result=$($bin/pegasus-exitcode --no-rename --success-message "Job succeeded" success_message_nonzero_exit.out 2>&1)
#    rc=$?
#    if [ $rc -ne 1 ]; then
#        echo "$result" >&2
#        return 1
#    fi
#}

#function test_all_success_messages_required {
#    result=$($bin/pegasus-exitcode --no-rename -s "Job succeeded" -s "Successfully finished" success_message_zero_exit.out 2>&1)
#    rc=$?
#    if [ $rc -ne 1 ]; then
#        echo "$result" >&2
#        return 1
#    fi
#}

if __name__ == '__main__':
    unittest.main()

