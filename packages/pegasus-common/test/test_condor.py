import io
import logging
import shutil
import subprocess
from collections import namedtuple
from pathlib import Path
from subprocess import Popen

import pytest

from Pegasus.client import condor

@pytest.fixture(scope="function")
def mock_subprocess(mocker):
    class Popen:
        def __init__(self):
            self.stdout = io.BytesIO(b'{"key":"value"}')
            self.stderr = io.BytesIO(b"some initial binary data: \x00\x01\n")
            self.returncode = 0

        def poll(self):
            return 0

        def __del__(self):
            self.stdout.close()
            self.stderr.close()

    mocker.patch("subprocess.Popen", return_value=Popen())    
    
def test_q(mock_subprocess):
    condor._q("ls")
    with pytest.raises(ValueError) as e:
        condor._q(None)
    assert str(e.value) == "cmd is required"
    
@pytest.mark.parametrize("log_lvl", [(logging.INFO), (logging.ERROR)])
def test__handle_stream(caplog, log_lvl):
    test_logger = logging.getLogger("handle_stream_test")
    caplog.set_level(log_lvl)

    # fork process to print 0\n1\n..4\n"
    proc = Popen(
        ["python3", "-c", 'exec("for i in range(5):\\n\\tprint(i)\\n")'],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        )

    stuff = []

    # invoke stream handler
    condor._handle_stream(
        proc=proc,
        stream=proc.stdout,
        dst=stuff,
        logger=test_logger,
        log_lvl=log_lvl,
        )

    assert stuff == [b"0\n", b"1\n", b"2\n", b"3\n", b"4\n"]

    for t in caplog.record_tuples:
        if t[0] == "handle_stream_test":
            assert t[1] == log_lvl

def test__handle_stream_no_logging(caplog):
    logging.getLogger("handle_stream_test")
    caplog.set_level(logging.DEBUG)

    # fork process to print 0\n1\n..4\n"
    proc = Popen(
        ["python3", "-c", 'exec("for i in range(5):\\n\\tprint(i)\\n")'],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        )

    stuff = []

    # invoke stream handler
    condor._handle_stream(proc=proc, stream=proc.stdout, dst=stuff)

    assert stuff == [b"0\n", b"1\n", b"2\n", b"3\n", b"4\n"]

    for t in caplog.record_tuples:
        if t[0] == "handle_stream_test":
            pytest.fail(
                "nothing should have been logged under logger: handle_stream_test"
            )

def test__handle_stream_invalid_log_lvl():
    test_logger = logging.getLogger("handle_stream_test")

    # fork process to print 0\n1\n..4\n"
    proc = Popen(
        ["python3", "-c", 'exec("for i in range(5):\\n\\tprint(i)\\n")'],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    stuff = []

    # invoke stream handler
    with pytest.raises(ValueError) as e:
        condor._handle_stream(
            proc=proc,
            stream=proc.stdout,
            dst=stuff,
            logger=test_logger,
            log_lvl="INVALID_LOG_LEVEL",
        )

    assert "invalid log_lvl: INVALID_LOG_LEVEL" in str(e)

    # for good measure
    proc.kill()
