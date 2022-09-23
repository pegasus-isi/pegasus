import subprocess
import threading
import logging
from functools import partial
from typing import BinaryIO, Dict, List, Union
from Pegasus.client import _client as cli

_logger = logging.getLogger("Pegasus.client.status")
_logger.propagate = False

def _handle_stream(
        proc: subprocess.Popen,
        stream: BinaryIO,
        dst: list,
        logger: logging.Logger = None,
        log_lvl: int = None,
    ):
    """Handler for processing and logging byte streams from subprocess.Popen.
    :param proc: subprocess.Popen object used to run a pegasus CLI tool
    :type proc: subprocess.Popen
    :param stream: either :code:`stdout` or :code:`stderr` of the given proc
    :type stream: BinaryIO
    :param dst: list where proc output from the given stream will be stored, line by line
    :type dst: list
    :param logger: the logger to use, defaults to None
    :type logger: logging.Logger, optional
    :param log_lvl: the log level to use (e.g. :code:`logging.INFO`, :code:`logging.ERROR`), defaults to None
    :type log_lvl: int, optional
    """

    def _log(logger: logging.Logger, log_lvl: int, msg: bytes):
        if logger:
            log_func = {
                    10: logger.debug,
                    20: logger.info,
                    30: logger.warning,
                    40: logger.error,
                    50: logger.critical,
                }
            try:
                    log_func[log_lvl](msg.decode().strip())
            except KeyError:
                    raise ValueError("invalid log_lvl: {}".format(log_lvl))

    log = partial(_log, logger, log_lvl)

    while True:
        line = stream.readline()

        if line:
                dst.append(line)
                log(line)

        # Has proc terminated? If so, collect remaining output and exit.
        if proc.poll() is not None:
                for l in stream.readlines():
                    dst.append(l)
                    log(l)
                break

def _exec(cmd, stream_stdout=True, stream_stderr=False):
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stream_handlers = []
    # out is not synchronized, don't access until after stdout_handler completes
    out = []
    stdout_handler = threading.Thread(
            target=_handle_stream,
            args=(
                proc,
                proc.stdout,
                out,
                _logger if stream_stderr else None,
                logging.INFO
            ),
        )
    stream_handlers.append(stdout_handler)
    stdout_handler.start()

    # err is not synchronized, don't access until after stderr_handler completes
    err = []
    stderr_handler = threading.Thread(
            target=_handle_stream,
            args=(
                proc,
                proc.stderr,
                err,
                _logger if stream_stderr else None,
                logging.ERROR,
            ),
        )
    stream_handlers.append(stderr_handler)
    stderr_handler.start()

    for sh in stream_handlers:
        sh.join()
    exit_code = proc.returncode
    result = cli.Result(cmd, exit_code, b"".join(out), b"".join(err))
    return result

def _q(cmd):
    """Returns the output of condor_q cmd in JSON format"""

    if not cmd:
        raise ValueError("cmd is required")
    rv = _exec(cmd=cmd)  
    return rv.json
