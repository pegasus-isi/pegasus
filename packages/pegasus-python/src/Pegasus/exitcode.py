import atexit
import datetime
import json
import os
import re
import sys
import uuid
from optparse import OptionParser

from Pegasus.cluster import RecordParser
from Pegasus.monitoring.metadata import Metadata
from Pegasus.tools import kickstart_parser

# logging
log = {
    "name": None,
    "timestamp": None,
    "exitcode": None,
    "app_exitcode": None,
    "retry": None,
}
tmp_log_files = []


class JobFailed(Exception):
    pass


def rotate_file(outfile, errfile):
    """Rename .out and .err files to .out.XXX and .err.XXX where XXX
    is the next sequence number. Returns the new name, or fails with
    an error message and a non-zero exit code."""

    # This is just to prevent the file from being accidentally renamed
    # again in testing.
    if re.search(r"\.out\.[0-9]{3}$", outfile):
        return outfile, errfile

    # Must end in .out
    if not outfile.endswith(".out"):
        raise JobFailed("%s does not look like a kickstart .out file" % outfile)

    # Find next file in sequence
    retry = None
    for i in range(0, 1000):
        candidate = "%s.%03d" % (outfile, i)
        if not os.path.isfile(candidate):
            retry = i
            break

    # unlikely to occur
    if retry is None:
        raise JobFailed("%s has been renamed too many times!" % (outfile))

    basename = outfile[:-4]
    log["retry"] = retry

    # rename .out to .out.000
    newout = "%s.out.%03d" % (basename, retry)
    os.rename(outfile, newout)

    # rename .err to .err.000 if it exists
    newerr = None
    if os.path.isfile(errfile):
        newerr = "%s.err.%03d" % (basename, retry)
        os.rename(errfile, newerr)

    return newout, newerr


def readfile(filename):
    # If the file does not exits, return empty string
    if filename is None or not os.path.isfile(filename):
        return ""

    f = open(filename)
    try:
        return f.read()
    finally:
        f.close()


def find_cluster_summary(txt):
    "Find and return the cluster summary record if it exists"

    b = txt.find("[cluster-summary")
    if b < 0:
        # Older seqexec used this name
        b = txt.find("[seqexec-summary")
    if b >= 0:
        e = txt.find("]", b)
        if e < 0:
            raise JobFailed("Invalid cluster-summary record")

        return RecordParser(txt[b : e + 1]).parse()

    # If we found a cluster-task, but no cluster-summary, then there is a problem
    b = txt.find("[cluster-task")
    if b >= 0:
        raise JobFailed("cluster-summary is missing")

    return None


def check_cluster_summary(record):
    "Make sure that the cluster-summary record is OK"

    if "stat" in record:
        stat = record["stat"]
        if stat != "ok":
            raise JobFailed("cluster-summary stat=%s" % stat)

    # If any of the tasks failed, then job failed
    if "failed" in record:
        failed = int(record["failed"])
        if failed > 0:
            raise JobFailed("cluster-summary failed=%d" % failed)

    # If no tasks were submitted, then it succeeded
    if "submitted" in record:
        submitted = int(record["submitted"])
        if submitted == 0:
            return 0

    # If there were no tasks, then the job was successful
    if "tasks" in record:
        tasks = int(record["tasks"])
        if tasks == 0:
            return 0

    # If the number of successes was zero, then the job failed
    if "succeeded" in record:
        succeeded = int(record["succeeded"])
        if succeeded == 0:
            raise JobFailed("cluster-summary succeeded=%d" % succeeded)

    return 0


def check_kickstart_records(txt):
    # Check the exitcodes of all kickstart records
    regex = re.compile(r'raw="(-?[0-9]+)"')
    succeeded = 0
    e = 0

    # yaml
    for m in re.finditer(r"raw: ([0-9]+)", txt):
        raw = int(m.group(1))
        log["app_exitcode"] = raw
        if raw != 0:
            raise JobFailed("task exited with raw status %d" % raw)
        succeeded = succeeded + 1

    # xml
    while True:
        b = txt.find("<status", e)
        if b < 0:
            break
        e = txt.find("</status>", b)
        if e < 0:
            raise JobFailed("mismatched <status>")
        e = e + len("</status>")
        m = regex.search(txt[b:e])
        if m:
            raw = int(m.group(1))
            log["app_exitcode"] = raw
        else:
            raise JobFailed("<status> was missing valid 'raw' attribute")
        if raw != 0:
            raise JobFailed("task exited with raw status %d" % raw)
        succeeded = succeeded + 1

    # Fail if there were no invocation records and no cluster-summary
    if succeeded == 0:
        raise JobFailed("No successful kickstart records")

    return 0


def unquote_message(message):
    def genchars():
        yield from message

    chars = genchars()
    output = []
    for c in chars:
        if c == "+":
            output.append(" ")
        elif c == "\\":
            try:
                c = next(chars)
            except StopIteration:
                output.append(c)
                break
            if c == "+":
                output.append("+")
            else:
                output.append("\\")
                output.append(c)
        else:
            output.append(c)

    return "".join(output)


def unquote_messages(messages):
    return [unquote_message(m) for m in messages]


def has_any_failure_messages(stdio, messages):
    """Return true if any of the messages appear in the files"""
    if len(messages) == 0:
        return False

    messages = unquote_messages(messages)

    for txt in stdio:
        for m in messages:
            if m in txt:
                return True

    return False


def has_all_success_messages(stdio, messages):
    """Return true if any of the messages don't appear in the files"""
    if len(messages) == 0:
        return True

    messages = unquote_messages(messages)

    found = set()
    for txt in stdio:
        for m in messages:
            if m in txt:
                found.add(m)

    for m in messages:
        if m not in found:
            return False

    return True


def get_errfile(outfile):
    """Get the stderr file name given the stdout file name"""
    i = outfile.rfind(".out")
    left = outfile[0:i]
    right = ""
    if i + 5 < len(outfile):
        right = outfile[i + 4 :]
    errfile = left + ".err" + right
    return errfile


def append_to_wf_metadata_log(files_metadata, logfile):
    """
    Writes out file metadata to a logfile in the File RC format

    :param files_metadata:    list of FileMetadata objects
    :param logfile:           the log file to append the info to
    :return:
    """
    # writing to log file (concurrency safe)
    with open(logfile, "a", encoding="utf8") as outfile:
        for file_metadata in files_metadata:
            res = file_metadata.convert_to_rce()
            outfile.write(res + "\n")


def exitcode(
    outfile,
    status=None,
    rename=True,
    failure_messages=[],
    success_messages=[],
    wf_metadata_log=None,
    generate_meta=True,
):
    if not os.path.isfile(outfile):
        raise JobFailed("%s does not exist" % outfile)

    errfile = get_errfile(outfile)

    # outfile Must end in .out
    if not outfile.endswith(".out"):
        raise JobFailed("%s does not look like a kickstart .out file" % outfile)

    meta_file = outfile[:-3] + "meta"

    # If we are renaming, then rename
    if rename:
        outfile, errfile = rotate_file(outfile, errfile)

    # First, check exitcode supplied by DAGMan, if any
    if status is not None:
        log["app_exitcode"] = status
        if status != 0:
            raise JobFailed("dagman reported non-zero exitcode: %d" % status)

    # Next, read the output and error files
    stdout = readfile(outfile)
    stderr = readfile(errfile)

    # Next, check the size of the output file
    # when a job is launched without kickstart stdout can be empty.
    # signified by non None status
    if status is None and len(stdout) == 0:
        raise JobFailed("Empty stdout")

    # Next, if we have failure messages, then fail if we find one in the
    # output of the job
    if has_any_failure_messages([stdout, stderr], failure_messages):
        raise JobFailed("Failure message found in output")

    # Next, if we have success messages, then fail if we don't find all
    # in the output of the job
    if not has_all_success_messages([stdout, stderr], success_messages):
        raise JobFailed("Success message missing from output")

    # Next, check exitcodes of all tasks
    cs = find_cluster_summary(stdout)
    if cs is not None:
        check_cluster_summary(cs)
    else:
        # PM-927 Only check kickstart records if -r is not supplied
        if status is None:
            check_kickstart_records(stdout)

    # Next check if metadata file needs to be generated
    if generate_meta:
        files_metadata = parse_metadata_from_kickstart(outfile)
        # always generate a meta file even if it is a zero byte file
        directory = os.path.dirname(meta_file)
        basename = os.path.basename(meta_file)
        Metadata.write_to_jsonfile(
            files_metadata, directory, basename, prefix="pegasus-exitcode"
        )

        if wf_metadata_log and files_metadata:
            # PM-1257 write files metadata to workflow log
            append_to_wf_metadata_log(files_metadata, wf_metadata_log)


def parse_metadata_from_kickstart(outfile):
    # First assume we will find rotated file
    parser = kickstart_parser.Parser(outfile)
    kickstart_output = parser.parse_stampede()

    # Start empty
    files = []
    for record in kickstart_output:
        if "invocation" in record:
            # Ok, we have an invocation record, extract the metadata information
            if "outputs" in record:
                for lfn in record["outputs"].keys():
                    files.append(record["outputs"][lfn])

    return files


def _log_info(info_msg):
    if len(tmp_log_files) > 0:
        tmp_log_files[0].write(info_msg + "\n")
    else:
        print(info_msg)


def _log_error(err_msg):
    if len(tmp_log_files) > 0:
        tmp_log_files[1].write(err_msg + "\n")
    else:
        print(err_msg)


def _write_logs(log_filename):
    if log_filename:
        # reading std_out and std_err files
        std_out = tmp_log_files[0]
        std_err = tmp_log_files[1]
        std_out.close()
        std_err.close()

        with open(std_out.name) as sout:
            log["std_out"] = sout.read()
        with open(std_err.name) as serr:
            log["std_err"] = serr.read()

        # writing to log file (concurrency safe)
        with open(log_filename, "a", encoding="utf8") as outfile:
            res = json.dumps(log, ensure_ascii=False)
            outfile.write(res + "\n")

    else:
        print(json.dumps(log))


def main(args):
    usage = "Usage: %prog [options] job.out"
    parser = OptionParser(usage)

    parser.add_option(
        "-r",
        "--return",
        action="store",
        type="int",
        dest="status",
        metavar="R",
        help="Return code reported by DAGMan. This can be specified in a "
        "DAG using the $RETURN variable.",
    )
    parser.add_option(
        "-n",
        "--no-rename",
        action="store_false",
        dest="rename",
        default=True,
        help="Don't rename kickstart.out and .err to .out.XXX and .err.XXX. "
        "Useful for testing.",
    )
    parser.add_option(
        "-N",
        "--no-metadata",
        action="store_false",
        dest="generate_meta",
        default=True,
        help="disable generation of metadata file after parsing of kickstart records",
    )
    parser.add_option(
        "-f",
        "--failure-message",
        action="append",
        dest="failure_messages",
        default=[],
        help="Failure message to find in job stdout/stderr. If this "
        "message exists in the stdout/stderr of the job, then the "
        "job will be considered a failure no matter what other "
        "output exists. If multiple failure messages are provided, "
        "then none of them can exist in the output or the job is "
        "considered a failure.",
    )
    parser.add_option(
        "-s",
        "--success-message",
        action="append",
        dest="success_messages",
        default=[],
        help="Success message to find in job stdout/stderr. If this "
        "message does not exist in the stdout/stderr of the job, "
        "then the job will be considered a failure no matter what "
        "other output exists. If multiple success messages are "
        "provided, then they must all exist in the output or "
        "the job is considered a failure.",
    )
    parser.add_option(
        "-l",
        "--log",
        action="store",
        type="string",
        dest="log_filename",
        help="Name of the common log file in which stdout/stderr will" "be redirected.",
    )
    parser.add_option(
        "-M",
        "--metadata-log",
        action="store",
        type="string",
        dest="wf_metadata_log",
        help="Name of the common log file in which the metadata parsed "
        "from .out file is placed in append mode for the workflow",
    )

    (options, args) = parser.parse_args(args)

    if len(args) != 1:
        parser.error("Please specify job.out")

    outfile = args[0]

    if options.log_filename:
        # temporary log files
        tmp_log_name = "_exit-code-" + str(uuid.uuid4())
        tmp_log_files.append(open(tmp_log_name + ".out", "w"))
        tmp_log_files.append(open(tmp_log_name + ".err", "w"))

    try:
        log["name"] = outfile
        log["timestamp"] = datetime.datetime.now().isoformat()
        exitcode(
            outfile,
            status=options.status,
            rename=options.rename,
            failure_messages=options.failure_messages,
            success_messages=options.success_messages,
            generate_meta=options.generate_meta,
            wf_metadata_log=options.wf_metadata_log,
        )
        log["exitcode"] = 0
        _write_logs(options.log_filename)
        sys.exit(0)
    except JobFailed as jf:
        _log_error(str(jf))
        log["exitcode"] = 1
        _write_logs(options.log_filename)
        sys.exit(1)


@atexit.register
def remove_temporary_files():
    # cleaning temporary files
    for tmp_file in tmp_log_files:
        os.remove(tmp_file.name)
