#!/usr/bin/env python3

"""
Pegasus utility functions for parsing a kickstart output file and returning wanted information.

Supports both YAML-format (Pegasus 5+) and XML-format (legacy) kickstart records.
"""

import logging
import re
import sys
import traceback
from enum import Enum
from pprint import pprint

import yaml
import yaml.constructor

from Pegasus.monitoring.metadata import FileMetadata

##
#  Copyright 2007-2010 University Of Southern California
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
##

# Revision : $Revision: 2012 $

yaml.constructor.SafeConstructor.yaml_constructors["tag:yaml.org,2002:timestamp"] = (
    yaml.constructor.SafeConstructor.yaml_constructors["tag:yaml.org,2002:str"]
)

PEGASUS_MULTIPART_MARKER = "---------------pegasus-multipart\n"

# Regular expressions used in the kickstart parser
re_parse_props = re.compile(r'(\S+)\s*=\s*([^",]+)')
re_parse_quoted_props = re.compile(r'(\S+)\s*=\s*"([^"]+)"')

logger = logging.getLogger(__name__)


class Parser:
    """
    This class is used to parse a kickstart output file, and return
    requested information.
    """

    def __init__(self, filename):
        """
        This function initializes the Parser class with the kickstart
        output file that should be parsed.
        """
        self._kickstart_output_file = filename
        self._ks_elements = {}
        self._keys = {}
        self._fh = None
        self._open_error = False
        self._parser = None

    def open(self):
        """
        This function opens a kickstart output file.
        """
        try:
            self._fh = open(self._kickstart_output_file)
        except Exception:
            # Error opening file
            self._fh = None
            self._open_error = True
            return False

        # Open succeeded
        self._open_error = False
        return True

    def close(self):
        """
        This function closes the kickstart output file.
        """
        try:
            self._fh.close()
        except Exception:
            return False

        return True

    def is_invocation_record(self, buffer=""):
        """
        Returns True if buffer contains an invocation record either xml or invocation
        """
        # first check for yaml
        if buffer.find("- invocation:") == -1:
            # no yaml check for xml
            if buffer.find("<invocation") == -1:
                return False
        return True

    def is_multipart_record(self, buffer=""):
        """
        Returns True if buffer contains a multipart record such as
        integrity_verification_attempts; integrity_summary and
        location records
        """
        if self.is_invocation_record(buffer):
            return False
        return buffer.find(PEGASUS_MULTIPART_MARKER) == 0

    def is_task_record(self, buffer=""):
        """
        Returns True if buffer contains a task record.
        """
        if buffer.find("[seqexec-task") != -1 or buffer.find("[cluster-task") != -1:
            return True
        return False

    def is_clustered_record(self, buffer=""):
        """
        Returns True if buffer contains a clustered record.
        """
        if (
            buffer.find("[seqexec-summary") != -1
            or buffer.find("[cluster-summary") != -1
        ):
            return True
        return False

    def parse_clustered_record(self, buffer=""):
        """
        Parses the clustered record in buffer, returning all found keys
        """
        self._keys = {}

        # Check if we have an invocation record
        if self.is_clustered_record(buffer) is False:
            return self._keys

        # Add clustered key to our response
        self._keys["clustered"] = True

        # Parse all quoted properties
        for my_key, my_val in re_parse_quoted_props.findall(buffer):
            self._keys[my_key] = my_val

        # And add unquoted properties as well
        for my_key, my_val in re_parse_props.findall(buffer):
            self._keys[my_key] = my_val

        return self._keys

    def parse_task_record(self, buffer=""):
        """
        Parses the task record in buffer, returning all found keys
        """
        self._keys = {}

        # Check if we have an invocation record
        if self.is_task_record(buffer) is False:
            return self._keys

        # Add task key to our response
        self._keys["task"] = True

        # Parse all quoted properties
        for my_key, my_val in re_parse_quoted_props.findall(buffer):
            self._keys[my_key] = my_val

        # And add unquoted properties as well
        for my_key, my_val in re_parse_props.findall(buffer):
            self._keys[my_key] = my_val

        return self._keys

    def parse(self, keys_dict, tasks=True, clustered=True):
        """
        This function parses the kickstart output file, looking for
        the keys specified in the keys_dict variable. It returns a
        list of dictionaries containing the found keys. Look at the
        parse_stampede function for details about how to pass keys
        using the keys_dict structure. The function will return an
        empty list if no records are found or if an error happens.
        """

        # Place keys_dict in the _ks_elements
        self._ks_elements = keys_dict
        my_reply = []

        # Try to open the file
        if self.open() is False:
            return my_reply

        logger.debug(
            f"Started reading records from kickstart file {self._kickstart_output_file}"
        )

        self.close()
        self._parser = YAMLParser(self._kickstart_output_file)

        return self._parser.parse(keys_dict, tasks, clustered)

    def parse_stampede(self):
        """
        This function works similarly to the parse function above,
        but does not require a keys_dict parameter as it uses a
        built-in list of keys specifically used in the Stampede
        schema.
        """

        stampede_elements = {
            "invocation": [
                "hostname",
                "resource",
                "user",
                "hostaddr",
                "transformation",
                "derivation",
            ],
            "mainjob": ["duration", "start"],
            "usage": ["utime", "stime", "maxrss"],
            "ram": ["total"],
            "uname": ["system", "release", "machine"],
            "file": ["name"],
            "status": ["raw"],
            "signalled": [
                "signal",
                "corefile",
                "action",
            ],  # action is the char data in signalled element
            "regular": ["exitcode"],
            "argument-vector": [],
            "cwd": [],
            "stdout": [],
            "stderr": [],
            "statinfo": ["lfn", "size", "ctime", "user"],
            "checksum": ["type", "value", "timing"],
            "type": ["type", "value"],
            "cpu": ["count", "speed", "vendor"],
        }

        return self.parse(stampede_elements, tasks=True, clustered=True)

    def parse_stdout_stderr(self):
        """
        This function extracts the stdout and stderr from a kickstart output file.
        It returns an array containing the output for each task in a job.
        """

        stdout_stderr_elements = {
            "invocation": ["hostname", "resource", "derivation", "transformation"],
            "file": ["name"],
            "regular": ["exitcode"],
            "failure": ["error"],
            "argument-vector": [],
            "cwd": [],
            "stdout": [],
            "stderr": [],
        }

        return self.parse(stdout_stderr_elements, tasks=False, clustered=False)


class _YAMLParserToken(Enum):
    """Internal class defining different tokens parser looks for"""

    UNDEFINED = ""
    INVOCATION = "- invocation:"  # - invocation:
    CLUSTER_TASK = "[cluster-task"  # [cluster-task
    CLUSTER_SUMMARY = "[cluster-summary"  # [cluster-summary
    SEQEXEC_TASK = "[seqexec-task"  # [seqexec-task
    SEQEXEC_SUMMARY = "[seqexec-summary"  # [seqexec-summary
    PEGASUS_MULTIPART = PEGASUS_MULTIPART_MARKER


class YAMLParser(Parser):
    """
    Represents the parser that parses kickstart YAML records.
    """

    def __init__(self, filename):
        super().__init__(filename)
        self._record_number = 0

    def parse(self, keys_dict, tasks=True, clustered=True):
        """
        This function parses the kickstart output file, looking for
        the keys specified in the keys_dict variable. It returns a
        list of dictionaries containing the found keys. Look at the
        parse_stampede function for details about how to pass keys
        using the keys_dict structure. The function will return an
        empty list if no records are found or if an error happens.
        """

        my_reply = []

        # Place keys_dict in the _ks_elements
        self._ks_elements = keys_dict

        # Try to open the file
        if self.open() is False:
            return my_reply

        logger.debug(
            f"Started reading records from kickstart file {self._kickstart_output_file}"
        )

        # Read first record
        record = self.read_record()

        # Loop while we still have record to read
        while record is not None:
            logger.debug(f"Record is \n{record}")
            if self.is_invocation_record(record) is True:
                # We have an invocation record, parse it!
                try:
                    my_record = self.parse_invocation_record(record)
                except Exception:
                    logger.warning(
                        f"KICKSTART-PARSE-ERROR --> error parsing YAML invocation record in file {self._kickstart_output_file}"
                    )
                    logger.warning(traceback.format_exc())
                    # Found error parsing this file, return empty reply
                    my_reply = []
                    # Finish the loop
                    break
                my_reply.append(my_record)
            elif self.is_clustered_record(record) is True:
                # Check if we want clustered records too
                if clustered:
                    # Clustered records are seqexec summary records for clustered jobs
                    # We have a clustered record, parse it!
                    my_reply.append(self.parse_clustered_record(record))
            elif self.is_task_record(record) is True:
                # Check if we want task records too
                if tasks:
                    # We have a clustered record, parse it!
                    my_reply.append(self.parse_task_record(record))
            elif self.is_multipart_record(record) is True:
                logger.debug(f"Multipart Record in file {self._kickstart_output_file}")
                # can return multiple yaml snippets
                my_records = self.parse_multipart_record(record)
                for record in my_records:
                    my_reply.append(record)
            else:
                # We have something else, this shouldn't happen!
                # Just skip it
                pass

            # Read next record
            record = self.read_record()

        # Lastly, close the file
        self.close()

        return my_reply

    def read_record(self):
        """
        This function reads an invocation record from the kickstart
        output file. We also look for the struct at the end of a file
        containing multiple records. It returns a string containing
        the record, or None if it is not found.
        """
        buffer = ""

        # valid token that is parsed
        token = _YAMLParserToken.UNDEFINED

        self._record_number += 1
        logger.debug(
            f"Started reading record number {self._record_number:d} from kickstart file {self._kickstart_output_file}"
        )

        # First, we find the beginning <invocation xmlns....
        while True:
            line = self._fh.readline()
            if line == "":
                # End of file, record not found
                return None
            if line.find(_YAMLParserToken.INVOCATION.value) != -1:
                # token = "- invocation:"
                token = _YAMLParserToken.INVOCATION
                break
            if line.find(_YAMLParserToken.CLUSTER_TASK.value) != -1:
                # token = "[cluster-task"
                token = _YAMLParserToken.CLUSTER_TASK
                break
            if line.find(_YAMLParserToken.CLUSTER_SUMMARY.value) != -1:
                # token = "[cluster-summary"
                token = _YAMLParserToken.CLUSTER_SUMMARY
                break
            if line.find(_YAMLParserToken.SEQEXEC_TASK.value) != -1:
                # deprecated token
                token = _YAMLParserToken.SEQEXEC_TASK
                break
            if line.find(_YAMLParserToken.SEQEXEC_SUMMARY.value) != -1:
                # deprecated token
                token = _YAMLParserToken.SEQEXEC_SUMMARY
                break
            if line.find(_YAMLParserToken.PEGASUS_MULTIPART.value) == 0:
                # token
                token = _YAMLParserToken.PEGASUS_MULTIPART
                break

        # Found something!
        if token == _YAMLParserToken.INVOCATION:
            # Found invocation record
            start = line.find("- invocation:")
            buffer = line[start:]
            # Check if we have everything in a single line
            # Not clear what to do for that for YAML records
        elif token == _YAMLParserToken.PEGASUS_MULTIPART:
            buffer = line[0:]
        elif (
            token == _YAMLParserToken.CLUSTER_SUMMARY
            or token == _YAMLParserToken.SEQEXEC_SUMMARY
        ):
            # Found line with cluster jobs summary
            start = line.find(token.value)
            buffer = line[start:]
            end = buffer.find("]")

            if end >= 0:
                end = end + len("]")
                logger.debug(
                    f"Finished reading record number {self._record_number:d} from kickstart file {self._kickstart_output_file}"
                )
                return buffer[:end]

            # clustered record should be in a single line!
            logger.warning(
                f"{self._kickstart_output_file}: {token} line is malformed... ignoring it..."
            )
            return ""
        elif (
            token == _YAMLParserToken.CLUSTER_TASK
            or token == _YAMLParserToken.SEQEXEC_TASK
        ):
            # Found line with task information
            start = line.find(token.value)
            buffer = line[start:]
            end = buffer.find("]")

            if end >= 0:
                end = end + len("]")
                logger.debug(
                    f"Finished reading record number {self._record_number:d} from kickstart file {self._kickstart_output_file}"
                )
                return buffer[:end]

            # task record should be in a single line!
            logger.warning(
                f"{self._kickstart_output_file}: {token} line is malformed... ignoring it..."
            )
            return ""
        else:
            return ""

        # Ok, now continue reading the file until we get a full record
        buffer = [buffer]

        while True:
            file_ptr = self._fh.tell()
            line = self._fh.readline()
            if line == "":
                # End of file
                break

            if (
                line.find("[cluster-") == 0
                or line.find(PEGASUS_MULTIPART_MARKER) == 0
                or line.find("[seqexec-") == 0
            ):
                # this is to trigger end of parsing of a single kickstart record
                logger.debug(
                    f"Hit end of invocation record in file {self._kickstart_output_file}: "
                )
                # back track file pointer
                self._fh.seek(file_ptr)
                break
            if line[0] in [" ", "-", "l", "\n"]:
                # for #2096 not clear if we need to check the first char of the line.
                # l is for location multipart record
                # We should check for the first char; else this parsing will break
                # where the hpc scheduler may add their own epilogue at the end of the job.out
                buffer.append(line)

        record = "".join(buffer)
        logger.debug(
            f"Finished reading record number {self._record_number:d} from kickstart file {self._kickstart_output_file}"
        )
        return record

    def dicts_remap(self, src, src_keys, dst, dst_keys):
        """
        Pulls data from a provided location in a src dict, and inserts
        the data at a provided location in the dst dic - this is used
        to transition from the old xml format to the new yaml format
        """
        for key in src_keys:
            if key in src:
                src = src[key]
            else:
                src = None
                break

        if src is None:
            return

        for key in dst_keys[:-1]:
            if key not in dst:
                dst[key] = {}
            dst = dst[key]

        dst[dst_keys[-1]] = src

    def map_yaml_to_ver2_format(self, data):
        """
        Maps from new yaml dict format to old v2 format we used with the xml records
        """
        # unmappable:
        #  "file": ["name"]

        # new format -> old format
        my_map = [
            [["hostname"], ["hostname"]],
            [["resource"], ["resource"]],
            [["user"], ["user"]],
            [["hostaddr"], ["hostaddr"]],
            [["transformation"], ["transformation"]],
            [["derivation"], ["derivation"]],
            [["mainjob", "duration"], ["duration"]],
            [["mainjob", "start"], ["start"]],
            [["mainjob", "usage", "utime"], ["utime"]],
            [["mainjob", "usage", "stime"], ["stime"]],
            [["mainjob", "usage", "maxrss"], ["maxrss"]],
            [["machine", "ram_total"], ["ram"]],
            [["machine", "uname_system"], ["system"]],
            [["machine", "uname_release"], ["release"]],
            [["machine", "uname_machine"], ["machine"]],
            [["machine", "cpu_count"], ["cpu_count"]],
            [["machine", "cpu_speed"], ["cpu_speed"]],
            [["machine", "cpu_vendor"], ["cpu_vendor"]],
            [["machine", "cpu_model"], ["cpu_model"]],
            [["machine", "cpu_name"], ["cpu_model"]],
            [["mainjob", "executable", "file_name"], ["name"]],
            [["mainjob", "status", "raw"], ["raw"]],
            [["mainjob", "status", "signalled_signal"], ["signal"]],
            [["mainjob", "status", "signalled_name"], ["action"]],
            [["mainjob", "status", "corefile"], ["corefile"]],
            [["mainjob", "status", "regular_exitcode"], ["exitcode"]],
            [["cwd"], ["cwd"]],
            [["files", "stdout", "data"], ["stdout"]],
            [["files", "stderr", "data"], ["stderr"]],
        ]

        #        stampede_elements = {"invocation": ["hostname", "resource", "user", "hostaddr", "transformation", "derivation"],
        #                             "mainjob": ["duration", "start"],
        #                             "usage": ["utime", "stime"],
        #                             "ram": ["total"],
        #                             "uname": ["system", "release", "machine"],
        #                             "file": ["name"],
        #                             "status": ["raw"],
        #                             "signalled": ["signal", "corefile", "action"], #action is the char data in signalled element
        #                             "regular": ["exitcode"],
        #                             "argument-vector": [],
        #                             "cwd": [],
        #                             "stdout": [],
        #                             "stderr": [],
        #                             "statinfo": ["lfn", "size", "ctime", "user" ],
        #                             "checksum": ["type", "value", "timing"],
        #                             "type": ["type", "value"]}

        new_data = {}
        new_data["invocation"] = True
        new_data["checksum"] = {}
        new_data["outputs"] = {}
        for mapping in my_map:
            self.dicts_remap(data, mapping[0], new_data, mapping[1])

        # some mappings are based on lfns
        if "files" in data:
            # GH-2155 compute the total sizes for input and output files
            (total_ip_size_mb, total_op_size_mb) = self.compute_total_input_output(
                **data["files"]
            )
            new_data["tot_ip_size_mb"] = total_ip_size_mb
            new_data["tot_op_size_mb"] = total_op_size_mb

            for lfn in data["files"]:
                file_data = data["files"][lfn]
                output = file_data["output"] if "output" in file_data.keys() else False
                if not output:
                    continue
                meta = FileMetadata()
                meta._id = lfn

                """
                add whatever 4.9 attributes are
                  {
                    "_type": "file",
                    "_id": "f.b2",
                    "_attributes": {
                      "ctime": "2019-02-19T16:42:52-08:00",
                      "checksum.timing": "0.144",
                      "user": "vahi",
                      "checksum.type": "sha256",
                      "checksum.value": "4a77bee20a28a446506ef7531ffc038053f52e5211d93a95fe5193746af8d23a",
                      "size": "123"
                    }
                  },
                """
                if "user" in data["files"][lfn]:
                    meta.add_attribute("user", str(file_data["user"]))
                if "size" in data["files"][lfn]:
                    meta.add_attribute("size", str(file_data["size"]))
                if "ctime" in data["files"][lfn]:
                    meta.add_attribute("ctime", file_data["ctime"])
                if "sha256" in data["files"][lfn]:
                    meta.add_attribute("checksum.type", "sha256")
                    meta.add_attribute("checksum.value", file_data["sha256"])
                    if "checksum_timing" in data["files"][lfn]:
                        meta.add_attribute(
                            "checksum.timing", str(file_data["checksum_timing"])
                        )
                # what else?

                new_data["outputs"][lfn] = meta

        return new_data

    def compute_total_input_output(self, **kwargs: dict[str, dict[str, str]]):
        """
        Takes in a dictionary indexed by LFN names, where each value is statinfo for the file.

        :param kwargs: lfn -> statinfo(dict)
        :return: total size of input files and output files in MB as determined from
                 the stat records captured in the kickstart record.
        """

        # kickstart returns values as stat reports in bytes
        total_input_size = 0
        total_output_size = 0

        for lfn, statinfo in kwargs.items():
            # ignore stdin, stdout, stderr and metadata infos
            if lfn in ["stdin", "stdout", "stderr", "metadata"]:
                continue

            if "size" not in statinfo:
                # should not happen. a statinfo record without a size
                continue

            if "output" in statinfo:
                if statinfo["output"]:
                    total_output_size += int(statinfo["size"])
            else:
                total_input_size += int(statinfo["size"])

        # convert to MB before returning and round to 6 digits
        # to account for small files that are in bytes
        total_ip_size_mb = round(total_input_size / (1024 * 1024), 6)
        total_op_size_mb = round(total_output_size / (1024 * 1024), 6)
        return total_ip_size_mb, total_op_size_mb

    def parse_invocation_record(self, buffer=""):
        """
        Parses the YAML record in buffer returning an invocation record
        :param buffer:
        :return:
        """
        entry = {}

        # Check if we have an invocation record
        if self.is_invocation_record(buffer) is False:
            return entry

        try:
            entry = yaml.safe_load(buffer)[0]
        except Exception as e:
            logger.warning(
                f"KICKSTART-PARSE-ERROR --> yaml error in {self._kickstart_output_file} : {str(e)}"
            )

        # translate from the yaml dict structure to what we want using the keys-dict
        return self.map_yaml_to_ver2_format(entry)

    def parse_multipart_record(self, buffer=""):
        """
        Parses the YAML record in buffer returning a multipart record in the job.out file
        Sample buffer
            ---------------pegasus-multipart
            - integrity_verification_attempts:
              - lfn: "f.a"
                pfn: "f.a"
                sha256: 8e8ecb610e893781b6c0a38e443a257cb8c0aa548b04946930bea987e5e090d6
                success: True
            - integrity_summary:
                succeeded: 1
                failed: 0
                duration: 0.182

        :param buffer:
        :return: a list of yaml objects
        """
        entries = {}

        # strip off the marker if present
        if buffer.find(PEGASUS_MULTIPART_MARKER) == 0:
            buffer = buffer[len(PEGASUS_MULTIPART_MARKER) :]

        try:
            entries = yaml.safe_load(buffer)
        except Exception as e:
            logger.warning(
                f"KICKSTART-PARSE-ERROR --> yaml error in multipart record {self._kickstart_output_file} : {str(e)}"
            )

        # For GH-2031 in case location record is malformed i.e includes html etc
        # our parser does not return the content so will be a None record returned
        if entries is None:
            logger.error(
                f"A multipart record in {self._kickstart_output_file} is malformed."
            )
            return {}

        # for integrity multipart a list of dict is returned as entries
        # However, for location record (which is just one) a dict is returned
        if type(entries) is dict:
            # for #2096 in case of location record; convert it to a list
            entries = [entries]

        for index, entry in enumerate(entries):
            entries[index]["multipart"] = True
        return entries


if __name__ == "__main__":
    # Let's run a test!
    print("Testing kickstart output file parsing...")

    # log to the console
    console = logging.StreamHandler()
    console.setLevel(logging.DEBUG)
    logger.addHandler(console)
    logger.debug("Logger has been configured")

    # Make sure we have an argument
    if len(sys.argv) < 2:
        print("For testing, please give a kickstart output filename!")
        sys.exit(1)

    # Create parser class
    p = Parser(sys.argv[1])

    # Parse file according to the Stampede schema
    output = p.parse_stampede()

    # Print output
    for record in output:
        pprint(record)
