#!/usr/bin/env python3

"""
Pegasus utility functions for pasing a kickstart output file and return wanted information

"""

import logging
import re
import sys
import traceback
from pprint import pprint
from xml.parsers import expat

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

yaml.constructor.SafeConstructor.yaml_constructors[
    "tag:yaml.org,2002:timestamp"
] = yaml.constructor.SafeConstructor.yaml_constructors["tag:yaml.org,2002:str"]

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
        Returns True if buffer contains an invocation record either xml or invocation
        """
        if self.is_invocation_record(buffer):
            return False
        else:
            return buffer.find("- ") == 0

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
            "Started reading records from kickstart file %s"
            % (self._kickstart_output_file)
        )

        # load the appropriate parser by checking for first instance of xml header or invocation
        yaml_parser = True
        for line in self._fh:
            if line.find("<?xml") != -1:
                yaml_parser = False
                break
            elif line.find("- invocation:") != -1:
                yaml_parser = True
                break

        self.close()
        self._parser = (
            YAMLParser(self._kickstart_output_file)
            if yaml_parser
            else XMLParser(self._kickstart_output_file)
        )

        return self._parser.parse(keys_dict, tasks, clustered)

    def parse_stampede(self):
        """
        This function works similarly to the parse function above,
        but does not require a keys_dict parameter as it uses a
        built-in list of keys speficically used in the Stampede
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


class YAMLParser(Parser):
    """
    Represents the parser that parses the kickstart xml records
    """

    def __init__(self, filename):
        super().__init__(filename)

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
            "Started reading records from kickstart file %s"
            % (self._kickstart_output_file)
        )

        self._record_number = 0
        # Read first record
        record = self.read_record()

        # Loop while we still have record to read
        while record is not None:
            logger.debug("Record is \n%s" % record)
            if self.is_invocation_record(record) is True:
                # We have an invocation record, parse it!
                try:
                    my_record = self.parse_invocation_record(record)
                except Exception:
                    logger.warning(
                        "KICKSTART-PARSE-ERROR --> error parsing YAML invocation record in file %s"
                        % (self._kickstart_output_file)
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
                logger.debug(
                    "Multipart Record in file %s" % (self._kickstart_output_file)
                )
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
        token = ""

        self._record_number += 1
        logger.debug(
            "Started reading record number %d from kickstart file %s"
            % (self._record_number, self._kickstart_output_file)
        )

        # First, we find the beginning <invocation xmlns....
        while True:
            line = self._fh.readline()
            if line == "":
                # End of file, record not found
                return None
            if line.find("- invocation:") != -1:
                token = "- invocation:"
                break
            if line.find("[cluster-task") != -1:
                token = "[cluster-task"
                break
            if line.find("[cluster-summary") != -1:
                token = "[cluster-summary"
                break
            if line.find("[seqexec-task") != -1:
                # deprecated token
                token = "[seqexec-task"
                break
            if line.find("[seqexec-summary") != -1:
                # deprecated token
                token = "[seqexec-summary"
                break
            if line.find("---------------pegasus-multipart") == 0:
                # token
                token = "pegasus-multipart"
                break

        # Found something!
        if token == "- invocation:":
            # Found invocation record
            start = line.find("- invocation:")
            buffer = line[start:]
            # Check if we have everything in a single line
            # Not clear what to do for that for YAML records
        elif token == "pegasus-multipart":
            buffer = ""
        elif token == "[cluster-summary" or token == "[seqexec-summary":
            # Found line with cluster jobs summary
            start = line.find(token)
            buffer = line[start:]
            end = buffer.find("]")

            if end >= 0:
                end = end + len("]")
                return buffer[:end]

            # clustered record should be in a single line!
            logger.warning(
                "%s: %s line is malformed... ignoring it..."
                % (self._kickstart_output_file, token)
            )
            return ""
        elif token == "[cluster-task" or token == "[seqexec-task":
            # Found line with task information
            start = line.find(token)
            buffer = line[start:]
            end = buffer.find("]")

            if end >= 0:
                end = end + len("]")
                return buffer[:end]

            # task record should be in a single line!
            logger.warning(
                "%s: %s line is malformed... ignoring it..."
                % (self._kickstart_output_file, token)
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
                or line.find("---------------pegasus-multipart") == 0
            ):
                # this is to trigger end of parsing of a single kickstart record
                logger.debug(
                    "Hit end of invocation record in file %s: "
                    % self._kickstart_output_file
                )
                # back track file pointer
                self._fh.seek(file_ptr)
                break
            elif line[0] in [" ", "-", "\n"]:
                buffer.append(line)

        record = "".join(buffer)
        logger.debug(
            "Finished reading record number %d from kickstart file %s"
            % (self._record_number, self._kickstart_output_file)
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
                "KICKSTART-PARSE-ERROR --> yaml error in %s : %s"
                % (self._kickstart_output_file, str(e))
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
        try:
            entries = yaml.safe_load(buffer)
        except Exception as e:
            logger.warning(
                "KICKSTART-PARSE-ERROR --> yaml error in multipart record %s : %s"
                % (self._kickstart_output_file, str(e))
            )

        for index, entry in enumerate(entries):
            entries[index]["multipart"] = True
        return entries


class XMLParser(Parser):
    """
    Represents the parser that parses the kickstart xml records
    """

    def __init__(self, filename):
        super().__init__(filename)
        self._parsing_job_element = False
        self._parsing_arguments = False
        self._parsing_main_job = False
        self._parsing_machine = False
        self._parsing_stdout = False
        self._parsing_stderr = False
        self._parsing_data = False
        self._parsing_cwd = False
        self._parsing_cpu = False
        self._parsing_final_statcall = False
        self._record_number = 0
        self._arguments = []
        self._stdout = ""
        self._stderr = ""
        self._cwd = ""
        self._lfn = ""  # filename parsed from statcall record

    def parse(self, keys_dict, tasks=True, clustered=True):
        """
        This function parses the previous XML based ks records,
        looking for
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
            "Started reading records from kickstart file %s"
            % (self._kickstart_output_file)
        )

        self._record_number = 0
        # Read first record
        my_buffer = self.read_record()

        # Loop while we still have record to read
        while my_buffer is not None:
            if self.is_invocation_record(my_buffer) is True:
                # We have an invocation record, parse it!
                try:
                    my_record = self.parse_invocation_record(my_buffer)
                except Exception:
                    logger.warning(
                        "KICKSTART-PARSE-ERROR --> error parsing invocation record in file %s"
                        % (self._kickstart_output_file)
                    )
                    logger.warning(traceback.format_exc())
                    # Found error parsing this file, return empty reply
                    my_reply = []
                    # Finish the loop
                    break
                my_reply.append(my_record)
            elif self.is_clustered_record(my_buffer) is True:
                # Check if we want clustered records too
                if clustered:
                    # Clustered records are seqexec summary records for clustered jobs
                    # We have a clustered record, parse it!
                    my_reply.append(self.parse_clustered_record(my_buffer))
            elif self.is_task_record(my_buffer) is True:
                # Check if we want task records too
                if tasks:
                    # We have a clustered record, parse it!
                    my_reply.append(self.parse_task_record(my_buffer))
            else:
                # We have something else, this shouldn't happen!
                # Just skip it
                pass

            # Read next record
            my_buffer = self.read_record()

        # Lastly, close the file
        self.close()

        return my_reply

    def parse_invocation_record(self, buffer=""):
        """
        Parses the xml record in buffer, returning the desired keys.
        """
        # Initialize variables
        self._parsing_arguments = False
        self._parsing_main_job = False
        self._parsing_machine = False
        self._parsing_stdout = False
        self._parsing_stderr = False
        self._parsing_data = False
        self._parsing_cwd = False
        self._parsing_cpu = False
        self._parsing_signalled = False
        self._arguments = []
        self._stdout = ""
        self._stderr = ""
        self._cwd = ""
        self._keys = {}

        # Check if we have an invocation record
        if self.is_invocation_record(buffer) is False:
            return self._keys

        # Add invocation key to our response
        self._keys["invocation"] = True

        # Prepend XML header
        buffer = '<?xml version="1.0" encoding="ISO-8859-1"?>\n' + buffer

        # Create parser
        self._my_parser = expat.ParserCreate()
        self._my_parser.StartElementHandler = self.start_element
        self._my_parser.EndElementHandler = self.end_element
        self._my_parser.CharacterDataHandler = self.char_data

        # Parse everything!
        self._my_parser.Parse(buffer)

        # Add cwd, arguments, stdout, and stderr to keys
        if "cwd" in self._ks_elements:
            self._keys["cwd"] = self._cwd

        if "argument-vector" in self._ks_elements:
            self._keys["argument-vector"] = " ".join(self._arguments)

        if "stdout" in self._ks_elements:
            self._keys["stdout"] = self._stdout

        if "stderr" in self._ks_elements:
            self._keys["stderr"] = self._stderr

        return self._keys

    def read_record(self):
        """
        This function reads an invocation record from the kickstart
        output file. We also look for the struct at the end of a file
        containing multiple records. It returns a string containing
        the record, or None if it is not found.
        """
        buffer = ""

        # valid token that is parsed
        token = ""

        self._record_number += 1
        logger.debug(
            "Started reading record number %d from kickstart file %s"
            % (self._record_number, self._kickstart_output_file)
        )

        # First, we find the beginning <invocation xmlns....
        while True:
            line = self._fh.readline()
            if line == "":
                # End of file, record not found
                return None
            if line.find("<invocation") != -1:
                token = "<invocation"
                break
            if line.find("[cluster-task") != -1:
                token = "[cluster-task"
                break
            if line.find("[cluster-summary") != -1:
                token = "[cluster-summary"
                break
            if line.find("[seqexec-task") != -1:
                # deprecated token
                token = "[seqexec-task"
                break
            if line.find("[seqexec-summary") != -1:
                # deprecated token
                token = "[seqexec-summary"
                break

        # Found something!
        # if line.find("<invocation") >= 0:
        if token == "<invocation":
            # Found invocation record
            start = line.find("<invocation")
            buffer = line[start:]
            end = buffer.find("</invocation>")

            # Check if we have everything in a single line
            if end >= 0:
                end = end + len("</invocation>")
                return buffer[:end]
        # elif line.find("[seqexec-summary") >= 0:
        elif token == "[cluster-summary" or token == "[seqexec-summary":
            # Found line with cluster jobs summary
            start = line.find(token)
            buffer = line[start:]
            end = buffer.find("]")

            if end >= 0:
                end = end + len("]")
                return buffer[:end]

            # clustered record should be in a single line!
            logger.warning(
                "%s: %s line is malformed... ignoring it..."
                % (self._kickstart_output_file, token)
            )
            return ""
        # elif line.find("[seqexec-task") >= 0:
        elif token == "[cluster-task" or token == "[seqexec-task":
            # Found line with task information
            start = line.find(token)
            buffer = line[start:]
            end = buffer.find("]")

            if end >= 0:
                end = end + len("]")
                return buffer[:end]

            # task record should be in a single line!
            logger.warning(
                "%s: %s line is malformed... ignoring it..."
                % (self._kickstart_output_file, token)
            )
            return ""
        else:
            return ""

        # Ok, now continue reading the file until we get a full record
        buffer = [buffer]

        while True:
            line = self._fh.readline()
            if line == "":
                # End of file, record not found
                return None
            # buffer = buffer + line
            buffer.append(line)
            if line.find("</invocation>") >= 0:
                break

        # Now, we got it, let's make sure
        end = line.find("</invocation>")
        if end == -1:
            return ""

        # end = end + len("</invocation>")
        invocation = "".join(buffer)
        logger.debug(
            "Finished reading record number %d from kickstart file %s"
            % (self._record_number, self._kickstart_output_file)
        )
        return invocation
        # return buffer[:end]

    def start_element(self, name, attrs):
        """
        Function called by the parser every time a new element starts
        """
        # Keep track if we are parsing the main job element
        if name == "mainjob":
            self._parsing_main_job = True
        if name == "machine":
            self._parsing_machine = True
        # Keep track if we are inside one of the job elements
        if (
            name == "setup"
            or name == "prejob"
            or name == "mainjob"
            or name == "postjob"
            or name == "cleanup"
        ):
            self._parsing_job_element = True
        if name == "argument-vector" and name in self._ks_elements:
            # Start parsing arguments
            self._parsing_arguments = True
        elif name == "cwd" and name in self._ks_elements:
            # Start parsing cwd
            self._parsing_cwd = True
        elif name == "checksum" and name in self._ks_elements:
            # PM-1180 <checksum type="sha256" value="f2307670158c64c4407971f8fad67772724b0bad92bfb48f386b0814ba24e3af"/>
            self._keys[name] = {}
            for attr_name in self._ks_elements[name]:
                if attr_name in attrs:
                    self._keys[name][attr_name] = attrs[attr_name]
        elif name == "cpu" and name in self._ks_elements:
            # PM-1398 <cpu count="4" speed="2600" vendor="GenuineIntel">Intel(R) Xeon(R) CPU E5-2690 v4 @ 2.60GHz</cpu>
            self._parsing_cpu = True
            self._keys[name] = {}
            for attr_name in self._ks_elements[name]:
                if attr_name in attrs:
                    # keep consitency with 5.0 yaml based naming
                    self._keys[name]["cpu_" + attr_name] = attrs[attr_name]
        elif name == "data":
            # Start parsing data for stdout and stderr output
            self._parsing_data = True
        elif name == "file" and name in self._ks_elements:
            if self._parsing_main_job is True:
                # Special case for name inside the mainjob element (will change this later)
                for my_element in self._ks_elements[name]:
                    if my_element in attrs:
                        self._keys[my_element] = attrs[my_element]
        elif name == "ram" and name in self._ks_elements:
            if self._parsing_machine is True:
                # Special case for ram inside the machine element (will change this later)
                for my_element in self._ks_elements[name]:
                    if my_element in attrs:
                        self._keys[my_element] = attrs[my_element]
        elif name == "uname" and name in self._ks_elements:
            if self._parsing_machine is True:
                # Special case for uname inside the machine element (will change this later)
                for my_element in self._ks_elements[name]:
                    if my_element in attrs:
                        self._keys[my_element] = attrs[my_element]
        elif name == "signalled":
            # PM-1109 grab the attributes we are interested in
            self._keys[name] = {}  # a dictionary indexed by attributes
            self._parsing_signalled = True
            self._keys[name]["action"] = ""  # grabbed later in char data
            for attr in attrs:
                if attr in self._ks_elements[name]:
                    self._keys[name][attr] = attrs[attr]
        elif name == "statcall":
            if "id" in attrs:
                if attrs["id"] == "stdout" and "stdout" in self._ks_elements:
                    self._parsing_stdout = True
                elif attrs["id"] == "stderr" and "stderr" in self._ks_elements:
                    self._parsing_stderr = True
                elif attrs["id"] == "final":
                    self._parsing_final_statcall = True
                    self._lfn = attrs["lfn"]
        elif name == "statinfo":
            if self._parsing_final_statcall is True:
                statinfo = FileMetadata()
                for my_element in self._ks_elements[name]:
                    if my_element in attrs:
                        statinfo.add_attribute(my_element, attrs[my_element])
                if "outputs" not in self._keys:
                    self._keys["outputs"] = {}  # a dictionary indexed by lfn
                lfn = self._lfn
                statinfo.set_id(lfn)
                if lfn is None or not statinfo:
                    logger.warning(
                        "Malformed/Empty stat record for output lfn %s %s"
                        % (lfn, statinfo)
                    )
                self._keys["outputs"][lfn] = statinfo
        elif name == "usage" and name in self._ks_elements:
            if self._parsing_main_job:
                # Special case for handling utime and stime, which need to be added
                for my_element in self._ks_elements[name]:
                    try:
                        self._keys[my_element] = float(attrs[my_element])
                    except ValueError:
                        logger.warning(
                            "cannot convert element %s to float!" % (my_element)
                        )
        else:
            # For all other elements, check if we want them
            if name in self._ks_elements:
                for my_element in self._ks_elements[name]:
                    if my_element in attrs:
                        self._keys[my_element] = attrs[my_element]

    def end_element(self, name):
        """
        Function called by the parser whenever we reach the end of an element
        """
        # Stop parsing argement-vector and cwd if we reached the end of those elements
        if name == "argument-vector":
            self._parsing_arguments = False
        elif name == "cwd":
            self._parsing_cwd = False
        elif name == "cpu":
            self._parsing_cpu = False
        elif name == "mainjob":
            self._parsing_main_job = False
        elif name == "machine":
            self._parsing_machine = False
        elif name == "signalled":
            self._parsing_signalled = False
        elif name == "statcall":
            if self._parsing_stdout is True:
                self._parsing_stdout = False
            if self._parsing_stderr is True:
                self._parsing_stderr = False
            if self._parsing_final_statcall is True:
                self._parsing_final_statcall = False
            if "outputs" in self._keys:
                if self._lfn in self._keys["outputs"]:
                    # PM-1180 get the statinfo and update with checksum
                    statinfo = self._keys["outputs"][self._lfn]
                    if "checksum" in self._keys:
                        for key in self._keys["checksum"]:
                            statinfo.add_attribute(
                                "checksum." + key, self._keys["checksum"][key]
                            )
                        self._keys["checksum"] = {}

        elif name == "data":
            self._parsing_data = False
        # Now, see if we left one of the job elements
        if (
            name == "setup"
            or name == "prejob"
            or name == "mainjob"
            or name == "postjob"
            or name == "cleanup"
        ):
            self._parsing_job_element = False

    def char_data(self, data=""):
        """
        Function called by the parser whenever there's character data in an element
        """
        if self._parsing_cwd is True:
            self._cwd += data

        if self._parsing_cpu is True:
            if "cpu_model" not in self._keys["cpu"]:
                self._keys["cpu"]["cpu_model"] = ""
            self._keys["cpu"]["cpu_model"] += data

        elif self._parsing_arguments is True:
            self._arguments.append(data.strip())

        elif self._parsing_stdout is True and self._parsing_data is True:
            self._stdout += data

        elif self._parsing_stderr is True and self._parsing_data is True:
            self._stderr += data

        elif self._parsing_signalled is True:
            self._keys["signalled"]["action"] += data


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
