#!/usr/bin/env python3

"""
Pegasus utility for transfer of files during workflow enactment

Usage: pegasus-transfer [options]
"""

##
#  Copyright 2007-2015 University Of Southern California
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

import hashlib
import io
import json
import logging
import math
import optparse
import os
import random
import re
import signal
import socket
import stat
import subprocess
import sys
import tempfile
import threading
import time
import traceback

from Pegasus.tools import worker_utils as utils

try:
    import configparser
except Exception:
    import ConfigParser as configparser

try:
    import queue
except Exception:
    import Queue as queue

try:
    # Python 3.0 and later
    from urllib import parse as urllib
except ImportError:
    # Fall back to Python 2's urllib
    import urllib as urllib


# see https://www.python.org/dev/peps/pep-0469/
try:
    dict.iteritems
except AttributeError:
    # Python 3
    def itervalues(d):
        return iter(d.values())

    def iteritems(d):
        return iter(d.items())


else:
    # Python 2
    def itervalues(d):
        return d.itervalues()

    def iteritems(d):
        return d.iteritems()


# Bad ulimits (for example -s 16000000) can prevent new threads from
# being created. Let's try setting the stack size to something sane
# but not worry if the attempt fails.
try:
    threading.stack_size(2 * 1024 ** 2)
except Exception:
    pass

# panorama real time monitoring
if "KICKSTART_MON_ENDPOINT_URL" in os.environ:
    import base64

    import urllib2

__author__ = "Mats Rynge <rynge@isi.edu>"


# --- regular expressions -----------------------------------------------------

re_parse_comment = re.compile(r"^# +[\w]+ +[\w]+ +([\w\-_]+)")
re_parse_url = re.compile(r"([\w]+)://([\w\.\-:@#]*)(/[\S ]*)?")


# --- classes -----------------------------------------------------------------


class PegasusURL:
    """
    A representation of a URL, including site label and priority
    """

    url = None
    file_type = None
    site_label = ""
    priority = 0
    # the url broken down, for convenience
    proto = ""
    host = ""
    path = ""

    def __init__(self, url, file_type, site_label, priority=0):
        # the url should be URL decoded to work with our shell callouts
        self.url = urllib.unquote(url)
        self.file_type = file_type
        # make the site label to match site labels in the env
        self.site_label = site_label.replace("-", "_")
        self.priority = priority
        # fill out the rest of the members
        self._parse_url()

    def __str__(self):
        return (
            str(self.url)
            + " , host="
            + str(self.host)
            + " , site_label="
            + str(self.site_label)
        )

    def _parse_url(self):

        # default protocol is file://
        if self.url.find(":") == -1:
            logger.debug("URL without protocol (" + self.url + ") - assuming file://")
            self.url = "file://" + self.url

        # file url is a special cases as it can contain relative paths and
        # env vars
        if self.url.find("file:") == 0:
            self.proto = "file"
            # file urls can either start with file://[\w]*/ or file: (no //)
            self.path = re.sub("^file:(//(localhost)?)?", "", self.url)
            self.path = expand_env_vars(self.path)
            return

        # symlink url is a special cases as it can contain relative paths and
        # env vars
        if self.url.find("symlink:") == 0:
            self.proto = "symlink"
            # symlink urls can either start with symlink://[\w]*/ or
            # symlink: (no //)
            self.path = re.sub("^symlink:(//)?", "", self.url)
            self.path = expand_env_vars(self.path)
            return

        # moveto url is a special cases as it can contain relative paths and
        # env vars
        if self.url.find("moveto:") == 0:
            self.proto = "moveto"
            self.path = re.sub("^moveto:(//)?", "", self.url)
            self.path = expand_env_vars(self.path)
            return

        # other than file/symlink urls
        r = re_parse_url.search(self.url)
        if not r:
            raise RuntimeError("Unable to parse URL: %s" % (self.url))

        # Parse successful
        self.proto = r.group(1)
        self.host = r.group(2)
        self.path = r.group(3)

        if self.path is None:
            self.path = ""

        # no double slashes in urls
        self.path = re.sub("//+", "/", self.path)

    def get_url(self):
        # srm-copy is using broken urls - wants an extra /
        if self.proto == "srm" or self.proto == "root":
            return "%s://%s/%s" % (self.proto, self.host, self.path)
        return "%s://%s%s" % (self.proto, self.host, self.path)

    def get_url_encoded(self):
        return "%s://%s%s" % (self.proto, self.host, urllib.quote(self.path))

    def get_url_dirname(self):
        dn = os.path.dirname(self.path)
        return "%s://%s%s" % (self.proto, self.host, dn)


class TransferBase(object):
    """
    Base class for mkdirs, removes and transfers
    """

    def __init__(self):
        self._sub_transfer_index = 0
        self._sub_transfer_count = 0

    def get_sub_transfer_index(self):
        return self._sub_transfer_index

    def move_to_next_sub_transfer(self):
        return


class Mkdir(TransferBase):
    """
    Represents a single mkdir request
    """

    def __init(self):
        super(Mkdir, self).__init__()
        self._target_url = None

    def set_url(self, site_label, url):
        self._target_url = PegasusURL(url, None, site_label)

    def get_url(self):
        return self._target_url.get_url()

    def get_site_label(self):
        return self._target_url.site_label

    def get_proto(self):
        return self._target_url.proto

    def get_host(self):
        return self._target_url.host

    def get_path(self):
        return self._target_url.path


class Remove(TransferBase):
    """
    Represents a single remove request
    """

    def __init(self):
        super(Remove, self).__init__()
        self._target_url = None
        self._recursive = False

    def set_recursive(self, should_recurse):
        if str(should_recurse).lower() in ["yes", "true", "t", "1"]:
            self._recursive = True
            return
        elif str(should_recurse).lower() in ["no", "false", "f", "0"]:
            self._recursive = False
            return
        self._recursive = should_recurse

    def set_url(self, site_label, url):
        self._target_url = PegasusURL(url, None, site_label)

    def get_recursive(self):
        return self._recursive

    def get_url(self):
        return self._target_url.get_url()

    def get_site_label(self):
        return self._target_url.site_label

    def get_proto(self):
        return self._target_url.proto

    def get_host(self):
        return self._target_url.host

    def get_path(self):
        return self._target_url.path

    def __cmp__(self, other):
        """
        compares first on protos, then on hosts, then on paths - useful
        for grouping similar types of transfers
        """
        if cmp(self._target_url.proto, other._target_url.proto) != 0:
            return cmp(self._target_url.proto, other._target_url.proto)
        if cmp(self._target_url.host, other._target_url.host) != 0:
            return cmp(self._target_url.host, other._target_url.host)
        if cmp(self._target_url.path, other._target_url.path) != 0:
            return cmp(self._target_url.path, other._target_url.path)
        return 0

    def __eq__(self, other):
        return (
            self._target_url.proto == other._target_url.proto
            and self._target_url.host == other._target_url.host
            and self._target_url.path == other._target_url.path
        )

    def __lt__(self, other):
        return (
            self._target_url.proto < other._target_url.proto
            or self._target_url.host < other._target_url.host
            or self._target_url.path < other._target_url.path
        )

    def __le__(self, other):
        return self.__lt__(other) or self.__eq__(other)

    def __gt__(self, other):
        return not (self.__lt__(other) or self.__eq__(other))

    def __ge__(self, other):
        return not (self.__lt__(other))


class Transfer(TransferBase):
    """
    Represents a single transfer request.
    """

    def __init__(self):
        """
        Initializes the transfer class
        """
        super(Transfer, self).__init__()

        self.lfn = ""

        self.linkage = "unknown"
        self.verify_symlink_source = True

        self._src_urls = []
        self._dst_urls = []

        self.attempts = 0
        self.allow_grouping = True  # can this transfer be grouped with others?
        self.generate_checksum = False  # should we generate checksums as we transfer?
        self.verify_checksum_remote = (
            False  # should we generate checksums as we transfer?
        )

    def __str__(self):
        return "%s -> %s" % (self._src_urls[0].get_url(), self._dst_urls[0].get_url())

    def set_lfn(self, lfn):
        self.lfn = lfn

    def set_linkage(self, linkage):
        self.linkage = linkage

    def set_verify_symlink_source(self, verify_symlink_source):
        self.verify_symlink_source = verify_symlink_source

    def add_src(self, site_label, url, file_type=None, priority=None):
        if priority is None:
            priority = random.randint(1, 100)
        u = PegasusURL(url, file_type, site_label, priority)
        self._src_urls.append(u)
        # keep the list sorted
        self._src_urls = sorted(
            self._src_urls, key=lambda url: url.priority, reverse=True
        )
        self._update_sub_transfer_count()

    def add_dst(self, site_label, url, file_type=None, priority=None):
        if priority is None:
            priority = random.randint(1, 100)
        u = PegasusURL(url, file_type, site_label, priority)
        self._dst_urls.append(u)
        # keep the list sorted
        self._dst_urls = sorted(
            self._dst_urls, key=lambda url: url.priority, reverse=True
        )
        self._update_sub_transfer_count()

    def src_url(self):
        return self._src_urls[0].get_url()

    def src_url_encoded(self):
        return self._src_urls[0].get_url_encoded()

    def dst_url(self):
        return self._dst_urls[0].get_url()

    def dst_url_encoded(self):
        return self._dst_urls[0].get_url_encoded()

    def dst_url_dirname(self):
        return self._dst_urls[0].get_url_dirname()

    def get_src_site_label(self):
        return self._src_urls[0].site_label

    def get_dst_site_label(self):
        return self._dst_urls[0].site_label

    def get_src_proto(self):
        return self._src_urls[0].proto

    def get_dst_proto(self):
        return self._dst_urls[0].proto

    def get_src_type(self):
        return self._src_urls[0].file_type

    def get_dst_type(self):
        return self._dst_urls[0].file_type

    def get_src_host(self):
        return self._src_urls[0].host

    def get_dst_host(self):
        return self._dst_urls[0].host

    def get_src_path(self):
        return self._src_urls[0].path

    def get_dst_path(self):
        return self._dst_urls[0].path

    def move_to_next_sub_transfer(self):
        """
        cycle to the next available src/dst pair
        """
        # TODO: How should we handle transfers with multiple sources and destinations? How will
        # it affect the number of tries we are willing to do
        t = self._src_urls.pop(0)
        self._src_urls.append(t)
        self._sub_transfer_index += 1
        if self._sub_transfer_index == self._sub_transfer_count:
            self._sub_transfer_index = 0

    def groupable(self):
        return self.allow_grouping

    def __cmp__(self, other):
        """
        compares first on protos, then on hosts, then on paths - useful
        for grouping similar types of transfers
        """
        if cmp(self._src_urls[0].proto, other._src_urls[0].proto) != 0:
            return cmp(self._src_urls[0].proto, other._src_urls[0].proto)
        if cmp(self._dst_urls[0].proto, other._dst_urls[0].proto) != 0:
            return cmp(self._dst_urls[0].proto, other._dst_urls[0].proto)
        if cmp(self._src_urls[0].host, other._src_urls[0].host) != 0:
            return cmp(self._src_urls[0].host, other._src_urls[0].host)
        if cmp(self._dst_urls[0].host, other._dst_urls[0].host) != 0:
            return cmp(self._dst_urls[0].host, other._dst_urls[0].host)
        if cmp(self._src_urls[0].path, other._src_urls[0].path) != 0:
            return cmp(self._src_urls[0].path, other._src_urls[0].path)
        if cmp(self._dst_urls[0].path, other._dst_urls[0].path) != 0:
            return cmp(self._dst_urls[0].path, other._dst_urls[0].path)
        return 0

    def __eq__(self, other):
        return (
            self._src_urls[0].proto == other._src_urls[0].proto
            and self._dst_urls[0].proto == other._dst_urls[0].proto
            and self._src_urls[0].host == other._src_urls[0].host
            and self._dst_urls[0].host == other._dst_urls[0].host
            and self._src_urls[0].path == other._src_urls[0].path
            and self._dst_urls[0].path == other._dst_urls[0].path
        )

    def __lt__(self, other):
        return (
            self._src_urls[0].proto < other._src_urls[0].proto
            or self._dst_urls[0].proto < other._dst_urls[0].proto
            or self._src_urls[0].host < other._src_urls[0].host
            or self._dst_urls[0].host < other._dst_urls[0].host
            or self._src_urls[0].path < other._src_urls[0].path
            or self._dst_urls[0].path < other._dst_urls[0].path
        )

    def __le__(self, other):
        return self.__lt__(other) or self.__eq__(other)

    def __gt__(self, other):
        return not (self.__lt__(other) or self.__eq__(other))

    def __ge__(self, other):
        return not (self.__lt__(other))

    def _update_sub_transfer_count(self):
        self._sub_transfer_count = len(self._src_urls) * len(self._dst_urls)


class TransferHandlerBase(object):
    """
    Base class for all transfer handlers. Derived classes should set the
    protocol map (for example ["http->file"]) and implement the following
    methods:
      do_mkdirs()
      do_transfer()
      do_removes)_
    """

    _name = "BaseHandler"
    _mkdir_cleanup_protocols = []
    _protocol_map = []

    lock = threading.Lock()

    def do_mkdirs(self, mkdir_list):
        """
        Creates the listed URLs - all derived classes should override this
        method
        """
        raise RuntimeErro("do_mkdirs() is not implemented in " + self._name)

    def do_transfers(self, transfer_list):
        """
        Handles a list of transfers - all derived classes should override this
        method

        The method should return 2 lists, one with successful transfer and one
        with failed transfers
        """
        raise RuntimeError("do_transfers() is not implemented in " + self._name)

    def do_removes(self, removes_list):
        """
        Removes the listed URLs - all derived classes should override this
        method
        """
        raise RuntimeError("do_removes() is not implemented in " + self._name)

    def protocol_check(self, src_proto, dst_proto):
        """
        Checks to see if a src/dst protocol pair can be handled by the handler.
        This is the base for the automatic handler detection in the TransferSet
        class.
        """
        if src_proto is None and dst_proto is not None:
            # mkdir or delete
            if dst_proto in self._mkdir_cleanup_protocols:
                return True
            return False
        item = str(src_proto) + "->" + str(dst_proto)
        return item in self._protocol_map

    def _pre_transfer_attempt(self, transfer):
        """
        A common callback from do_transfers for things like integrity checking
        """

        # do we need to calculate integrity checksums?
        if transfer.generate_checksum and transfer.get_src_proto() == "file":
            self._generate_integrity_checksum(
                transfer.lfn, transfer.get_src_path(), transfer.linkage
            )

    def _post_transfer_attempt(self, transfer, was_successful, t_start, t_end=None):
        """
        A common callback from do_transfers to collect statistics for transfers
        """

        if t_end is None:
            t_end = time.time()

        # do we need to calculate integrity checksums?
        if (
            was_successful
            and transfer.generate_checksum
            and transfer.get_dst_proto() == "file"
        ):
            self._generate_integrity_checksum(
                transfer.lfn, transfer.get_dst_path(), transfer.linkage
            )

        # keep track of transfer attempts
        transfer.attempts += 1

        stats.add_stats(transfer, was_successful, t_start, t_end)

        # Introduce errors! The PEGASUS_TRANSFER_ERROR_RATE env variable can
        # be used to introduce transfer error at some rate (valid values in
        # precent: 0-100). This is useful for testing the data integrity
        # detection and failover components of Pegasus. This only works
        # for transfer with a file:// destination.
        if (
            "PEGASUS_TRANSFER_ERROR_RATE" in os.environ
            and transfer.get_dst_proto() == "file"
        ):
            rate = int(os.environ["PEGASUS_TRANSFER_ERROR_RATE"])
            if not (rate >= 0 and rate <= 100):
                logger.error(
                    "Invalid value for PEGASUS_TRANSFER_ERROR_RATE: %s"
                    % (os.environ["PEGASUS_TRANSFER_ERROR_RATE"])
                )
                rate = 0
            random.seed()
            if random.randint(1, 100) <= rate:
                # modify the file by adding a few bytes to it
                logger.info("Introducing an error into %s" % (transfer.get_dst_path()))
                try:
                    f = open(transfer.get_dst_path(), "a")
                    f.write("Here be dragons! Eh, I mean pegai!")
                    f.close()
                except Exception as e:
                    logger.error(e)

    def _generate_integrity_checksum(self, lfn, fname, linkage):
        """
        Call out to pegasus-integrity to generate a checksum for the file.
        The information goes to $KICKSTART_INTEGRITY_DATA so that kickstart
        can pick it up at the end.
        """
        global integrity_checksummed

        tools = utils.Tools()

        if tools.find("pegasus-integrity", "help", None, [prog_dir]) is None:
            logger.error(
                "Unable to do integrity checking because pegasus-integrity not found"
            )
            return

        if lfn is None or lfn == "":
            logger.error("lfn is required when enabling checksumming")
            return

        if lfn in integrity_checksummed:
            return

        self.lock.acquire()
        cmd = '%s --generate-fullstat-yaml="%s=%s"' % (
            tools.full_path("pegasus-integrity"),
            lfn,
            fname,
        )
        try:
            tc = utils.TimedCommand(cmd)
            tc.run()
            # track what lfns we have checksummed so far
            integrity_checksummed.append(lfn)
        except RuntimeError as err:
            logger.error(err)
        finally:
            # do not do any timing here - it is in the kickstart record
            # stats.add_integrity_generate(linkage, tc.get_duration())
            self.lock.release()

    def _check_similar(self, a, b):
        """
        compares two transfers, and determines if they are similar enough to be
        grouped together in one transfer input file - this is used by some
        protocols who can run grouped transfers as a set with a single command
        """
        if isinstance(a, Transfer) and isinstance(b, Transfer):
            if a.get_src_host() != b.get_src_host():
                return False
            if a.get_dst_host() != b.get_dst_host():
                return False
            if os.path.dirname(a.get_src_path()) != os.path.dirname(b.get_src_path()):
                return False
            if os.path.dirname(a.get_dst_path()) != os.path.dirname(b.get_dst_path()):
                return False
            if a.generate_checksum != b.generate_checksum:
                return False

            # also check that we are not renaming the files
            if os.path.basename(a.get_src_path()) != os.path.basename(a.get_dst_path()):
                return False
            if os.path.basename(b.get_src_path()) != os.path.basename(b.get_dst_path()):
                return False

            return True
        elif isinstance(a, Remove) and isinstance(b, Remove):
            if a.get_host() != b.get_host():
                return False
            if os.path.dirname(a.get_path()) != os.path.dirname(b.get_path()):
                return False
            return True
        return False

    def _similar_groups(self, in_list, max_transfers_in_group=None):
        """
        returns a list of lists of grouped transfers according to the
        _check_similar() function

        """
        out_lists = []
        curr_list = []
        prev = None

        # first list
        out_lists.append(curr_list)

        while len(in_list) > 0:

            transfer = in_list.pop()

            # do we need a new list based on max size?
            # do we need a new list because of dissimilarities?
            if (
                max_transfers_in_group is not None
                and len(curr_list) == max_transfers_in_group
            ) or (prev is not None and not self._check_similar(transfer, prev)):
                curr_list = []
                out_lists.append(curr_list)

            curr_list.append(transfer)
            prev = transfer

        return out_lists

    def _verify_read_access(self, path):
        """
        Sometimes we need to verify that a local file exists, and that we have
        read acess. Note that because of access mechanisms on some of the file
        systems we have to deal with, such as CVMFS, checking POSIX permissions
        is not enough. Here we try to open() the file to make sure it works.
        """
        if not os.path.exists(path):
            return False

        # for non-zero sized files, try to read a little bit at the beginning
        check_bytes = 0
        try:
            s = os.stat(path)
            check_bytes = min(s[stat.ST_SIZE], 1024)
        except Exception:
            return False

        try:
            f = open(path, "rb")
            if check_bytes > 0:
                logger.debug(
                    "Reading %d bytes from %s to make sure the file is accessible"
                    % (check_bytes, path)
                )
                f.read(check_bytes)
            f.close()
        except Exception:
            return False

        return True


class FileHandler(TransferHandlerBase):
    """
    Uses system commands to act on file:// URLs
    """

    _name = "FileHandler"
    _mkdir_cleanup_protocols = ["file"]
    _protocol_map = ["file->file"]

    def do_mkdirs(self, transfers):
        successful_l = []
        failed_l = []
        for t in transfers:
            cmd = "/bin/mkdir -p '%s' " % (t.get_path())
            try:
                tc = utils.TimedCommand(cmd)
                tc.run()
            except RuntimeError as err:
                logger.error(err)
                failed_l.append(t)
                continue
            successful_l.append(t)
        return [successful_l, failed_l]

    def do_transfers(self, transfers):
        successful_l = []
        failed_l = []
        for t in transfers:
            t_start = time.time()
            prepare_local_dir(os.path.dirname(t.get_dst_path()))

            # src has to exist and be readable
            if not verify_local_file(t.get_src_path()):
                failed_l.append(t)
                self._post_transfer_attempt(t, False, t_start)
                continue

            self._pre_transfer_attempt(t)

            if os.path.exists(t.get_src_path()) and os.path.exists(t.get_dst_path()):
                # make sure src and target are not the same file - have to
                # compare at the inode level as paths can differ
                src_inode = os.stat(t.get_src_path())[stat.ST_INO]
                dst_inode = os.stat(t.get_dst_path())[stat.ST_INO]
                if src_inode == dst_inode:
                    logger.warning(
                        "cp: src (%s) and dst (%s) already exists and are the same file"
                        % (t.get_src_path(), t.get_dst_path())
                    )
                    successful_l.append(t)
                    self._post_transfer_attempt(t, True, t_start)
                    continue

            # first check that the src file exists, and that it can be opened
            if t.verify_symlink_source and not self._verify_read_access(
                t.get_src_path()
            ):
                logger.error(
                    "ln: src (%s) does not exist, or we do not have permission to read it"
                    % t.get_src_path()
                )
                failed_l.append(t)
                self._post_transfer_attempt(t, False, t_start)
                continue

            # some of the time, PegasusLite can tell us to take shortcut and symlink the files
            if symlink_file_transfer:
                cmd = "ln -f -s '%s' '%s'" % (t.get_src_path(), t.get_dst_path())
            else:
                cmd = "/bin/cp -f -R -L '%s' '%s'" % (
                    t.get_src_path(),
                    t.get_dst_path(),
                )
            try:
                tc = utils.TimedCommand(cmd)
                tc.run()
            except RuntimeError as err:
                logger.error(err)
                failed_l.append(t)
                self._post_transfer_attempt(t, False, t_start)
                continue
            successful_l.append(t)
            self._post_transfer_attempt(t, True, t_start)

        return [successful_l, failed_l]

    def do_removes(self, transfers):
        successful_l = []
        failed_l = []
        for t in transfers:
            cmd = "/bin/rm -f"
            if t.get_recursive():
                cmd += " -r "
            cmd += " '%s' " % (t.get_path())
            try:
                tc = utils.TimedCommand(cmd)
                tc.run()
            except RuntimeError as err:
                logger.error(err)
                failed_l.append(t)
                continue
            successful_l.append(t)
        return [successful_l, failed_l]


class GridFtpHandler(TransferHandlerBase):
    """
    Transfers to/from and between GridFTP servers
    """

    _name = "GridFtpHandler"
    _mkdir_cleanup_protocols = ["gsiftp", "sshftp"]
    _protocol_map = [
        "file->gsiftp",
        "gsiftp->file",
        "gsiftp->gsiftp",
        "ftp->ftp",
        "ftp->gsiftp",
        "gsiftp->ftp",
        "file->sshftp",
        "sshftp->file",
        "sshftp->sshftp",
    ]

    def do_mkdirs(self, mkdirs_l):
        successful_l = []
        failed_l = []

        # different tools depending on if this a sshftp or gsiftp request
        if mkdirs_l[0].get_proto() == "gsiftp":

            tools = utils.Tools()

            # prefer gfal if installed
            if (
                "PEGASUS_FORCE_GUC" not in os.environ
                and tools.find("gfal-mkdir", "--version", "\(gfal2 ([\.0-9]+)\)")
                is not None
            ):
                handler = GFALHandler()
                [successful_l, failed_l] = handler.do_mkdirs(mkdirs_l)
                return [successful_l, failed_l]

            if tools.find("globus-url-copy") is None:
                logger.error(
                    "Unable to do gsiftp mkdir because gfal-mkdir/globus-url-copy could not be found"
                )
                return [[], mkdirs_l]

            for target in mkdirs_l:

                env = {}

                key = "X509_USER_PROXY_" + target.get_site_label()
                if key in os.environ:
                    env["X509_USER_PROXY"] = os.environ[key]
                key = "SSH_PRIVATE_KEY_" + target.get_site_label()
                if key in os.environ:
                    env["SSH_PRIVATE_KEY"] = os.environ[key]

                success = False

                cmd = tools.full_path("globus-url-copy")
                cmd += " -create-dest"
                cmd += " file:///dev/null"
                cmd += " " + target.get_url() + "/.create-dir"
                try:
                    tc = utils.TimedCommand(cmd, env_overrides=env)
                    tc.run()
                    success = True
                except RuntimeError as err:
                    logger.error(err)

            if success:
                successful_l.append(target)
            else:
                failed_l.append(target)
        else:
            handler = ScpHandler()
            [successful_l, failed_l] = handler.do_mkdirs(mkdirs_l)

        return [successful_l, failed_l]

    def do_transfers(self, transfers_l):
        """
        gsiftp - gdal or globus-url-copy
        """
        successful_l = []
        failed_l = []

        if len(transfers_l) == 0:
            return

        tools = utils.Tools()

        # prefer gfal if installed
        if (
            "PEGASUS_FORCE_GUC" not in os.environ
            and transfers_l[0].get_src_proto() != "sshftp"
            and transfers_l[0].get_dst_proto() != "sshftp"
            and tools.find("gfal-copy", "--version", "\(gfal2 ([\.0-9]+)\)") is not None
        ):
            handler = GFALHandler()
            [successful_l, failed_l] = handler.do_transfers(transfers_l)
            return [successful_l, failed_l]

        if tools.find("globus-url-copy", "-version", "([0-9]+\.[0-9]+)") is None:
            logger.error(
                "Unable to do gsiftp transfers because gfal-copy/globus-url-copy could not be found"
            )
            return [[], transfers_l]

        # create lists with similar (same src host/path, same dst host/path)
        # url pairs
        while len(transfers_l) > 0:

            similar_list = []

            curr = transfers_l.pop()
            prev = curr
            third_party = (
                curr.get_src_proto() == "gsiftp" and curr.get_dst_proto() == "gsiftp"
            ) or (curr.get_src_proto() == "sshftp" and curr.get_dst_proto() == "sshftp")

            while self._check_similar(curr, prev):

                similar_list.append(curr)

                if len(transfers_l) == 0:
                    break
                else:
                    prev = curr
                    curr = transfers_l.pop()

            if not self._check_similar(curr, prev):
                # the last pair is not part of the set and needs to be added
                # back to the beginning of the list
                transfers_l.insert(0, curr)

            if len(similar_list) == 0:
                break

            if len(similar_list) > 1:
                logger.debug(
                    "%d similar gsiftp transfers grouped together" % (len(similar_list))
                )

            # we now have a list of similar transfers - break up and send the
            # first one with create dir and the rest with no create dir options
            first = similar_list.pop()
            attempt = first.attempts
            mkdir_done = self._exec_transfers([first], attempt, True, third_party)

            # first attempt get some extra tries - this is to drill down on
            # guc options
            if attempt == 0 and not mkdir_done:
                mkdir_done = self._exec_transfers([first], attempt, True, third_party)
                if not mkdir_done:
                    mkdir_done = self._exec_transfers(
                        [first], attempt, True, third_party
                    )

            if mkdir_done:
                successful_l.append(first)
                # run the rest of the group - but limit the number of entries
                # for each pipeline
                chunks = self._split_similar(similar_list)
                for l in chunks:
                    if self._exec_transfers(l, attempt, False, third_party):
                        for i, t in enumerate(l):
                            successful_l.append(t)
                    else:
                        for i, t in enumerate(l):
                            failed_l.append(t)
            else:
                # mkdir job failed - all subsequent jobs will fail
                failed_l.append(first)
                for i, t in enumerate(similar_list):
                    failed_l.append(t)

        return [successful_l, failed_l]

    def do_removes(self, removes_l):
        """
        use gfal is available, otherwise use guc which is
        implemented as copy of /dev/null over the file we want to delete
        """

        successful_l = []
        failed_l = []

        if len(removes_l) == 0:
            return

        # prefer gfal if available
        if (
            "PEGASUS_FORCE_GUC" not in os.environ
            and removes_l[0].get_proto() != "sshftp"
            and tools.find("gfal-rm", "--version", "\(gfal2 ([\.0-9]+)\)") is not None
        ):
            handler = GFALHandler()
            [successful_l, failed_l] = handler.do_removes(removes_l)
            return [successful_l, failed_l]

        # ignore recursive deletes
        if removes_l[0].get_recursive():
            return [removes_l, []]

        # convert the rm to transfer of /dev/null
        transfer_l = []
        for r in removes_l:
            t = Transfer()
            t.add_src("local", "file:///dev/null")
            t.add_dst(r.get_site_label(), r.get_url())
            transfer_l.append(t)

        [s, f] = self.do_transfers(transfer_l)
        if len(f) > 0:
            return [[], removes_l]
        return [removes_l, []]

    def _exec_transfers(self, transfers, attempt, create_dest, third_party):
        """
        sub to do_transfers() - transfers a list of urls
        """
        global gsiftp_failures
        env = {}

        # if srcs are local, verify them first
        local_ok = True
        for t in transfers:
            self._pre_transfer_attempt(t)
            if t.get_src_proto() == "file":
                # src has to exist and be readable
                if not verify_local_file(t.get_src_path()):
                    local_ok = False
        if not local_ok:
            for t in transfers:
                self._post_transfer_attempt(t, False, time.time())
            return False

        # create tmp file with transfer src/dst pairs
        num_pairs = 0
        try:
            tmp_fd, tmp_name = tempfile.mkstemp(
                prefix="pegasus-transfer-", suffix=".lst"
            )
            tmp_file = io.open(tmp_fd, "w+")
        except Exception:
            raise RuntimeError(
                "Unable to create tmp file for" + " globus-url-copy transfers"
            )

        for i, t in enumerate(transfers):
            num_pairs += 1
            logger.debug("   adding %s %s" % (t.src_url_encoded(), t.dst_url_encoded()))
            tmp_file.write("%s %s\n" % (t.src_url_encoded(), t.dst_url_encoded()))

        tmp_file.close()

        if num_pairs > 1:
            logger.info(
                "Grouped %d similar gsiftp transfers together in"
                " temporary file %s" % (num_pairs, tmp_name)
            )

        t_start = time.time()
        transfer_success = False

        # for transfer to file://, run a normal mkdir command instead of
        # having g-u-c use -create-dest
        if transfers[0].get_dst_proto() == "file":
            prepare_local_dir(os.path.dirname(transfers[0].get_dst_path()))
            # override the create_dest flag as the directory now exists
            create_dest = False

        # build command line for globus-url-copy
        tools = utils.Tools()
        cmd = tools.full_path("globus-url-copy")

        # credential handling
        cmd_creds, data_cred_req, env = self._guc_creds(transfers[0], third_party)
        cmd += cmd_creds

        # options
        cmd += self._guc_options(attempt, create_dest, third_party, data_cred_req)

        cmd += " -f " + tmp_name
        try:
            tc = utils.TimedCommand(cmd, env_overrides=env)
            tc.run()
            transfer_success = True
        except Exception as err:
            logger.error(err)
            gsiftp_failures += 1

        # as we don't know which transfers in the set succeeded/failed, we have
        # to mark them all the same (for example, one failure means all transfers
        # gets marked as failed)
        for t in transfers:
            self._post_transfer_attempt(t, transfer_success, t_start)

        os.unlink(tmp_name)

        return transfer_success

    def _guc_creds(self, sample_transfer, third_party):
        """
        determine and set up credentials for the transfer
        """

        cmd = ""
        data_cred_req = False
        env = {}

        # gsi credentials for gsiftp transfers, ssh credentials
        # for sshftp

        if (
            sample_transfer.get_src_proto() == "gsiftp"
            or sample_transfer.get_dst_proto() == "gsiftp"
        ):
            # separate credentials for src and dst? note that if one is
            # set, so must the other even if a credential is not required
            src_cred = None
            dst_cred = None
            key = "X509_USER_PROXY_" + sample_transfer.get_src_site_label()
            if key in os.environ:
                src_cred = os.environ[key]
            key = "X509_USER_PROXY_" + sample_transfer.get_dst_site_label()
            if key in os.environ:
                dst_cred = os.environ[key]

            # only set src-cred / dst-cred if at least one is specified
            if src_cred is not None or dst_cred is not None:

                if src_cred is None:
                    if "X509_USER_PROXY" in os.environ:
                        src_cred = os.environ["X509_USER_PROXY"]
                    else:
                        src_cred = dst_cred

                if dst_cred is None:
                    if "X509_USER_PROXY" in os.environ:
                        dst_cred = os.environ["X509_USER_PROXY"]
                    else:
                        dst_cred = src_cred

                cmd += " -src-cred " + src_cred + " -dst-cred " + dst_cred
                check_cred_fs_permissions(src_cred)
                check_cred_fs_permissions(dst_cred)

            # if the src and dest credentials are different, we might need
            # a data channel credential
            if third_party and src_cred != dst_cred:
                data_cred_req = True

        else:
            # sshftp transfers
            if sample_transfer.get_dst_proto() == "file":
                # sshftp -> file
                key = "SSH_PRIVATE_KEY_" + sample_transfer.get_src_site_label()
                if key in os.environ:
                    env["SSH_PRIVATE_KEY"] = os.environ[key]
            else:
                # file -> sshftp
                key = "SSH_PRIVATE_KEY_" + sample_transfer.get_dst_site_label()
                if key in os.environ:
                    env["SSH_PRIVATE_KEY"] = os.environ[key]
            check_cred_fs_permissions(env["SSH_PRIVATE_KEY"])

        return cmd, data_cred_req, env

    def _guc_options(self, attempt, create_dest, third_party, data_cred_req):
        """
        determine a set of globus-url-copy options based on how previous
        transfers went
        """
        global gsiftp_failures

        utils.Tools()
        options = " -r"

        # make output from guc match our current log level
        if logger.isEnabledFor(logging.DEBUG):
            options += " -verbose"

        # should we try to create directories?
        if create_dest:
            options += " -create-dest"

        # Only do third party transfers for gsiftp->gsiftp. For other
        # combinations, fall back to settings which will for well over for
        # example NAT
        if third_party:

            # pipeline is experimental so only allow this for the first attempt
            if gsiftp_failures == 0:
                options += " -pipeline"

            # parallism
            options += " -parallel 4"

            # -fast should be supported by all servers today
            options += " -fast"

            if data_cred_req:
                if gsiftp_failures == 0:
                    options += " -data-cred auto"
                else:
                    options += " -no-data-channel-authentication"
        else:
            # gsiftp<->file transfers
            options += " -no-third-party-transfers" + " -no-data-channel-authentication"

        return options

    def _check_similar(self, a, b):
        """
        compares two url_pairs, and determins if they are similar enough to be
        grouped together in one transfer input file
        """
        if a.get_src_host() != b.get_src_host():
            return False
        if a.get_dst_host() != b.get_dst_host():
            return False
        if os.path.dirname(a.get_src_path()) != os.path.dirname(b.get_src_path()):
            return False
        if os.path.dirname(a.get_dst_path()) != os.path.dirname(b.get_dst_path()):
            return False
        return True

    def _split_similar(self, full_list):
        """
        splits up a long list of similar transfers into smaller
        pieces which can easily be handled by g-u-c
        """
        chunks = []
        size = 1000
        num_chunks = int(math.ceil(len(full_list) / float(size)))
        for i in range(num_chunks):
            start = i * size
            end = min((i + 1) * size, len(full_list))
            chunks.append(full_list[start:end])
        return chunks


class HttpHandler(TransferHandlerBase):
    """
    pulls from http/https/ftp using wget or curl
    """

    _name = "HttpHandler"
    _mkdir_cleanup_protocols = []
    _protocol_map = ["http->file", "https->file", "ftp->file"]

    def do_transfers(self, transfers):

        tools = utils.Tools()
        env = {}

        # Open Science Grid sites can inform us about local Squid proxies
        if "OSG_SQUID_LOCATION" in os.environ and not "http_proxy" in os.environ:
            env["http_proxy"] = os.environ["OSG_SQUID_LOCATION"]

        # but only allow squid caching for the first try - after that go to
        # the source
        if transfers[0].attempts >= 1 and (
            "http_proxy" in env or "http_proxy" in os.environ
        ):
            logger.info("Disabling HTTP proxy due to previous failures")
            if "http_proxy" in env:
                del env["http_proxy"]
            if "http_proxy" in os.environ:
                del os.environ["http_proxy"]

        if (
            tools.find("wget", "--version", "([0-9]+\.[0-9]+)") is None
            and tools.find("curl", "--version", " ([0-9]+\.[0-9]+)") is None
        ):
            logger.error(
                "Unable to do http/https transfers because neither curl nor wget could not be found"
            )
            return [[], transfers]

        successful_l = []
        failed_l = []
        for t in transfers:

            self._pre_transfer_attempt(t)

            prepare_local_dir(os.path.dirname(t.get_dst_path()))

            # try wget first, then curl
            if tools.full_path("wget") is not None:
                cmd = tools.full_path("wget")
                if logger.isEnabledFor(logging.DEBUG):
                    cmd += " -v"
                else:
                    cmd += " -nv"
                cmd += (
                    " --no-cookies --no-check-certificate"
                    + " --timeout=300 --tries=1"
                    + " -O '"
                    + t.get_dst_path()
                    + "'"
                    + " '"
                    + t.src_url()
                    + "'"
                )
            else:
                cmd = tools.full_path("curl")
                if not logger.isEnabledFor(logging.DEBUG):
                    cmd += " -s -S"
                cmd += (
                    " --insecure --location"
                    + " -o '"
                    + t.get_dst_path()
                    + "'"
                    + " '"
                    + t.src_url()
                    + "'"
                )

            t_start = time.time()
            try:
                tc = utils.TimedCommand(cmd, env_overrides=env)
                tc.run()
                stats_add(t.get_dst_path())
            except RuntimeError as err:
                logger.error(err)
                # wget and curl might leave 0 sized files behind after failures
                # make sure those get cleaned up
                try:
                    os.unlink(transfer.get_dst_path())
                except Exception:
                    pass
                self._post_transfer_attempt(t, False, t_start)
                failed_l.append(t)
                continue
            self._post_transfer_attempt(t, True, t_start)
            successful_l.append(t)

        return [successful_l, failed_l]


class HPSSHandler(TransferHandlerBase):
    """
    Pulls from HPSS using htar
    Sets up credential for transfer based on environment variable HPSS_CREDENTIAL.
    """

    _name = "HPSSHandler"
    _mkdir_cleanup_protocols = []
    _protocol_map = ["hpss->file", "hpps->file"]

    def _setup_creds(self):
        """
        Set up credentials for transfer based on an environment variable HPSS_CREDENTIAL
        If set copies credential to the default credential location  $HOME/.netrc
        If  not specified, makes sure the default credential $HOME/.netrc is available
        """

        user_defined_cred = (
            os.environ["HPSS_CREDENTIAL"] if "HPSS_CREDENTIAL" in os.environ else None
        )
        default_cred = os.path.join(os.environ["HOME"], ".netrc")

        if user_defined_cred:
            # user defined credential exists and is a file
            check_cred_fs_permissions(user_defined_cred)

            if os.path.exists(default_cred):
                # first check if they are not the same file
                src_inode = os.stat(user_defined_cred)[stat.ST_INO]
                dst_inode = os.stat(default_cred)[stat.ST_INO]
                if src_inode == dst_inode:
                    logger.warning(
                        "user supplied credential %s in environment points to the default credential path %s"
                        % (user_defined_cred, default_cred)
                    )
                    return

            # copy user defined credential to default credential path
            cmd = "/bin/cp -f '%s' '%s'" % (user_defined_cred, default_cred)
            try:
                tc = utils.TimedCommand(cmd)
                tc.run()
            except RuntimeError as err:
                raise Exception(err)
        else:
            check_cred_fs_permissions(default_cred)

        return

    def check_cred_fs_permissions(path):
        """
        Checks to make sure a given credential exists and is protected
        by the file system permissions.
        """
        if not os.path.exists(path):
            raise Exception("HPSS Credential file %s does not exist" % (path))
        if (os.stat(path).st_mode & 0o777) != 0o600:
            logger.warning("%s found to have weak permissions. chmod to 0600." % (path))
            os.chmod(path, 0o600)

    def do_transfers(self, transfers):

        tools = utils.Tools()

        if tools.find("htar") is None:
            logger.error("Unable to do hpss transfers because htar could not be found")
            return [[], transfers]

        successful_l = []
        failed_l = []

        # PM-1378 make sure credentials are setup
        self._setup_creds()

        # create lists with similar tar file names
        # url pairs
        while len(transfers) > 0:

            similar_list = []

            curr = transfers.pop()
            prev = curr

            while self._check_similar(curr, prev):

                similar_list.append(curr)

                if len(transfers) == 0:
                    break
                else:
                    prev = curr
                    curr = transfers.pop()

            if not self._check_similar(curr, prev):
                # the last pair is not part of the set and needs to be added
                # back to the beginning of the list
                transfers.insert(0, curr)

            if len(similar_list) == 0:
                break

            logger.debug(
                "%d similar hpss transfers grouped together" % (len(similar_list))
            )

            # we now have a list of similar transfers
            # break in chunks based on destination directory
            chunks = self._split_similar(similar_list)
            logger.debug(
                "similar hpss transfers broken into %d chunks based on destination directory"
                % (len(chunks))
            )
            for l in chunks:
                successful, failed = self._exec_transfers(l)
                for t in successful:
                    successful_l.append(t)
                for t in failed:
                    failed_l.append(t)

        return [successful_l, failed_l]

    def _exec_transfers(self, transfers):
        """
        Actually execute the transfers using htar command
        :param transfers:
        :return:
        """
        successful_l = []
        failed_l = []

        # create tmp file with transfer src/dst pairs
        num_pairs = 0
        try:
            tmp_fd, tmp_name = tempfile.mkstemp(
                prefix="pegasus-transfer-", suffix=".lst"
            )
            tmp_file = io.open(tmp_fd, "w+")
        except Exception:
            raise RuntimeError("Unable to create tmp file for" + " hpss transfers")

        tar = self._get_tar_file(transfers[0].get_src_path())

        # guess the destination directory based on the first LFN
        destination_dir = self._compute_destination_directory(transfers[0])

        files_to_move = {}  # dict indexed by LFN and value as src file in hpss tar
        for i, t in enumerate(transfers):
            num_pairs += 1
            src_file = self._get_file_in_tar(t.get_src_path())
            if src_file != t.lfn:
                # we need post transfer move as directory needs to be flattened
                # for example hpss/set2/f.c from tar has to be moved to f.c
                logger.debug(
                    "file %s from tar has to be moved to %s" % (src_file, t.lfn)
                )
                files_to_move[t.lfn] = src_file

            dir = self._compute_destination_directory(t)
            if dir is None or dir != destination_dir:
                logger.error(
                    "Destination directory %s for transfer %s does not match directory %s"
                    % (dir, t.lfn, destination_dir)
                )
            logger.debug("   adding %s " % src_file)
            tmp_file.write("%s \n" % (src_file))

        tmp_file.close()

        if num_pairs > 1:
            logger.info(
                "Grouped %d similar gsiftp transfers together in"
                " temporary file %s for extracting from tar %s"
                % (num_pairs, tmp_name, tar)
            )

        # build command line for htar
        transfer_success = False
        t_start = time.time()
        tools = utils.Tools()
        cmd = tools.full_path("htar")

        # no credential handling for time being

        # options
        cmd += " -xvf " + tar + " -L " + tmp_name

        # todo enable checksum verification -Hverify=crc

        try:
            prepare_local_dir(os.path.dirname(destination_dir))
            logger.debug("Executing command " + cmd)
            tc = utils.TimedCommand(cmd, cwd=destination_dir)
            tc.run()
            transfer_success = True
        except Exception as err:
            logger.error(err)

        if transfer_success:
            # htar always returns success even if a file does not exist in the archive
            # check for destination files to make sure and mark accordingly
            for t in transfers:

                # check if file has to be moved after untarring
                if t.lfn in files_to_move:
                    # mv src_file to t.lfn
                    src_file = os.path.join(destination_dir, files_to_move[t.lfn])
                    dst_file = os.path.join(destination_dir, t.lfn)
                    # account for deep LFN
                    prepare_local_dir(os.path.dirname(dst_file))
                    try:
                        os.rename(src_file, dst_file)
                    except Exception as err:
                        # only log. we will catch subsequently in verify_local_file
                        logger.error(
                            "Error renaming %s to %s :%s" % (src_file, dst_file, err)
                        )

                if verify_local_file(t.get_dst_path()):
                    self._post_transfer_attempt(t, True, t_start)
                    successful_l.append(t)
                else:
                    self._post_transfer_attempt(t, False, t_start)
                    failed_l.append(t)
        else:
            # htar command failed. mark all as failed
            for t in transfers:
                self._post_transfer_attempt(t, False, t_start)
                failed_l.append(t)

        os.unlink(tmp_name)

        return successful_l, failed_l

    def _compute_destination_directory(self, t):
        """
        Compute a directory from the path based on the LFN of the file transfer
        and the destination directory path
        :param t:

        :return:
        """

        lfn = t.lfn
        if lfn is None:
            return None
        path = t.get_dst_path()
        if path is None:
            return None

        return path[: path.find(lfn)]

    def _get_tar_file(self, url):
        """
        Returns the hpss tar file embedded in the URL
        :param url:
        :return:
        """

        # tar file should be the first component of the path
        tar = url
        parent = os.path.dirname(tar)
        while parent:
            if parent == "/":
                break
            tar = parent
            parent = os.path.dirname(tar)

        # remove leading / if any
        tar = os.path.basename(tar)

        if not tar.endswith(".tar"):
            logger.error("Unable to determine HPSS tar from URL %s", url)
            return None

        return tar

    def _get_file_in_tar(self, path):
        """
        Extracts the filename of the file from the URL
        :param path:
        :return: name of the file else None
        """

        # return None if unable to determine tar file
        tar = self._get_tar_file(path)
        if tar is None:
            return None

        file = path[path.find(tar) + len(tar) :]

        # remove leading slash
        if file[0] == "/":
            return file[1:]

    def _check_similar(self, a, b):
        """
        compares two url_pairs, and determines if they are similar enough to be
        grouped together in one transfer input file
        For HPSS we group them on the same tar name
        """

        if self._get_tar_file(a.get_src_path()) == self._get_tar_file(b.get_src_path()):
            return True

        return False

    def _compare_urls(self, url1, url2):
        """
        Compares two HPSS URLs based on destination directory
        :param item2:
        :return:
        """

        dir1 = self._compute_destination_directory(url1)
        dir2 = self._compute_destination_directory(url2)

        if dir1 < dir2:
            return -1
        elif dir1 > dir2:
            return 1
        else:
            return 0

    def _split_similar(self, full_list):
        """
        splits up a long list of similar transfers and group them by
        destination directory
        """

        # first sort on destination directories
        sorted_list = sorted(full_list, cmp=self._compare_urls)

        # chunks are created based on destination directory
        start = 0
        end = 0
        chunks = []
        prev = self._compute_destination_directory(sorted_list[0])
        for t in sorted_list:
            curr = self._compute_destination_directory(t)
            if prev != curr:
                chunks.append(sorted_list[start:end])
                start = end

            end += 1
            prev = curr

        if start != end:
            # grab the last chunk
            chunks.append(sorted_list[start:end])

        return chunks


class IRodsHandler(TransferHandlerBase):
    """
    Handler for iRods - http://www.irods.org/
    """

    _name = "IRodsHandler"
    _mkdir_cleanup_protocols = ["irods"]
    _protocol_map = ["file->irods", "irods->file"]

    def do_mkdirs(self, mkdir_list):

        tools = utils.Tools()
        if tools.find("iget", "-h", "Version[ \t]+([\.0-9a-zA-Z]+)") is None:
            logger.error(
                "Unable to do irods transfers becuase iget could not be found in the current path"
            )
            return [[], mkdir_list]

        try:
            env = self._irods_login(mkdir_list[0].get_site_label())
        except Exception as loginErr:
            logger.error(loginErr)
            logger.error("Unable to log into irods")
            return [[], mkdir_list]

        successful_l = []
        failed_l = []
        for t in mkdir_list:

            cmd = "imkdir"
            if "IRODS_TICKET" in env:
                cmd += " -t " + env["IRODS_TICKET"]
            cmd += " -p '" + t.get_path() + "'"
            try:
                tc = utils.TimedCommand(cmd, env_overrides=env)
                tc.run()
            except RuntimeError as err:
                logger.error(err)
                failed_l.append(t)
                continue
            successful_l.append(t)

        return [successful_l, failed_l]

    def do_transfers(self, transfer_list):
        """
        irods - use the icommands to interact with irods
        """

        tools = utils.Tools()
        if tools.find("iget", "-h", "Version[ \t]+([\.0-9a-zA-Z]+)") is None:
            logger.error(
                "Unable to do irods transfers becuase iget could not be found in the current path"
            )
            return [[], transfer_list]

        successful_l = []
        failed_l = []
        for t in transfer_list:

            self._pre_transfer_attempt(t)

            # log in to irods
            env = None
            sitename = t.get_src_site_label()
            if t.get_dst_proto() == "irods":
                sitename = t.get_dst_site_label()
            try:
                env = self._irods_login(sitename)
            except Exception as loginErr:
                logger.error(loginErr)
                logger.error("Unable to log into irods")
                return [[], transfer_list]

            if t.get_dst_proto() == "file":
                # irods->file
                prepare_local_dir(os.path.dirname(t.get_dst_path()))
                cmd = "iget -v -f -T -K -N 4"
                if len(t.get_src_host()) > 0 and t.attempts <= 1:
                    cmd += " -R " + t.get_src_host()
                if "IRODS_TICKET" in env:
                    cmd += " -t " + env["IRODS_TICKET"]
                cmd += " '" + t.get_src_path() + "'"
                cmd += " '" + t.get_dst_path() + "'"
            else:
                # file->irods
                # src has to exist and be readable
                if not verify_local_file(t.get_src_path()):
                    failed_l.append(t)
                    self._post_transfer_attempt(t, False, t_start)
                    continue
                cmd = "imkdir -p '" + os.path.dirname(t.get_dst_path()) + "'"
                try:
                    tc = utils.TimedCommand(
                        cmd, env_overrides=env, timeout_secs=10 * 60
                    )
                    tc.run()
                except Exception:
                    # ignore errors from the mkdir command
                    pass
                cmd = "iput -v -f -T -K -N 4"
                if len(t.get_dst_host()) > 0 and t.attempts <= 1:
                    cmd += " -R " + t.get_dst_host()
                if "IRODS_TICKET" in env:
                    cmd += " -t " + env["IRODS_TICKET"]
                cmd += " '" + t.get_src_path() + "'"
                cmd += " '" + t.get_dst_path() + "'"

            t_start = time.time()
            try:
                tc = utils.TimedCommand(cmd, env_overrides=env)
                tc.run()
                # stats
                if t.get_dst_proto() == "file":
                    stats_add(t.get_dst_path())
                else:
                    stats_add(t.get_src_path())
            except RuntimeError as err:
                logger.error(err)
                self._post_transfer_attempt(t, False, t_start)
                failed_l.append(t)
                continue
            self._post_transfer_attempt(t, True, t_start)
            successful_l.append(t)

        return [successful_l, failed_l]

    def do_removes(self, removes_list):
        tools = utils.Tools()
        if tools.find("iget", "-h", "Version[ \t]+([\.0-9a-zA-Z]+)") is None:
            logger.error(
                "Unable to do irods transfers becuase iget could not be found in the current path"
            )
            return [[], removes_list]

        try:
            env = self._irods_login(removes_list[0].get_site_label())
        except Exception as loginErr:
            logger.error(loginErr)
            logger.error("Unable to log into irods")
            return [[], removes_list]

        successful_l = []
        failed_l = []
        for t in removes_list:

            cmd = "irm -f"
            if t.get_recursive():
                cmd += " -r"
            if "IRODS_TICKET" in env:
                cmd += " -t " + env["IRODS_TICKET"]
            cmd += " '" + t.get_path() + "'"
            try:
                tc = utils.TimedCommand(cmd, env_overrides=env, log_outerr=False)
                tc.run()
            except RuntimeError as err:
                if tc.get_exit_code() == 3:
                    # file does not exist, which is success
                    pass
                else:
                    logger.error(err)
                    self._log_filter_output(tc.get_outerr())
                    failed_l.append(t)
                    continue
            self._log_filter_output(tc.get_outerr())
            successful_l.append(t)

        return [successful_l, failed_l]

    def _irods_login(self, sitename):
        """
        log in to irods by using the iinit command - if the file already exists,
        we are already logged in
        """
        self.lock.acquire()
        try:
            # first, set up the env
            env = {}

            # irods requires a password hash file
            env["IRODS_AUTHENTICATION_FILE"] = os.getcwd() + "/.irodsA"

            key = "IRODS_ENVIRONMENT_FILE_" + sitename
            if key in os.environ:
                env["IRODS_ENVIRONMENT_FILE"] = os.environ[key]
            else:
                env["IRODS_ENVIRONMENT_FILE"] = os.environ["IRODS_ENVIRONMENT_FILE"]

            # read password from env file
            if not "IRODS_ENVIRONMENT_FILE" in env:
                raise RuntimeError(
                    "Missing IRODS_ENVIRONMENT_FILE - unable to do irods transfers"
                )

            check_cred_fs_permissions(env["IRODS_ENVIRONMENT_FILE"])

            password = None
            ticket = None
            h = open(env["IRODS_ENVIRONMENT_FILE"], "r")
            for line in h:
                # json - irods 4.0
                items = line.split(":", 2)
                key = items[0].lower().strip(" \t'\"\r\n")

                if key == "irodspassword" or key == "irods_password":
                    password = items[1].strip(" \t'\",\r\n")

                if key == "irodsticket" or key == "irods_ticket":
                    ticket = items[1].strip(" \t'\",\r\n")

            h.close()

            if password is None:
                raise RuntimeError("No irodsPassword specified in irods env file")

            if ticket is not None:
                env["IRODS_TICKET"] = ticket

            if os.path.exists(env["IRODS_AUTHENTICATION_FILE"]):
                # no need to log in again
                return env

            if password is not None:
                h = open(".irodsAc", "w")
                h.write(password + "\n")
                h.close()
                cmd = "cat .irodsAc | iinit"
                tc = utils.TimedCommand(cmd, env_overrides=env, timeout_secs=5 * 60)
                tc.run()
                os.unlink(".irodsAc")
                check_cred_fs_permissions(env["IRODS_AUTHENTICATION_FILE"])

        finally:
            self.lock.release()

        return env

    def _log_filter_output(self, output):
        filtered = ""
        for line in output.split("\n"):
            line = line.rstrip()
            if re.search("does not exist", line) is not None:
                continue
            filtered += line + "\n"
        if len(filtered) > 10:
            logger.info(filtered)


class S3Handler(TransferHandlerBase):
    """
    Handler for S3 and S3 compatible services
    """

    _name = "S3Handler"
    _mkdir_cleanup_protocols = ["s3", "s3s"]
    _protocol_map = [
        "file->s3",
        "file->s3s",
        "s3->file",
        "s3s->file",
        "s3->s3",
        "s3->s3s",
        "s3s->s3",
        "s3s->s3s",
    ]

    def do_mkdirs(self, mkdir_l):
        tools = utils.Tools()
        if tools.find("pegasus-s3", "help", None, [prog_dir]) is None:
            logger.error("Unable to do S3 transfers because pegasus-s3 not found")
            return [[], mkdir_l]

        successful_l = []
        failed_l = []
        for t in mkdir_l:

            # extract the bucket part
            re_bucket = re.compile(r"(s3(s){0,1}://\w+@\w+/+[\w\-]+)")
            bucket = t.get_url()
            r = re_bucket.search(bucket)
            if r:
                bucket = r.group(1)
            else:
                raise RuntimeError("Unable to parse bucket: %s" % (bucket))

            env = self._s3_cred_env(t.get_site_label())

            cmd = tools.full_path("pegasus-s3")
            if logger.isEnabledFor(logging.DEBUG):
                cmd += " -v"
            cmd += " mkdir " + bucket

            try:
                tc = utils.TimedCommand(cmd, env_overrides=env)
                tc.run()
            except RuntimeError as err:
                logger.error(err)
                failed_l.append(t)
                continue
            successful_l.append(t)

        return [successful_l, failed_l]

    def do_transfers(self, transfers):

        tools = utils.Tools()
        if tools.find("pegasus-s3", "help", None, [prog_dir]) is None:
            logger.error(
                "Unable to do S3 transfers because pegasus-s3 could not be found"
            )
            return [[], transfers]

        successful_l = []
        failed_l = []
        for t in transfers:

            self._pre_transfer_attempt(t)

            t_start = time.time()
            env = {}

            # use cp for s3->s3 transfers, and get/put when one end is a file://
            if (t.get_src_proto() == "s3" or t.get_src_proto() == "s3s") and (
                t.get_dst_proto() == "s3" or t.get_dst_proto() == "s3s"
            ):
                # s3 -> s3
                env = self._s3_cred_env(t.get_src_site_label())
                cmd = tools.full_path("pegasus-s3") + " cp -f -c '%s' '%s'" % (
                    t.src_url(),
                    t.dst_url(),
                )
            elif t.get_dst_proto() == "file":
                # this is a 'get'
                env = self._s3_cred_env(t.get_src_site_label())
                prepare_local_dir(os.path.dirname(t.get_dst_path()))
                cmd = tools.full_path("pegasus-s3") + " get '%s' '%s'" % (
                    t.src_url(),
                    t.get_dst_path(),
                )
            else:
                # this is a 'put'
                if t.get_src_proto() == "file":
                    # src has to exist and be readable
                    if not verify_local_file(t.get_src_path()):
                        failed_l.append(t)
                        self._post_transfer_attempt(t, False, t_start)
                        continue
                env = self._s3_cred_env(t.get_dst_site_label())
                cmd = tools.full_path("pegasus-s3") + " put -f -b '%s' '%s'" % (
                    t.get_src_path(),
                    t.dst_url(),
                )

            try:
                tc = utils.TimedCommand(cmd, env_overrides=env)
                tc.run()
            except Exception as err:
                logger.error(err)
                self._post_transfer_attempt(t, False, t_start)
                failed_l.append(t)
                continue

            self._post_transfer_attempt(t, True, t_start)
            successful_l.append(t)

        return [successful_l, failed_l]

    def do_removes(self, removes_list):
        tools = utils.Tools()
        if tools.find("pegasus-s3", "help", None, [prog_dir]) is None:
            logger.error(
                "Unable to do S3 transfers because pegasus-s3 could not be found"
            )
            return [[], removes_list]

        try:
            tmp_fd, tmp_name = tempfile.mkstemp(
                prefix="pegasus-transfer-", suffix=".lst"
            )
            tmp_file = io.open(tmp_fd, "w+")
        except Exception:
            raise RuntimeError("Unable to create tmp file for pegasus-s3 cleanup")

        successful_l = []
        failed_l = []
        for t in removes_list:

            fixed_url = t.get_url()

            # PM-790: recursive deletes are really a pattern matching. For example,
            # if told to remove the foo/bar directory, we need to translate it into
            # foo/bar/*
            if t.get_recursive():
                # first make sure there are no trailing slashes
                last_char = len(fixed_url) - 1
                while (
                    len(fixed_url) > 0 and last_char > 0 and fixed_url[last_char] == "/"
                ):
                    fixed_url = fixed_url[0:last_char]
                    last_char -= 1
                fixed_url += "/*"
                logger.info("Transformed remote URL to " + fixed_url)

            tmp_file.write("%s\n" % (fixed_url))

        tmp_file.close()

        env = self._s3_cred_env(removes_list[0].get_site_label())

        cmd = tools.full_path("pegasus-s3")
        if logger.isEnabledFor(logging.DEBUG):
            cmd += " -v"
        cmd += " rm -f -F " + tmp_name

        success = False
        try:
            tc = utils.TimedCommand(cmd, env_overrides=env)
            tc.run()
            success = True
        except RuntimeError as err:
            logger.error(err)

        # as we don't know which removes in the set succeeded/failed, we have
        # to mark them all the same (for example, one failure means all removes
        # gets marked as failed)
        for t in removes_list:
            if success:
                successful_l.append(t)
            else:
                failed_l.append(t)

        try:
            os.unlink(tmp_name)
        except Exception:
            pass

        return [successful_l, failed_l]

    def _s3_cred_env(self, site_label):
        env = {}
        if "PEGASUS_CREDENTIALS" in os.environ:
            env["PEGASUS_CREDENTIALS"] = os.environ["PEGASUS_CREDENTIALS"]
        key = "PEGASUS_CREDENTIALS_" + site_label
        if key in os.environ:
            env["PEGASUS_CREDENTIALS"] = os.environ[key]
        if "PEGASUS_CREDENTIALS" not in env:
            raise RuntimeError(
                "At least one of the PEGASUS_CREDENTIALS_"
                + site_label
                + " or PEGASUS_CREDENTIALS"
                + " environment variables has to be set"
            )
        check_cred_fs_permissions(env["PEGASUS_CREDENTIALS"])
        return env


class GlobusOnlineHandler(TransferHandlerBase):
    """
    Handler for Globus Online transfers
    """

    _name = "GlobusOnlineHandler"
    _mkdir_cleanup_protocols = ["go", "go"]
    _protocol_map = [
        "go->go",
    ]

    def do_mkdirs(self, mkdir_l):
        tools = utils.Tools()
        if tools.find("pegasus-globus-online", "help", None, [prog_dir]) is None:
            logger.error("Unable to locate pegasus-globus-online in the $PATH")
            return [[], mkdir_l]

        if not os.path.isfile(os.path.expanduser("~/.pegasus/globus.conf")):
            logger.error("Unable to locate globus config file ~/.pegasus/globus.conf")
            return [[], mkdir_l]

        successful_l = []
        failed_l = []

        cred_details = self._creds(mkdir_l[0])

        spec = {
            "endpoint": mkdir_l[0].get_host(),
            "client_id": cred_details["client_id"],
            "transfer_at": cred_details["transfer_at"],
            "transfer_rt": cred_details["transfer_rt"],
            "transfer_at_exp": cred_details["transfer_at_exp"],
            "files": list(),
        }

        for t in mkdir_l:
            spec["files"].append(t.get_path())

        logger.debug(json.dumps(spec, indent=2))
        try:
            tmp_fd, tmp_name = tempfile.mkstemp(
                prefix="pegasus-transfer-", suffix=".json"
            )
            tmp_file = io.open(tmp_fd, "w+")
        except Exception:
            raise RuntimeError("Unable to create tmp file for pegasus-globus-online")
        tmp_file.write(json.dumps(spec, indent=2))
        tmp_file.close()

        cmd = tools.full_path("pegasus-globus-online") + " --mkdir --file " + tmp_name
        if logger.isEnabledFor(logging.DEBUG):
            cmd += " --debug"
        try:
            tc = utils.TimedCommand(cmd)
            tc.run()
            successful_l = list(mkdir_l)
        except RuntimeError as err:
            logger.error(err)
            failed_l = list(mkdir_l)

        os.unlink(tmp_name)

        return [successful_l, failed_l]

    def do_transfers(self, transfers_l):
        tools = utils.Tools()
        if tools.find("pegasus-globus-online", "help", None, [prog_dir]) is None:
            logger.error("Unable to locate pegasus-globus-online in the $PATH")
            return [[], transfers_l]

        if not os.path.isfile(os.path.expanduser("~/.pegasus/globus.conf")):
            logger.error("Unable to locate globus config file ~/.pegasus/globus.conf")
            return [[], mkdir_l]

        successful_l = []
        failed_l = []

        # src
        src_ep = transfers_l[0].get_src_host()

        # dst
        dst_ep = transfers_l[0].get_dst_host()

        cred_details = self._creds(transfers_l[0])

        spec = {
            "src_endpoint": src_ep,
            "dst_endpoint": dst_ep,
            "client_id": cred_details["client_id"],
            "transfer_at": cred_details["transfer_at"],
            "transfer_rt": cred_details["transfer_rt"],
            "transfer_at_exp": cred_details["transfer_at_exp"],
            "files": list(),
        }

        for t in transfers_l:
            t_spec = {"src": t.get_src_path(), "dst": t.get_dst_path()}
            spec["files"].append(t_spec)

        logger.debug(json.dumps(spec, indent=2))
        try:
            tmp_fd, tmp_name = tempfile.mkstemp(
                prefix="pegasus-transfer-", suffix=".json"
            )
            tmp_file = io.open(tmp_fd, "w+")
        except Exception:
            raise RuntimeError("Unable to create tmp file for pegasus-globus-online")
        tmp_file.write(json.dumps(spec, indent=2))
        tmp_file.close()

        cmd = (
            tools.full_path("pegasus-globus-online") + " --transfer --file " + tmp_name
        )
        if logger.isEnabledFor(logging.DEBUG):
            cmd += " --debug"
        try:
            tc = utils.TimedCommand(cmd)
            tc.run()
            successful_l = list(transfers_l)
        except RuntimeError as err:
            logger.error(err)
            failed_l = list(transfers_l)

        os.unlink(tmp_name)

        return [successful_l, failed_l]

    def do_removes(self, removes_l):
        tools = utils.Tools()
        if tools.find("pegasus-globus-online", "help", None, [prog_dir]) is None:
            logger.error("Unable to locate pegasus-globus-online in the $PATH")
            return [[], removes_l]

        if not os.path.isfile(os.path.expanduser("~/.pegasus/globus.conf")):
            logger.error("Unable to locate globus config file ~/.pegasus/globus.conf")
            return [[], mkdir_l]

        successful_l = []
        failed_l = []

        cred_details = self._creds(removes_l[0])

        spec = {
            "endpoint": removes_l[0].get_host(),
            "client_id": cred_details["client_id"],
            "transfer_at": cred_details["transfer_at"],
            "transfer_rt": cred_details["transfer_rt"],
            "transfer_at_exp": cred_details["transfer_at_exp"],
            "recursive": removes_l[0].get_recursive(),
            "files": list(),
        }

        for t in removes_l:
            spec["files"].append(t.get_path())

        logger.debug(json.dumps(spec, indent=2))
        try:
            tmp_fd, tmp_name = tempfile.mkstemp(
                prefix="pegasus-transfer-", suffix=".json"
            )
            tmp_file = io.open(tmp_fd, "w+")
        except Exception:
            raise RuntimeError("Unable to create tmp file for pegasus-globus-online")
        tmp_file.write(json.dumps(spec, indent=2))
        tmp_file.close()

        cmd = tools.full_path("pegasus-globus-online") + " --remove --file " + tmp_name
        if logger.isEnabledFor(logging.DEBUG):
            cmd += " --debug"
        try:
            tc = utils.TimedCommand(cmd)
            tc.run()
            successful_l = list(removes_l)
        except RuntimeError as err:
            logger.error(err)
            failed_l = list(removes_l)

        os.unlink(tmp_name)

        return [successful_l, failed_l]

    def _creds(self, sample_transfer):
        """
        The credential used for Globus Online is just to contact the service
        """

        logger.info("Parsing globus config file for OAuth credentials")

        config = configparser.ConfigParser()
        config.read(os.path.expanduser("~/.pegasus/globus.conf"))

        cred_details = {
            "client_id": None,
            "transfer_at": None,
            "transfer_rt": None,
            "transfer_at_exp": 0,
        }

        try:
            cred_details["client_id"] = config.get("oauth", "client_id")
        except (configparser.NoSectionError, configparser.NoOptionError):
            logger.error("No client_id was supplied")
            raise RuntimeError("No client_id was supplied for Globus App")

        try:
            cred_details["transfer_at"] = config.get("oauth", "transfer_at")
        except (configparser.NoSectionError, configparser.NoOptionError):
            logger.info("No transfer_access_token was supplied")

        try:
            cred_details["transfer_at_exp"] = config.getint("oauth", "transfer_at_exp")
        except (configparser.NoSectionError, configparser.NoOptionError):
            logger.info(
                "No transfer_access_token_expiration was supplied, defaults to 0"
            )

        try:
            cred_details["transfer_rt"] = config.get("oauth", "transfer_rt")
        except (configparser.NoSectionError, configparser.NoOptionError):
            logger.info("No transfer_refresh_token was supplied")
            if (cred_details["transfer_at"] is None) or (
                cred_details["transfer_at_exp"] < int(time.time()) - 3600
            ):
                logger.error("transfer_access_token is missing or expiring soon")
                raise RuntimeError(
                    "Globus transfer_access_token is missing or expiring soon"
                )

        logger.info("End of parsing globus config file for OAuth credentials")

        return cred_details


class GSHandler(TransferHandlerBase):
    """
    Handler for Google Storage services
    """

    _name = "GSHandler"
    _mkdir_cleanup_protocols = ["gs"]
    _protocol_map = [
        "file->gs",
        "gs->file",
        "gs->gs",
    ]

    def do_mkdirs(self, mkdir_l):
        tools = utils.Tools()
        if tools.find("gsutil", "version", "gsutil version: ([\.0-9a-zA-Z]+)") is None:
            logger.error(
                "Unable to do Google Storage transfers because the gsutil tool could not be found"
            )
            return [[], mkdir_l]

        successful_l = []
        failed_l = []

        env = self._gsutil_env(mkdir_l[0].get_site_label())

        for t in mkdir_l:

            # extract the bucket part
            re_bucket = re.compile(r"(gs://[\w-]+/)[/\w-]*")
            bucket = t.get_url()
            r = re_bucket.search(bucket)
            if r:
                bucket = r.group(1)
            else:
                raise RuntimeError("Unable to parse bucket: %s" % (bucket))

            # first ensure that the bucket exists
            cmd = "gsutil mb %s" % (bucket)
            try:
                tc = utils.TimedCommand(cmd, env_overrides=env)
                tc.run()
            except RuntimeError as err:
                # if the bucket already exists, we call it a success
                if "already exists" not in tc.get_outerr():
                    logger.error(err)
                    failed_l.append(t)
                    continue
            successful_l.append(t)

        self._clean_tmp()
        return [successful_l, failed_l]

    def do_transfers(self, transfers_l):

        tools = utils.Tools()
        if tools.find("gsutil", "version", "gsutil version: ([\.0-9a-zA-Z]+)") is None:
            logger.error(
                "Unable to do Google Storage transfers because the gsutil tool could not be found"
            )
            return [[], transfers_l]

        successful_l = []
        failed_l = []

        if transfers_l[0].get_src_proto() == "gs":
            env = self._gsutil_env(transfers_l[0].get_src_site_label())
        else:
            env = self._gsutil_env(transfers_l[0].get_dst_site_label())

        for t in transfers_l:

            self._pre_transfer_attempt(t)

            t_start = time.time()
            cmd = ""

            # use cp for gs->gs transfers, and get/put when one end is a file://
            if t.get_src_proto() == "gs" and t.get_dst_proto() == "gs":
                # gs -> gs
                cmd = "gsutil -q cp '%s' '%s'" % (t.src_url(), t.dst_url())
            elif t.get_dst_proto() == "file":
                # this is a 'get'
                prepare_local_dir(os.path.dirname(t.get_dst_path()))
                cmd = "gsutil -q cp '%s' '%s'" % (t.src_url(), t.get_dst_path())
            else:
                # this is a 'put'
                # src has to exist and be readable
                if not verify_local_file(t.get_src_path()):
                    failed_l.append(t)
                    self._post_transfer_attempt(t, False, t_start)
                    continue
                cmd = "gsutil -q cp '%s' '%s'" % (t.get_src_path(), t.dst_url())

            try:
                tc = utils.TimedCommand(cmd, env_overrides=env)
                tc.run()
            except Exception as err:
                logger.error(err)
                self._post_transfer_attempt(t, False, t_start)
                failed_l.append(t)

            self._post_transfer_attempt(t, True, t_start)
            successful_l.append(t)

        self._clean_tmp()
        return [successful_l, failed_l]

    def do_removes(self, removes_l):
        tools = utils.Tools()
        if tools.find("gsutil", "version", "gsutil version: ([\.0-9a-zA-Z]+)") is None:
            logger.error(
                "Unable to do Google Storage transfers because the gsutil tool could not be found"
            )
            return [[], removes_l]

        successful_l = []
        failed_l = []

        env = self._gsutil_env(removes_l[0].get_site_label())

        for t in removes_l:

            # first ensure that the bucket exists
            cmd = "gsutil rm"
            if t.get_recursive():
                cmd += " -r"
            cmd += " " + t.get_url()

            try:
                tc = utils.TimedCommand(cmd, env_overrides=env)
                tc.run()
            except RuntimeError as err:
                # file not found is success
                if "No URLs matched" not in tc.get_outerr():
                    logger.error(err)
                    failed_l.append(t)
                    continue
            successful_l.append(t)

        self._clean_tmp()
        return [successful_l, failed_l]

    def _gsutil_env(self, site_name):

        env = {}

        if "BOTO_CONFIG" in os.environ:
            env["BOTO_CONFIG"] = os.environ["BOTO_CONFIG"]
        key = "BOTO_CONFIG_" + site_name
        if key in os.environ:
            env["BOTO_CONFIG"] = os.environ[key]
        if "BOTO_CONFIG" not in env:
            raise RuntimeError(
                "At least one of the BOTO_CONFIG_"
                + site_name
                + " or BOTO_CONFIG"
                + " environment variables has to be set"
            )

        if "GOOGLE_PKCS12" in os.environ:
            env["GOOGLE_PKCS12"] = os.environ["GOOGLE_PKCS12"]
        key = "GOOGLE_PKCS12_" + site_name
        if key in os.environ:
            env["GOOGLE_PKCS12"] = os.environ[key]
        if "GOOGLE_PKCS12" not in env:
            raise RuntimeError(
                "At least one of the GOOGLE_PKCS12_"
                + site_name
                + " or GOOGLE_PKCS12"
                + " environment variables has to be set"
            )

        check_cred_fs_permissions(env["BOTO_CONFIG"])
        check_cred_fs_permissions(env["GOOGLE_PKCS12"])

        # we need to update the boto config file to specify the full
        # path to the PKCS12 file
        try:
            tmp_fd, self._tmp_name = tempfile.mkstemp(
                prefix="pegasus-transfer-", suffix=".lst"
            )
            tmp_file = io.open(tmp_fd, "w+")
        except Exception:
            raise RuntimeError("Unable to create tmp file for gs boto file")
        try:
            conf = configparser.SafeConfigParser()
            conf.read(env["BOTO_CONFIG"])
            conf.set("Credentials", "gs_service_key_file", env["GOOGLE_PKCS12"])
            conf.write(tmp_file)
            tmp_file.close()
        except Exception as err:
            logger.error(err)
            raise RuntimeError("Unable to convert boto config file")
        env["BOTO_CONFIG"] = self._tmp_name
        check_cred_fs_permissions(env["BOTO_CONFIG"])

        return env

    def _clean_tmp(self):
        try:
            os.unlink(self._clean_tmp())
        except Exception:
            pass


class GFALHandler(TransferHandlerBase):

    _name = "GFALHandler"
    _mkdir_cleanup_protocols = ["gfal"]
    _protocol_map = ["root->file", "file->root", "srm->file", "file->srm"]

    def do_mkdirs(self, mkdir_list):

        tools = utils.Tools()
        if tools.find("gfal-mkdir", "--version", "\(gfal2 ([\.0-9]+)\)") is None:
            logger.error(
                "Unable to do xrood/srm mkdir because gfal-mkdir could not be found"
            )
            return [[], mkdir_list]

        successful_l = []
        failed_l = []
        for t in mkdir_list:

            cmd = "gfal-mkdir -p"
            if logger.isEnabledFor(logging.DEBUG):
                cmd = cmd + " -v"
            cmd = cmd + " '%s'" % (t.get_url())

            env_overrides = self._gfal_creds(t)

            try:
                self._gfal_validate_cred(env_overrides)
                tc = utils.TimedCommand(cmd, env_overrides=env_overrides)
                tc.run()
            except Exception as err:
                logger.error(err)
                failed_l.append(t)
                continue

            successful_l.append(t)

        return [successful_l, failed_l]

    def do_transfers(self, transfers_l):

        tools = utils.Tools()
        if tools.find("gfal-copy", "--version", "\(gfal2 ([\.0-9]+)\)") is None:
            logger.error(
                "Unable to do xrootr/srm transfers because gfal-copy could not be found"
            )
            return [[], transfers_l]

        successful_l = []
        failed_l = []

        for t in transfers_l:

            self._pre_transfer_attempt(t)

            t_start = time.time()

            if t.get_dst_proto() == "file":
                prepare_local_dir(os.path.dirname(t.get_dst_path()))

            if t.get_src_proto() == "file":
                # src has to exist and be readable
                if not verify_local_file(t.get_src_path()):
                    failed_l.append(t)
                    self._post_transfer_attempt(t, False, t_start)
                    continue

            cmd = "gfal-copy -f -p -t 7200 -T 7200"
            if logger.isEnabledFor(logging.DEBUG):
                cmd = cmd + " -v"
            cmd = cmd + " '%s' '%s'" % (t.src_url(), t.dst_url())

            try:
                tc = utils.TimedCommand(cmd, env_overrides=self._gfal_creds(t))
                tc.run()
            except Exception as err:
                logger.error(err)
                self._post_transfer_attempt(t, False, t_start)
                failed_l.append(t)
                continue

            self._post_transfer_attempt(t, True, t_start)
            successful_l.append(t)

        return [successful_l, failed_l]

    def do_removes(self, mkdir_list):

        tools = utils.Tools()
        if tools.find("gfal-rm", "--version", "\(gfal2 ([\.0-9]+)\)") is None:
            logger.error(
                "Unable to do xrood/srm removes because gfal-rm could not be found"
            )
            return [[], mkdir_list]

        successful_l = []
        failed_l = []
        for t in mkdir_list:

            cmd = "gfal-rm"
            if logger.isEnabledFor(logging.DEBUG):
                cmd = cmd + " -v"
            if t.get_recursive():
                cmd += " -r"
            cmd = cmd + " '%s'" % (t.get_url())

            try:
                tc = utils.TimedCommand(cmd, env_overrides=self._gfal_creds(t))
                tc.run()
            except Exception as err:
                logger.error(err)
                # gfal-rm is finicky when it comes to exit codes, let's
                # just ignore errors during cleanup

            successful_l.append(t)

        return [successful_l, failed_l]

    def _gfal_validate_cred(self, env_overrides):
        fname = env_overrides["X509_USER_PROXY"]

        if not (os.path.exists(fname)):
            raise RuntimeError(
                "X509 proxy file "
                + fname
                + " does not exist."
                + " Maybe you forgot to initialize the proxy, or the"
                + " submit host might be configured with"
                + " MOUNT_UNDER_SCRATCH which can hide directories for"
                + " local universe jobs."
            )

    def _gfal_creds(self, sample_transfer):

        env_override = {}

        # mkdirs and removes
        if isinstance(sample_transfer, Mkdir) or isinstance(sample_transfer, Remove):
            key = "X509_USER_PROXY_" + sample_transfer.get_site_label()
            if key in os.environ:
                env_override["X509_USER_PROXY"] = os.environ[key]
            return env_override

        # must be a standard transfer
        if sample_transfer.get_src_proto() == "file":
            key = "X509_USER_PROXY_" + sample_transfer.get_src_site_label()
            if key in os.environ:
                env_override["X509_USER_PROXY"] = os.environ[key]
        else:
            key = "X509_USER_PROXY_" + sample_transfer.get_dst_site_label()
            if key in os.environ:
                env_override["X509_USER_PROXY"] = os.environ[key]

        return env_override


class ScpHandler(TransferHandlerBase):
    """
    Uses scp to copy to/from remote hosts
    """

    _name = "ScpHandler"
    _mkdir_cleanup_protocols = ["scp"]
    _protocol_map = ["scp->file", "file->scp"]

    _base_args = " -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no"

    def do_mkdirs(self, mkdir_list):
        successful_l = []
        failed_l = []
        for t in mkdir_list:
            cmd = "/usr/bin/ssh"
            cmd += self._base_args
            key = "SSH_PRIVATE_KEY_" + t.get_site_label()
            if key in os.environ:
                cmd += " -i " + os.environ[key]
            elif "SSH_PRIVATE_KEY" in os.environ:
                cmd += " -i " + os.environ["SSH_PRIVATE_KEY"]

            # split up the host into host/port components
            host = self._extract_hostname(t.get_host())
            port = self._extract_port(t.get_host())

            cmd += " -p " + str(port)
            cmd += " " + host
            cmd += " '/bin/mkdir -p " + t.get_path() + "'"
            tc = utils.TimedCommand(cmd, log_outerr=False)
            try:
                tc.run()
            except Exception as err:
                logger.error(err)
                self._log_filter_ssh_output(tc.get_outerr())
                failed_l.append(t)
                continue
            self._log_filter_ssh_output(tc.get_outerr())
            successful_l.append(t)

        return [successful_l, failed_l]

    def do_transfers(self, transfers):
        global remote_dirs_created
        successful_l = []
        failed_l = []

        # number of transfers to group depends on the maximum allowed command line lenght
        max_transfers_in_group = max_cmd_len / 500

        # limit the size of the groups to keep command lines short
        for t_group in self._similar_groups(
            transfers, max_transfers_in_group=max_transfers_in_group
        ):

            for t in t_group:
                self._pre_transfer_attempt(t)

            t_base = t_group[0]

            cmd = "/usr/bin/scp"
            cmd += " -r -B"
            cmd += self._base_args
            t_start = time.time()
            try:
                if t_base.get_dst_proto() == "file":
                    # scp -> file
                    key = "SSH_PRIVATE_KEY_" + t_base.get_src_site_label()
                    if key in os.environ:
                        check_cred_fs_permissions(os.environ[key])
                        cmd += " -i " + os.environ[key]
                    elif "SSH_PRIVATE_KEY" in os.environ:
                        check_cred_fs_permissions(os.environ["SSH_PRIVATE_KEY"])
                        cmd += " -i " + os.environ["SSH_PRIVATE_KEY"]

                    cmd += " -P " + self._extract_port(t_base.get_src_host())
                    prepare_local_dir(os.path.dirname(t_base.get_dst_path()))
                    # scp wants escaped remote paths, even with quotes
                    for t in t_group:
                        src_path = re.sub(" ", "\\ ", t.get_src_path())
                        cmd += (
                            " '"
                            + self._extract_hostname(t.get_src_host())
                            + ":"
                            + src_path
                            + "'"
                        )
                    if len(t_group) > 1:
                        cmd += " '" + os.path.dirname(t_base.get_dst_path()) + "/'"
                    else:
                        cmd += " '" + t_base.get_dst_path() + "'"
                else:
                    # file -> scp

                    local_ok = True
                    for t in t_group:
                        # src has to exist and be readable
                        if not verify_local_file(t.get_src_path()):
                            local_ok = False
                    if not local_ok:
                        for t in t_group:
                            failed_l.append(t)
                            self._post_transfer_attempt(t, False, time.time())
                        continue

                    key = "SSH_PRIVATE_KEY_" + t_base.get_dst_site_label()
                    if key in os.environ:
                        check_cred_fs_permissions(os.environ[key])
                        cmd += " -i " + os.environ[key]
                    elif "SSH_PRIVATE_KEY" in os.environ:
                        check_cred_fs_permissions(os.environ["SSH_PRIVATE_KEY"])
                        cmd += " -i " + os.environ["SSH_PRIVATE_KEY"]

                    mkdir_key = (
                        "scp://"
                        + self._extract_hostname(t_base.get_dst_host())
                        + ":"
                        + os.path.dirname(t_base.get_dst_path())
                    )
                    if not mkdir_key in remote_dirs_created:
                        self._prepare_scp_dir(
                            t_base.get_dst_site_label(),
                            t_base.get_dst_host(),
                            os.path.dirname(t_base.get_dst_path()),
                        )
                        remote_dirs_created[mkdir_key] = True

                    cmd += " -P " + self._extract_port(t_base.get_dst_host())

                    for t in t_group:
                        cmd += " '" + t.get_src_path() + "'"

                    # scp wants escaped remote paths, even with quotes
                    dst_path = re.sub(" ", "\\ ", t_base.get_dst_path())
                    if len(t_group) > 1:
                        cmd += (
                            " '"
                            + self._extract_hostname(t_base.get_dst_host())
                            + ":"
                            + os.path.dirname(dst_path)
                            + "/'"
                        )
                    else:
                        cmd += (
                            " '"
                            + self._extract_hostname(t_base.get_dst_host())
                            + ":"
                            + dst_path
                            + "'"
                        )

                    stats_add(t.get_src_path())

                t_start = time.time()
                tc = utils.TimedCommand(cmd, log_outerr=False)
                tc.run()
            except RuntimeError as err:
                self._log_filter_ssh_output(tc.get_outerr())
                logger.error(err)
                for t in t_group:
                    self._post_transfer_attempt(t, False, t_start)
                    failed_l.append(t)
                continue
            self._log_filter_ssh_output(tc.get_outerr())
            for t in t_group:
                self._post_transfer_attempt(t, True, t_start)
                successful_l.append(t)

        return [successful_l, failed_l]

    def do_removes(self, transfers_l):
        successful_l = []
        failed_l = []

        # number of removes to group depends on the maximum allowed command line lenght
        max_transfers_in_group = max_cmd_len / 500

        # limit the size of the groups to keep command lines short
        for t_group in self._similar_groups(
            transfers_l, max_transfers_in_group=max_transfers_in_group
        ):

            t_base = t_group[0]

            cmd = "/usr/bin/ssh"
            cmd += self._base_args
            key = "SSH_PRIVATE_KEY_" + t_base.get_site_label()
            if key in os.environ:
                cmd += " -i " + os.environ[key]
            elif "SSH_PRIVATE_KEY" in os.environ:
                cmd += " -i " + os.environ["SSH_PRIVATE_KEY"]

            # split up the host into host/port components
            host = self._extract_hostname(t_base.get_host())
            port = self._extract_port(t_base.get_host())

            cmd += " -p " + str(port)
            cmd += " " + host
            cmd += " '/bin/rm -f"
            if t_base.get_recursive():
                cmd += " -r"
            for t in t_group:
                cmd += ' "' + t.get_path() + '"'
            cmd += "'"
            tc = utils.TimedCommand(cmd, log_outerr=False)
            try:
                tc.run()
            except Exception as err:
                logger.error(err)
                self._log_filter_ssh_output(tc.get_outerr())
                for t in t_group:
                    failed_l.append(t)
                continue
            self._log_filter_ssh_output(tc.get_outerr())
            for t in t_group:
                successful_l.append(t)

        return [successful_l, failed_l]

    def _prepare_scp_dir(self, rsite, rhost, rdir):
        """
        makes sure a local path exists before putting files into it
        """
        cmd = "/usr/bin/ssh"
        cmd += self._base_args
        key = "SSH_PRIVATE_KEY_" + rsite
        if key in os.environ:
            check_cred_fs_permissions(os.environ[key])
            cmd += " -i " + os.environ[key]
        elif "SSH_PRIVATE_KEY" in os.environ:
            check_cred_fs_permissions(os.environ["SSH_PRIVATE_KEY"])
            cmd += " -i " + os.environ["SSH_PRIVATE_KEY"]
        cmd += " -p " + self._extract_port(rhost)
        cmd += " " + self._extract_hostname(rhost) + " '/bin/mkdir -p " + rdir + "'"
        tc = utils.TimedCommand(cmd, log_outerr=False)
        try:
            tc.run()
        except Exception:
            # let the real command show the error
            pass
        self._log_filter_ssh_output(tc.get_outerr())

    def _extract_hostname(self, host):
        """
        returns only the host component (strips :...)
        """
        return re.sub(":.*", "", host)

    def _extract_port(self, host):
        """
        returns the port to connect to, defaults to 22
        """
        port = "22"
        r = re.search(":([0-9]+)", host)
        if r:
            port = r.group(1)
        return str(port)

    def _log_filter_ssh_output(self, output):
        filtered = ""
        for line in output.split("\n"):
            line = line.rstrip()
            if (
                re.search("Could not create directory|Permanently added", line)
                is not None
            ):
                continue
            filtered += line + "\n"
        if len(filtered) > 0:
            logger.info(filtered)


class GSIScpHandler(TransferHandlerBase):
    """
    Uses gsiscp to copy to/from remote hosts
    """

    _name = "GSIScpHandler"
    _mkdir_cleanup_protocols = ["gsiscp"]
    _protocol_map = ["gsiscp->file", "file->gsiscp"]

    _base_args = " -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no"

    def do_mkdirs(self, mkdir_list):

        tools = utils.Tools()
        if tools.find("gsissh", "-V", "(.*)") is None:
            logger.error("Unable to do gsiscp mkdir because gsissh could not be found")
            return [[], mkdir_list]

        successful_l = []
        failed_l = []
        for t in mkdir_list:
            cmd = tools.full_path("gsissh")
            cmd += self._base_args

            # split up the host into host/port components
            host = self._extract_hostname(t.get_host())
            port = self._extract_port(t.get_host())

            cmd += " -p " + str(port)
            cmd += " " + host
            cmd += " '/bin/mkdir -p " + t.get_path() + "'"
            tc = utils.TimedCommand(
                cmd, log_outerr=False, env_overrides=self._gsi_creds(t)
            )
            try:
                tc.run()
            except Exception as err:
                logger.error(err)
                self._log_filter_ssh_output(tc.get_outerr())
                failed_l.append(t)
                continue
            self._log_filter_ssh_output(tc.get_outerr())
            successful_l.append(t)

        return [successful_l, failed_l]

    def do_transfers(self, transfers):
        global remote_dirs_created
        successful_l = []
        failed_l = []

        tools = utils.Tools()
        if tools.find("gsiscp", "-V", "(.*)") is None:
            logger.error("Unable to do gsiscp mkdir because gsiscp could not be found")
            return [[], mkdir_list]

        # number of transfers to group depends on the maximum allowed command line lenght
        max_transfers_in_group = max_cmd_len / 500

        # limit the size of the groups to keep command lines short
        for t_group in self._similar_groups(
            transfers, max_transfers_in_group=max_transfers_in_group
        ):

            for t in t_group:
                self._pre_transfer_attempt(t)

            t_base = t_group[0]

            cmd = tools.full_path("gsiscp")
            cmd += " -r -B"
            cmd += self._base_args
            t_start = time.time()
            try:
                if t_base.get_dst_proto() == "file":
                    # scp -> file
                    cmd += " -P " + self._extract_port(t_base.get_src_host())
                    prepare_local_dir(os.path.dirname(t_base.get_dst_path()))
                    # scp wants escaped remote paths, even with quotes
                    for t in t_group:
                        src_path = re.sub(" ", "\\ ", t.get_src_path())
                        cmd += (
                            " '"
                            + self._extract_hostname(t.get_src_host())
                            + ":"
                            + src_path
                            + "'"
                        )
                    if len(t_group) > 1:
                        cmd += " '" + os.path.dirname(t_base.get_dst_path()) + "/'"
                    else:
                        cmd += " '" + t_base.get_dst_path() + "'"
                else:
                    # file -> scp

                    # src has to exist and be readable
                    if not verify_local_file(t.get_src_path()):
                        failed_l.append(t)
                        self._post_transfer_attempt(t, False, t_start)
                        continue

                    mkdir_key = (
                        "scp://"
                        + self._extract_hostname(t_base.get_dst_host())
                        + ":"
                        + os.path.dirname(t_base.get_dst_path())
                    )
                    if not mkdir_key in remote_dirs_created:
                        self._prepare_scp_dir(
                            t_base.get_dst_site_label(),
                            t_base.get_dst_host(),
                            os.path.dirname(t_base.get_dst_path()),
                        )
                        remote_dirs_created[mkdir_key] = True

                    cmd += " -P " + self._extract_port(t_base.get_dst_host())

                    for t in t_group:
                        cmd += " '" + t.get_src_path() + "'"

                    # scp wants escaped remote paths, even with quotes
                    dst_path = re.sub(" ", "\\ ", t_base.get_dst_path())
                    if len(t_group) > 1:
                        cmd += (
                            " '"
                            + self._extract_hostname(t_base.get_dst_host())
                            + ":"
                            + os.path.dirname(dst_path)
                            + "/'"
                        )
                    else:
                        cmd += (
                            " '"
                            + self._extract_hostname(t_base.get_dst_host())
                            + ":"
                            + dst_path
                            + "'"
                        )

                    stats_add(t.get_src_path())

                t_start = time.time()
                tc = utils.TimedCommand(
                    cmd, log_outerr=False, env_overrides=self._gsi_creds(t_base)
                )
                tc.run()
            except RuntimeError as err:
                self._log_filter_ssh_output(tc.get_outerr())
                logger.error(err)
                for t in t_group:
                    self._post_transfer_attempt(t, False, t_start)
                    failed_l.append(t)
                continue
            self._log_filter_ssh_output(tc.get_outerr())
            for t in t_group:
                self._post_transfer_attempt(t, True, t_start)
                successful_l.append(t)

        return [successful_l, failed_l]

    def do_removes(self, transfers_l):
        successful_l = []
        failed_l = []

        tools = utils.Tools()
        if tools.find("gsissh", "-V", "(.*)") is None:
            logger.error("Unable to do gsiscp mkdir because gsissh could not be found")
            return [[], mkdir_list]

        # number of removes to group depends on the maximum allowed command line lenght
        max_transfers_in_group = max_cmd_len / 500

        # limit the size of the groups to keep command lines short
        for t_group in self._similar_groups(
            transfers_l, max_transfers_in_group=max_transfers_in_group
        ):

            t_base = t_group[0]

            cmd = tools.full_path("gsissh")
            cmd += self._base_args

            # split up the host into host/port components
            host = self._extract_hostname(t_base.get_host())
            port = self._extract_port(t_base.get_host())

            cmd += " -p " + str(port)
            cmd += " " + host
            cmd += " '/bin/rm -f"
            if t_base.get_recursive():
                cmd += " -r"
            for t in t_group:
                cmd += ' "' + t.get_path() + '"'
            cmd += "'"
            tc = utils.TimedCommand(
                cmd, log_outerr=False, env_overrides=self._gsi_creds(t_base)
            )
            try:
                tc.run()
            except Exception as err:
                logger.error(err)
                self._log_filter_ssh_output(tc.get_outerr())
                for t in t_group:
                    failed_l.append(t)
                continue
            self._log_filter_ssh_output(tc.get_outerr())
            for t in t_group:
                successful_l.append(t)

        return [successful_l, failed_l]

    def _prepare_scp_dir(self, rsite, rhost, rdir):
        """
        makes sure a local path exists before putting files into it
        """
        cmd = "gsissh"
        cmd += self._base_args
        cmd += " -p " + self._extract_port(rhost)
        cmd += " " + self._extract_hostname(rhost) + " '/bin/mkdir -p " + rdir + "'"
        tc = utils.TimedCommand(cmd, log_outerr=False)
        try:
            tc.run()
        except Exception:
            # let the real command show the error
            pass
        self._log_filter_ssh_output(tc.get_outerr())

    def _extract_hostname(self, host):
        """
        returns only the host component (strips :...)
        """
        return re.sub(":.*", "", host)

    def _extract_port(self, host):
        """
        returns the port to connect to, defaults to 22
        """
        port = "22"
        r = re.search(":([0-9]+)", host)
        if r:
            port = r.group(1)
        return str(port)

    def _log_filter_ssh_output(self, output):
        filtered = ""
        for line in output.split("\n"):
            line = line.rstrip()
            if (
                re.search("Could not create directory|Permanently added", line)
                is not None
            ):
                continue
            filtered += line + "\n"
        if len(filtered) > 0:
            logger.info(filtered)

    def _gsi_creds(self, sample_transfer):

        env_override = {}

        # mkdirs and removes
        if isinstance(sample_transfer, Mkdir) or isinstance(sample_transfer, Remove):
            key = "X509_USER_PROXY_" + sample_transfer.get_site_label()
            if key in os.environ:
                env_override["X509_USER_PROXY"] = os.environ[key]
            return env_override

        # must be a standard transfer
        if sample_transfer.get_src_proto() == "file":
            key = "X509_USER_PROXY_" + sample_transfer.get_src_site_label()
            if key in os.environ:
                env_override["X509_USER_PROXY"] = os.environ[key]
        else:
            key = "X509_USER_PROXY_" + sample_transfer.get_dst_site_label()
            if key in os.environ:
                env_override["X509_USER_PROXY"] = os.environ[key]

        return env_override


class StashHandler(TransferHandlerBase):
    """
    Uses the OSG stashcp command to trasfer from/to stash
    """

    _name = "StashHandler"
    _mkdir_cleanup_protocols = ["stash"]
    _protocol_map = ["stash->file", "file->stash"]

    def do_mkdirs(self, transfers):

        # noop for now
        return [transfers, []]

        tools = utils.Tools()
        if tools.find("stashcp") is None:
            logger.error("Unable to do Stash mkdir because stashcp could not be found")
            return [[], transfers_l]

        successful_l = []
        failed_l = []
        for t in transfers:

            # copy a 0-byte file to create the dir
            cmd = "%s /dev/null '%s/.create'" % (
                tools.full_path("stashcp"),
                t.get_url(),
            )
            try:
                tc = utils.TimedCommand(cmd)
                tc.run()
            except RuntimeError as err:
                logger.error(err)
                failed_l.append(t)
                continue
            successful_l.append(t)
        return [successful_l, failed_l]

    def do_transfers(self, transfers_l):

        tools = utils.Tools()
        if tools.find("stashcp") is None:
            logger.error(
                "Unable to do Stash transfers because stashcp could not be found"
            )
            return [[], transfers_l]

        # ensure we do not use http_proxy for curl
        if "http_proxy" in os.environ:
            os.environ["http_proxy"] = ""

        successful_l = []
        failed_l = []
        for t in transfers_l:
            self._pre_transfer_attempt(t)
            t_start = time.time()

            if t.get_dst_proto() == "stash":
                # write file:// to stash://

                # src has to exist and be readable
                if not verify_local_file(t.get_src_path()):
                    self._post_transfer_attempt(t, False, t_start)
                    failed_l.append(t)
                    continue

                if os.path.exists("/mnt/ceph/osg/public"):
                    # hack for now - local cp
                    src_path = t.get_src_path()
                    dst_path = re.sub("^/osgconnect", "", t.get_dst_path())

                    prepare_local_dir(os.path.dirname(dst_path))
                    cmd = "/bin/cp '%s' '%s'" % (src_path, dst_path)
                else:
                    cmd = "%s '%s' '%s'" % (
                        tools.full_path("stashcp"),
                        t.get_src_path(),
                        t.dst_url(),
                    )
            else:
                # read
                # stashcp wants just the path with a single leading slash
                src_path = t.src_url()
                src_path = re.sub("^stash:", "", src_path)
                src_path = re.sub("^/+", "", src_path)
                src_path = "/" + src_path

                local_dir = os.path.dirname(t.get_dst_path())
                prepare_local_dir(local_dir)
                # use --methods as we want to exclude cvmfs - it can take a
                # long time to update, and we have seen partial files being
                # published there in the past
                cmd = "%s '%s' '%s'" % (
                    tools.full_path("stashcp"),
                    src_path,
                    local_dir,
                )
                remote_fname = os.path.basename(t.get_src_path())
                local_fname = os.path.basename(t.get_dst_path())
                if remote_fname != local_fname:
                    cmd += " && mv '%s' '%s'" % (remote_fname, local_fname)

            try:
                tc = utils.TimedCommand(cmd)
                tc.run()
            except RuntimeError as err:
                logger.error(err)
                self._post_transfer_attempt(t, False, t_start)
                failed_l.append(t)
                continue

            self._post_transfer_attempt(t, True, t_start)
            successful_l.append(t)

        return [successful_l, failed_l]

    def do_removes(self, transfers_l):
        # local rm for now
        successful_l = []
        failed_l = []
        for t in transfers_l:
            cmd = "/bin/rm -f"
            if t.get_recursive():
                cmd += " -r "
            cmd += " '/stash%s' " % (t.get_path())
            try:
                tc = utils.TimedCommand(cmd)
                tc.run()
            except RuntimeError as err:
                logger.error(err)
                failed_l.append(t)
                continue
            successful_l.append(t)
        return [successful_l, failed_l]


class SymlinkHandler(TransferHandlerBase):
    """
    Sets up symlinks - this is often used when data is local, but needs a
    reference in cwd
    """

    _name = "SymlinkHandler"
    _mkdir_cleanup_protocols = ["symlink"]
    _protocol_map = ["file->symlink", "symlink->symlink"]

    def do_transfers(self, transfer_l):
        successful_l = []
        failed_l = []
        for t in transfer_l:
            self._pre_transfer_attempt(t)
            t_start = time.time()

            prepare_local_dir(os.path.dirname(t.get_dst_path()))

            # we do not allow dangling symlinks
            if t.verify_symlink_source and not os.path.exists(t.get_src_path()):
                logger.warning(
                    "Symlink source (%s) does not exist" % (t.get_src_path())
                )
                self._post_transfer_attempt(t, False, t_start)
                failed_l.append(t)
                continue

            if os.path.exists(t.get_src_path()) and os.path.exists(t.get_dst_path()):
                # make sure src and target are not the same file - have to
                # compare at the inode level as paths can differ
                src_inode = os.stat(t.get_src_path())[stat.ST_INO]
                dst_inode = os.stat(t.get_dst_path())[stat.ST_INO]
                if src_inode == dst_inode:
                    logger.warning(
                        "symlink: src (%s) and dst (%s) already exists"
                        % (t.get_src_path(), t.get_dst_path())
                    )
                    self._post_transfer_attempt(t, True, t_start)
                    successful_l.append(t)
                    continue

            cmd = "ln -f -s '%s' '%s'" % (t.get_src_path(), t.get_dst_path())
            try:
                tc = utils.TimedCommand(cmd, timeout_secs=60)
                tc.run()
            except RuntimeError as err:
                logger.error(err)
                self._post_transfer_attempt(t, False, t_start)
                failed_l.append(t)
                continue

            self._post_transfer_attempt(t, True, t_start)
            successful_l.append(t)

        return [successful_l, failed_l]

    def do_removes(self, transfers):
        successful_l = []
        failed_l = []
        for t in transfers:
            cmd = "/bin/rm -f"
            cmd += " '%s' " % (t.get_path())
            try:
                tc = utils.TimedCommand(cmd)
                tc.run()
            except RuntimeError as err:
                logger.error(err)
                failed_l.append(t)
                continue
            successful_l.append(t)
        return [successful_l, failed_l]


class MovetoHandler(TransferHandlerBase):
    """
    Enables moving of files - this should only be used internally
    by the planner
    """

    _name = "MovetoHandler"
    _mkdir_cleanup_protocols = []
    _protocol_map = ["file->moveto"]

    def do_transfers(self, transfer_l):
        successful_l = []
        failed_l = []
        for t in transfer_l:
            self._pre_transfer_attempt(t)
            t_start = time.time()

            print(t.get_dst_path())
            prepare_local_dir(os.path.dirname(t.get_dst_path()))

            # we do not allow dangling symlinks
            if not os.path.exists(t.get_src_path()):
                logger.warning("Moveto source (%s) does not exist" % (t.get_src_path()))
                self._post_transfer_attempt(t, False, t_start)
                failed_l.append(t)
                continue

            cmd = "mv '%s' '%s'" % (t.get_src_path(), t.get_dst_path())
            try:
                tc = utils.TimedCommand(cmd, timeout_secs=600)
                tc.run()
            except RuntimeError as err:
                logger.error(err)
                self._post_transfer_attempt(t, False, t_start)
                failed_l.append(t)
                continue

            self._post_transfer_attempt(t, True, t_start)
            successful_l.append(t)

        return [successful_l, failed_l]


class DockerHandler(TransferHandlerBase):
    """
    Use "docker save" to import images from DockerHub
    """

    _name = "DockerHandler"
    _protocol_map = ["docker->file", "docker->file::docker"]

    def do_transfers(self, transfers_l):

        tools = utils.Tools()
        if tools.find("docker", "--version", "([0-9]+\.[0-9]+\.[0-9]+)") is None:
            logger.error(
                "Unable to do pull Docker images as docker command could not be found"
            )
            return [[], transfers_l]

        successful_l = []
        failed_l = []
        for t in transfers_l:
            self._pre_transfer_attempt(t)
            t_start = time.time()

            # docker wants just the path
            src_path = t.src_url()
            src_path = re.sub("^docker:/+", "", src_path)

            prepare_local_dir(os.path.dirname(t.get_dst_path()))
            cmd = "%s pull '%s' && %s save -o '%s' '%s'" % (
                tools.full_path("docker"),
                src_path,
                tools.full_path("docker"),
                t.get_dst_path(),
                src_path,
            )
            try:
                tc = utils.TimedCommand(cmd)
                tc.run()
            except RuntimeError as err:
                logger.error(err)
                self._post_transfer_attempt(t, False, t_start)
                failed_l.append(t)
                continue

            self._post_transfer_attempt(t, True, t_start)
            successful_l.append(t)

        return [successful_l, failed_l]


class SingularityHandler(TransferHandlerBase):
    """
    Use "singularity pull" to import images from Singularity Hub, Singularity Library, and Docker.

    Singularity Hub and Docker compatability requires Singularity version 2.3 or greater.
    Singularity Library compatability requires Singularity version 3.0 or greater.
    """

    _name = "SingularityHandler"
    _protocol_map = [
        "shub->file",
        "shub->file::singularity",
        "library->file",
        "library->file::singularity",
        "docker->file::singularity",
    ]

    def do_transfers(self, transfers_l):

        tools = utils.Tools()
        if tools.find("singularity", "--version", "^([0-9]+\.[0-9]+)") is None:
            logger.error(
                "Unable to do pull Singularity images as singularity command could not be found"
            )
            return [[], transfers_l]

        successful_l = []
        failed_l = []
        for t in transfers_l:
            self._pre_transfer_attempt(t)
            t_start = time.time()

            # singularity pull only accepts a filename, not a full path, so
            # download and then move to the correct location

            target_name = hashlib.sha224(t.get_dst_path().encode("utf-8")).hexdigest()

            prepare_local_dir(os.path.dirname(t.get_dst_path()))

            cmd = "%s pull --allow-unauthenticated '%s' '%s' && mv %s* '%s'" % (
                tools.full_path("singularity"),
                target_name,
                t.src_url(),
                target_name,
                t.get_dst_path(),
            )

            logger.debug("Using Singularity command: '%s'" % (cmd))

            try:
                tc = utils.TimedCommand(cmd)
                tc.run()
            except RuntimeError as err:
                logger.error(err)
                self._post_transfer_attempt(t, False, t_start)
                failed_l.append(t)
                continue

            self._post_transfer_attempt(t, True, t_start)
            successful_l.append(t)

        return [successful_l, failed_l]


class WebdavHandler(TransferHandlerBase):
    """
    Uses curl to do webdav transfers
    """

    _name = "WebdavHandler"
    _mkdir_cleanup_protocols = ["webdav", "webdavs"]
    _protocol_map = ["webdav->file", "webdavs->file", "file->webdav", "file->webdavs"]

    def do_mkdirs(self, mkdir_list):
        successful_l = []
        failed_l = []

        if tools.find("curl", "--version", " ([0-9]+\.[0-9]+)") is None:
            logger.error(
                "Unable to do webdav transfers because curl could not be found"
            )
            return [[], mkdir_list]

        username, password = self._creds(mkdir_list[0].get_host())

        for t in mkdir_list:
            if not self._create_dir(t.get_url(), username, password):
                failed_l.append(t)
                continue
            successful_l.append(t)

        return [successful_l, failed_l]

    def do_transfers(self, transfers):
        successful_l = []
        failed_l = []

        if tools.find("curl", "--version", " ([0-9]+\.[0-9]+)") is None:
            logger.error(
                "Unable to do webdav transfers because curl could not be found"
            )
            return [[], mkdir_list]

        # disable http proxies
        env_overrides = {"http_proxy": ""}

        if transfers[0].get_dst_proto() == "file":
            username, password = self._creds(transfers[0].get_src_host())
        else:
            username, password = self._creds(transfers[0].get_dst_host())

        for t in transfers:

            if t.get_dst_proto() == "file":
                # webdav -> file
                prepare_local_dir(os.path.dirname(t.get_dst_path()))
                url = re.sub("^webdav", "http", t.src_url())
                cmd = tools.full_path("curl")
                if not logger.isEnabledFor(logging.DEBUG):
                    cmd += " --silent"
                cmd += (
                    " --fail --show-error --location-trusted"
                    + " --user '"
                    + username
                    + ":"
                    + password
                    + "'"
                    + " --anyauth"
                    + " -o '"
                    + t.get_dst_path()
                    + "'"
                    + " '"
                    + url
                    + "'"
                )
            else:
                # file -> webdav
                url = re.sub("^webdav", "http", t.dst_url())

                # might have to create dir first
                self._create_dir(os.path.dirname(url), username, password)

                cmd = tools.full_path("curl")
                if not logger.isEnabledFor(logging.DEBUG):
                    cmd += " --silent"
                cmd += (
                    " --fail --show-error --location-trusted"
                    + " --user '"
                    + username
                    + ":"
                    + password
                    + "'"
                    + " --anyauth"
                    + " -T '"
                    + t.get_src_path()
                    + "'"
                    + " '"
                    + url
                    + "'"
                )

            # ensure we are not logging credentials
            cmd_display = cmd.replace(password, "XXX")

            self._pre_transfer_attempt(t)
            try:
                t_start = time.time()
                tc = utils.TimedCommand(
                    cmd, cmd_display=cmd_display, env_overrides=env_overrides
                )
                tc.run()
            except RuntimeError as err:
                logger.error(err)
                self._post_transfer_attempt(t, False, t_start)
                failed_l.append(t)
                continue

            # also make sure the file was actually downloaded
            if t.get_dst_proto() == "file" and not os.path.exists(t.get_dst_path()):
                logger.error(
                    "Expected local file is missing - marking transfer as failed"
                )
                failed_l.append(t)
                continue

            # also make sure the file was actually downloaded
            if t.get_dst_proto() == "file" and os.path.getsize(t.get_dst_path()) == 0:
                logger.error("Downloaded file is 0 bytes - marking transfer as failed")
                failed_l.append(t)
                continue

            # success
            self._post_transfer_attempt(t, True, t_start)
            successful_l.append(t)

        return [successful_l, failed_l]

    def do_removes(self, transfers_l):
        successful_l = []
        failed_l = []

        if tools.find("curl", "--version", " ([0-9]+\.[0-9]+)") is None:
            logger.error(
                "Unable to do webdav transfers because curl could not be found"
            )
            return [[], transfers_l]

        username, password = self._creds(transfers_l[0].get_host())

        for t in transfers_l:
            url = re.sub("^webdav", "http", t.get_url())
            cmd = tools.full_path("curl")
            if not logger.isEnabledFor(logging.DEBUG):
                cmd += " --silent"
            cmd += (
                " --fail --show-error --location-trusted"
                + " --user '"
                + username
                + ":"
                + password
                + "'"
                + " --anyauth"
                + " -X DELETE '"
                + url
                + "'"
            )

            # ensure we are not logging credentials
            cmd_display = cmd.replace(password, "XXX")

            tc = utils.TimedCommand(cmd, cmd_display=cmd_display, log_outerr=False)
            try:
                tc.run()
            except Exception as err:
                logger.error(err)
                failed_l.append(t)
                break
            successful_l.append(t)

        return [successful_l, failed_l]

    def _creds(self, host):
        username = credentials.get(host, "username")
        password = credentials.get(host, "password")
        return username, password

    def _create_dir(self, url, username, password):
        global remote_dirs_created

        # early short-circuit
        if url in remote_dirs_created:
            return True

        # split the URL
        r = re_parse_url.search(url)
        if not r:
            raise RuntimeError("Unable to parse URL: %s" % (url))

        # Parse successful
        proto = re.sub("^webdav", "http", r.group(1))
        host = r.group(2)
        path = r.group(3)

        # walk the path
        cur_path = ""
        for entry in self._split_path(path):
            if entry == "/":
                continue
            cur_path += "/" + entry
            url = proto + "://" + host + cur_path
            logger.debug("Creating dir for " + url)
            cmd = tools.full_path("curl")
            if not logger.isEnabledFor(logging.DEBUG):
                cmd += " --silent"
            cmd += (
                " --show-error --location-trusted --user '"
                + username
                + ":"
                + password
                + "'"
                + " --anyauth"
                + " -X MKCOL '"
                + url
                + "'"
            )

            # ensure we are not logging credentials
            cmd_display = cmd.replace(password, "XXX")

            tc = utils.TimedCommand(cmd, cmd_display=cmd_display)
            try:
                tc.run()
            except Exception as err:
                logger.error(err)
                return False
            remote_dirs_created[url] = True
        return True

    def _split_path(self, path):
        (head, tail) = os.path.split(path)
        return (
            self._split_path(head) + [tail] if head and head != path else [head or tail]
        )


class Stats:
    """
    Keeps global stats for transfers
    """

    _detected_3rd_party = False

    def __init__(self):
        self._t_start_global = time.time()
        self._t_end_global = 0
        self._total_count = 0
        self._total_bytes = 0
        self._site_pair_count = {}
        self._site_pair_bytes = {}

        # holder for transfers - this will be printed at the end
        self._yaml = ""

        # integrity timings
        self._integrity_verify_count_succeeded = {}
        self._integrity_verify_count_failed = {}
        self._integrity_verify_duration = {}
        self._integrity_generate_count = {}
        self._integrity_generate_duration = {}

    def add_stats(self, transfer, was_successful, t_start, t_end):

        key = transfer.get_src_site_label() + "->" + transfer.get_dst_site_label()
        if key not in self._site_pair_count:
            self._site_pair_count[key] = 0
            self._site_pair_bytes[key] = 0
        self._site_pair_count[key] += 1
        self._total_count += 1

        # do we have a local component?
        local_filename = None
        bytes = 0
        if transfer.get_src_proto() == "file":
            local_filename = transfer.get_src_path()
        elif transfer.get_dst_proto() == "file":
            local_filename = transfer.get_dst_path()

        if local_filename is None:
            self._detected_3rd_party = True
        else:
            try:
                s = os.stat(local_filename)
                bytes = s[stat.ST_SIZE]
                self._total_bytes += bytes
                self._site_pair_bytes[key] += bytes
            except Exception:
                pass  # ignore

        # add data - we chose to not use PyYAML here as we don't want it as dep
        data = (
            '  - src_url: "%s"\n'
            '    src_label: "%s"\n'
            '    dst_url: "%s"\n'
            '    dst_label: "%s"\n'
            "    success: %s\n"
            "    start: %.0f\n"
            "    duration: %.1f\n"
        ) % (
            transfer.src_url(),
            transfer.get_src_site_label(),
            transfer.dst_url(),
            transfer.get_dst_site_label(),
            str(was_successful),
            t_start,
            t_end - t_start,
        )
        if transfer.lfn:
            data += '    lfn: "%s"\n' % (transfer.lfn)
        if bytes > 0:
            data += "    bytes: %d\n" % (bytes)
        self._yaml += data

        # call out to panorama if asked to do so, but make sure that failures
        # do not stop us
        if "KICKSTART_MON_ENDPOINT_URL" in os.environ:
            try:
                p = Panorama()
                p.one_transfer(transfer, was_successful, t_start, t_end, bytes)
            except Exception as e:
                logger.warn("Panorama send failure: " + e)

    def all_transfers_done(self):
        self._t_end_global = time.time()

    def add_integrity_generate(self, linkage, duration):
        # this is not currently used, and thus does not handle
        # success/failed counts
        if linkage is None or linkage == "":
            linkage = "unknown"
        if linkage not in self._integrity_generate_count:
            self._integrity_generate_count[linkage] = 0
            self._integrity_generate_duration[linkage] = 0.0
        self._integrity_generate_count[linkage] += 1
        self._integrity_generate_duration[linkage] += duration

    def add_integrity_verify(self, linkage, duration, success):
        if linkage is None or linkage == "":
            linkage = "unknown"
        if linkage not in self._integrity_verify_count_succeeded:
            self._integrity_verify_count_succeeded[linkage] = 0
            self._integrity_verify_count_failed[linkage] = 0
            self._integrity_verify_duration[linkage] = 0.0
        if success:
            self._integrity_verify_count_succeeded[linkage] += 1
        else:
            self._integrity_verify_count_failed[linkage] += 1
        self._integrity_verify_duration[linkage] += duration

    def stats_summary(self):

        # stats go to the multipart dir
        if "PEGASUS_MULTIPART_DIR" in os.environ:
            try:
                fh = open(
                    "%s/%d-transfer"
                    % (os.environ["PEGASUS_MULTIPART_DIR"], int(time.time())),
                    "w",
                )
                fh.write("- transfer_attempts:\n")
                fh.write(self._yaml)
                fh.close()
            except Exception as e:
                logger.error(
                    "Unable to write stats to $PEGASUS_MULTIPART_DIR: " + str(e)
                )

        # integrity timings
        for linkage in self._integrity_verify_count_succeeded.keys():
            data = {
                "ts": int(time.time()),
                "monitoring_event": "int.metric",
                "payload": [
                    {
                        "event": "check",
                        "file_type": linkage,
                        "succeeded": self._integrity_verify_count_succeeded[linkage],
                        "failed": self._integrity_verify_count_failed[linkage],
                        "duration": "%.3f" % self._integrity_verify_duration[linkage],
                    }
                ],
            }
            logger.info(
                "@@@MONITORING_PAYLOAD - START@@@"
                + json.dumps(data)
                + "@@@MONITORING_PAYLOAD - END@@@"
            )
        for linkage in self._integrity_generate_count.keys():
            data = {
                "ts": int(time.time()),
                "monitoring_event": "int.metric",
                "payload": [
                    {
                        "event": "compute",
                        "file_type": linkage,
                        "succeeded": self._integrity_generate_count[linkage],
                        "failed": 0,
                        "duration": "%.3f" % self._integrity_generate_duration[linkage],
                    }
                ],
            }
            logger.info(
                "@@@MONITORING_PAYLOAD - START@@@"
                + json.dumps(data)
                + "@@@MONITORING_PAYLOAD - END@@@"
            )

        # standard stats
        if self._t_end_global is None or self._t_end_global == 0:
            self.all_transfers_done()

        if self._total_count == 0:
            logger.info("Stats: no local files in the transfer set")
            return

        if self._detected_3rd_party:
            logger.info("Unable to provide stats for third party gsiftp/srm transfers")
            return

        total_secs = self._t_end_global - self._t_start_global
        Bps = self._total_bytes / total_secs

        logger.info(
            "Stats: Total %d transfers, %sB transferred in %.0f seconds. Rate: %sB/s (%sb/s)"
            % (
                self._total_count,
                iso_prefix_formatted(self._total_bytes),
                total_secs,
                iso_prefix_formatted(Bps),
                iso_prefix_formatted(Bps * 8),
            )
        )

        for key, value in iteritems(self._site_pair_count):
            Bps = self._site_pair_bytes[key] / total_secs
            logger.info(
                "       Between sites %s : %d transfers, %sB transferred in %.0f seconds. Rate: %sB/s (%sb/s)"
                % (
                    key,
                    self._site_pair_count[key],
                    iso_prefix_formatted(self._site_pair_bytes[key]),
                    total_secs,
                    iso_prefix_formatted(Bps),
                    iso_prefix_formatted(Bps * 8),
                )
            )


class Panorama:
    """
    Singleton for sending Panorama live stats
    """

    # singleton
    instance = None

    def __init__(self):
        if not Panorama.instance:
            Panorama.instance = Panorama.__Panorama()

    def __getattr__(self, name):
        return getattr(self.instance, name)

    class __Panorama:
        def one_transfer(self, transfer, was_successful, t_start, t_end, filesize):

            if "KICKSTART_MON_ENDPOINT_URL" not in os.environ:
                return

            # status follows UNIX exit code convention
            status = 1
            if was_successful:
                status = 0

            payload = "ts=%.0f" % (time.time())
            payload += " event=data_transfer"
            payload += " level=INFO"
            payload += " status=" + str(status)
            payload += " wf_uuid=" + os.environ["PEGASUS_WF_UUID"]
            payload += " dag_job_id=" + os.environ["PEGASUS_DAG_JOB_ID"]
            payload += " hostname=" + socket.getfqdn()
            payload += " condor_job_id=" + os.environ["CONDOR_JOBID"]
            payload += " src_url=" + transfer.src_url()
            payload += " src_site_name=" + transfer.get_src_site_label()
            payload += " dst_url=" + transfer.dst_url()
            payload += " dst_site_name=" + transfer.get_dst_site_label()
            payload += " transfer_start_ts=%.0f" % (t_start)
            payload += " transfer_duration=%.0f" % (t_end - t_start)
            if filesize is not None and filesize > 0:
                payload += " bytes_transferred=%.0f" % (filesize)
            payload += "  "

            logger.debug(payload)
            data = (
                '{"properties":{},"routing_key":"%s","payload":"%s","payload_encoding":"base64"}'
                % (os.environ["PEGASUS_WF_UUID"], base64.encodestring(payload))
            )
            logger.debug(data)
            req = urllib2.Request(os.environ["KICKSTART_MON_ENDPOINT_URL"], data)
            base64string = base64.encodestring(
                os.environ["KICKSTART_MON_ENDPOINT_CREDENTIALS"]
            )[:-1]
            authheader = "Basic %s" % base64string
            req.add_header("Authorization", authheader)
            try:
                u = urllib2.urlopen(req)
            except IOError as e:
                logger.error("Unable to publish to Panorama: " + str(e))
                return
            data = u.read()


class SimilarWorkSet:
    """
    A transfer set is a set of similar transfers, similar in the sense
    that all the transfers have the same source and destination protocols
    """

    _transfers = None
    _available_handlers = []
    _primary_handler = None
    _secondary_handler = None
    _tmp_file = None
    _excessive_failures = False

    def __init__(self, transfers_l, completed_q, failed_q):

        self._transfers = transfers_l
        self._completed_q = completed_q
        self._failed_q = failed_q

        # load all the handlers - does the order matter?
        self._available_handlers.append(FileHandler())
        self._available_handlers.append(GridFtpHandler())
        self._available_handlers.append(HttpHandler())
        self._available_handlers.append(IRodsHandler())
        self._available_handlers.append(S3Handler())
        self._available_handlers.append(GlobusOnlineHandler())
        self._available_handlers.append(GSHandler())
        self._available_handlers.append(GFALHandler())
        self._available_handlers.append(ScpHandler())
        self._available_handlers.append(GSIScpHandler())
        self._available_handlers.append(StashHandler())
        self._available_handlers.append(SymlinkHandler())
        self._available_handlers.append(MovetoHandler())
        self._available_handlers.append(DockerHandler())
        self._available_handlers.append(SingularityHandler())
        self._available_handlers.append(HPSSHandler())
        self._available_handlers.append(WebdavHandler())

        # mkdirs and removes
        if isinstance(transfers_l[0], Mkdir) or isinstance(transfers_l[0], Remove):
            proto = transfers_l[0].get_proto()
            for h in self._available_handlers:
                if h.protocol_check(None, proto):
                    self._primary_handler = h
                    logger.debug("Selected %s for handling these transfers" % (h._name))
                    return
            raise RuntimeError("Unable to find handlers for target '%s'" % (proto))

        ## normal transfer below this
        # supported container protocols
        container_protos = ("docker", "shub", "library")

        src_proto = transfers_l[0].get_src_proto()
        transfers_l[0].get_src_type()

        dst_proto = transfers_l[0].get_dst_proto()
        dst_type = transfers_l[0].get_dst_type()

        # if file_type is set, it adds to the protocol in the handler mapping,
        # but only for docker/singularity to file://
        # if transfers_l[0].get_src_type() is not None and \
        #   src_proto == 'file':
        #    src_proto = src_proto + '::' + transfers_l[0].get_src_type()
        if src_proto in container_protos:
            if dst_type is not None and dst_proto == "file":
                dst_proto = dst_proto + "::" + dst_type

        # can we find one handler which can handle both source
        # and destination protocols directly?
        # (sometimes we want to force split transfer to do things like
        #  local checksumming)
        if not self.force_split_transfers(transfers_l[0]):
            for h in self._available_handlers:
                if h.protocol_check(src_proto, dst_proto):
                    self._primary_handler = h
                    logger.debug("Selected %s for handling these transfers" % (h._name))
                    return

        # we need to split the transfer from src to local file,
        # and then transfer the local file to the dst

        # carry the types so that for example docker->gsiftp::docker
        # becomes docker->file::docker + file->gsiftp
        # TODO: improve logic here - currently we have to limit this
        # to docker and singularity transfers
        middle_in_proto = "file"
        if src_proto in container_protos and dst_type is not None:
            middle_in_proto = "file::" + dst_type
        middle_out_proto = "file"
        # if transfers_l[0].get_src_type() is not None:
        #    middle_out_proto = 'file::' + transfers_l[0].get_src_type()

        for h in self._available_handlers:
            if h.protocol_check(src_proto, middle_in_proto):
                self._primary_handler = h
                break
        for h in self._available_handlers:
            # symlink destinations are a special case as a symlink to a temporary
            # files does not make sense - override as a file->file transfer to force
            # the file to be copied
            if dst_proto == "symlink":
                dst_proto = "file"
            if h.protocol_check(middle_out_proto, dst_proto):
                self._secondary_handler = h
                break
        if self._primary_handler is None or self._secondary_handler is None:
            raise RuntimeError(
                "Unable to find handlers for '%s' to '%s' transfers"
                % (src_proto, dst_proto)
            )

        logger.debug(
            "Selected %s and %s for handling these transfers"
            % (self._primary_handler._name, self._secondary_handler._name)
        )

    def do_transfers(self):
        """
        given a list of transfers, figure out what handlers are needed
        and then execute the transfers
        """

        assert self._transfers is not None

        success_list = []
        verify_list = []
        failed_list = []

        # mkdirs
        if isinstance(self._transfers[0], Mkdir):
            try:
                (success_list, failed_list) = self._primary_handler.do_mkdirs(
                    self._transfers
                )
            except Exception:
                logger.error("Exception while doing mkdirs")
                raise
            # accounting
            for t in success_list:
                self._completed_q.put(t)
            for t in failed_list:
                self._failed_q.put(t)
            return

        # removes
        if isinstance(self._transfers[0], Remove):
            try:
                (success_list, failed_list) = self._primary_handler.do_removes(
                    self._transfers
                )
            except Exception:
                logger.error("Exception while doing removes")
                raise
            # accounting
            for t in success_list:
                self._completed_q.put(t)
            for t in failed_list:
                self._failed_q.put(t)
            return

        # actual transfers
        self._tmp_name = None
        if self._secondary_handler is not None:
            # we have a two stage transfer to deal with and we need a temp file
            self._tmp_name = self.get_temp_file()
            # open the permission up to make sure files downstream
            # get sane permissions to inherit
            os.chmod(self._tmp_name, 0o0644)
            logger.debug("Using temporary file %s for transfers" % (self._tmp_name))

        # standard src->dst single transfer case
        # We are being extra careful to detect failures here. Recoverable errors during the transfers
        # are reported as failed_l in the return from a handler, while exceptions are considered fatal

        if self._secondary_handler is None:
            # one handler to rule them all!
            try:
                (success_list, failed_list) = self._primary_handler.do_transfers(
                    self._transfers
                )
            except Exception:
                logger.exception("Exception while doing transfer:")
                raise

        else:
            for transfer in self._transfers:

                # break up the transfer into two, but keep a handle to the main
                # transfer as that is the one which will have to go back to the
                # failed queue in case of failure
                t_one = Transfer()
                t_one.lfn = transfer.lfn
                t_one.add_src(transfer.get_src_site_label(), transfer.src_url())
                t_one.add_dst("local", "file://" + self._tmp_name)
                # checksum after the first step
                t_one.generate_checksum = transfer.generate_checksum

                t_two = Transfer()
                t_two.lfn = transfer.lfn
                t_two.add_src("local", "file://" + self._tmp_name)
                t_two.add_dst(transfer.get_dst_site_label(), transfer.dst_url())
                try:
                    [s, f] = self._primary_handler.do_transfers([t_one])
                    if len(s) == 1:
                        os.chmod(self._tmp_name, 0o0644)
                        [s, f] = self._secondary_handler.do_transfers([t_two])
                except Exception:
                    logger.exception("Exception while doing transfer:")
                    raise
                if len(s) == 1:
                    success_list.append(transfer)
                else:
                    failed_list.append(transfer)

        # remove temp file
        self.clean_up_temp_file(self._tmp_name)

        # verify that the remotely stored file's checksum matches - this means
        # pulling the file back to the local filesystem and verifying the checksum
        verify_list = success_list
        success_list = []
        for t in verify_list:
            if t.verify_checksum_remote:

                local_name = None
                temp_name = None

                # which handler to use depends on if it is a single or two
                # stage handler
                handler = self._primary_handler
                if self._secondary_handler is not None:
                    handler = self._secondary_handler

                # local files are a special case
                if t.get_dst_proto() == "file":
                    local_name = t.get_dst_path()
                else:
                    # first verify that we can actually pull the file back
                    if not handler.protocol_check(t.get_dst_proto(), "file"):
                        logger.warn(
                            "Unable to pull file from "
                            + t.get_dst_proto()
                            + " to local file. Skipping integrity check."
                        )
                        continue

                    logger.info("Pulling back " + t.lfn + " to verify checksum")
                    temp_name = self.get_temp_file()
                    t_verify = Transfer()
                    t_verify.lfn = t.lfn
                    t_verify.add_src(t.get_dst_site_label(), t.dst_url())
                    t_verify.add_dst("local", "file://" + temp_name)
                    (success_verify, failed_verify,) = handler.do_transfers([t_verify])
                    if failed_verify is []:
                        failed_list.append(t)
                        self.clean_up_temp_file(temp_name)
                        continue
                    local_name = temp_name

                # verify checksum of the now locally accessible file
                if self.verify_integrity_checksum(t.lfn, local_name, t.linkage):
                    success_list.append(t)
                else:
                    failed_list.append(t)

                if temp_name is not None:
                    self.clean_up_temp_file(temp_name)
            else:
                # no verification needed
                success_list.append(t)

        # accounting
        for t in success_list:
            self._completed_q.put(t)
        for t in failed_list:
            self._failed_q.put(t)
        count_success = self._completed_q.qsize()
        count_failed = self._failed_q.qsize()
        count_total = count_success + count_failed

        # determine if we saw excessive failures
        if count_total > 10 and (count_failed / float(count_total)) > 0.8:
            self._excessive_failures = True

    def excessive_failures(self):
        """
        Did the last transfer set see excessive failures?
        """
        return self._excessive_failures

    def has_gridftp_transfers(self):
        """
        Check if this transfer set has gridftp transfers
        """
        if (
            self._transfers[0].src_proto == "gsiftp"
            or self._transfers[0].dst_proto == "gsiftp"
        ):
            return True
        return False

    def force_split_transfers(self, transfer):
        """
        Examine the transfer and determine if we want to force split transfers
        for example in the case where we have gsiftp->gsiftp and asked to
        do checksumming
        """
        # only worry about checksumming now
        if transfer.generate_checksum is None or transfer.generate_checksum is False:
            return False

        if (
            transfer.get_src_proto() == "gsiftp"
            and transfer.get_dst_proto() == "gsiftp"
        ):
            return True

    def verify_integrity_checksum(self, lfn, fname, linkage):
        """
        Call out to pegasus-integrity to verify a checksum for the file.
        """

        tools = utils.Tools()

        if tools.find("pegasus-integrity", "help", None, [prog_dir]) is None:
            logger.error(
                "Unable to do integrity checking because pegasus-integrity not found"
            )
            return

        if lfn is None or lfn == "":
            logger.error("lfn is required when enabling checksumming")
            return

        cmd = '%s --verify="%s=%s"' % (tools.full_path("pegasus-integrity"), lfn, fname)
        try:
            tc = utils.TimedCommand(cmd)
            tc.run()
            stats.add_integrity_verify(linkage, tc.get_duration(), True)
        except RuntimeError as err:
            logger.error(err)
            stats.add_integrity_verify(linkage, tc.get_duration(), False)
            return False
        return True

    def get_temp_file(self):
        """
        Creates a new temporary file, returns the filename
        """
        # temp file to transfer to
        tmp_fd, self._tmp_name = tempfile.mkstemp(
            prefix="pegasus-transfer-", suffix=".data"
        )
        # the temp file is only used in shell callouts, so let's close
        # the fd to make sure we are not leaving any open fds around
        try:
            os.close(tmp_fd)
        except Exception:
            pass
        return self._tmp_name

    def clean_up_temp_file(self, fname):
        """
        Cleans up a temp file
        """
        try:
            os.unlink(fname)
        except Exception:
            pass


class WorkThread(threading.Thread):
    """
    A thread which processes SimilarWorkSets
    """

    def __init__(self, thread_id, queue, current_attempt, failed_queue):
        threading.Thread.__init__(self)
        self.thread_id = thread_id
        self.queue = queue
        self.current_attempt = current_attempt
        self.failed_queue = failed_queue
        self.exception = None
        self.tb = None
        self.daemon = True

    def run(self):
        # give the threads a slow start
        time.sleep(self.thread_id * 2)
        logger.debug("Started new WorkThread with id " + str(self.thread_id))
        try:
            # Just keep grabbing SimilarWorkSets and executing them until
            # there are no more to process, then exit
            while True:
                ts = self.queue.get(False)
                logger.debug(
                    "Thread "
                    + str(self.thread_id)
                    + " is executing transfer "
                    + str(ts)
                )
                ts.do_transfers()
                # if we see excessive failures, stop the thread early
                if ts.excessive_failures():
                    break

        except queue.Empty:
            return
        except Exception as e:
            self.exception = e
            self.tb = traceback.format_exc()


class Alarm(Exception):
    pass


# --- global variables ----------------------------------------------------------------

prog_dir = os.path.realpath(os.path.join(os.path.dirname(sys.argv[0])))
prog_base = os.path.split(sys.argv[0])[1]  # Name of this program

logger = logging.getLogger("Pegasus")

# threads we have currently running
threads = []

# common credentials
credentials = configparser.ConfigParser()

# should file transfers symlink rather than copy
symlink_file_transfer = False

# track remote directories created so that don't have to
# try to create them over and over again
remote_dirs_created = {}

# track which lfns we have already checksummed
integrity_checksummed = []

# many commands need to know how long the command line can be
max_cmd_len = 0

# gsiftp failure count - used to provide sane globus-url-copy options
gsiftp_failures = 0

# stats
stats = Stats()

# singleton - but should we make it a global instead?
tools = utils.Tools()


# --- functions ----------------------------------------------------------------


def setup_logger(debug_flag):

    # log to the console
    console = logging.StreamHandler()

    # default log level - make logger/console match
    logger.setLevel(logging.INFO)
    console.setLevel(logging.INFO)

    # debug - from command line
    if debug_flag:
        logger.setLevel(logging.DEBUG)
        console.setLevel(logging.DEBUG)

    # formatter
    formatter = logging.Formatter("%(asctime)s %(levelname)7s:  %(message)s")
    console.setFormatter(formatter)
    logger.addHandler(console)
    logger.debug("Logger has been configured")


def prog_sigint_handler(signum, frame):
    logger.warn("Exiting due to signal %d" % (signum))
    myexit(1)


def alarm_handler(signum, frame):
    raise Alarm


def expand_env_vars(s):
    re_env_var = re.compile(r"\${?([a-zA-Z][a-zA-Z0-9_]+)}?")
    s = re.sub(re_env_var, get_env_var, s)
    return s


def get_env_var(match):
    name = match.group(1)
    value = ""
    logger.debug("Looking up " + name + " environment variable")
    if name in os.environ:
        value = os.environ[name]
    return value


def backticks(cmd_line):
    """
    what would a python program be without some perl love?
    """
    return (
        subprocess.Popen(cmd_line, shell=True, stdout=subprocess.PIPE)
        .communicate()[0]
        .decode()
    )


def max_cmd_length():
    """
    os.sysconf('SC_ARG_MAX') does not always work, so use brute force
    to determine the maximum command line length the system can handle
    """

    for n in range(10, 20):
        s = "X" * (2 ** n)
        cmd = "echo " + s + " >/dev/null"
        try:
            backticks(cmd)
        except Exception:
            return (2 ** (n - 1)) / 2

    # we shouldn't really get here, but if we do, 2^20/2
    return (2 ** 20) / 2


def env_setup():

    global max_cmd_len

    # PATH setup
    path = "/usr/bin:/bin"
    if "PATH" in os.environ:
        path = os.environ["PATH"]
    path_entries = path.split(":")
    if "" in path_entries:
        path_entries.remove("")

    # is /usr/bin /bin in the path?
    if not ("/usr/bin" in path_entries):
        path_entries.append("/usr/bin")
    if not ("/bin" in path_entries):
        path_entries.append("/bin")
    # need /usr/sbin for mksquashfs
    if not ("/usr/sbin" in path_entries):
        path_entries.append("/usr/sbin")

    # fink on macos x
    if os.path.exists("/sw/bin") and not ("/sw/bin" in path_entries):
        path_entries.append("/sw/bin")

    # PYTHONHOME can cause problems when we call out to other tools
    if "PYTHONHOME" in os.environ:
        logger.warning(
            "PYTHONHOME was found in the environment."
            + " Unsetting to make sure callouts to transfer tools"
            + " will work."
        )
        del os.environ["PYTHONHOME"]

    # need LD_LIBRARY_PATH for Globus tools
    ld_library_path = ""
    if "LD_LIBRARY_PATH" in os.environ:
        ld_library_path = os.environ["LD_LIBRARY_PATH"]
    ld_library_path_entries = ld_library_path.split(":")
    if "" in ld_library_path_entries:
        ld_library_path_entries.remove("")

    # if GLOBUS_LOCATION is set, we might want to update PATH and LD_LIBRARY_PATH
    if "GLOBUS_LOCATION" in os.environ:
        if os.environ["GLOBUS_LOCATION"] + "/bin" not in path_entries:
            path_entries.append(os.environ["GLOBUS_LOCATION"] + "/bin")
        if os.environ["GLOBUS_LOCATION"] + "/lib" not in ld_library_path_entries:
            ld_library_path_entries.append(os.environ["GLOBUS_LOCATION"] + "/lib")

    os.environ["PATH"] = ":".join(path_entries)
    os.environ["LD_LIBRARY_PATH"] = ":".join(ld_library_path_entries)
    os.environ["DYLD_LIBRARY_PATH"] = ":".join(ld_library_path_entries)
    logger.info("PATH=" + os.environ["PATH"])
    logger.info("LD_LIBRARY_PATH=" + os.environ["LD_LIBRARY_PATH"])

    max_cmd_len = max_cmd_length()
    logger.debug("Maximum command line argument length to be used: %d" % (max_cmd_len))


def check_cred_fs_permissions(path):
    """
    Checks to make sure a given credential is protected by the file system
    permissions. If left too open (for example after a transfer over GASS,
    chmod it to be readable only by us.
    """
    if not os.path.exists(path):
        raise Exception("Credential file %s does not exist" % (path))
    if (os.stat(path).st_mode & 0o777) != 0o600:
        logger.warning("%s found to have weak permissions. chmod to 0600." % (path))
        os.chmod(path, 0o600)


def verify_local_file(path):
    """
    makes sure a local file exists and is readable
    """
    if not (os.path.exists(path)):
        logger.error("Expected local file does not exist: " + path)
        return False

    # check readability
    try:
        f = open(path, "r")
        f.close()
    except Exception:
        logger.error("File is not readable: " + path)
        return False

    return True


def prepare_local_dir(path):
    """
    makes sure a local path exists before putting files into it
    """
    if not (os.path.exists(path)):
        logger.debug("Creating local directory " + path)
        try:
            os.makedirs(path, 0o0755)
        except os.error as err:
            # if dir already exists, ignore the error
            if not (os.path.isdir(path)):
                raise RuntimeError(err)


def transfers_groupable(a, b):
    """
    compares two url_pairs, and determines if they are similar enough to be
    grouped together for one tool
    """
    if type(a) is not type(b):
        return False

    if isinstance(a, Mkdir):
        return False

    if isinstance(a, Remove):
        if a.get_proto() != b.get_proto():
            return False
        return True

    # standard transfer
    if not a.groupable() or not b.groupable():
        return False
    if a.get_src_proto() != b.get_src_proto():
        return False
    if a.get_dst_proto() != b.get_dst_proto():
        return False
    return True


def stats_add(filename):
    global stats_total_bytes
    try:
        s = os.stat(filename)
        stats_total_bytes = stats_total_bytes + s[stat.ST_SIZE]
    except Exception:
        pass  # ignore


def stats_summarize():
    if stats_total_bytes == 0:
        logger.info("Stats: no local files in the transfer set")
        return

    total_secs = stats_end - stats_start
    Bps = stats_total_bytes / total_secs

    logger.info(
        "Stats: %sB transferred in %.0f seconds. Rate: %sB/s (%sb/s)"
        % (
            iso_prefix_formatted(stats_total_bytes),
            total_secs,
            iso_prefix_formatted(Bps),
            iso_prefix_formatted(Bps * 8),
        )
    )
    logger.info("NOTE: stats do not include third party gsiftp/srm transfers")


def iso_prefix_formatted(n):
    prefix = ""
    n = float(n)
    if n > (1024 * 1024 * 1024 * 1024):
        prefix = "T"
        n = n / (1024 * 1024 * 1024 * 1024)
    elif n > (1024 * 1024 * 1024):
        prefix = "G"
        n = n / (1024 * 1024 * 1024)
    elif n > (1024 * 1024):
        prefix = "M"
        n = n / (1024 * 1024)
    elif n > (1024):
        prefix = "K"
        n = n / (1024)
    return "%.1f %s" % (n, prefix)


def json_object_decoder(obj):
    """
    utility function used by json.load() to parse some known objects into equilvalent Python objects
    """
    if "type" in obj and obj["type"] == "transfer":
        t = Transfer()
        if "lfn" in obj:
            t.set_lfn(obj["lfn"])
        if "linkage" in obj:
            t.set_linkage(obj["linkage"])
        if "verify_symlink_source" in obj:
            t.set_verify_symlink_source(obj["verify_symlink_source"])
        if "generate_checksum" in obj:
            t.generate_checksum = obj["generate_checksum"]
        if "verify_checksum_remote" in obj:
            t.verify_checksum_remote = obj["verify_checksum_remote"]
        # src
        for surl in obj["src_urls"]:
            file_type = None
            if "type" in surl:
                file_type = surl["type"]
            priority = None
            if "priority" in surl:
                priority = int(surl["priority"])
            t.add_src(surl["site_label"], surl["url"], file_type, priority)
        for durl in obj["dest_urls"]:
            file_type = None
            if "type" in durl:
                file_type = durl["type"]
            priority = None
            if "priority" in durl:
                priority = int(durl["priority"])
            t.add_dst(durl["site_label"], durl["url"], file_type, priority)
        return t
    elif "type" in obj and obj["type"] == "mkdir":
        m = Mkdir()
        m.set_url(obj["target"]["site_label"], obj["target"]["url"])
        return m
    elif "type" in obj and obj["type"] == "remove":
        r = Remove()
        r.set_url(obj["target"]["site_label"], obj["target"]["url"])
        if "recursive" in obj["target"]:
            r.set_recursive(obj["target"]["recursive"])
        return r
    return obj


def read_v1_format(input, inputs_l):
    line_nr = 0
    pair_nr = 0
    line_state = 3  # 0=SrcComment, 1=SrcUrl, 2=DstComment, 3=DstUrl
    url_pair = None
    src_sitename = None
    dst_sitename = None
    try:
        for line in input.split("\n"):
            line_nr += 1
            if len(line) > 4:
                line = line.rstrip("\n")

                # src comment
                if line_state == 3 and line[0] == "#":
                    line_state = 0
                    if url_pair is None:
                        pair_nr += 1
                        url_pair = Transfer()
                    r = re_parse_comment.search(line)
                    if r:
                        src_sitename = r.group(1)
                    else:
                        logger.critical(
                            "Unable to parse comment on line %d" % (line_nr)
                        )
                        myexit(1)

                # src url
                elif line_state == 0 or line_state == 3:
                    line_state = 1
                    if url_pair is None:
                        pair_nr += 1
                        url_pair = Transfer()
                    url_pair.add_src(src_sitename, line)

                # dst comment
                elif line_state == 1 and line[0] == "#":
                    line_state = 2
                    r = re_parse_comment.search(line)
                    if r:
                        dst_sitename = r.group(1)
                    else:
                        logger.critical(
                            "Unable to parse comment on line %d" % (line_nr)
                        )
                        myexit(1)

                # dst url
                elif line_state == 2 or line_state == 1:
                    line_state = 3
                    url_pair.add_dst(dst_sitename, line)
                    inputs_l.append(url_pair)
                    url_pair = None

    except Exception as err:
        logger.critical("Error reading url list: %s" % (err))
        myexit(1)


def read_json_format(input, inputs_l):
    """
    Reads transfers from the new JSON based input format
    """
    try:
        data = json.loads(input, object_hook=json_object_decoder)
    except Exception as err:
        logger.critical("Error parsing the transfer specification JSON: " + str(err))
        myexit(1)

    for entry in data:
        if (
            isinstance(entry, Mkdir)
            or isinstance(entry, Transfer)
            or isinstance(entry, Remove)
        ):
            inputs_l.append(entry)
        else:
            logger.critical("Unkown JSON entry: %s" % (str(entry)))
            myexit(1)


def myexit(rc):
    """
    system exit without a stack trace
    """
    try:
        sys.exit(rc)
    except SystemExit:
        sys.exit(rc)


# --- main ----------------------------------------------------------------------------


def main():
    global threads
    global credentials
    global stats_start
    global stats_end
    global symlink_file_transfer

    # dup stderr onto stdout
    sys.stderr = sys.stdout

    # Configure command line option parser
    prog_usage = "usage: %s [options]" % (prog_base)
    parser = optparse.OptionParser(usage=prog_usage)

    parser.add_option(
        "-f",
        "--file",
        action="store",
        dest="file",
        help="File containing URL pairs to be transferred."
        + " If not given, list is read from stdin.",
    )
    parser.add_option(
        "-m",
        "--max-attempts",
        action="store",
        type="int",
        dest="max_attempts",
        default=3,
        help="Number of attempts allowed for each transfer." + " Default is 3.",
    )
    parser.add_option(
        "-n",
        "--threads",
        action="store",
        type="int",
        dest="threads",
        default=0,
        help="Number of threads to process transfers."
        + " Default is 8. This option can also be set"
        + " via the PEGASUS_TRANSFER_THREADS environment"
        + " variable. The command line option takes"
        + " precedence over the environment variable.",
    )
    parser.add_option(
        "-s",
        "--symlink",
        action="store_true",
        dest="symlink",
        help="Allow symlinking of file URLs."
        + " If the source and destination URLs chosen"
        + " are both file URLs with the same site_label"
        + " then the source file will be symlinked"
        + " to the destination rather than being copied.",
    )
    parser.add_option(
        "-d",
        "--debug",
        action="store_true",
        dest="debug",
        help="Enables debugging ouput.",
    )

    # Parse command line options
    (options, args) = parser.parse_args()
    setup_logger(options.debug)

    # Die nicely when asked to (Ctrl+C, system shutdown)
    signal.signal(signal.SIGINT, prog_sigint_handler)

    attempts_max = options.max_attempts

    if options.threads is None or options.threads == 0:
        if "PEGASUS_TRANSFER_THREADS" in os.environ:
            options.threads = int(os.environ["PEGASUS_TRANSFER_THREADS"])
        else:
            options.threads = 8

    # stdin or file input?
    input_data = None
    if options.file is None:
        logger.info("Reading URL pairs from stdin")
        input_file = sys.stdin
    else:
        logger.info("Reading transfer specification from %s" % (options.file))
        try:
            input_file = open(options.file, "r")
        except Exception as err:
            logger.critical("Error opening input file: %s" % (err))
            myexit(1)
    try:
        input_data = input_file.read()
        input_file.close()
    except Exception as err:
        logger.critical("Error reading transfer list: %s" % (err))
        myexit(1)

    # store options in global variables
    symlink_file_transfer = options.symlink

    # queues to track the work
    inputs_l = []
    ready_q = queue.Queue()
    failed_q = queue.Queue()
    completed_q = queue.Queue()

    # determine format, and read the transfer specification
    if input_data[0:5] == "# src":
        read_v1_format(input_data, inputs_l)
    else:
        read_json_format(input_data, inputs_l)

    total_transfers = len(inputs_l)
    logger.info("%d transfers loaded" % (total_transfers))

    # we will now sort the list as some tools (gridftp) can optimize when
    # given a group of similar transfers
    inputs_l.sort()
    for t in inputs_l:
        ready_q.put(t)

    # check environment
    try:
        env_setup()
    except Exception as err:
        logger.critical(err)
        myexit(1)

    # load common credentials, if available
    if "PEGASUS_CREDENTIALS" in os.environ:
        logger.debug("Loading credentials from " + os.environ["PEGASUS_CREDENTIALS"])
        if not os.path.isfile(os.environ["PEGASUS_CREDENTIALS"]):
            logger.critical(
                "Credentials file does not exist: " + os.environ["PEGASUS_CREDENTIALS"]
            )
            myexit(1)
        mode = os.stat(os.environ["PEGASUS_CREDENTIALS"]).st_mode
        if mode & (stat.S_IRWXG | stat.S_IRWXO):
            logger.critical("Permissions of credentials file %s are too liberal" % cfg)
            myexit(1)
        credentials = configparser.ConfigParser()
        try:
            credentials.read(os.environ["PEGASUS_CREDENTIALS"])
        except Exception as err:
            logger.critical("Unable to load credentials: " + str(err))
            myexit(1)

    # start the stats time
    stats_start = time.time()

    # Attempt transfers until the queue is empty. We create SimilarWorkSets
    # of the transfers, and then hand then of to our worker threads. But
    # note that we are only doing the threads for one attempt at a time.
    # After failures, transfers might be regrouped, and then handed of to the
    # thread pool again.
    done = False
    too_many_failures = False
    attempt_current = 0
    approx_transfer_per_thread = total_transfers / (float)(options.threads)

    # also cap the approx_transfer_per_thread so that we can fail early
    # if we have to
    approx_transfer_per_thread = min(approx_transfer_per_thread, 100)

    while not done:

        tset_q = queue.Queue()

        attempt_current = attempt_current + 1
        logger.info("-" * 80)
        logger.info("Starting transfers - attempt %d" % (attempt_current))

        # this outer loop is for trying all url pair combinations of a transfer
        # before moving on and marking it as failed
        while not ready_q.empty():

            # organize the transfers
            while not ready_q.empty():
                t_main = ready_q.get()

                # create a list of transfers to pass to underlying tool
                t_list = []
                t_list.append(t_main)

                try:
                    t_next = ready_q.get(False)
                    while t_next is not None:
                        if len(
                            t_list
                        ) < approx_transfer_per_thread and transfers_groupable(
                            t_main, t_next
                        ):
                            t_list.append(t_next)
                            t_next = ready_q.get(False)
                        else:
                            # done, put the last transfer back
                            ready_q.put(t_next)
                            t_next = None
                except queue.Empty:
                    pass

                # magic!
                ts = SimilarWorkSet(t_list, completed_q, failed_q)
                tset_q.put(ts)

            # pool of worker threads
            t_id = 0
            num_threads = min(options.threads, tset_q.qsize())
            if attempt_current > 2:
                num_threads = 1
            logger.debug("Using %d threads for this set of transfers" % (num_threads))
            for i in range(num_threads):
                t_id += 1
                t = WorkThread(t_id, tset_q, attempt_current, failed_q)
                threads.append(t)
                t.start()

            # wait for the threads to finish all the transfers
            for t in threads:
                t.join()
                # do we need to do any better error handling here?
                if t.exception is not None:
                    logger.critical(t.tb)
                    myexit(2)
            threads = []

            # transfers might have multiple sources/destinations and
            # we should try them all in each round
            failed_q_updated = queue.Queue()
            while not failed_q.empty():
                t = failed_q.get()
                t.move_to_next_sub_transfer()
                # see if there are more pairs to try
                if t.get_sub_transfer_index() > 0:
                    ready_q.put(t)
                else:
                    failed_q_updated.put(t)
            failed_q = failed_q_updated

            # if we get here and the transfer sets queue (tset_q) is not
            # empty, that means the work threads saw too many failures
            # and short-circuited the rest of the transfers
            if not tset_q.empty() and not failed_q.empty():
                logger.error("Too many failures to continue trying - exiting early")
                too_many_failures = True
                break

        # when we get here, all the tries for one round has been attempted

        logger.debug("%d items in failed_q" % (failed_q.qsize()))

        # are we done?
        if attempt_current == attempts_max or failed_q.empty() or too_many_failures:
            done = True
            break

        # retry failed transfers with a random delay - useful when
        # large workflows overwhelm data services
        if not failed_q.empty() and attempt_current < attempts_max:
            d = min(5 ** (attempt_current + 2) + random.randint(1, 20), 300)
            logger.debug("Sleeping for %d seconds before the next attempt" % (d))
            time.sleep(d)
        while not failed_q.empty():
            t = failed_q.get()
            if attempt_current >= 2:
                # only allow grouping the first 2 attempts, then fall back
                # to single file transfers
                t.allow_grouping = False
            ready_q.put(t)

    logger.info("-" * 80)

    # end the stats timer and show summary
    stats.stats_summary()

    if not failed_q.empty():
        logger.critical("Some transfers failed! See above," + " and possibly stderr.")

        myexit(1)

    logger.info("All transfers completed successfully.")

    myexit(0)


if __name__ == "__main__":
    main()
