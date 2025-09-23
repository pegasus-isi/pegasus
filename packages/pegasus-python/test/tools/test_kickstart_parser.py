from tempfile import NamedTemporaryFile

import pytest
import yaml

from Pegasus.tools import kickstart_parser

LOCATION_MULTIPART_RECORD = """---------------pegasus-multipart
location:
    geohash: s000
    ip: 10.101.104.66
    latitude: 0
    longitude: 0
    organization: N/A
    subdomain: ads.isi.edu
"""

LOCATION_SERVICE_DOWN_MULTIPART_RECORD = """---------------pegasus-multipart
<html>
<head><title>503 Service Temporarily Unavailable</title></head>
<body>
<center><h1>503 Service Temporarily Unavailable</h1></center>
<hr><center>nginx</center>
</body>
</html>
"""

INTEGRITY_MULTIPART_RECORD = """---------------pegasus-multipart
- integrity_verification_attempts:
  - lfn: "f.a"
    pfn: "f.a"
    sha256: abc
    success: True
  - lfn: "preprocess"
    pfn: "preprocess"
    sha256: def
    success: True
- integrity_summary:
    succeeded: 2
    failed: 0
    duration: 0.146"""

SINGLE_KICKSTART_RECORD = """- invocation: True
  version: 3.0
  start: 2025-03-12T12:39:36.752-07:00
  duration: 30.310
  transformation: "preprocess"
  derivation: "ID0000001"
  resource: "condorpool"
  wf-label: "diamond"
  wf-stamp: "2025-03-07T15:01:09-08:00"
  interface: 
  hostaddr: 0.0.0.0
  pid: 32401
  uid: 3520
  user: vahi
  gid: 337
  group: gridstaff
  umask: 0o0022
  mainjob:
    start: 2025-03-12T12:39:36.754-07:00
    duration: 30.308
    pid: 32402
    usage:
      utime: 0.026
      stime: 0.013
      maxrss: 7208
      minflt: 3205
      majflt: 1
      nswap: 0
      inblock: 0
      outblock: 0
      msgsnd: 0
      msgrcv: 0
      nsignals: 0
      nvcsw: 1
      nivcsw: 34
    status:
      raw: 0
      regular_exitcode: 0
    executable:
      file_name: /opt/condor/8.8.7/local.corbusier/execute/dir_32279/preprocess
      mode: 0o100755
      size: 773
      inode: 423379607
      nlink: 1
      blksize: 4096
      blocks: 8
      mtime: 2025-03-12T12:39:31-07:00
      atime: 2025-03-12T12:39:31-07:00
      ctime: 2025-03-12T12:39:31-07:00
      uid: 3520
      user: vahi
      gid: 337
      group: gridstaff
    argument_vector:
      - "-i"
      - "f.a"
      - "-o"
      - "f.b1"
      - "-o"
      - "f.b2"
    procs:
  jobids:
    condor: 644.0
  cwd: /opt/condor/8.8.7/local.corbusier/execute/dir_32279
  usage:
    utime: 0.002
    stime: 0.004
    maxrss: 1412
    minflt: 725
    majflt: 0
    nswap: 0
    inblock: 0
    outblock: 0
    msgsnd: 0
    msgrcv: 0
    nsignals: 0
    nvcsw: 1
    nivcsw: 9
  machine:
    page-size: 4096
    uname_system: darwin
    uname_nodename: corbusier.ads.isi.edu
    uname_release: 23.6.0
    uname_machine: x86_64
    ram_total: 50331648
    ram_avail: 5094388
    ram_active: 22379852
    ram_inactive: 19286688
    ram_wired: 3546940
    swap_total: 0
    swap_avail: 0
    swap_used: 0
    boot: 2025-03-12T09:05:13.660-07:00
    cpu_count: 24
    cpu_speed: 3300
    cpu_vendor: GenuineIntel
    cpu_name: Intel(R) Xeon(R) W-3235 CPU @ 3.30GHz
    load_min1: 2.44
    load_min5: 2.18
    load_min15: 2.31
    proc_total: 731
    proc_running: 7
    proc_sleeping: 724
  files:
    f.b1:
      lfn: "f.b1"
      file_name: /opt/condor/8.8.7/local.corbusier/execute/dir_32279/f.b1
      mode: 0o100644
      size: 61
      inode: 423383873
      nlink: 1
      blksize: 4096
      blocks: 8
      mtime: 2025-03-12T12:40:07-07:00
      atime: 2025-03-12T12:40:07-07:00
      ctime: 2025-03-12T12:40:07-07:00
      uid: 3520
      user: vahi
      gid: 337
      group: gridstaff
      output: True
      sha256: 839037d553aebfd8442ea24724a94a1e8fe6b134243c9848527594ba6172ddcc
      checksum_timing: 0.00
    stdin:
      file_name: /dev/null
      mode: 0o20666
      size: 0
      inode: 316
      nlink: 1
      blksize: 131072
      blocks: 0
      mtime: 2025-03-12T12:39:36-07:00
      atime: 2025-03-12T09:05:12-07:00
      ctime: 2025-03-12T12:39:36-07:00
      uid: 0
      user: root
      gid: 0
      group: wheel
    stdout:
      temporary_name: /opt/condor/8.8.7/local.corbusier/execute/dir_32279/ks.out.3phHwE
      descriptor: 3
      mode: 0o100600
      size: 54
      inode: 423383738
      nlink: 1
      blksize: 4096
      blocks: 8
      mtime: 2025-03-12T12:40:07-07:00
      atime: 2025-03-12T12:39:36-07:00
      ctime: 2025-03-12T12:40:07-07:00
      uid: 3520
      user: vahi
      gid: 337
      group: gridstaff
      data_truncated: false
      data: |
        Sleeping for 30 seconds...
        Generating output files...

    stderr:
      temporary_name: /opt/condor/8.8.7/local.corbusier/execute/dir_32279/ks.err.wXGYD4
      descriptor: 4
      mode: 0o100600
      size: 0
      inode: 423383739
      nlink: 1
      blksize: 4096
      blocks: 0
      mtime: 2025-03-12T12:39:36-07:00
      atime: 2025-03-12T12:39:36-07:00
      ctime: 2025-03-12T12:39:36-07:00
      uid: 3520
      user: vahi
      gid: 337
      group: gridstaff
    metadata:
      temporary_name: /opt/condor/8.8.7/local.corbusier/execute/dir_32279/ks.meta.PLwJhT
      descriptor: 5
      mode: 0o100600
      size: 0
      inode: 423383740
      nlink: 1
      blksize: 4096
      blocks: 0
      mtime: 2025-03-12T12:39:36-07:00
      atime: 2025-03-12T12:39:36-07:00
      ctime: 2025-03-12T12:39:36-07:00
      uid: 3520
      user: vahi
      gid: 337
      group: gridstaff
    """


def compare_record_as_yaml(record, key, expected_yaml_value):
    assert record.get("multipart") == True
    assert record.get(key) is not None, f"key {key} is not present in record {record}"
    actual = yaml.dump(record.get(key), sort_keys=True)
    assert actual == expected_yaml_value


class TestYAMLKickstartParser:
    def create_test_file(self, content):
        f = NamedTemporaryFile(mode="w+", delete=True)
        f.write(content)
        f.seek(0)
        return f

    def test_read_vanilla_kickstart_record(self):
        with NamedTemporaryFile(mode="w+", delete=True) as f:
            f.write(SINGLE_KICKSTART_RECORD)
            f.seek(0)

            parser = kickstart_parser.YAMLParser(filename=f.name)
            # Try to open the file
            assert parser.open()
            record = parser.read_record()
            assert record == SINGLE_KICKSTART_RECORD
            f.close()

    @pytest.mark.parametrize(
        "content,expected",
        [
            (LOCATION_MULTIPART_RECORD, LOCATION_MULTIPART_RECORD),
            (SINGLE_KICKSTART_RECORD, SINGLE_KICKSTART_RECORD),
            (INTEGRITY_MULTIPART_RECORD, INTEGRITY_MULTIPART_RECORD),
            (
                LOCATION_SERVICE_DOWN_MULTIPART_RECORD,
                "---------------pegasus-multipart\n",
            ),
        ],
    )
    def test_read_record(self, content, expected):
        f = self.create_test_file(content)
        parser = kickstart_parser.YAMLParser(filename=f.name)

        # Try to open the file
        assert parser.open()
        record = parser.read_record()
        assert record == expected
        parser.close()

    def test_parse_location_multipart_record(self):
        f = self.create_test_file(LOCATION_MULTIPART_RECORD)
        parser = kickstart_parser.YAMLParser(filename=f.name)

        expected_location_multipart_record = yaml.safe_load(
            LOCATION_MULTIPART_RECORD[len(kickstart_parser.PEGASUS_MULTIPART_MARKER) :]
        )
        expected_location_attributes_as_yaml = yaml.dump(
            expected_location_multipart_record.get("location"), sort_keys=True
        )

        # Try to open the file
        assert parser.open()
        parsed_records = parser.parse({}, tasks=True, clustered=True)
        parser.close()
        assert len(parsed_records) == 1
        compare_record_as_yaml(
            parsed_records.pop(), "location", expected_location_attributes_as_yaml
        )

    def test_parse_integrity_multipart_record(self):
        f = self.create_test_file(INTEGRITY_MULTIPART_RECORD)
        parser = kickstart_parser.YAMLParser(filename=f.name)

        expected_integrity_multipart_records = yaml.safe_load(
            INTEGRITY_MULTIPART_RECORD[len(kickstart_parser.PEGASUS_MULTIPART_MARKER) :]
        )
        expected_integrity_verification_as_yaml = yaml.dump(
            expected_integrity_multipart_records[0].get(
                "integrity_verification_attempts"
            ),
            sort_keys=True,
        )
        expected_integrity_summary_as_yaml = yaml.dump(
            expected_integrity_multipart_records[1].get("integrity_summary"),
            sort_keys=True,
        )

        # Try to open the file
        assert parser.open()
        parsed_records = parser.parse({}, tasks=True, clustered=True)
        parser.close()
        assert len(parsed_records) == 2

        compare_record_as_yaml(
            parsed_records.pop(),
            "integrity_summary",
            expected_integrity_summary_as_yaml,
        )
        compare_record_as_yaml(
            parsed_records.pop(),
            "integrity_verification_attempts",
            expected_integrity_verification_as_yaml,
        )
