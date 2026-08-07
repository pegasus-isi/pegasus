import configparser
import io
import json
import logging
import os
import stat
import sys
import tempfile
from pathlib import Path

import pytest

from Pegasus import transfer


class TestReadJSONFormat:
    def test_read_json_format(self):
        data = [
            {
                "type": "transfer",
                "id": 1,
                "src_urls": [{"site_label": "web", "url": "http://pegasus.isi.edu"}],
                "dest_urls": [{"site_label": "local", "url": "file:///tmp/index.html"}],
            }
        ]

        data_str = json.dumps(data)
        inputs_loaded = []
        transfer.read_json_format(input=data_str, inputs_l=inputs_loaded)

        expected_transfer_obj = transfer.Transfer()
        expected_transfer_obj._src_urls = [
            transfer.PegasusURL(
                url="http://pegasus.isi.edu", file_type="x", site_label="web"
            )
        ]
        expected_transfer_obj._dst_urls = [
            transfer.PegasusURL(
                url="file:///tmp/index.html", file_type="x", site_label="local"
            )
        ]

        assert inputs_loaded[0] == expected_transfer_obj

    def test_read_json_format_parse_error(self):
        data = [
            {
                "type": "transfer",
                "id": 1,
                "src_urls": [{"site_lael": "web", "url": "http://pegasus.isi.edu"}],
                "dest_urls": [{"site_label": "local", "url": "file:///tmp/index.html"}],
            }
        ]

        data_str = json.dumps(data)
        inputs_loaded = []
        with pytest.raises(RuntimeError) as e:
            transfer.read_json_format(input=data_str, inputs_l=inputs_loaded)

        assert "Error parsing the transfer" in str(e)

    def test_read_json_format_unknown_entry_error(self):
        data = [
            {
                "type": "badtype",
                "id": 1,
                "src_urls": [{"site_lael": "web", "url": "http://pegasus.isi.edu"}],
                "dest_urls": [{"site_label": "local", "url": "file:///tmp/index.html"}],
            }
        ]

        data_str = json.dumps(data)
        inputs_loaded = []
        with pytest.raises(RuntimeError) as e:
            transfer.read_json_format(input=data_str, inputs_l=inputs_loaded)

        assert "Unknown JSON entry:" in str(e)


@pytest.fixture
def cleanup_pegasus_credentials_env():
    """If PEGASUS_CREDENTIALS was set, remove it from the environment"""
    yield
    if os.environ["PEGASUS_CREDENTIALS"] != None:
        del os.environ["PEGASUS_CREDENTIALS"]


class TestLoadCredentials:
    def test_load_credentials(self, cleanup_pegasus_credentials_env):
        with tempfile.NamedTemporaryFile(mode="w+", delete=True) as f:
            creds = """
            [amazon]
            endpoint = https://s3.amazonaws.com/

            [joe@amazon]
            access_key = 99001122
            secret_key = abababababababababababababababab
            """
            f.write(creds)
            f.seek(0)

            os.environ["PEGASUS_CREDENTIALS"] = f.name

            credentials = transfer.load_credentials()
            assert credentials["amazon"]["endpoint"] == "https://s3.amazonaws.com/"
            assert credentials["joe@amazon"]["access_key"] == "99001122"
            assert (
                credentials["joe@amazon"]["secret_key"]
                == "abababababababababababababababab"
            )

    def test_loading_credentials_file_that_doesnt_exist(
        self, cleanup_pegasus_credentials_env
    ):
        os.environ["PEGASUS_CREDENTIALS"] = "bad_credentials_file..."
        with pytest.raises(RuntimeError) as e:
            transfer.load_credentials()

        assert "Credentials file does not exist" in str(e)

    def test_invalid_credentials_file_permissions(
        self, cleanup_pegasus_credentials_env
    ):
        with tempfile.NamedTemporaryFile(mode="w+", delete=True) as f:
            os.environ["PEGASUS_CREDENTIALS"] = f.name
            f.write("stuff")

            # change permissions to one that would cause load_credentials to
            # raise an exception
            os.chmod(path=f.name, mode=stat.S_IRWXG)

            with pytest.raises(RuntimeError) as e:
                transfer.load_credentials()

            assert "Permissions of credentials file" in str(e)

    def test_unable_to_read_credentials(self, caplog, cleanup_pegasus_credentials_env):
        caplog.set_level(logging.CRITICAL, logger="Pegasus")

        with tempfile.NamedTemporaryFile(mode="w+", delete=True) as f:
            f.write("x;kwj0923fjalksdjf438hfjojTHIS_IS_A_BAD_CONFIG_ldkfjals")
            f.seek(0)

            os.environ["PEGASUS_CREDENTIALS"] = f.name

            with pytest.raises(configparser.MissingSectionHeaderError) as e:
                transfer.load_credentials()

            assert "Unable to load credentials" in str(caplog.record_tuples)


class TestPegasusTransferInvocation:
    def test_error_opening_input_file(self):
        with pytest.raises(FileNotFoundError) as e:
            transfer.pegasus_transfer(
                max_attempts=3, num_threads=8, file="badfile", symlink=True
            )

    def test_transfers_read_from_stdin_succeeded(self, caplog):
        caplog.set_level(logging.INFO, logger="Pegasus")
        with tempfile.TemporaryDirectory() as td:
            temp_dir = Path(td)

            downloaded_files = []
            xfers = []
            NUM_XFERS = 5
            for i in range(NUM_XFERS):
                xfers.append(
                    {
                        "type": "transfer",
                        "id": i,
                        "src_urls": [
                            {"site_label": "web", "url": "http://pegasus.isi.edu"}
                        ],
                        "dest_urls": [
                            {
                                "site_label": "local",
                                "url": "file://" + str(temp_dir / f"index{i}.html"),
                            }
                        ],
                    }
                )
                downloaded_files.append(f"index{i}.html")

            sys.stdin = io.StringIO(json.dumps(xfers))

            is_successful = transfer.pegasus_transfer(
                max_attempts=1, num_threads=8, file=None, symlink=True
            )

            assert is_successful == True

            logs = str(caplog.record_tuples)
            assert f"{NUM_XFERS} transfers loaded" in logs
            assert "All transfers completed" in logs

            for f in downloaded_files:
                assert (temp_dir / f).exists()

    def test_transfers_read_from_file_succeeded(self, caplog):
        caplog.set_level(logging.DEBUG, logger="Pegasus")
        with tempfile.TemporaryDirectory() as td:
            temp_dir = Path(td)

            downloaded_files = []
            xfers = []
            NUM_XFERS = 8
            for i in range(NUM_XFERS):
                xfers.append(
                    {
                        "type": "transfer",
                        "id": i,
                        "src_urls": [
                            {"site_label": "web", "url": "http://pegasus.isi.edu"}
                        ],
                        "dest_urls": [
                            {
                                "site_label": "local",
                                "url": "file://" + str(temp_dir / f"index{i}.html"),
                            }
                        ],
                    }
                )
                downloaded_files.append(f"index{i}.html")

            with tempfile.NamedTemporaryFile("w+", delete=True) as f:
                f.write(json.dumps(xfers))
                f.seek(0)

                is_successful = transfer.pegasus_transfer(
                    max_attempts=1, num_threads=8, file=f.name, symlink=True
                )

            assert is_successful == True

            logs = str(caplog.record_tuples)
            assert f"{NUM_XFERS} transfers loaded" in logs
            assert f"Using {8} threads for this set of transfers" in logs
            assert "All transfers completed" in logs

            for f in downloaded_files:
                assert (temp_dir / f).exists()

    def test_some_transfers_failed(self, caplog):
        caplog.set_level(logging.CRITICAL, logger="Pegasus")

        with tempfile.NamedTemporaryFile(mode="w+", delete=True) as f:
            data = [
                {
                    "type": "transfer",
                    "id": 1,
                    "src_urls": [
                        {
                            "site_label": "web",
                            "url": "http://BADURL0983012830181028310831adna02a0o.scitech.group",
                        }
                    ],
                    "dest_urls": [
                        {"site_label": "local", "url": "file:///tmp/index.html"}
                    ],
                }
            ]

            f.write(json.dumps(data))
            f.seek(0)

            is_successful = transfer.pegasus_transfer(
                max_attempts=1, num_threads=8, file=f.name, symlink=True
            )

            assert is_successful == False
            assert "Some transfers failed!" in str(caplog.record_tuples)


class TestDirectoryStaging:
    """PM-112: tar/untar support for whole-directory staging."""

    def test_tar_directory_round_trip(self):
        with tempfile.TemporaryDirectory() as tmp:
            src_dir = os.path.join(tmp, "mydir")
            os.mkdir(src_dir)
            with open(os.path.join(src_dir, "a.txt"), "w") as f:
                f.write("hello")
            os.mkdir(os.path.join(src_dir, "sub"))
            with open(os.path.join(src_dir, "sub", "b.txt"), "w") as f:
                f.write("world")

            tarball_path = transfer.tar_directory(src_dir)

            assert tarball_path == os.path.join(tmp, "mydir.tar.gz")
            assert os.path.isfile(tarball_path)

            # untar into a fresh location and verify the tree round-trips
            extract_root = os.path.join(tmp, "extracted")
            os.mkdir(extract_root)
            transfer.untar_directory(tarball_path, extract_root)

            extracted_dir = os.path.join(extract_root, "mydir")
            assert os.path.isdir(extracted_dir)
            with open(os.path.join(extracted_dir, "a.txt")) as f:
                assert f.read() == "hello"
            with open(os.path.join(extracted_dir, "sub", "b.txt")) as f:
                assert f.read() == "world"

    def test_tar_directory_rejects_non_local_directory(self):
        with tempfile.TemporaryDirectory() as tmp:
            not_a_dir = os.path.join(tmp, "not-here")

            with pytest.raises(RuntimeError) as e:
                transfer.tar_directory(not_a_dir)

            assert "not a local directory" in str(e.value)

    def test_untar_directory_rejects_path_traversal(self):
        import tarfile

        with tempfile.TemporaryDirectory() as tmp:
            evil_tarball = os.path.join(tmp, "evil.tar.gz")
            with tarfile.open(evil_tarball, "w:gz") as tf:
                info = tarfile.TarInfo(name="../escaped.txt")
                data = b"pwned"
                info.size = len(data)
                tf.addfile(info, io.BytesIO(data))

            extract_dir = os.path.join(tmp, "safe")
            os.mkdir(extract_dir)

            with pytest.raises(RuntimeError) as e:
                transfer.untar_directory(evil_tarball, extract_dir)

            assert "escapes the extraction directory" in str(e.value)

    def test_untar_directory_rejects_symlink_escape(self):
        import tarfile

        with tempfile.TemporaryDirectory() as tmp:
            # a symlink member whose *target* escapes extract_dir - the
            # member's own name ("link") stays inside extract_dir, so only
            # checking member.name (and not the link target) would miss this
            evil_tarball = os.path.join(tmp, "evil.tar.gz")
            with tarfile.open(evil_tarball, "w:gz") as tf:
                info = tarfile.TarInfo(name="link")
                info.type = tarfile.SYMTYPE
                info.linkname = "../escaped"
                tf.addfile(info)

            extract_dir = os.path.join(tmp, "safe")
            os.mkdir(extract_dir)

            with pytest.raises(RuntimeError) as e:
                transfer.untar_directory(evil_tarball, extract_dir)

            assert "link target escapes the extraction directory" in str(e.value)

    def test_json_object_decoder_parses_directory_fields(self):
        obj = {
            "type": "transfer",
            "lfn": "mydir",
            "directory": True,
            "directory_action": "tar",
            "src_urls": [{"site_label": "local", "url": "file:///tmp/mydir"}],
            "dest_urls": [{"site_label": "storage", "url": "file:///out/mydir"}],
        }

        t = transfer.json_object_decoder(obj)

        assert t.directory is True
        assert t.directory_action == "tar"
        # a directory transfer's src/dst get rewritten by the tar/untar step,
        # so it can never be grouped with other transfers
        assert t.allow_grouping is False

    def test_json_object_decoder_defaults_for_non_directory_transfer(self):
        obj = {
            "type": "transfer",
            "lfn": "f.txt",
            "src_urls": [{"site_label": "local", "url": "file:///tmp/f.txt"}],
            "dest_urls": [{"site_label": "storage", "url": "file:///out/f.txt"}],
        }

        t = transfer.json_object_decoder(obj)

        assert t.directory is False
        assert t.directory_action is None
        assert t.allow_grouping is True
