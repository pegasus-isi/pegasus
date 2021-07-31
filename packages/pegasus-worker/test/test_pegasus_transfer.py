import configparser
import importlib
import io
import json
import logging
import os
import stat
import sys
import tempfile
from pathlib import Path

import pytest

pegasus_transfer = importlib.import_module("Pegasus.cli.pegasus-transfer")
pegasus_transfer_func = pegasus_transfer.pegasus_transfer
load_credentials = pegasus_transfer.load_credentials
read_json_format = pegasus_transfer.read_json_format
Transfer = pegasus_transfer.Transfer
PegasusURL = pegasus_transfer.PegasusURL


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
        inputs_loaded = list()
        read_json_format(input=data_str, inputs_l=inputs_loaded)

        expected_transfer_obj = Transfer()
        expected_transfer_obj._src_urls = [
            PegasusURL(url="http://pegasus.isi.edu", file_type="x", site_label="web")
        ]
        expected_transfer_obj._dst_urls = [
            PegasusURL(url="file:///tmp/index.html", file_type="x", site_label="local")
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
        inputs_loaded = list()
        with pytest.raises(RuntimeError) as e:
            read_json_format(input=data_str, inputs_l=inputs_loaded)

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
        inputs_loaded = list()
        with pytest.raises(RuntimeError) as e:
            read_json_format(input=data_str, inputs_l=inputs_loaded)

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

            credentials = load_credentials()
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
            load_credentials()

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
                load_credentials()

            assert "Permissions of credentials file" in str(e)

    def test_unable_to_read_credentials(self, caplog, cleanup_pegasus_credentials_env):
        caplog.set_level(logging.CRITICAL, logger="Pegasus")

        with tempfile.NamedTemporaryFile(mode="w+", delete=True) as f:
            f.write("x;kwj0923fjalksdjf438hfjojTHIS_IS_A_BAD_CONFIG_ldkfjals")
            f.seek(0)

            os.environ["PEGASUS_CREDENTIALS"] = f.name

            with pytest.raises(configparser.MissingSectionHeaderError) as e:
                load_credentials()

            assert "Unable to load credentials" in str(caplog.record_tuples)


class TestPegasusTransferInvocation:
    def test_error_opening_input_file(self):
        with pytest.raises(FileNotFoundError) as e:
            pegasus_transfer_func(
                max_attempts=3, num_threads=8, file="badfile", symlink=True
            )

    def test_transfers_read_from_stdin_succeeded(self, caplog):
        caplog.set_level(logging.INFO, logger="Pegasus")
        with tempfile.TemporaryDirectory() as td:
            temp_dir = Path(td)

            downloaded_files = list()
            xfers = list()
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
                                "url": "file://"
                                + str(temp_dir / "index{}.html".format(i)),
                            }
                        ],
                    }
                )
                downloaded_files.append("index{}.html".format(i))

            sys.stdin = io.StringIO(json.dumps(xfers))

            is_successful = pegasus_transfer_func(
                max_attempts=1, num_threads=8, file=None, symlink=True
            )

            assert is_successful == True

            logs = str(caplog.record_tuples)
            assert "{} transfers loaded".format(NUM_XFERS) in logs
            assert "All transfers completed" in logs

            for f in downloaded_files:
                assert (temp_dir / f).exists()

    def test_transfers_read_from_file_succeeded(self, caplog):
        caplog.set_level(logging.DEBUG, logger="Pegasus")
        with tempfile.TemporaryDirectory() as td:
            temp_dir = Path(td)

            downloaded_files = list()
            xfers = list()
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
                                "url": "file://"
                                + str(temp_dir / "index{}.html".format(i)),
                            }
                        ],
                    }
                )
                downloaded_files.append("index{}.html".format(i))

            with tempfile.NamedTemporaryFile("w+", delete=True) as f:
                f.write(json.dumps(xfers))
                f.seek(0)

                is_successful = pegasus_transfer_func(
                    max_attempts=1, num_threads=8, file=f.name, symlink=True
                )

            assert is_successful == True

            logs = str(caplog.record_tuples)
            assert "{} transfers loaded".format(NUM_XFERS) in logs
            assert "Using {} threads for this set of transfers".format(8) in logs
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
                            "url": "http://BADURL0983012830181028310831adna02a0o.net",
                        }
                    ],
                    "dest_urls": [
                        {"site_label": "local", "url": "file:///tmp/index.html"}
                    ],
                }
            ]

            f.write(json.dumps(data))
            f.seek(0)

            is_successful = pegasus_transfer_func(
                max_attempts=1, num_threads=8, file=f.name, symlink=True
            )

            assert is_successful == False
            assert "Some transfers failed!" in str(caplog.record_tuples)
