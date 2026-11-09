import json
import logging
import os
import stat

import flask
import pytest

from Pegasus.service import _config
from Pegasus.service._config import (
    ConfigError,
    em_runtime_path,
    is_process_alive,
    load_user_config,
    read_em_runtime,
    remove_em_runtime,
    validate_port,
    write_em_runtime,
)


@pytest.fixture
def app():
    return flask.Flask(__name__)


@pytest.fixture
def pegasus_dir(tmp_path):
    d = tmp_path / "dot-pegasus"
    d.mkdir()
    return d


def _mode(path):
    return stat.S_IMODE(os.stat(path).st_mode)


class TestLoadUserConfig:
    def test_no_files(self, app, pegasus_dir):
        before = dict(app.config)
        assert load_user_config(app, pegasus_dir) is None
        assert dict(app.config) == before

    def test_missing_directory(self, app, tmp_path):
        assert load_user_config(app, tmp_path / "does-not-exist") is None

    def test_json(self, app, pegasus_dir, caplog):
        (pegasus_dir / "service.json").write_text(
            json.dumps(
                {
                    "AUTHENTICATION": "NoAuthentication",
                    "EM_PORT": 18123,
                    "ADMIN_USERS": ["a", "b"],
                    "lowercase": "ignored",
                }
            )
        )
        with caplog.at_level(logging.WARNING):
            loaded = load_user_config(app, pegasus_dir)

        assert loaded == pegasus_dir / "service.json"
        assert app.config["AUTHENTICATION"] == "NoAuthentication"
        assert app.config["EM_PORT"] == 18123
        assert app.config["ADMIN_USERS"] == ["a", "b"]
        assert "lowercase" not in app.config
        assert "lowercase" in caplog.text

    def test_service_py_ignored(self, app, pegasus_dir):
        (pegasus_dir / "service.py").write_text('USERNAME = "u"\n')
        assert load_user_config(app, pegasus_dir) is None
        assert app.config.get("USERNAME") != "u"

    def test_malformed_json(self, app, pegasus_dir):
        conf = pegasus_dir / "service.json"
        conf.write_text('{"AUTHENTICATION": "NoAuthentication",}')

        with pytest.raises(ConfigError, match=str(conf)) as e:
            load_user_config(app, pegasus_dir)
        assert "line 1" in str(e.value)

    @pytest.mark.parametrize("content", ["[]", '"x"', "1", "null"])
    def test_non_object(self, app, pegasus_dir, content):
        (pegasus_dir / "service.json").write_text(content)
        with pytest.raises(ConfigError, match="must contain a JSON object"):
            load_user_config(app, pegasus_dir)


class TestValidatePort:
    @pytest.mark.parametrize("value", [1024, 8000, 65535, "8000", 8000.0])
    def test_valid(self, value):
        assert validate_port(value) == int(value)

    @pytest.mark.parametrize(
        "value", [0, 80, 1023, 65536, 70000, -1, "abc", None, True, 8000.5, [8000]]
    )
    def test_invalid(self, value):
        with pytest.raises(ValueError, match="--port"):
            validate_port(value, source="--port")


class TestEMRuntime:
    def test_round_trip(self, pegasus_dir):
        path = write_em_runtime(18123, "tok", pegasus_dir)

        assert path == em_runtime_path(pegasus_dir)
        assert _mode(path) == 0o600
        assert read_em_runtime(pegasus_dir) == {
            "port": 18123,
            "token": "tok",
            "pid": os.getpid(),
        }

    def test_null_token(self, pegasus_dir):
        write_em_runtime(18123, None, pegasus_dir)
        assert read_em_runtime(pegasus_dir)["token"] is None

    def test_creates_directory(self, tmp_path):
        d = tmp_path / "new" / "dot-pegasus"
        write_em_runtime(18123, "tok", d)
        assert read_em_runtime(d)["port"] == 18123

    def test_tightens_existing_permissions(self, pegasus_dir):
        path = em_runtime_path(pegasus_dir)
        path.write_text("{}")
        os.chmod(path, 0o644)

        write_em_runtime(18123, "tok", pegasus_dir)
        assert _mode(path) == 0o600

    def test_read_missing(self, pegasus_dir):
        assert read_em_runtime(pegasus_dir) is None

    @pytest.mark.parametrize("content", ["not json", "[1, 2]"])
    def test_read_malformed(self, pegasus_dir, content):
        em_runtime_path(pegasus_dir).write_text(content)
        assert read_em_runtime(pegasus_dir) is None

    def test_remove_own(self, pegasus_dir):
        write_em_runtime(18123, "tok", pegasus_dir)
        assert remove_em_runtime(pegasus_dir) is True
        assert not em_runtime_path(pegasus_dir).exists()

    def test_remove_other_pid_is_noop(self, pegasus_dir):
        write_em_runtime(18123, "tok", pegasus_dir, pid=os.getpid() + 1)
        assert remove_em_runtime(pegasus_dir) is False
        assert em_runtime_path(pegasus_dir).exists()

    def test_remove_missing(self, pegasus_dir):
        assert remove_em_runtime(pegasus_dir) is False

    def test_default_dir(self, monkeypatch, pegasus_dir):
        monkeypatch.setattr(_config, "DEFAULT_PEGASUS_DIR", str(pegasus_dir))
        assert em_runtime_path() == pegasus_dir / "em.server.json"


class TestIsProcessAlive:
    def test_self(self):
        assert is_process_alive(os.getpid()) is True

    @pytest.mark.parametrize("pid", [None, 0, -1, "123", True])
    def test_invalid(self, pid):
        assert is_process_alive(pid) is False

    def test_dead(self, mocker):
        mocker.patch("os.kill", side_effect=ProcessLookupError)
        assert is_process_alive(12345) is False

    def test_other_user(self, mocker):
        mocker.patch("os.kill", side_effect=PermissionError)
        assert is_process_alive(1) is True
