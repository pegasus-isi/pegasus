"""
User config (`~/.pegasus/service.json`, optional) and ensemble manager runtime
state (`~/.pegasus/em.server.json`, mode 0600: port, token, pid).
"""

import json
import logging
import os
import secrets
from pathlib import Path

log = logging.getLogger(__name__)

DEFAULT_PEGASUS_DIR = "~/.pegasus"
USER_CONFIG_FILE = "service.json"
EM_RUNTIME_FILE = "em.server.json"

PORT_MIN = 1024
PORT_MAX = 65535


class ConfigError(Exception):
    """Invalid user configuration file."""


def _pegasus_dir(pegasus_dir=None) -> Path:
    return Path(os.path.expanduser(pegasus_dir or DEFAULT_PEGASUS_DIR))


def load_user_config(app, pegasus_dir=None):
    """Load ``service.json`` into ``app.config``; return its path, or None if absent."""
    path = _pegasus_dir(pegasus_dir) / USER_CONFIG_FILE
    if not path.is_file():
        return None

    try:
        with path.open() as f:
            data = json.load(f)
    except json.JSONDecodeError as e:
        raise ConfigError(
            f"Invalid JSON in {path} (line {e.lineno}, column {e.colno}): {e.msg}"
        ) from e

    if not isinstance(data, dict):
        raise ConfigError(
            f"{path} must contain a JSON object, found {type(data).__name__}"
        )

    ignored = sorted(k for k in data if not k.isupper())
    if ignored:
        log.warning("Ignoring non-uppercase keys in %s: %s", path, ", ".join(ignored))

    app.config.from_mapping(data)
    return path


def validate_port(value, source="port"):
    """Return ``value`` as an int port in [1024, 65535], else raise ValueError."""
    try:
        port = int(value)
    except (TypeError, ValueError):
        port = None

    # int() accepts bools and truncates floats
    if (
        port is None
        or isinstance(value, bool)
        or (isinstance(value, float) and value != port)
    ):
        raise ValueError(f"Invalid {source}: {value!r} is not an integer")

    if not PORT_MIN <= port <= PORT_MAX:
        raise ValueError(
            f"Invalid {source}: {port} must be between {PORT_MIN} and {PORT_MAX}"
        )
    return port


def generate_token() -> str:
    return secrets.token_urlsafe(32)


def em_runtime_path(pegasus_dir=None) -> Path:
    return _pegasus_dir(pegasus_dir) / EM_RUNTIME_FILE


def write_em_runtime(port, token, pegasus_dir=None, pid=None) -> Path:
    """Write the runtime file with mode 0600."""
    path = em_runtime_path(pegasus_dir)
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)

    data = {"port": port, "token": token, "pid": pid or os.getpid()}

    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    try:
        os.fchmod(fd, 0o600)  # O_CREAT's mode doesn't apply to existing files
        with os.fdopen(fd, "w") as f:
            fd = None
            json.dump(data, f)
            f.write("\n")
    finally:
        if fd is not None:
            os.close(fd)
    return path


def read_em_runtime(pegasus_dir=None):
    """Return the runtime file contents, or None if missing or unreadable."""
    path = em_runtime_path(pegasus_dir)
    try:
        with path.open() as f:
            data = json.load(f)
    except FileNotFoundError:
        return None
    except (OSError, ValueError) as e:
        log.warning("Unable to read %s: %s", path, e)
        return None

    if not isinstance(data, dict):
        log.warning("Ignoring malformed %s", path)
        return None
    return data


def remove_em_runtime(pegasus_dir=None, pid=None) -> bool:
    """Remove the runtime file if it belongs to ``pid`` (default: this process)."""
    pid = pid or os.getpid()
    data = read_em_runtime(pegasus_dir)
    if not data or data.get("pid") != pid:
        return False
    try:
        em_runtime_path(pegasus_dir).unlink()
    except OSError as e:
        log.warning("Unable to remove %s: %s", em_runtime_path(pegasus_dir), e)
        return False
    return True


def is_process_alive(pid) -> bool:
    if not isinstance(pid, int) or isinstance(pid, bool) or pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True  # exists, owned by another user
    return True
