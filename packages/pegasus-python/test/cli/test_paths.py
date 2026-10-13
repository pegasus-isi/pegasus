"""
Pegasus must resolve its modules, console scripts and binaries from its own
install, even when another Pegasus (e.g. the .deb's /usr/bin/pegasus) is on PATH.
"""

import importlib
import importlib.metadata as md
import os
import subprocess
import sysconfig
from pathlib import Path

import pytest

import Pegasus
from Pegasus.cli import _paths, main
from Pegasus.cli._paths import get_bin_dir

DIST = md.distribution("pegasus-wms")
SITE = Path(DIST.locate_file("")).resolve()
EDITABLE = '"editable": true' in (DIST.read_text("direct_url.json") or "")

requires_wheel = pytest.mark.skipif(
    EDITABLE, reason="editable installs import Pegasus from the source tree"
)


def _write_exe(path):
    path.write_text("#!/bin/sh\n")
    path.chmod(0o755)
    return path


def _fake_install(root, scripts=("pegasus",)):
    """Lay out a pegasus-wms install under root: site/<dist-info> and bin/."""
    dist_info = root / "site" / "pegasus_wms-1.0.dist-info"
    dist_info.mkdir(parents=True)
    (dist_info / "METADATA").write_text(
        "Metadata-Version: 2.1\nName: pegasus-wms\nVersion: 1.0\n"
    )
    bin_dir = root / "bin"
    bin_dir.mkdir()
    for name in scripts:
        _write_exe(bin_dir / name)
    (dist_info / "RECORD").write_text("".join(f"../bin/{n},,\n" for n in scripts))
    return md.PathDistribution(dist_info), bin_dir


@pytest.fixture
def decoy(tmp_path, monkeypatch):
    """Put another install's pegasus tools first on PATH."""
    decoy = tmp_path / "decoy"
    decoy.mkdir()
    for name in ("pegasus", "pegasus-transfer"):
        _write_exe(decoy / name)
    monkeypatch.setenv("PATH", f"{decoy}{os.pathsep}{os.environ.get('PATH', '')}")
    return decoy


# ── get_bin_dir ──────────────────────────────────────────────────────────────


def test_get_bin_dir_uses_record_not_path(tmp_path, monkeypatch, decoy):
    dist, bin_dir = _fake_install(tmp_path / "install")
    monkeypatch.setattr(_paths, "distribution", lambda name: dist)

    assert get_bin_dir() == str(bin_dir.resolve())


def test_get_bin_dir_falls_back_without_dist(monkeypatch, decoy):
    def missing(name):
        raise md.PackageNotFoundError(name)

    monkeypatch.setattr(_paths, "distribution", missing)

    assert get_bin_dir() == sysconfig.get_path("scripts")


@pytest.mark.parametrize("recorded", [True, False], ids=["deleted", "unrecorded"])
def test_get_bin_dir_falls_back_without_script(tmp_path, monkeypatch, recorded):
    dist, bin_dir = _fake_install(
        tmp_path / "install", scripts=("pegasus",) if recorded else ()
    )
    (bin_dir / "pegasus").unlink(missing_ok=True)
    monkeypatch.setattr(_paths, "distribution", lambda name: dist)

    assert get_bin_dir() == sysconfig.get_path("scripts")


# ── _exec_binary ─────────────────────────────────────────────────────────────


@pytest.fixture
def execv(monkeypatch):
    calls = []
    monkeypatch.setattr(main.os, "execv", lambda path, argv: calls.append(argv))
    return calls


def test_exec_binary_prefers_own_bin_dir(tmp_path, monkeypatch, decoy, execv):
    _, bin_dir = _fake_install(tmp_path / "install", ("pegasus", "pegasus-transfer"))
    monkeypatch.setattr(main, "get_bin_dir", lambda: str(bin_dir))

    main._exec_binary("pegasus-transfer", ["-h"])

    assert execv == [[str(bin_dir / "pegasus-transfer"), "-h"]]


def test_exec_binary_falls_back_to_path(tmp_path, monkeypatch, decoy, execv):
    _, bin_dir = _fake_install(tmp_path / "install")
    monkeypatch.setattr(main, "get_bin_dir", lambda: str(bin_dir))

    main._exec_binary("pegasus-transfer", [])

    assert execv == [[str(decoy / "pegasus-transfer")]]


def test_exec_binary_not_found(tmp_path, monkeypatch, execv):
    _, bin_dir = _fake_install(tmp_path / "install")
    monkeypatch.setattr(main, "get_bin_dir", lambda: str(bin_dir))
    monkeypatch.setenv("PATH", str(tmp_path / "empty"))

    with pytest.raises(SystemExit) as e:
        main._exec_binary("pegasus-transfer", [])

    assert e.value.code == 1
    assert execv == []


# ── the installed pegasus-wms ────────────────────────────────────────────────


def _location(module):
    return getattr(module, "__file__", None) or next(iter(module.__path__))


@requires_wheel
def test_pegasus_namespace_from_one_install():
    for path in Pegasus.__path__:
        assert Path(path).resolve().is_relative_to(SITE)


@requires_wheel
@pytest.mark.parametrize(
    "name",
    [
        "Pegasus.api",
        "Pegasus.client",
        "Pegasus.db",
        "Pegasus.data",
        "Pegasus.cli.main",
        "Pegasus.cli._paths",
    ],
)
def test_module_from_pegasus_wms(name):
    module = importlib.import_module(name)

    assert Path(_location(module)).resolve().is_relative_to(SITE)


def test_installed_bin_dir_ignores_path(decoy):
    bin_dir = Path(get_bin_dir())

    assert bin_dir != decoy
    assert (bin_dir / "pegasus").is_file()


def test_pegasus_config_bin_without_venv_on_path(decoy):
    """Mirrors running <venv>/bin/pegasus-config without activating the venv."""
    pegasus_config = Path(get_bin_dir()) / "pegasus-config"
    if not pegasus_config.is_file():
        pytest.skip("pegasus-config console script is not installed")

    result = subprocess.run(
        [str(pegasus_config), "--bin"],
        env={**os.environ, "PATH": os.pathsep.join([str(decoy), "/usr/bin", "/bin"])},
        capture_output=True,
        text=True,
        check=True,
    )

    assert result.stdout.strip() == get_bin_dir()
