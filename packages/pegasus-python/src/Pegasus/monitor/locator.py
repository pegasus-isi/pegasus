"""Resolve a Pegasus run without mutating any workflow artifact."""

##
#  Copyright 2026 University Of Southern California
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
##

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from pathlib import Path
from urllib.parse import unquote, urlparse

from .models import WorkflowIdentity

from Pegasus import braindump

_BRAINDUMP_NAME = "braindump.yml"
_RUN_DIRECTORY = re.compile(r"^run(?P<number>\d+)$")
_WORKFLOW_URL = "pegasus.catalog.workflow.url"
_MONITORD_OUTPUT = "pegasus.monitord.output"


class WorkflowLocationError(ValueError):
    """The requested path does not identify exactly one Pegasus workflow."""


class RemapMode(str, Enum):
    AUTO = "auto"
    ALWAYS = "always"
    NEVER = "never"


class DatabaseBackend(str, Enum):
    SQLITE = "sqlite"
    POSTGRESQL = "postgresql"
    MYSQL = "mysql"
    UNSUPPORTED = "unsupported"


@dataclass(frozen=True, slots=True)
class WorkflowLocation:
    """Resolved selected-workflow and top-level storage paths.

    Recorded paths are kept verbatim for scheduler matching.  Local paths are
    the monitor-side remapping and are the only paths used for file access.
    """

    workflow: WorkflowIdentity
    braindump_path: Path
    root_braindump_path: Path
    recorded_submit_dir: Path
    recorded_basedir: Path
    submit_dir: Path
    basedir: Path
    root_submit_dir: Path
    dag_name: str
    properties_path: Path | None
    database_uri: str
    database_backend: DatabaseBackend
    database_path: Path | None
    jobstate_path: Path
    jobstate_path_overridden: bool

    @property
    def is_subworkflow(self) -> bool:
        return self.workflow.wf_uuid != self.workflow.root_wf_uuid


@dataclass(frozen=True, slots=True)
class _LoadedBraindump:
    path: Path
    data: braindump.Braindump


def _load(path: Path) -> _LoadedBraindump:
    try:
        with path.open("r", encoding="utf-8") as stream:
            data = braindump.load(stream)
    except (OSError, TypeError, ValueError) as error:
        raise WorkflowLocationError(f"cannot read {path}: {error}") from error

    missing = [
        name
        for name in ("wf_uuid", "root_wf_uuid", "submit_dir")
        if not getattr(data, name, None)
    ]
    if missing:
        raise WorkflowLocationError(
            f"{path} is missing required field(s): {', '.join(missing)}"
        )
    return _LoadedBraindump(path.resolve(), data)


def _valid_top_level(path: Path) -> _LoadedBraindump | None:
    try:
        loaded = _load(path)
    except WorkflowLocationError:
        return None
    if loaded.data.wf_uuid != loaded.data.root_wf_uuid:
        return None
    return loaded


def _planning_timestamp(value: str | None) -> float | None:
    if not value:
        return None
    try:
        parsed = datetime.strptime(value, "%Y%m%dT%H%M%S%z")
    except ValueError:
        return None
    return parsed.timestamp()


def _select_latest(base: Path) -> _LoadedBraindump:
    candidates = [
        loaded
        for path in sorted(base.rglob(_BRAINDUMP_NAME))
        if (loaded := _valid_top_level(path)) is not None
    ]
    if not candidates:
        raise WorkflowLocationError(
            f"no valid top-level {_BRAINDUMP_NAME} found under {base}"
        )

    numeric: list[tuple[int, _LoadedBraindump]] = []
    nonstandard: list[_LoadedBraindump] = []
    for loaded in candidates:
        rundir = loaded.data.rundir or loaded.path.parent.name
        match = _RUN_DIRECTORY.fullmatch(str(rundir))
        if match:
            numeric.append((int(match.group("number")), loaded))
        else:
            nonstandard.append(loaded)

    if numeric:
        highest = max(number for number, _ in numeric)
        finalists = [loaded for number, loaded in numeric if number == highest]
        if len(finalists) > 1:
            timestamps = [
                _planning_timestamp(item.data.timestamp) for item in finalists
            ]
            if any(timestamp is None for timestamp in timestamps):
                paths = ", ".join(str(item.path) for item in finalists)
                raise WorkflowLocationError(
                    "ambiguous latest workflow run with missing or malformed "
                    f"planning timestamp: {paths}"
                )
            newest = max(timestamp for timestamp in timestamps if timestamp is not None)
            finalists = [
                item
                for item in finalists
                if _planning_timestamp(item.data.timestamp) == newest
            ]
        if len(finalists) != 1:
            paths = ", ".join(str(item.path) for item in finalists)
            raise WorkflowLocationError(f"ambiguous latest workflow run: {paths}")
        return finalists[0]

    if len(nonstandard) == 1:
        return nonstandard[0]
    paths = ", ".join(str(item.path) for item in nonstandard)
    raise WorkflowLocationError(f"ambiguous nonstandard workflow runs: {paths}")


def _selected_braindump(target: Path) -> _LoadedBraindump:
    target = target.expanduser().resolve()
    if target.is_file():
        if target.name != _BRAINDUMP_NAME:
            raise WorkflowLocationError(
                f"expected {_BRAINDUMP_NAME}, got file {target}"
            )
        return _load(target)
    if not target.is_dir():
        raise WorkflowLocationError(f"workflow path does not exist: {target}")
    direct = target / _BRAINDUMP_NAME
    if direct.is_file():
        return _load(direct)
    return _select_latest(target)


def _root_braindump(selected: _LoadedBraindump) -> _LoadedBraindump:
    if selected.data.wf_uuid == selected.data.root_wf_uuid:
        return selected
    expected = selected.data.root_wf_uuid
    for directory in (selected.path.parent, *selected.path.parent.parents):
        candidate = directory / _BRAINDUMP_NAME
        if not candidate.is_file() or candidate == selected.path:
            continue
        try:
            loaded = _load(candidate)
        except WorkflowLocationError:
            continue
        if loaded.data.wf_uuid == expected and loaded.data.root_wf_uuid == expected:
            return loaded
    raise WorkflowLocationError(
        f"cannot locate top-level workflow {expected!r} above {selected.path.parent}"
    )


def _remapped_paths(
    loaded: _LoadedBraindump, mode: RemapMode
) -> tuple[Path, Path, Path, Path]:
    recorded_submit = Path(loaded.data.submit_dir).expanduser()
    recorded_basedir = Path(loaded.data.basedir or recorded_submit.parent).expanduser()
    actual_submit = loaded.path.parent.resolve()
    remap = mode is RemapMode.ALWAYS or (
        mode is RemapMode.AUTO and not recorded_submit.exists()
    )
    if not remap:
        return (
            recorded_submit,
            recorded_basedir,
            recorded_submit,
            recorded_basedir,
        )

    try:
        offset = recorded_submit.relative_to(recorded_basedir)
    except ValueError:
        local_basedir = actual_submit.parent
    else:
        depth = len(offset.parts)
        local_basedir = (
            actual_submit.parents[depth - 1]
            if depth and depth <= len(actual_submit.parents)
            else actual_submit.parent
        )
    return recorded_submit, recorded_basedir, actual_submit, local_basedir


def _property_path(
    loaded: _LoadedBraindump,
    recorded_submit: Path,
    local_submit: Path,
    mode: RemapMode,
) -> Path | None:
    value = loaded.data.properties
    if not value:
        return None
    path = Path(value).expanduser()
    recorded = path if path.is_absolute() else recorded_submit / path
    if mode is RemapMode.NEVER or (mode is RemapMode.AUTO and recorded.exists()):
        return recorded
    if path.is_absolute():
        try:
            relative = path.relative_to(recorded_submit)
        except ValueError:
            relative = Path(path.name)
    else:
        relative = path
    return (local_submit / relative).resolve()


def _read_properties(path: Path | None) -> dict[str, str]:
    """Use Pegasus's Java-properties parser without any database helper."""

    if path is None or not path.is_file():
        return {}
    try:
        from Pegasus.tools import properties as pegasus_properties

        # The shared parser looks up the complete ``${name}`` token in its
        # explicit mapping.  Supplying Pegasus's system values in that form
        # preserves its escaping, comments, and continuation behavior while
        # enabling the intended substitutions.
        substitutions = {
            f"${{{key}}}": str(value)
            for key, value in pegasus_properties.system.items()
        }
        return pegasus_properties.parse_properties(str(path), substitutions)
    except (OSError, SystemExit) as error:
        raise WorkflowLocationError(
            f"cannot read properties file {path}: {error}"
        ) from error


def _map_database_path(
    path: Path,
    *,
    recorded_submit: Path,
    recorded_basedir: Path,
    local_submit: Path,
    local_basedir: Path,
    mode: RemapMode,
) -> Path:
    path = path.expanduser()
    if not path.is_absolute():
        return (local_submit / path).resolve()
    if mode is RemapMode.NEVER or (mode is RemapMode.AUTO and path.exists()):
        return path
    for recorded, local in (
        (recorded_submit, local_submit),
        (recorded_basedir, local_basedir),
    ):
        try:
            return (local / path.relative_to(recorded)).resolve()
        except ValueError:
            continue
    return path


def _database_location(
    uri: str,
    *,
    recorded_submit: Path,
    recorded_basedir: Path,
    local_submit: Path,
    local_basedir: Path,
    mode: RemapMode,
) -> tuple[str, DatabaseBackend, Path | None]:
    original = uri.strip()
    normalized = original[5:] if original.lower().startswith("jdbc:") else original
    parsed = urlparse(normalized)
    scheme = parsed.scheme.lower()
    if scheme.startswith("postgres"):
        return original, DatabaseBackend.POSTGRESQL, None
    if scheme.startswith("mysql") or scheme.startswith("mariadb"):
        return original, DatabaseBackend.MYSQL, None
    if scheme and scheme != "sqlite":
        return original, DatabaseBackend.UNSUPPORTED, None

    if scheme == "sqlite":
        if parsed.netloc not in ("", "localhost") or parsed.path in ("", "/:memory:"):
            return original, DatabaseBackend.UNSUPPORTED, None
        if normalized.startswith("sqlite:///"):
            # Three slashes introduce a path; a fourth is the leading slash of
            # an absolute path.  This mirrors SQLAlchemy/Pegasus URL handling.
            raw_path = unquote(normalized[len("sqlite:///") :].split("?", 1)[0])
        else:
            raw_path = unquote(parsed.path)
        path = Path(raw_path)
    else:
        path = Path(normalized)
    mapped = _map_database_path(
        path,
        recorded_submit=recorded_submit,
        recorded_basedir=recorded_basedir,
        local_submit=local_submit,
        local_basedir=local_basedir,
        mode=mode,
    )
    return original, DatabaseBackend.SQLITE, mapped


class WorkflowLocator:
    """Locate one workflow from a run, braindump, or workflow base path."""

    def locate(
        self,
        target: str | Path,
        *,
        remap_submit_dir: str | RemapMode = RemapMode.AUTO,
        jobstate_path: str | Path | None = None,
    ) -> WorkflowLocation:
        try:
            mode = RemapMode(remap_submit_dir)
        except ValueError as error:
            raise WorkflowLocationError(
                "remap_submit_dir must be auto, always, or never"
            ) from error

        selected = _selected_braindump(Path(target))
        root = _root_braindump(selected)
        identity = WorkflowIdentity(
            str(selected.data.wf_uuid), str(selected.data.root_wf_uuid)
        )
        if root.data.wf_uuid != identity.root_wf_uuid:
            raise WorkflowLocationError(
                "selected and top-level workflow identities differ"
            )

        rec_submit, rec_base, local_submit, local_base = _remapped_paths(selected, mode)
        root_rec_submit, root_rec_base, root_submit, root_base = _remapped_paths(
            root, mode
        )
        dag_name = str(root.data.dag or "")
        if not dag_name:
            raise WorkflowLocationError(
                f"top-level braindump {root.path} has no DAG filename"
            )
        prop_path = _property_path(root, root_rec_submit, root_submit, mode)
        props = _read_properties(prop_path)
        configured_uri = props.get(_WORKFLOW_URL) or props.get(_MONITORD_OUTPUT)
        if configured_uri:
            db_uri, backend, db_path = _database_location(
                configured_uri,
                recorded_submit=root_rec_submit,
                recorded_basedir=root_rec_base,
                local_submit=root_submit,
                local_basedir=root_base,
                mode=mode,
            )
        else:
            stem = Path(dag_name).name.removesuffix(".dag")
            db_path = root_submit / f"{stem}.stampede.db"
            db_uri = f"sqlite:///{db_path}"
            backend = DatabaseBackend.SQLITE

        if jobstate_path is None:
            live_path = local_submit / str(selected.data.jsd or "jobstate.log")
            overridden = False
        else:
            supplied = Path(jobstate_path).expanduser()
            live_path = (
                supplied.resolve()
                if supplied.is_absolute()
                else (Path.cwd() / supplied).resolve()
            )
            overridden = True

        return WorkflowLocation(
            workflow=identity,
            braindump_path=selected.path,
            root_braindump_path=root.path,
            recorded_submit_dir=rec_submit,
            recorded_basedir=rec_base,
            submit_dir=local_submit,
            basedir=local_base,
            root_submit_dir=root_submit,
            dag_name=dag_name,
            properties_path=prop_path,
            database_uri=db_uri,
            database_backend=backend,
            database_path=db_path,
            jobstate_path=live_path,
            jobstate_path_overridden=overridden,
        )
