"""Deterministic workflow locator coverage."""

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

import json
from pathlib import Path

import pytest

from Pegasus.monitor.locator import (
    DatabaseBackend,
    WorkflowLocationError,
    WorkflowLocator,
)


def _braindump(
    directory: Path,
    *,
    wf_uuid: str = "root-uuid",
    root_wf_uuid: str = "root-uuid",
    recorded_submit: str = "/container/work/run0001",
    recorded_basedir: str = "/container/work",
    rundir: str = "run0001",
    timestamp: str = "20260818T010000-0400",
    dag: str = "diamond-0.dag",
    properties: str | None = "pegasus.properties",
) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    values = {
        "wf_uuid": wf_uuid,
        "root_wf_uuid": root_wf_uuid,
        "submit_dir": recorded_submit,
        "basedir": recorded_basedir,
        "rundir": rundir,
        "timestamp": timestamp,
        "dag": dag,
        "jsd": "jobstate.log",
        "properties": properties,
    }
    path = directory / "braindump.yml"
    path.write_text(json.dumps(values), encoding="utf-8")
    return path


@pytest.mark.parametrize("as_file", [False, True])
def test_locates_direct_run_and_preserves_recorded_paths(tmp_path, as_file):
    run = tmp_path / "host" / "run0001"
    braindump = _braindump(run)

    location = WorkflowLocator().locate(braindump if as_file else run)

    assert location.workflow.wf_uuid == "root-uuid"
    assert location.recorded_submit_dir == Path("/container/work/run0001")
    assert location.recorded_basedir == Path("/container/work")
    assert location.submit_dir == run.resolve()
    assert location.root_submit_dir == run.resolve()
    assert location.database_path == run.resolve() / "diamond-0.stampede.db"
    assert location.database_backend is DatabaseBackend.SQLITE
    assert location.jobstate_path == run.resolve() / "jobstate.log"
    assert not location.jobstate_path_overridden


def test_never_remap_trusts_recorded_paths(tmp_path):
    run = tmp_path / "run0001"
    _braindump(run)

    location = WorkflowLocator().locate(run, remap_submit_dir="never")

    assert location.submit_dir == Path("/container/work/run0001")
    assert location.basedir == Path("/container/work")
    assert location.database_path == Path(
        "/container/work/run0001/diamond-0.stampede.db"
    )


def test_auto_keeps_existing_recorded_submit_directory(tmp_path):
    recorded = tmp_path / "recorded" / "run0001"
    recorded.mkdir(parents=True)
    actual = tmp_path / "copied" / "run0001"
    _braindump(
        actual,
        recorded_submit=str(recorded),
        recorded_basedir=str(recorded.parent),
    )

    location = WorkflowLocator().locate(actual)

    assert location.submit_dir == recorded


def test_latest_run_uses_numeric_rundir_not_lexical_path(tmp_path):
    _braindump(tmp_path / "z" / "run0009", rundir="run0009")
    newest = _braindump(tmp_path / "a" / "run0010", rundir="run0010")
    _braindump(tmp_path / "zz-nonstandard", rundir="manual")

    location = WorkflowLocator().locate(tmp_path)

    assert location.braindump_path == newest.resolve()


def test_latest_run_uses_planning_timestamp_only_for_numeric_tie(tmp_path):
    _braindump(
        tmp_path / "a" / "run0007",
        rundir="run0007",
        timestamp="20260818T010000-0400",
    )
    newest = _braindump(
        tmp_path / "b" / "run0007",
        rundir="run0007",
        timestamp="20260818T020000-0400",
    )

    assert WorkflowLocator().locate(tmp_path).braindump_path == newest.resolve()


def test_latest_run_rejects_remaining_ambiguity(tmp_path):
    for branch in ("a", "b"):
        _braindump(
            tmp_path / branch / "run0007",
            rundir="run0007",
            timestamp="20260818T010000-0400",
        )

    with pytest.raises(WorkflowLocationError, match="ambiguous latest"):
        WorkflowLocator().locate(tmp_path)


def test_latest_run_rejects_malformed_timestamp_tie(tmp_path):
    _braindump(
        tmp_path / "a" / "run0007",
        rundir="run0007",
        timestamp="not-a-planning-time",
    )
    _braindump(
        tmp_path / "b" / "run0007",
        rundir="run0007",
        timestamp="20260818T010000-0400",
    )

    with pytest.raises(WorkflowLocationError, match="malformed planning timestamp"):
        WorkflowLocator().locate(tmp_path)


def test_sole_nonstandard_top_level_candidate_is_accepted(tmp_path):
    candidate = _braindump(tmp_path / "manual", rundir="manual")

    assert WorkflowLocator().locate(tmp_path).braindump_path == candidate.resolve()


def test_multiple_nonstandard_candidates_are_rejected(tmp_path):
    _braindump(tmp_path / "one", rundir="manual-a")
    _braindump(tmp_path / "two", rundir="manual-b")

    with pytest.raises(WorkflowLocationError, match="ambiguous nonstandard"):
        WorkflowLocator().locate(tmp_path)


def test_direct_subworkflow_uses_root_database_but_keeps_selected_scope(tmp_path):
    root = tmp_path / "run0001"
    _braindump(
        root,
        recorded_submit="/container/root/run0001",
        recorded_basedir="/container/root",
    )
    (root / "pegasus.properties").write_text(
        "pegasus.monitord.output=sqlite:////container/root/run0001/custom.db\n",
        encoding="utf-8",
    )
    child = root / "00" / "00" / "child"
    _braindump(
        child,
        wf_uuid="child-uuid",
        root_wf_uuid="root-uuid",
        recorded_submit="/container/root/run0001/00/00/child",
        recorded_basedir="/container/root",
        dag="child-0.dag",
    )

    location = WorkflowLocator().locate(child)

    assert location.workflow.wf_uuid == "child-uuid"
    assert location.workflow.root_wf_uuid == "root-uuid"
    assert location.is_subworkflow
    assert location.root_braindump_path == (root / "braindump.yml").resolve()
    assert location.root_submit_dir == root.resolve()
    assert location.database_path == root.resolve() / "custom.db"
    assert location.jobstate_path == child.resolve() / "jobstate.log"


def test_subworkflow_without_matching_ancestor_root_is_rejected(tmp_path):
    child = tmp_path / "child"
    _braindump(child, wf_uuid="child", root_wf_uuid="missing-root")

    with pytest.raises(WorkflowLocationError, match="cannot locate top-level"):
        WorkflowLocator().locate(child)


def test_workflow_url_precedes_monitord_output(tmp_path):
    run = tmp_path / "run0001"
    _braindump(run)
    (run / "pegasus.properties").write_text(
        "pegasus.monitord.output=sqlite:////container/work/run0001/old.db\n"
        "pegasus.catalog.workflow.url=sqlite:////container/work/run0001/new.db\n",
        encoding="utf-8",
    )

    location = WorkflowLocator().locate(run)

    assert location.database_path == run.resolve() / "new.db"


def test_relative_sqlite_url_is_resolved_beneath_local_submit_dir(tmp_path):
    run = tmp_path / "run0001"
    _braindump(run)
    (run / "pegasus.properties").write_text(
        "pegasus.catalog.workflow.url=sqlite:///state/custom.db\n",
        encoding="utf-8",
    )

    location = WorkflowLocator().locate(run)

    assert location.database_path == run.resolve() / "state/custom.db"


def test_properties_use_pegasus_substitution_escaping_and_continuation(
    tmp_path, monkeypatch
):
    from Pegasus.tools import properties as pegasus_properties

    run = tmp_path / "run0001"
    _braindump(run)
    monkeypatch.setitem(pegasus_properties.system, "user.dir", str(run.resolve()))
    (run / "pegasus.properties").write_text(
        "# generated properties\n"
        "pegasus.catalog.workflow.url = "
        "sqlite\\:///${user.dir}/state/\\\n"
        "workflow.db # trailing comment\n",
        encoding="utf-8",
    )

    location = WorkflowLocator().locate(run)

    assert location.database_path == run.resolve() / "state/workflow.db"


def test_planning_timestamp_tie_break_compares_absolute_instants(tmp_path):
    later_local_but_earlier_instant = _braindump(
        tmp_path / "a" / "run0007",
        rundir="run0007",
        timestamp="20260818T030000+0200",
    )
    later_instant = _braindump(
        tmp_path / "b" / "run0007",
        rundir="run0007",
        timestamp="20260818T020000+0000",
    )

    location = WorkflowLocator().locate(tmp_path)

    assert location.braindump_path == later_instant.resolve()
    assert location.braindump_path != later_local_but_earlier_instant.resolve()


@pytest.mark.parametrize(
    ("uri", "backend"),
    [
        ("postgresql://db.example/workflows", DatabaseBackend.POSTGRESQL),
        ("mysql://db.example/workflows", DatabaseBackend.MYSQL),
    ],
)
def test_external_database_url_is_explicitly_unsupported_not_guessed(
    tmp_path, uri, backend
):
    run = tmp_path / "run0001"
    _braindump(run)
    (run / "pegasus.properties").write_text(
        f"pegasus.catalog.workflow.url={uri}\n", encoding="utf-8"
    )

    location = WorkflowLocator().locate(run)

    assert location.database_backend is backend
    assert location.database_path is None
    assert location.database_uri == uri


def test_explicit_jobstate_path_is_not_searched_or_remapped(tmp_path, monkeypatch):
    run = tmp_path / "run0001"
    _braindump(run)
    invocation = tmp_path / "invocation"
    invocation.mkdir()
    monkeypatch.chdir(invocation)

    location = WorkflowLocator().locate(run, jobstate_path="elsewhere/live.log")

    assert location.jobstate_path == invocation / "elsewhere/live.log"
    assert location.jobstate_path_overridden


def test_default_jobstate_path_is_returned_even_when_absent(tmp_path):
    run = tmp_path / "run0001"
    _braindump(run)

    location = WorkflowLocator().locate(run)

    assert not location.jobstate_path.exists()
    assert location.jobstate_path == run.resolve() / "jobstate.log"


def test_non_braindump_file_is_rejected(tmp_path):
    other = tmp_path / "workflow.yml"
    other.write_text("{}", encoding="utf-8")

    with pytest.raises(WorkflowLocationError, match="expected braindump.yml"):
        WorkflowLocator().locate(other)
