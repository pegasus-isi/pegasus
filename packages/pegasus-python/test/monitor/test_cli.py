"""CLI contract and source-wiring tests for pegasus-monitor."""

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

import asyncio
import gc
import io
import json
import signal
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest
from rich.console import Console
from rich.live import Live

from Pegasus.monitor import cli
from Pegasus.monitor.display import (
    DisplayAnalysis,
    DisplayContext,
    DisplayOptions,
    rendering_gc_guard,
)
from Pegasus.monitor.models import SchedulerQueryKind, WorkflowIdentity


class _Output(io.StringIO):
    def __init__(self, *, tty: bool = False):
        super().__init__()
        self._tty = tty
        self.width = 100

    def isatty(self) -> bool:
        return self._tty


class _Live:
    def __init__(self, renderable, **_kwargs):
        self.renderable = renderable

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None

    def update(self, renderable, **_kwargs):
        self.renderable = renderable


class _Console:
    def __init__(self, *, file):
        self.file = file
        self.width = 100
        self.height = 40

    def print(self, _renderable):
        return None


def _runtime(
    tmp_path: Path, *, authoritative: bool = True, on_start=None, on_render=None
):
    calls = SimpleNamespace(
        locator=0,
        stampede=0,
        tail=0,
        scheduler=0,
        scheduler_queries=[],
        coordinator_closed=0,
        tail_factory_seen=None,
        disabled_kinds=None,
        credential_values=None,
        console_prints=0,
    )
    braindump_path = tmp_path / "braindump.yml"
    braindump_path.write_text(
        "\n".join(
            (
                "user: alice",
                "wf_uuid: wf-uuid",
                "root_wf_uuid: wf-uuid",
                "submit_dir: /recorded/run0001",
                "basedir: /recorded",
                "dax_label: diamond",
                "planner_version: 6.0.0",
                "timestamp: 20260818T120000-0400",
            )
        ),
        encoding="utf-8",
    )
    workflow = WorkflowIdentity("wf-uuid", "wf-uuid")
    location = SimpleNamespace(
        workflow=workflow,
        braindump_path=braindump_path,
        submit_dir=tmp_path,
        recorded_submit_dir=Path("/recorded/run0001"),
        basedir=tmp_path.parent,
        recorded_basedir=Path("/recorded"),
        root_submit_dir=tmp_path,
        dag_name="diamond-0.dag",
        jobstate_path=tmp_path / "jobstate.log",
        database_path=tmp_path / "diamond-0.stampede.db",
    )
    snapshot = SimpleNamespace(
        sequence=1,
        effective=SimpleNamespace(),
        has_authoritative_base=authoritative,
        authoritative_complete=True,
    )
    if not authoritative:
        snapshot.effective = None
        snapshot.authoritative_complete = False

    class Locator:
        def __init__(self):
            calls.locator += 1

        def locate(self, target, **kwargs):
            assert target
            assert kwargs["remap_submit_dir"] in {"auto", "always", "never"}
            return location

    class Stampede:
        def __init__(self, selected):
            calls.stampede += 1
            assert selected is location

    class Tail:
        def __init__(self, path):
            calls.tail += 1
            assert path == location.jobstate_path

    class CoordinatorConfig:
        def __init__(self, **values):
            self.values = values

    class Coordinator:
        def __init__(
            self,
            selected_workflow,
            _stampede,
            *,
            tail_factory,
            scheduler,
            config,
        ):
            assert selected_workflow == workflow
            assert config.values["database_interval"] > 0
            calls.tail_factory_seen = tail_factory
            if tail_factory is not None:
                tail_factory()
            self.scheduler = scheduler
            self.latest = snapshot

        async def bootstrap(self):
            return snapshot

        async def start(self):
            if on_start is not None:
                on_start()
            return snapshot

        async def poll_scheduler_once(self, kind):
            calls.scheduler_queries.append(kind)

        async def close(self):
            calls.coordinator_closed += 1

    class Diagnostics:
        def analyze(self, *_args, **_kwargs):
            return SimpleNamespace(findings=(), errors=(), stall=None, idle=None)

    def scheduler_factory(**values):
        calls.scheduler += 1
        calls.credential_values = values
        assert set(values) == {
            "schedd",
            "collector",
            "token",
            "cert",
            "key",
            "password_file",
        }
        return object()

    def analyze_why_idle(_snapshot, **kwargs):
        calls.disabled_kinds = kwargs["disabled_scheduler_kinds"]
        return SimpleNamespace()

    def render_dashboard(*_args, **_kwargs):
        if on_render is not None:
            on_render()
        return "dashboard"

    class Console(_Console):
        def print(self, renderable):
            calls.console_prints += 1
            return super().print(renderable)

    runtime = cli.RuntimeComponents(
        Locator,
        Stampede,
        Tail,
        Coordinator,
        CoordinatorConfig,
        DisplayContext,
        DisplayOptions,
        DisplayAnalysis,
        Diagnostics,
        lambda _snapshot: SimpleNamespace(),
        analyze_why_idle,
        render_dashboard,
        lambda *_args, **_kwargs: "rendered\n",
        rendering_gc_guard,
        Console,
        _Live,
        scheduler_factory,
        SchedulerQueryKind,
    )
    return runtime, calls


def test_parser_preserves_v1_surface_and_rejects_deferred_flags():
    parser = cli.build_parser()
    parsed = parser.parse_args(
        [
            "-i",
            "3",
            "-a",
            "--no-sort-by-activity",
            "-e",
            "7",
            "--once",
            "--why-idle",
            "--remap-submit-dir",
            "always",
            "--diagnose",
            "--no-condor",
            "--no-live-events",
            "--jobstate-path",
            "events.log",
            "--schedd",
            "schedd.example",
            "--collector",
            "collector.example:9618",
            "--token",
            "token",
            "--cert",
            "cert",
            "--key",
            "key",
            "--password-file",
            "password",
            "run0001",
        ]
    )
    assert parsed.target == "run0001"
    assert parsed.interval == 3
    assert parsed.all_jobs
    assert not parsed.sort_by_activity
    assert parsed.events == 7
    for unsupported in (
        "--include-subworkflows",
        "--condor-poll",
        "--no-condor-poll",
    ):
        with pytest.raises(SystemExit) as raised:
            parser.parse_args([unsupported])
        assert raised.value.code == 2

    for invalid in ("0", "-1", "nan", "inf"):
        with pytest.raises(SystemExit) as raised:
            parser.parse_args(["--interval", invalid])
        assert raised.value.code == 2


def test_version_and_argparse_failures_construct_no_runtime(monkeypatch):
    monkeypatch.setattr(
        cli,
        "_load_runtime",
        lambda: pytest.fail("runtime should not be imported or constructed"),
    )
    with pytest.raises(SystemExit) as version:
        cli.main(["--version"])
    assert version.value.code == 0
    with pytest.raises(SystemExit) as invalid:
        cli.main(["--events", "-1"])
    assert invalid.value.code == 2


def test_non_tty_live_rejected_before_runtime_construction(monkeypatch):
    monkeypatch.setattr(
        cli,
        "_load_runtime",
        lambda: pytest.fail("runtime should not be constructed"),
    )
    errors = _Output()
    assert cli.main(["run"], stdout=_Output(), stderr=errors) == 1
    assert "use --once" in errors.getvalue()


def test_live_height_budget_caps_rows_but_reserves_analysis():
    args = cli.build_parser().parse_args(["--diagnose", "--events", "100", "run"])
    runtime = SimpleNamespace(display_options_type=DisplayOptions)
    options = cli._display_options(args, runtime, width=100, height=40, live=True)
    assert options.event_limit == 5
    assert options.job_row_limit <= 4
    assert options.analysis_line_limit >= 3


def test_confirmed_idle_stall_enrichment_queries_only_priority_and_negotiator():
    calls = []
    idle = object()
    snapshot = SimpleNamespace(effective=object())

    class Coordinator:
        latest = snapshot

        async def poll_scheduler_once(self, kind):
            calls.append(kind)

    runtime = SimpleNamespace(
        scheduler_kind=SchedulerQueryKind,
        analyze_why_idle=lambda value, **kwargs: (
            idle
            if value is snapshot
            and kwargs["workflow_owner"] == "alice"
            and kwargs["disabled_scheduler_kinds"] == frozenset()
            else pytest.fail("unexpected why-idle inputs")
        ),
    )
    args = SimpleNamespace(no_condor=False)
    context = SimpleNamespace(owner="alice")
    result = asyncio.run(cli._enrich_stall(Coordinator(), context, args, runtime))
    assert result is idle
    assert calls == [
        SchedulerQueryKind.PRIORITY,
        SchedulerQueryKind.NEGOTIATOR,
    ]


def test_stall_enrichment_key_requires_detected_stall_with_queued_jobs():
    detected = SimpleNamespace(
        event=SimpleNamespace(value="stall_detected"),
        queued_jobs=2,
        kind="idle_too_long",
        since_monotonic=10.0,
    )
    analysis = SimpleNamespace(diagnostics=SimpleNamespace(stall=detected))
    assert cli._stall_enrichment_key(analysis) == ("idle_too_long", 10.0)
    detected.queued_jobs = 0
    assert cli._stall_enrichment_key(analysis) is None


def test_stall_enrichment_result_is_discarded_after_resolution():
    old_key = ("idle_too_long", 10.0)
    resolved = DisplayAnalysis(
        diagnostics=SimpleNamespace(
            stall=SimpleNamespace(event=SimpleNamespace(value="stall_resolved"))
        )
    )
    active_key = cli._updated_active_stall(old_key, resolved)

    result = cli._apply_stall_enrichment(
        resolved,
        object(),
        old_key,
        active_key,
        SimpleNamespace(display_analysis_type=DisplayAnalysis),
    )

    assert active_key is None
    assert result is resolved
    assert result.why_idle is None


def test_stall_enrichment_result_is_discarded_after_stall_replacement():
    old_key = ("idle_too_long", 10.0)
    replacement = DisplayAnalysis(
        diagnostics=SimpleNamespace(
            stall=SimpleNamespace(
                event=SimpleNamespace(value="stall_detected"),
                queued_jobs=3,
                kind="no_progress",
                since_monotonic=20.0,
            )
        )
    )
    active_key = cli._updated_active_stall(old_key, replacement)

    result = cli._apply_stall_enrichment(
        replacement,
        object(),
        old_key,
        active_key,
        SimpleNamespace(display_analysis_type=DisplayAnalysis),
    )

    assert active_key == ("no_progress", 20.0)
    assert result is replacement
    assert result.why_idle is None


def test_stall_enrichment_result_applies_to_matching_active_stall():
    active_key = ("idle_too_long", 10.0)
    enriched_idle = object()
    analysis = DisplayAnalysis(
        stats=object(),
        diagnostics=SimpleNamespace(stall=None),
        errors=("existing",),
    )

    result = cli._apply_stall_enrichment(
        analysis,
        enriched_idle,
        active_key,
        active_key,
        SimpleNamespace(display_analysis_type=DisplayAnalysis),
    )

    assert result.stats is analysis.stats
    assert result.diagnostics is analysis.diagnostics
    assert result.why_idle is enriched_idle
    assert result.errors == analysis.errors


def test_once_arms_tail_queries_queue_and_always_closes(tmp_path):
    runtime, calls = _runtime(tmp_path)
    output = _Output()
    assert cli.main(["--once", str(tmp_path)], runtime=runtime, stdout=output) == 0
    assert calls.tail == 1
    assert calls.scheduler == 1
    assert calls.scheduler_queries == [
        SchedulerQueryKind.QUEUE,
        SchedulerQueryKind.HISTORY,
    ]
    assert calls.coordinator_closed == 1
    assert output.getvalue() == "rendered\n"


def test_credentials_are_forwarded_without_process_global_mutation(tmp_path):
    runtime, calls = _runtime(tmp_path)
    assert (
        cli.main(
            [
                "--once",
                "--schedd",
                "schedd.example",
                "--collector",
                "collector.example:9618",
                "--token",
                "/tokens",
                "--cert",
                "/cert",
                "--key",
                "/key",
                "--password-file",
                "/password",
                str(tmp_path),
            ],
            runtime=runtime,
            stdout=_Output(),
        )
        == 0
    )
    assert calls.credential_values == {
        "schedd": "schedd.example",
        "collector": "collector.example:9618",
        "token": "/tokens",
        "cert": "/cert",
        "key": "/key",
        "password_file": "/password",
    }


def test_completed_why_idle_queries_history_too(tmp_path):
    runtime, calls = _runtime(tmp_path)
    assert (
        cli.main(["--why-idle", str(tmp_path)], runtime=runtime, stdout=_Output()) == 0
    )
    assert calls.scheduler_queries == [
        SchedulerQueryKind.QUEUE,
        SchedulerQueryKind.POOL,
        SchedulerQueryKind.PRIORITY,
        SchedulerQueryKind.NEGOTIATOR,
        SchedulerQueryKind.HISTORY,
    ]
    assert calls.coordinator_closed == 1


def test_no_condor_constructs_no_observer_and_marks_analysis_disabled(tmp_path):
    runtime, calls = _runtime(tmp_path)
    assert (
        cli.main(
            ["--why-idle", "--no-condor", str(tmp_path)],
            runtime=runtime,
            stdout=_Output(),
        )
        == 0
    )
    assert calls.scheduler == 0
    assert calls.scheduler_queries == []
    assert calls.disabled_kinds == frozenset(SchedulerQueryKind)


def test_no_live_events_does_not_arm_tail(tmp_path):
    runtime, calls = _runtime(tmp_path)
    assert (
        cli.main(
            ["--once", "--no-live-events", str(tmp_path)],
            runtime=runtime,
            stdout=_Output(),
        )
        == 0
    )
    assert calls.tail_factory_seen is None
    assert calls.tail == 0


def test_once_without_authoritative_database_returns_one_and_closes(tmp_path):
    runtime, calls = _runtime(tmp_path, authoritative=False)
    assert cli.main(["--once", str(tmp_path)], runtime=runtime, stdout=_Output()) == 1
    assert calls.tail == 1
    assert calls.coordinator_closed == 1


def test_live_completion_and_cleanup(tmp_path):
    runtime, calls = _runtime(tmp_path)
    assert (
        cli.main(
            [str(tmp_path)], runtime=runtime, stdout=_Output(tty=True), stderr=_Output()
        )
        == 0
    )
    assert calls.coordinator_closed == 1
    assert calls.scheduler_queries == [SchedulerQueryKind.HISTORY]


def test_live_rendering_suspends_gc_and_restores_enabled_state(tmp_path):
    observed_gc_states = []

    def observe_render():
        observed_gc_states.append(("dashboard", gc.isenabled()))

    runtime, calls = _runtime(tmp_path, on_render=observe_render)

    class Live(_Live):
        def __enter__(self):
            observed_gc_states.append(("live_enter", gc.isenabled()))
            return super().__enter__()

        def __exit__(self, *_args):
            observed_gc_states.append(("live_exit", gc.isenabled()))
            return super().__exit__(*_args)

        def update(self, renderable, **kwargs):
            observed_gc_states.append(("live_update", gc.isenabled()))
            return super().update(renderable, **kwargs)

    class Console(_Console):
        def print(self, renderable):
            observed_gc_states.append(("console_print", gc.isenabled()))
            return super().print(renderable)

    gc.enable()
    runtime = replace(runtime, live_type=Live, console_type=Console)

    assert cli.main([str(tmp_path)], runtime=runtime, stdout=_Output(tty=True)) == 0
    assert observed_gc_states
    assert all(not enabled for _operation, enabled in observed_gc_states)
    assert gc.isenabled()
    assert calls.coordinator_closed == 1


def test_real_live_session_has_no_background_refresh_thread():
    runtime = SimpleNamespace(
        live_type=Live,
        rendering_gc_guard=rendering_gc_guard,
    )
    output = io.StringIO()
    console = Console(
        file=output,
        force_terminal=True,
        color_system=None,
        width=80,
        height=24,
    )

    with cli._live_rendering_session(
        runtime,
        lambda: "initial",
        console=console,
    ) as live:
        assert live.auto_refresh is False
        assert live._refresh_thread is None
        cli._refresh_live(runtime, live, lambda: "updated")
        assert live._refresh_thread is None

    assert live._refresh_thread is None


def test_live_rendering_restores_gc_after_refresh_failure(tmp_path):
    observed_gc_states = []

    class Live(_Live):
        def __exit__(self, *_args):
            observed_gc_states.append(("live_exit", gc.isenabled()))
            return super().__exit__(*_args)

        def update(self, _renderable, **_kwargs):
            observed_gc_states.append(("live_update", gc.isenabled()))
            raise RuntimeError("refresh failed")

    gc.enable()
    runtime, calls = _runtime(tmp_path)
    runtime = replace(runtime, live_type=Live)
    errors = _Output()

    assert (
        cli.main(
            [str(tmp_path)],
            runtime=runtime,
            stdout=_Output(tty=True),
            stderr=errors,
        )
        == 1
    )
    assert "refresh failed" in errors.getvalue()
    assert observed_gc_states == [("live_update", False), ("live_exit", False)]
    assert gc.isenabled()
    assert calls.coordinator_closed == 1


def test_live_rendering_preserves_already_disabled_gc(tmp_path):
    observed_gc_states = []

    def observe_render():
        observed_gc_states.append(gc.isenabled())

    runtime, calls = _runtime(tmp_path, on_render=observe_render)
    gc.disable()
    try:
        assert cli.main([str(tmp_path)], runtime=runtime, stdout=_Output(tty=True)) == 0
        assert observed_gc_states
        assert not any(observed_gc_states)
        assert not gc.isenabled()
    finally:
        gc.enable()

    assert calls.coordinator_closed == 1


def test_tail_only_live_observation_exits_cleanly_on_interrupt(tmp_path, monkeypatch):
    runtime, calls = _runtime(tmp_path, authoritative=False)

    async def interrupt(_delay):
        raise KeyboardInterrupt

    monkeypatch.setattr(cli.asyncio, "sleep", interrupt)
    assert (
        cli.main(
            [str(tmp_path)], runtime=runtime, stdout=_Output(tty=True), stderr=_Output()
        )
        == 0
    )
    assert calls.tail == 1
    assert calls.coordinator_closed == 1


def test_sigterm_requests_clean_shutdown_and_restores_handler(tmp_path, monkeypatch):
    installed = []
    previous = object()

    monkeypatch.setattr(cli.signal, "getsignal", lambda _signal: previous)

    def install(selected, handler):
        installed.append((selected, handler))

    monkeypatch.setattr(cli.signal, "signal", install)

    def terminate():
        assert installed
        installed[0][1](cli.signal.SIGTERM, None)

    runtime, calls = _runtime(tmp_path, authoritative=False, on_render=terminate)

    assert (
        cli.main(
            [str(tmp_path)], runtime=runtime, stdout=_Output(tty=True), stderr=_Output()
        )
        == 0
    )
    assert calls.coordinator_closed == 1
    assert calls.console_prints == 0
    assert installed[0][0] == cli.signal.SIGTERM
    assert installed[-1] == (cli.signal.SIGTERM, previous)


def test_braindump_context_falls_back_to_dag_stem(tmp_path):
    runtime, _calls = _runtime(tmp_path)
    path = tmp_path / "braindump.yml"
    content = path.read_text(encoding="utf-8")
    path.write_text(content.replace("dax_label: diamond\n", ""), encoding="utf-8")
    captured = {}

    def render_text(context, *_args, **_kwargs):
        captured["context"] = context
        return "ok\n"

    runtime = replace(runtime, render_text=render_text)
    assert cli.main(["--once", str(tmp_path)], runtime=runtime, stdout=_Output()) == 0
    assert captured["context"].label == "diamond-0"


@pytest.mark.parametrize("extra", [[], ["--why-idle"]])
def test_once_diagnose_queries_history_for_removed_jobs(tmp_path, extra):
    runtime, calls = _runtime(tmp_path)
    assert (
        cli.main(
            ["--once", "--diagnose", *extra, str(tmp_path)],
            runtime=runtime,
            stdout=_Output(),
        )
        == 0
    )
    assert SchedulerQueryKind.QUEUE in calls.scheduler_queries
    assert SchedulerQueryKind.HISTORY in calls.scheduler_queries


def test_once_diagnose_no_condor_does_not_query_history(tmp_path):
    runtime, calls = _runtime(tmp_path)
    assert (
        cli.main(
            ["--once", "--diagnose", "--no-condor", str(tmp_path)],
            runtime=runtime,
            stdout=_Output(),
        )
        == 0
    )
    assert calls.scheduler_queries == []


def test_redirected_diagnose_defaults_to_once(tmp_path):
    runtime, calls = _runtime(tmp_path)
    assert (
        cli.main(["--diagnose", str(tmp_path)], runtime=runtime, stdout=_Output()) == 0
    )
    assert calls.coordinator_closed == 1
    assert SchedulerQueryKind.HISTORY in calls.scheduler_queries


@pytest.mark.parametrize("mode", ["--remote", "--replay", "--serve"])
def test_cli_errors_strip_terminal_controls(monkeypatch, mode):
    def fail(*args, **kwargs):
        raise RuntimeError("ssh/condor: café界\x1b]0;PWNED\x07\x9b2J")

    monkeypatch.setattr(cli, "_run_replay_mode", fail)
    monkeypatch.setattr(cli, "_run_remote_mode", fail)
    monkeypatch.setattr(cli, "_run_server_launch_mode", fail)
    output, errors = _Output(), _Output()
    assert (
        cli.main([mode, "host:file"], stdout=output, stderr=errors, runtime=object())
        == 1
    )
    assert "café界" in errors.getvalue()
    assert "\x1b" not in errors.getvalue()
    assert "\x9b" not in errors.getvalue()


def test_parser_errors_strip_terminal_controls(capsys):
    with pytest.raises(SystemExit):
        cli.build_parser().parse_args(["--evil\x1b[2J"])
    assert "\x1b" not in capsys.readouterr().err


def _json_runtime(tmp_path, **options):
    """The fake runtime with a builder that records what the CLI handed it."""

    runtime, calls = _runtime(tmp_path, **options)
    calls.documents = []

    def build(base, **values):
        calls.documents.append(values)
        return {**base, "summary": {"built": True}}

    return replace(runtime, build_json_document=build), calls


def _document(output):
    text = output.getvalue()
    assert text.count("\n") == 1 and text.endswith("\n")
    return json.loads(text)


@pytest.mark.parametrize(
    "arguments",
    [
        ["--json"],
        ["--json", "--serve"],
        ["--json", "--once", "--serve-foreground"],
        ["--json", "--diagnose", "--stop-server"],
        ["--json", "--once", "--interval", "nan"],
        ["--js"],
    ],
)
def test_json_usage_errors_exit_two_with_one_document(monkeypatch, capsys, arguments):
    monkeypatch.setattr(
        cli,
        "_load_runtime",
        lambda: pytest.fail("runtime should not be imported or constructed"),
    )
    output = _Output()

    with pytest.raises(SystemExit) as raised:
        cli.main(arguments, stdout=output)

    assert raised.value.code == 2
    document = _document(output)
    assert document["exit_code"] == 2
    assert document["error"]["code"] == "usage_error"
    assert document["error"]["message"] in capsys.readouterr().err


def test_usage_errors_without_json_write_nothing_to_stdout(capsys):
    output = _Output()

    with pytest.raises(SystemExit) as raised:
        cli.main(["--once", "--serve"], stdout=output)

    assert raised.value.code == 2
    assert output.getvalue() == ""
    assert "server modes cannot be combined" in capsys.readouterr().err


@pytest.mark.parametrize(
    ("flags", "modes", "queries"),
    [
        (["--once"], ["once"], ["QUEUE", "HISTORY"]),
        (["--diagnose"], ["diagnose"], ["QUEUE", "HISTORY"]),
        (
            ["--why-idle"],
            ["why_idle"],
            ["QUEUE", "POOL", "PRIORITY", "NEGOTIATOR", "HISTORY"],
        ),
        (["--once", "--diagnose"], ["once", "diagnose"], ["QUEUE", "HISTORY"]),
    ],
)
def test_json_one_shot_modes_emit_a_document_instead_of_text(
    tmp_path, flags, modes, queries
):
    runtime, calls = _json_runtime(tmp_path)
    output = _Output(tty=True)

    code = cli.main([*flags, "--json", str(tmp_path)], runtime=runtime, stdout=output)

    assert code == 0
    document = _document(output)
    assert document["modes"] == modes
    assert document["origin"] == "live"
    assert document["target"] == str(tmp_path)
    assert (document["exit_code"], document["error"]) == (0, None)
    assert document["summary"] == {"built": True}
    assert [kind.name for kind in calls.scheduler_queries] == queries
    assert calls.console_prints == 0
    assert calls.coordinator_closed == 1
    assert len(calls.documents) == 1


def test_json_passes_condor_and_event_options_to_the_builder(tmp_path):
    runtime, calls = _json_runtime(tmp_path)

    cli.main(
        ["--once", "--json", "--no-condor", "-e", "3", str(tmp_path)],
        runtime=runtime,
        stdout=_Output(),
    )

    assert calls.scheduler == 0
    assert calls.documents[0]["condor_disabled"] is True
    assert calls.documents[0]["event_limit"] == 3


def test_json_without_authoritative_snapshot_keeps_exit_one(tmp_path):
    runtime, calls = _json_runtime(tmp_path, authoritative=False)
    output, errors = _Output(), io.StringIO()

    code = cli.main(
        ["--once", "--json", str(tmp_path)],
        runtime=runtime,
        stdout=output,
        stderr=errors,
    )

    assert code == 1
    document = _document(output)
    assert document["exit_code"] == 1
    assert document["error"]["code"] == "no_authoritative_snapshot"
    assert document["summary"] == {"built": True}
    assert "--once requires an authoritative Stampede snapshot" in errors.getvalue()
    assert calls.coordinator_closed == 1


def test_json_reports_cleanup_failure_in_the_single_document(tmp_path):
    runtime, _calls = _json_runtime(tmp_path)

    async def close(_self):
        raise OSError("close failed")

    runtime.coordinator_type.close = close
    output = _Output()

    code = cli.main(
        ["--once", "--json", str(tmp_path)],
        runtime=runtime,
        stdout=output,
        stderr=io.StringIO(),
    )

    assert code == 1
    document = _document(output)
    assert document["error"] == {"code": "cleanup_failed", "message": "close failed"}
    assert document["summary"] == {"built": True}


def test_json_interrupt_is_an_error_document_not_a_silent_success(
    tmp_path, monkeypatch
):
    runtime, _calls = _json_runtime(tmp_path)

    def interrupted(coroutine):
        coroutine.close()
        raise KeyboardInterrupt

    monkeypatch.setattr(cli.asyncio, "run", interrupted)
    output = _Output()

    code = cli.main(["--once", "--json", str(tmp_path)], runtime=runtime, stdout=output)

    assert code == 1
    assert _document(output)["error"]["code"] == "interrupted"


def test_json_sigterm_before_a_result_is_an_error_document(tmp_path):
    runtime, _calls = _json_runtime(tmp_path)

    async def bootstrap(_self):
        signal.raise_signal(signal.SIGTERM)
        await asyncio.sleep(30)

    runtime.coordinator_type.bootstrap = bootstrap
    output = _Output()

    code = cli.main(["--once", "--json", str(tmp_path)], runtime=runtime, stdout=output)

    assert code == 1
    assert _document(output)["error"]["code"] == "interrupted"


def test_json_error_messages_strip_terminal_controls(tmp_path):
    runtime, _calls = _json_runtime(tmp_path)

    def locate(_self, _target, **_kwargs):
        raise ValueError("bad \x1b[31mpath\x1b[0m")

    runtime.locator_type.locate = locate
    output = _Output()

    code = cli.main(
        ["--once", "--json", str(tmp_path)],
        runtime=runtime,
        stdout=output,
        stderr=io.StringIO(),
    )

    assert code == 1
    assert "\x1b" not in output.getvalue()
    assert "\\u001b" not in output.getvalue()
    assert _document(output)["error"]["code"] == "monitor_error"


def test_text_one_shot_output_is_unchanged_by_the_json_seam(tmp_path):
    runtime, calls = _json_runtime(tmp_path)
    output = _Output()

    assert cli.main(["--once", str(tmp_path)], runtime=runtime, stdout=output) == 0
    assert output.getvalue() == "rendered\n"
    assert calls.documents == []


def test_sub_workflow_context_names_its_own_dag_not_the_root_one(tmp_path):
    # Trial sample once-hier-sub.json: workflow.dag_name was pm-root-0.dag for a
    # sub-workflow whose braindump says pm-inner-0.dag.
    from Pegasus.monitor.locator import WorkflowLocator

    def braindump(directory, wf_uuid, dag, label):
        directory.mkdir(parents=True)
        (directory / "braindump.yml").write_text(
            json.dumps(
                {
                    "wf_uuid": wf_uuid,
                    "root_wf_uuid": "root-uuid",
                    "submit_dir": str(directory),
                    "basedir": str(tmp_path),
                    "dag": dag,
                    "dax_label": label,
                    "user": "submit",
                    "planner_version": "6.0.0.dev0",
                    "jsd": "jobstate.log",
                }
            ),
            encoding="utf-8",
        )

    root = tmp_path / "run0002"
    child = root / "00" / "00" / "pm-inner_subwf.000"
    braindump(root, "root-uuid", "pm-root-0.dag", "pm-root")
    braindump(child, "child-uuid", "pm-inner-0.dag", "pm-inner")
    runtime = cli._load_runtime()

    sub_location = WorkflowLocator().locate(child)
    sub = cli._load_display_context(sub_location, runtime)
    top = cli._load_display_context(WorkflowLocator().locate(root), runtime)

    assert (sub.wf_uuid, sub.root_wf_uuid) == ("child-uuid", "root-uuid")
    assert sub.dag_name == "pm-inner-0.dag"
    assert sub.label == "pm-inner"
    assert sub.submit_dir == child.resolve()
    assert sub.root_submit_dir == root.resolve()
    # The shared Stampede database is still named after the root DAG.
    assert sub_location.database_path == root.resolve() / "pm-root-0.stampede.db"
    assert top.dag_name == "pm-root-0.dag"


@pytest.mark.parametrize(
    ("arguments", "modes", "origin", "target"),
    [
        # Trial sample error-usage-json-serve.json: target was null.
        (["--json", "--serve", "run0001"], [], "live", "run0001"),
        (["--once", "--json", "--serve", "run0001"], ["once"], "live", "run0001"),
        (
            ["--json", "--dia", "--why", "run0001", "--stop-server"],
            ["diagnose", "why_idle"],
            "live",
            "run0001",
        ),
        (
            ["--json", "--once", "--replay", "events.jsonl", "--log", "x.jsonl"],
            ["once"],
            "replay",
            "events.jsonl",
        ),
        # Nothing parsed: the target cannot be told from an option value.
        (["--json", "--once", "--interval", "nan", "run0001"], ["once"], "live", None),
    ],
)
def test_json_usage_error_names_the_parsed_target_and_modes(
    arguments, modes, origin, target, capsys
):
    output = _Output()

    with pytest.raises(SystemExit) as raised:
        cli.main(arguments, stdout=output)

    capsys.readouterr()
    assert raised.value.code == 2
    document = _document(output)
    assert document["error"]["code"] == "usage_error"
    assert document["modes"] == modes
    assert document["origin"] == origin
    assert document["target"] == target
