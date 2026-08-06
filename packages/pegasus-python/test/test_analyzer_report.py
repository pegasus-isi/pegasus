"""Unit tests for the Markdown analyzer report builder."""

import io
import re

from rich.console import Console

from Pegasus import analyzer_report
from Pegasus.client.analyzer import (
    AnalyzerOutput,
    JobInstance,
    Jobs,
    Options,
    Task,
    Workflow,
)

ANSI_RE = re.compile(r"\x1b\[")


def make_output(wf_status="failure", jobs=None):
    out = AnalyzerOutput()
    out.root_wf_uuid = "uuid-1234"
    wf = Workflow(
        wf_uuid="uuid-1234",
        submit_dir="/some/submit/dir/run0001",
        wf_status=wf_status,
        wf_name="wf",
        jobs=jobs or Jobs(total=1, success=1),
    )
    out.workflows = {"root": wf}
    return out


def failed_job_instance(**overrides):
    ji = JobInstance(
        job_name="ls_ID0000001",
        state="POST_SCRIPT_FAILED",
        site="condorpool",
        hostname="workflow.isi.edu",
        work_dir="/work/dir",
        submit_file="00/00/ls_ID0000001.sub",
        stdout_file="00/00/ls_ID0000001.out",
        stderr_file="00/00/ls_ID0000001.err",
        executable="/bin/ls.sh",
        argv="-",
        subwf_dir="-",
        stdout_text="#@ 1 stderr\n/bin/ls: invalid option -- 'z'",
        stderr_text="PegasusLite: version 5.0.5",
        tasks={
            "ID0000001": Task(
                task_submit_seq=1,
                exitcode=2,
                executable="/usr/bin/ls",
                arguments="-z",
                transformation="ls",
                abs_task_id="ID0000001",
            )
        },
    )
    for k, v in overrides.items():
        setattr(ji, k, v)
    return ji


def test_summary_structure_and_counts():
    jobs = Jobs(
        total=5,
        success=1,
        failed=1,
        held=0,
        unsubmitted=3,
        job_details={"failed_jobs_details": {"ls_ID0000001": failed_job_instance()}},
    )
    md = analyzer_report.build_report_markdown(Options(), make_output(jobs=jobs))

    assert "## Summary" in md
    assert "/some/submit/dir/run0001" in md
    # GFM counts table: bold header + bold first column, padded cells.
    assert re.search(
        r"\|\s*\*\*Jobs\*\*\s*\|\s*\*\*Count\*\*\s*\|\s*\*\*%\*\*\s*\|", md
    )
    assert re.search(r"\|\s*\*\*Failed\*\*\s*\|\s*1\s*\|\s*20\.00%\s*\|", md)  # 1/5


def test_failed_job_data_present():
    jobs = Jobs(
        total=5,
        success=1,
        failed=1,
        unsubmitted=3,
        job_details={"failed_jobs_details": {"ls_ID0000001": failed_job_instance()}},
    )
    md = analyzer_report.build_report_markdown(Options(), make_output(jobs=jobs))

    assert "## Failed jobs' details" in md
    assert "ls_ID0000001" in md
    assert "POST_SCRIPT_FAILED" in md
    assert "condorpool" in md
    assert "/usr/bin/ls" in md
    assert "exitcode" in md
    # kickstart stderr surfaced in a fenced block.
    assert "invalid option" in md
    assert "```" in md
    # failed-workflow identity line.
    assert "uuid-1234" in md


def test_held_job_data_present():
    jobs = Jobs(
        total=2,
        success=1,
        held=1,
        job_details={
            "held_jobs_details": {
                "chebi_ID0000001": {
                    "submit_file": "chebi_ID0000001.sub",
                    "last_job_instance_id": 3,
                    "reason": "Transfer output files failure",
                }
            }
        },
    )
    md = analyzer_report.build_report_markdown(Options(), make_output(jobs=jobs))
    assert "## Held jobs' details" in md
    assert "chebi_ID0000001" in md
    assert "Transfer output files failure" in md


def test_summary_mode_omits_details():
    jobs = Jobs(
        total=5,
        success=1,
        failed=1,
        unsubmitted=3,
        job_details={"failed_jobs_details": {"ls_ID0000001": failed_job_instance()}},
    )
    md = analyzer_report.build_report_markdown(
        Options(summary_mode=True), make_output(jobs=jobs)
    )
    assert "## Summary" in md
    assert "uuid-1234" in md  # workflow failure line still present
    assert "## Failed jobs' details" not in md
    assert "invalid option" not in md


def test_quiet_mode_suppresses_dumps_but_keeps_task_summary():
    jobs = Jobs(
        total=1,
        failed=1,
        job_details={"failed_jobs_details": {"ls_ID0000001": failed_job_instance()}},
    )
    md = analyzer_report.build_report_markdown(
        Options(quiet_mode=True), make_output(jobs=jobs)
    )
    assert "ls_ID0000001" in md
    assert "Task #1 — Summary" in md
    assert "invalid option" not in md  # content dump suppressed


def test_success_workflow_reports_done():
    jobs = Jobs(total=5, success=5)
    md = analyzer_report.build_report_markdown(
        Options(), make_output(wf_status="success", jobs=jobs)
    )
    assert "## Done" in md
    assert "end of status report" in md
    assert "One or more workflows failed!" not in md
    assert "Failed jobs' details" not in md


def test_markup_unsafe_values_survive_verbatim():
    """Values containing Markdown metacharacters must not be reinterpreted."""
    ji = failed_job_instance(
        argv="--pattern [a-z]* --flag *bold* _under_ #hash",
        submit_file="/path/with [brackets]/file.sub",
    )
    jobs = Jobs(
        total=1,
        failed=1,
        job_details={"failed_jobs_details": {"ls_ID0000001": ji}},
    )
    md = analyzer_report.build_report_markdown(
        Options(print_invocation=True), make_output(jobs=jobs)
    )
    assert "[a-z]*" in md
    assert "*bold*" in md
    assert "_under_" in md
    assert "[brackets]" in md

    # And after Rich renders it, the literal text is preserved (no italics/links).
    console = Console(
        file=io.StringIO(), force_terminal=True, width=200, color_system=None
    )
    analyzer_report.emit(console, md)
    rendered = console.file.getvalue()
    assert "bold" in rendered
    assert "under" in rendered


def test_backtick_collision_in_dump_is_fenced_safely():
    ji = failed_job_instance(
        stdout_text="#@ 1 stderr\nhere is a ``` fenced ``` snippet inside output",
    )
    jobs = Jobs(
        total=1,
        failed=1,
        job_details={"failed_jobs_details": {"ls_ID0000001": ji}},
    )
    md = analyzer_report.build_report_markdown(Options(), make_output(jobs=jobs))
    assert "fenced" in md
    # The enclosing fence must be longer than the embedded triple-backtick run.
    assert "````" in md


def test_emit_non_terminal_is_raw_markdown_without_ansi():
    md = analyzer_report.build_report_markdown(
        Options(), make_output(jobs=Jobs(total=1, success=1), wf_status="success")
    )
    console = Console(file=io.StringIO())  # StringIO is not a tty
    assert console.is_terminal is False
    analyzer_report.emit(console, md)
    out = console.file.getvalue()
    assert not ANSI_RE.search(out)  # no escape codes redirected to a file
    assert "## Summary" in out  # literal Markdown preserved
    assert "## Done" in out


def test_emit_terminal_renders_markdown():
    md = "## Summary\n\nhello\n"
    console = Console(file=io.StringIO(), force_terminal=True, width=120)
    assert console.is_terminal is True
    analyzer_report.emit(console, md)
    out = console.file.getvalue()
    # Heading marker is consumed by the renderer (not literal in rendered output).
    assert "Summary" in out
    assert "## Summary" not in out


def test_code_theme_explicit_override(monkeypatch):
    monkeypatch.setenv("PEGASUS_ANALYZER_CODE_THEME", "github-dark")
    monkeypatch.setenv("COLORFGBG", "0;15")  # light hint, must be ignored
    assert analyzer_report._code_theme() == "github-dark"


def test_code_theme_from_colorfgbg(monkeypatch):
    monkeypatch.delenv("PEGASUS_ANALYZER_CODE_THEME", raising=False)
    monkeypatch.setenv("COLORFGBG", "0;15")  # light background
    assert analyzer_report._code_theme() == analyzer_report.LIGHT_CODE_THEME
    monkeypatch.setenv("COLORFGBG", "15;0")  # dark background
    assert analyzer_report._code_theme() == analyzer_report.DARK_CODE_THEME
    monkeypatch.setenv("COLORFGBG", "15;default;0")  # 3-field form, dark bg
    assert analyzer_report._code_theme() == analyzer_report.DARK_CODE_THEME


def test_code_theme_defaults_to_dark(monkeypatch):
    monkeypatch.delenv("PEGASUS_ANALYZER_CODE_THEME", raising=False)
    monkeypatch.delenv("COLORFGBG", raising=False)
    assert analyzer_report._code_theme() == analyzer_report.DARK_CODE_THEME


def _held(reason="held"):
    return {"submit_file": "x.sub", "last_job_instance_id": 1, "reason": reason}


def test_rule_between_held_jobs_not_after_last():
    jobs = Jobs(
        total=3,
        held=2,
        job_details={"held_jobs_details": {"a": _held("r1"), "b": _held("r2")}},
    )
    md = analyzer_report.build_report_markdown(Options(), make_output(jobs=jobs))
    # Exactly one rule between the two jobs, none trailing.
    assert md.count("\n---\n") == 1


def test_no_rule_with_single_held_job():
    jobs = Jobs(total=1, held=1, job_details={"held_jobs_details": {"a": _held()}})
    md = analyzer_report.build_report_markdown(Options(), make_output(jobs=jobs))
    assert "\n---\n" not in md


def test_rule_between_failed_jobs_not_after_last():
    jobs = Jobs(
        total=2,
        failed=2,
        job_details={
            "failed_jobs_details": {
                "j1": failed_job_instance(job_name="j1"),
                "j2": failed_job_instance(job_name="j2"),
            }
        },
    )
    md = analyzer_report.build_report_markdown(Options(), make_output(jobs=jobs))
    assert md.count("\n---\n") == 1
    # Rule sits between the two job headings, separating them.
    assert md.index("j1") < md.index("\n---\n") < md.index("j2")
