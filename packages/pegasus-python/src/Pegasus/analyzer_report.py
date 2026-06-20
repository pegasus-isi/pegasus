""":mod:`analyzer_report` renders :class:`Pegasus.client.analyzer.AnalyzerOutput`
as Markdown and emits it adaptively.

One Markdown string is the single source of truth:

* On an interactive terminal it is rendered with Rich (styled headings, GFM
  tables, syntax-highlighted code blocks).
* When redirected to a file or pipe (or with ``plain=True``) the raw Markdown
  is written verbatim -- literal ``#`` headings and ``| pipe |`` tables that
  stay readable, re-renderable, and friendly to log scrapers / AI agents.

The same raw Markdown string is what gets sent to the Pegasus AI agent.
"""

from __future__ import annotations

import os
import subprocess
from typing import TYPE_CHECKING

from rich.console import Console
from rich.markdown import Markdown
from rich.padding import Padding
from rich.theme import Theme

from Pegasus.client import analyzer

if TYPE_CHECKING:
    from collections.abc import Sequence

# Rich's default inline-code style is "bold cyan on black"; the black background
# is invisible on dark terminals but a jarring box on light ones. Drop the
# background for a consistent bold-cyan look on every terminal. (Fenced code
# blocks use the code_theme panel instead -- see _code_theme.)
INLINE_CODE_STYLE = "bold cyan"

# Pygments themes for fenced code blocks. A theme with a solid background renders
# code as a distinct panel; "ansi_*" themes have no fill and blend into the
# terminal. Pick one that suits the terminal's background (see _code_theme).
DARK_CODE_THEME = "monokai"
LIGHT_CODE_THEME = "default"
# Override env var wins over auto-detection (any valid Pygments theme name).
CODE_THEME_ENV = "PEGASUS_ANALYZER_CODE_THEME"


def _code_theme() -> str:
    """Choose a code-block theme based on the terminal background.

    Resolution order: the ``PEGASUS_ANALYZER_CODE_THEME`` override, then the
    ``COLORFGBG`` hint many terminals export, else assume a dark background.
    ``COLORFGBG`` is ``"fg;bg"`` (or ``"fg;default;bg"``); a background index of
    0-6 or 8 is dark, everything else light -- the same heuristic vim uses.
    """
    override = os.environ.get(CODE_THEME_ENV)
    if override:
        return override
    parts = os.environ.get("COLORFGBG", "").split(";")
    if parts and parts[-1].strip().isdigit():
        if int(parts[-1]) not in (0, 1, 2, 3, 4, 5, 6, 8):
            return LIGHT_CODE_THEME
    return DARK_CODE_THEME


# --- console / emit ------------------------------------------------------------------
def make_console() -> Console:
    """Build a Console that auto-detects whether stdout is a terminal.

    ``emoji``/``highlight`` are disabled so captured job output (``:word:``
    sequences, numbers, paths) is never reinterpreted.
    """
    return Console(
        emoji=False,
        highlight=False,
        theme=Theme({"markdown.code": INLINE_CODE_STYLE}),
    )


def emit(
    console: Console, md: str, *, plain: bool = False, indent_length: int = 0
) -> None:
    """Render ``md`` to ``console`` for a terminal, else write it verbatim.

    ``indent_length`` only affects the rendered (terminal) path -- leading
    whitespace would change Markdown semantics, so it is a no-op for raw output.
    """
    if console.is_terminal and not plain:
        renderable = Markdown(md, code_theme=_code_theme(), hyperlinks=False)
        if indent_length:
            renderable = Padding(renderable, (0, 0, 0, indent_length * 2))
        console.print(renderable)
    else:
        console.file.write(md if md.endswith("\n") else md + "\n")
    # Flush so progressively-emitted sections (e.g. the AI header printed before
    # the slow agent call) appear immediately rather than buffering.
    console.file.flush()


# --- Markdown helpers ----------------------------------------------------------------
def _dash(value: object) -> str:
    """Mirror the analyzer's ``value or "-"`` convention."""
    return "-" if value is None or value == "" else str(value)


def _max_backtick_run(s: str) -> int:
    """Length of the longest consecutive run of backticks in ``s``."""
    longest = 0
    run = 0
    for ch in s:
        run = run + 1 if ch == "`" else 0
        longest = max(longest, run)
    return longest


def _code(value: object) -> str:
    """A backtick-safe inline code span for an arbitrary scalar value."""
    s = _dash(value)
    # Longest run of backticks in the value decides the fence length.
    ticks = "`" * (_max_backtick_run(s) + 1)
    pad = " " if (s.startswith("`") or s.endswith("`") or s.strip("`") == "") else ""
    return f"{ticks}{pad}{s}{pad}{ticks}"


def _fenced(text: str | None, lang: str = "bash") -> str:
    """A fenced code block whose fence is longer than any backtick run inside."""
    s = "" if text is None else str(text)
    fence = "`" * max(3, _max_backtick_run(s) + 1)
    body = s if s.endswith("\n") or s == "" else s + "\n"
    return f"{fence}{lang}\n{body}{fence}"


def _cell(value: object) -> str:
    """Sanitize a value for a GFM table cell (escape pipes, flatten newlines)."""
    return str(value).replace("|", "\\|").replace("\n", " ").replace("\r", " ")


def _bold(text: str) -> str:
    """Wrap non-empty text in Markdown bold."""
    return f"**{text}**" if text else text


def _gfm_table(
    headers: Sequence[str],
    rows: Sequence[Sequence[object]],
    aligns: Sequence[str],
) -> list[str]:
    """Render a padded GFM table whose columns line up in raw text.

    ``aligns`` is one of ``"l"``/``"r"`` per column. The header row and the
    first column are emphasized in bold. Padding is cosmetic -- the table is
    still valid GFM, so Rich renders it identically in a terminal while the
    redirected file stays aligned and easy to scan.
    """
    cells = [[_bold(_cell(c)) for c in headers]]
    for r in rows:
        cells.append([_bold(_cell(c)) if i == 0 else _cell(c) for i, c in enumerate(r)])
    widths = [max(len(row[i]) for row in cells) for i in range(len(headers))]
    widths = [max(w, 3) for w in widths]  # keep separators readable

    def fmt(row):
        padded = [
            (c.rjust(w) if a == "r" else c.ljust(w))
            for c, w, a in zip(row, widths, aligns)
        ]
        return "| " + " | ".join(padded) + " |"

    sep = [
        ("-" * (w - 1) + ":") if a == "r" else ("-" * w) for w, a in zip(widths, aligns)
    ]
    return [fmt(cells[0]), "| " + " | ".join(sep) + " |"] + [fmt(r) for r in cells[1:]]


def _kv_table(pairs: Sequence[tuple[str, object]]) -> list[str]:
    """A 2-column Field/Value GFM table; values render as inline code."""
    rows = [(label, _code(value)) for label, value in pairs]
    return _gfm_table(["Field", "Value"], rows, ["l", "l"])


# --- section builders ----------------------------------------------------------------
def _summary(options: analyzer.Options, wf: analyzer.Workflow) -> list[str]:
    counts = wf.jobs
    out = []

    # PM-1762: failed workflow but zero failed jobs.
    if wf.wf_status == "failure" and counts.failed == 0 and not options.use_files:
        out.append(
            "> It seems your workflow failed with zero failed jobs. Please check "
            "the dagman.out and the monitord.log file in "
            f"{_code(options.input_dir or options.top_dir)}"
        )
        out.append("")

    out.append("## Summary")
    out.append("")
    out.extend(
        _kv_table(
            [
                ("Submit Directory", wf.submit_dir),
                ("Workflow Status", wf.wf_status),
            ]
        )
    )
    out.append("")

    def pct(n: int) -> str:
        return f"{100 * (1.0 * n / (counts.total or 1)):3.2f}%"

    rows = [
        ("Total Jobs", counts.total, ""),
        ("Succeeded", counts.success, pct(counts.success)),
        ("Failed", counts.failed, pct(counts.failed)),
        ("Held", counts.held, pct(counts.held)),
        ("Unsubmitted", counts.unsubmitted, pct(counts.unsubmitted)),
    ]
    if options.use_files:
        unknown = len(wf.jobs.job_details.get("unknown_jobs_details", {}))
        rows.append(("Unknown", unknown, pct(unknown)))

    out.extend(_gfm_table(["Jobs", "Count", "%"], rows, ["l", "r", "r"]))
    out.append("")
    return out


def _tasks(options: analyzer.Options, ji: analyzer.JobInstance) -> list[str]:
    out = []
    ji_stdout_text = ji.stdout_text
    ji_stderr_text = ji.stderr_text
    job_tasks = ji.tasks

    some_tasks_failed = any(t.exitcode != 0 for t in job_tasks.values())
    # PM-798: only print the condor job stderr if the kickstart record has none.
    print_job_stderr = True

    for task in job_tasks.values():
        if task.exitcode == 0 and some_tasks_failed:
            continue

        out.append(f"#### Task #{task.task_submit_seq} — Summary")
        out.append("")
        out.extend(
            _kv_table(
                [
                    ("site", ji.site),
                    ("hostname", ji.hostname),
                    ("executable", task.executable),
                    ("arguments", task.arguments),
                    ("exitcode", task.exitcode),
                    ("working dir", ji.work_dir),
                ]
            )
        )
        out.append("")

        if options.quiet_mode:
            continue

        label = (
            f"Task #{task.task_submit_seq} - {task.transformation} - {task.abs_task_id}"
        )

        if options.use_files and ji_stdout_text != "-":
            if ji_stdout_text:
                print_job_stderr = False
                out.append(f"#### {label} - stdout")
                out.append("")
                out.append(_fenced(ji_stdout_text))
                out.append("")
            continue

        # DB mode: kickstart record packs stdout/stderr behind "#@ N stdout"
        # and "#@ N stderr" markers.
        stdout_marker = f"#@ {task.task_submit_seq:d} stdout"
        stderr_marker = f"#@ {task.task_submit_seq:d} stderr"

        start = ji_stdout_text.find(stdout_marker)
        if start >= 0:
            start = start + len(stdout_marker) + 1
            end = ji_stdout_text.find("#@", start)
            end = len(ji_stdout_text) if end < 0 else end - 1
            if end - start > 0:
                print_job_stderr = False
                out.append(f"#### {label} - stdout")
                out.append("")
                out.append(_fenced(ji_stdout_text[start:end]))
                out.append("")

        start = ji_stdout_text.find(stderr_marker)
        if start >= 0:
            start = start + len(stderr_marker) + 1
            end = ji_stdout_text.find("#@", start)
            end = len(ji_stdout_text) if end < 0 else end - 1
            if end - start > 0:
                print_job_stderr = False
                out.append(f"#### {label} - Kickstart stderr")
                out.append("")
                out.append(_fenced(ji_stdout_text[start:end]))
                out.append("")

        # PM-808: print job-instance stdout for prescript failures only.
        if task.task_submit_seq == -1 and ji_stdout_text is not None:
            out.append(f"#### {label} - stdout")
            out.append("")
            out.append(_fenced(ji_stdout_text))
            out.append("")

    # Job-level stderr from the .err file.
    if ji_stderr_text and ji_stderr_text.strip("\n\t \r") != "" and print_job_stderr:
        out.append(f"#### Job stderr - {_dash(ji.job_name)}")
        out.append("")
        out.append(_fenced(ji_stderr_text))
        out.append("")

    return out


def _job_instance(
    options: analyzer.Options, ji: analyzer.JobInstance
) -> tuple[list[str], str | None]:
    """Return ``(markdown_lines, sub_wf_cmd)`` for a single job instance."""
    out = []
    sub_wf_cmd = None

    out.append(f"### Job: {_code(ji.job_name)}")
    out.append("")
    out.extend(
        _kv_table(
            [
                ("Last State", ji.state),
                ("Site", ji.site),
                ("Submit File", ji.submit_file),
                ("Output File", ji.stdout_file),
                ("Error File", ji.stderr_file),
            ]
        )
    )
    out.append("")

    if options.print_invocation:
        out.append("To re-run this job, use:")
        out.append("")
        out.append(_fenced(f"{_dash(ji.executable)} {_dash(ji.argv)}", lang="bash"))
        out.append("")

    if options.print_pre_script and len(ji.pre_executable or "") > 0:
        out.append("**SCRIPT PRE:**")
        out.append("")
        out.append(
            _fenced(f"{ji.pre_executable or ''} {ji.pre_argv or ''}", lang="bash")
        )
        out.append("")

    if ji.subwf_dir != "-":
        user_cmd = f" {analyzer.prog_base}"
        if options.use_files:
            if options.output_dir is not None:
                user_cmd += f" --output-dir {options.output_dir}"
            sub_wf_cmd = f"{user_cmd} -d {_subwf_files_dir(ji)}"
        else:
            sub_wf_cmd = (
                f"{user_cmd} -d {_subwf_db_dir(options, ji)} "
                f"--top-dir {options.top_dir or options.input_dir}"
            )
        if not options.recurse_mode:
            out.append(
                "This job contains sub workflows! Run the command below for "
                "more information:"
            )
            out.append("")
            out.append(_fenced(sub_wf_cmd, lang="bash"))
            out.append("")

    out.extend(_tasks(options, ji))
    return out, sub_wf_cmd


def _subwf_files_dir(ji: analyzer.JobInstance) -> str:
    return os.path.split(ji.subwf_dir)[0]


def _subwf_db_dir(options: analyzer.Options, ji: analyzer.JobInstance) -> str:
    my_wfdir = os.path.normpath(ji.subwf_dir)
    if my_wfdir.find(ji.submit_dir) >= 0:
        my_wfdir = os.path.normpath(my_wfdir.replace(ji.submit_dir + os.sep, "", 1))
        my_wfdir = os.path.join(options.input_dir, my_wfdir)
    return my_wfdir


def _join_with_rule(blocks: Sequence[list[str]]) -> list[str]:
    """Flatten per-job blocks, separating them with a horizontal rule.

    A rule is placed between jobs but not after the last. A blank line is ensured
    before ``---`` so Markdown reads it as a thematic break, not a heading rule.
    """
    out = []
    for i, block in enumerate(blocks):
        out.extend(block)
        if i != len(blocks) - 1:
            if out and out[-1] != "":
                out.append("")
            out.append("---")
            out.append("")
    return out


def _held_jobs(held_jobs: dict[str, dict]) -> list[str]:
    out = ["## Held jobs' details", ""]
    blocks = []
    for name, held in held_jobs.items():
        block = [f"### Job: {_code(name)}", ""]
        block += _kv_table(
            [
                ("Submit File", held["submit_file"]),
                ("Last job_instance_id", held["last_job_instance_id"]),
                ("Reason", held["reason"]),
            ]
        )
        block.append("")
        blocks.append(block)
    out.extend(_join_with_rule(blocks))
    return out


def _job_section(
    heading: str, options: analyzer.Options, jobs: dict[str, analyzer.JobInstance]
) -> list[str]:
    """Render a heading followed by each job instance, back to back."""
    out = [heading, ""]
    for ji in jobs.values():
        lines, _ = _job_instance(options, ji)
        out.extend(lines)
    return out


def _failing_jobs(
    options: analyzer.Options, failing_jobs: dict[str, analyzer.JobInstance]
) -> list[str]:
    return _job_section("## Failing jobs' details", options, failing_jobs)


def _failed_jobs(
    options: analyzer.Options, failed_jobs: dict[str, analyzer.JobInstance]
) -> list[str]:
    out = ["## Failed jobs' details", ""]
    blocks = []
    for ji in failed_jobs.values():
        lines, sub_wf_cmd = _job_instance(options, ji)
        if sub_wf_cmd is not None and options.recurse_mode:
            block = ["### Failed Sub Workflow", ""]
            try:
                result = subprocess.run(
                    sub_wf_cmd, shell=True, capture_output=True, text=True
                )
                block.append(result.stdout)
                if result.stderr:
                    block.append(result.stderr)
            except Exception as e:
                block.append(f"[Error running sub workflow command: {e}]")
            block.append("")
            blocks.append(block)
        else:
            blocks.append(lines)
    out.extend(_join_with_rule(blocks))
    return out


def _unknown_jobs(
    options: analyzer.Options, unknown_jobs: dict[str, analyzer.JobInstance]
) -> list[str]:
    return _job_section("## Unknown jobs' details", options, unknown_jobs)


def _workflow(options: analyzer.Options, wf: analyzer.Workflow) -> list[str]:
    out = []
    out.extend(_summary(options, wf))
    counts = wf.jobs
    details = wf.jobs.job_details

    if not options.summary_mode:
        if counts.held > 0:
            out.extend(_held_jobs(details["held_jobs_details"]))
        if "failing_jobs_details" in details:
            out.extend(_failing_jobs(options, details["failing_jobs_details"]))
        if counts.failed > 0:
            out.extend(_failed_jobs(options, details["failed_jobs_details"]))
        if "unknown_jobs_details" in details:
            out.extend(_unknown_jobs(options, details["unknown_jobs_details"]))

    if counts.failed > 0:
        out.append("**Workflow failed**")
        out.append("")
        out.extend(
            _kv_table(
                [
                    ("UUID", wf.wf_uuid),
                    ("Submit Directory", wf.submit_dir),
                ]
            )
        )
        out.append("")

    return out


# --- public entry point --------------------------------------------------------------
def build_report_markdown(
    options: analyzer.Options, output: analyzer.AnalyzerOutput
) -> str:
    """Render an :class:`AnalyzerOutput` as a Markdown report string."""
    out = []
    for wf in output.workflows.values():
        out.extend(_workflow(options, wf))

    if len(output.get_failed_workflows()) > 0:
        out.append("**One or more workflows failed!**")
    else:
        out.append("## Done")
        out.append("")
        out.append(f"{analyzer.prog_base}: end of status report")
    out.append("")

    return "\n".join(out)
