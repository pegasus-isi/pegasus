"""Unified Pegasus CLI.

Entry point for both:
  - ``pegasus <subcommand>``  (the Click group registered as ``pegasus``)
  - Legacy ``pegasus-<tool>`` entry points (each function below)

Python CLI scripts delegate via runpy so their own option parsing and help
text remain intact.  Java tools delegate to _java.run_java_tool().
"""

import os
import runpy
import sys
from pathlib import Path

import click

_CLI_DIR = Path(__file__).parent
_WORKER_CLI_DIR = (
    _CLI_DIR.parent.parent.parent.parent  # packages/
    / "pegasus-worker"
    / "src"
    / "Pegasus"
    / "cli"
)


# ── Low-level helpers ────────────────────────────────────────────────────────


def _run_script(script_name: str, args, *, search_worker: bool = False) -> None:
    """Run a hyphenated CLI script as __main__ via runpy."""
    path = _CLI_DIR / f"{script_name}.py"
    if not path.exists() and search_worker:
        path = _WORKER_CLI_DIR / f"{script_name}.py"
    if not path.exists():
        click.echo(f"pegasus: {script_name}: script not found at {path}", err=True)
        sys.exit(1)
    sys.argv = [script_name] + list(args)
    runpy.run_path(str(path), run_name="__main__")


def _run_java(main_class: str, args) -> None:
    from Pegasus.cli._java import get_system_properties, run_java_tool

    run_java_tool(main_class, args=args, system_properties=get_system_properties())


# ── Command factory helpers ──────────────────────────────────────────────────

_PASSTHROUGH = {"ignore_unknown_options": True, "allow_extra_args": True}


def _script_cmd(name: str, script: str, help: str, *, worker: bool = False):
    """Return a Click command that delegates to a Python CLI script."""

    @click.command(
        name=name,
        help=help,
        add_help_option=False,
        context_settings=_PASSTHROUGH,
    )
    @click.argument("args", nargs=-1, type=click.UNPROCESSED)
    def _cmd(args):
        _run_script(script, args, search_worker=worker)

    return _cmd


def _java_cmd(name: str, main_class: str, help: str):
    """Return a Click command that delegates to a Java class."""

    @click.command(
        name=name,
        help=help,
        add_help_option=False,
        context_settings=_PASSTHROUGH,
    )
    @click.argument("args", nargs=-1, type=click.UNPROCESSED)
    def _cmd(args):
        _run_java(main_class, args)

    return _cmd


# ── Main Click group ─────────────────────────────────────────────────────────


@click.group()
def cli():
    """Pegasus Workflow Management System."""


# Python subcommands
cli.add_command(
    _script_cmd("analyzer", "pegasus-analyzer", "Analyze a Pegasus workflow"),
    name="analyzer",
)
cli.add_command(
    _script_cmd("config", "pegasus-config", "Print Pegasus installation configuration"),
    name="config",
)
cli.add_command(
    _script_cmd(
        "cwl-converter",
        "pegasus-cwl-converter",
        "Convert CWL workflows to Pegasus format",
    ),
    name="cwl-converter",
)
cli.add_command(
    _script_cmd("dagman", "pegasus-dagman", "Run DAGMan for a Pegasus workflow"),
    name="dagman",
)
cli.add_command(
    _script_cmd(
        "db-admin", "pegasus-db-admin", "Administer the Pegasus workflow database"
    ),
    name="db-admin",
)
cli.add_command(_script_cmd("em", "pegasus-em", "Ensemble manager"), name="em")
cli.add_command(
    _script_cmd("exitcode", "pegasus-exitcode", "Set job exit codes in the workflow"),
    name="exitcode",
)
cli.add_command(
    _script_cmd(
        "graphviz", "pegasus-graphviz", "Generate a Graphviz diagram of a workflow"
    ),
    name="graphviz",
)
cli.add_command(
    _script_cmd("init", "pegasus-init", "Initialize a Pegasus workflow"), name="init"
)
cli.add_command(
    _script_cmd("metadata", "pegasus-metadata", "Query workflow metadata"),
    name="metadata",
)
cli.add_command(
    _script_cmd("monitord", "pegasus-monitord", "Pegasus monitoring daemon"),
    name="monitord",
)
cli.add_command(
    _script_cmd(
        "preflight-check",
        "pegasus-preflight-check",
        "Check Pegasus runtime requirements",
    ),
    name="preflight-check",
)
cli.add_command(
    _script_cmd("remove", "pegasus-remove", "Remove a running Pegasus workflow"),
    name="remove",
)
cli.add_command(
    _script_cmd("run", "pegasus-run", "Submit a planned workflow for execution"),
    name="run",
)
cli.add_command(
    _script_cmd("service", "pegasus-service", "Start the Pegasus dashboard service"),
    name="service",
)
cli.add_command(
    _script_cmd("statistics", "pegasus-statistics", "Generate workflow statistics"),
    name="statistics",
)
cli.add_command(
    _script_cmd("status", "pegasus-status", "Show workflow execution status"),
    name="status",
)
cli.add_command(
    _script_cmd("submitdir", "pegasus-submitdir", "Manage workflow submit directories"),
    name="submitdir",
)

# Worker subcommands (scripts live in packages/pegasus-worker/)
cli.add_command(
    _script_cmd(
        "transfer",
        "pegasus-transfer",
        "Transfer files for a Pegasus workflow",
        worker=True,
    ),
    name="transfer",
)
cli.add_command(
    _script_cmd("s3", "pegasus-s3", "Interact with S3 storage", worker=True), name="s3"
)
cli.add_command(
    _script_cmd(
        "integrity",
        "pegasus-integrity",
        "Check file integrity for a workflow",
        worker=True,
    ),
    name="integrity",
)
cli.add_command(
    _script_cmd(
        "checkpoint", "pegasus-checkpoint", "Checkpoint a Pegasus workflow", worker=True
    ),
    name="checkpoint",
)
cli.add_command(
    _script_cmd(
        "globus-online",
        "pegasus-globus-online",
        "Transfer files via Globus Online",
        worker=True,
    ),
    name="globus-online",
)
cli.add_command(
    _script_cmd(
        "globus-online-init",
        "pegasus-globus-online-init",
        "Initialize Globus Online credentials",
        worker=True,
    ),
    name="globus-online-init",
)

# Java subcommands
cli.add_command(
    _java_cmd(
        "plan",
        "edu.isi.pegasus.planner.client.CPlanner",
        "Plan a Pegasus workflow from a DAX",
    ),
    name="plan",
)
cli.add_command(
    _java_cmd(
        "version",
        "edu.isi.pegasus.planner.client.VersionNumber",
        "Print Pegasus version",
    ),
    name="version",
)
cli.add_command(
    _java_cmd(
        "rc-client",
        "edu.isi.pegasus.planner.client.RCClient",
        "Interact with the Replica Catalog",
    ),
    name="rc-client",
)
cli.add_command(
    _java_cmd(
        "rc-converter",
        "edu.isi.pegasus.planner.client.RCConverter",
        "Convert Replica Catalog format",
    ),
    name="rc-converter",
)
cli.add_command(
    _java_cmd(
        "tc-converter",
        "edu.isi.pegasus.planner.client.TCConverter",
        "Convert Transformation Catalog format",
    ),
    name="tc-converter",
)
cli.add_command(
    _java_cmd(
        "sc-converter",
        "edu.isi.pegasus.planner.client.SCClient",
        "Convert Site Catalog format",
    ),
    name="sc-converter",
)
cli.add_command(
    _java_cmd(
        "aws-batch",
        "edu.isi.pegasus.aws.batch.client.PegasusAWSBatch",
        "Run workflows on AWS Batch",
    ),
    name="aws-batch",
)


# ── halt (was a shell script) ────────────────────────────────────────────────


@click.command(add_help_option=False, context_settings=_PASSTHROUGH)
@click.argument("args", nargs=-1, type=click.UNPROCESSED)
def _halt_cmd(args):
    """Halt a running Pegasus workflow gracefully."""
    _do_halt(list(args))


cli.add_command(_halt_cmd, name="halt")


# ── configure-glite (was a shell script) ────────────────────────────────────


@click.command(add_help_option=False, context_settings=_PASSTHROUGH)
@click.argument("args", nargs=-1, type=click.UNPROCESSED)
def _configure_glite_cmd(args):
    """Configure Condor glite/BLAHPD integration."""
    _do_configure_glite(list(args))


cli.add_command(_configure_glite_cmd, name="configure-glite")


# ── Implementations of former shell-only commands ────────────────────────────


def _do_halt(argv: list) -> None:
    """Implement pegasus-halt: touch .halt files for each .dag in a submit dir."""
    run_dir = argv[0] if argv else None

    if run_dir is None and Path("braindump.yml").exists():
        run_dir = str(Path.cwd())

    if not run_dir:
        click.echo("Please specify a run directory", err=True)
        click.echo("Usage: pegasus halt <rundir>", err=True)
        sys.exit(1)

    run_path = Path(run_dir)
    if not run_path.exists():
        click.echo(f"{run_dir} does not exist!", err=True)
        sys.exit(1)
    if not (run_path / "braindump.yml").exists():
        click.echo(
            f"{run_dir} does not contain a braindump file and is probably not a run directory.",
            err=True,
        )
        sys.exit(1)

    for dag_file in run_path.rglob("*.dag"):
        halt_file = Path(str(dag_file) + ".halt")
        halt_file.touch()

    click.echo(
        "Workflow has been given the halt signal, and will gracefully exit once\n"
        "all current jobs have finished. The workflow can be restarted from\n"
        "where it was left off with the pegasus-run command."
    )


def _do_configure_glite(argv: list) -> None:
    """Implement pegasus-configure-glite: configure Condor BLAHPD integration."""
    import shutil
    import subprocess

    if len(argv) > 1 or (argv and argv[0] in ("-h", "--help")):
        click.echo("Usage: pegasus configure-glite [-h] [GLITE_LOCATION]")
        sys.exit(0 if argv and argv[0] in ("-h", "--help") else 1)

    import platform

    if platform.system() == "Darwin":
        click.echo(
            "WARNING: Condor doesn't normally ship with glite on OSX, so this is unlikely to work"
        )

    if not shutil.which("condor_config_val"):
        click.echo(
            "ERROR: Unable to find condor_config_val. Ensure Condor is installed and in PATH.",
            err=True,
        )
        sys.exit(1)

    def _condor_val(key: str) -> str:
        result = subprocess.run(
            ["condor_config_val", key],
            capture_output=True,
            text=True,
        )
        return result.stdout.strip() if result.returncode == 0 else ""

    blahpd_location = argv[0] if argv else _condor_val("BLAHPD_LOCATION")
    glite_location = _condor_val("GLITE_LOCATION")

    # Resolve glite files directory
    from Pegasus.cli._java import get_system_properties

    props = get_system_properties()
    share_dir = Path(props.get("pegasus.home.sharedstatedir", ""))
    pegasus_glite_dir = share_dir / "htcondor" / "glite"

    if blahpd_location and Path(blahpd_location).is_dir():
        blahpd_libexec = Path(blahpd_location) / "libexec" / "blahp"
        blahpd_config = Path(blahpd_location) / "etc" / "blah.config"
        blahpd_scripts = Path(blahpd_location) / "etc" / "blahp"
        if blahpd_location == "/usr":
            blahpd_location = ""
    elif glite_location and Path(glite_location).is_dir():
        blahpd_config = Path(glite_location) / "etc" / "batch_gahp.config"
        blahpd_scripts = Path(glite_location) / "bin"
        blahpd_libexec = blahpd_scripts
    else:
        click.echo("ERROR: BLAHPD_LOCATION / GLITE_LOCATION not found.", err=True)
        sys.exit(1)

    for path in (blahpd_config, blahpd_scripts):
        if not path.exists():
            click.echo(
                f"ERROR: Missing {path}. Check your BLAHPD_LOCATION / GLITE_LOCATION.",
                err=True,
            )
            sys.exit(1)

    # Install local_submit_attributes scripts and create symlinks
    for src in pegasus_glite_dir.glob("*local_submit_attributes.sh"):
        dst = blahpd_scripts / src.name
        import shutil as _shutil

        _shutil.copy2(src, dst)
        click.echo(f"Installing {src.name} into {blahpd_scripts}/")
        symlink = blahpd_libexec / src.name
        if symlink.is_symlink():
            symlink.unlink()
        elif symlink.exists():
            symlink.rename(str(symlink) + ".bak")
        os.symlink(f"../../../etc/blahp/{src.name}", symlink)
        click.echo(f"Created symlink {src.name} in {blahpd_libexec}")

    # Install remaining scripts
    for src in pegasus_glite_dir.glob("*.sh"):
        if "local_submit_attributes" not in src.name:
            import shutil as _shutil

            _shutil.copy2(src, blahpd_libexec / src.name)
            click.echo(f"Installing {src.name} into {blahpd_libexec}/")
    for src in pegasus_glite_dir.glob("*.py"):
        if "local_submit_attributes" not in src.name:
            import shutil as _shutil

            _shutil.copy2(src, blahpd_libexec / src.name)
            click.echo(f"Installing {src.name} into {blahpd_libexec}/")


# ── Legacy standalone entry points (console_scripts in pyproject.toml) ───────
# Each function is referenced directly as an entry point.  For Python scripts
# we delegate to _run_script(); for Java tools we call _run_java() directly.


def pegasus_plan():
    _run_java("edu.isi.pegasus.planner.client.CPlanner", sys.argv[1:])


def pegasus_aws_batch():
    _run_java("edu.isi.pegasus.aws.batch.client.PegasusAWSBatch", sys.argv[1:])


def pegasus_version():
    _run_java("edu.isi.pegasus.planner.client.VersionNumber", sys.argv[1:])


def pegasus_rc_client():
    _run_java("edu.isi.pegasus.planner.client.RCClient", sys.argv[1:])


def pegasus_rc_converter():
    _run_java("edu.isi.pegasus.planner.client.RCConverter", sys.argv[1:])


def pegasus_tc_converter():
    _run_java("edu.isi.pegasus.planner.client.TCConverter", sys.argv[1:])


def pegasus_sc_converter():
    _run_java("edu.isi.pegasus.planner.client.SCClient", sys.argv[1:])


def pegasus_status():
    _run_script("pegasus-status", sys.argv[1:])


def pegasus_run():
    _run_script("pegasus-run", sys.argv[1:])


def pegasus_analyzer():
    _run_script("pegasus-analyzer", sys.argv[1:])


def pegasus_statistics():
    _run_script("pegasus-statistics", sys.argv[1:])


def pegasus_remove():
    _run_script("pegasus-remove", sys.argv[1:])


def pegasus_monitord():
    _run_script("pegasus-monitord", sys.argv[1:])


def pegasus_dagman():
    _run_script("pegasus-dagman", sys.argv[1:])


def pegasus_db_admin():
    _run_script("pegasus-db-admin", sys.argv[1:])


def pegasus_exitcode():
    _run_script("pegasus-exitcode", sys.argv[1:])


def pegasus_graphviz():
    _run_script("pegasus-graphviz", sys.argv[1:])


def pegasus_init():
    _run_script("pegasus-init", sys.argv[1:])


def pegasus_metadata():
    _run_script("pegasus-metadata", sys.argv[1:])


def pegasus_preflight_check():
    _run_script("pegasus-preflight-check", sys.argv[1:])


def pegasus_service():
    _run_script("pegasus-service", sys.argv[1:])


def pegasus_submitdir():
    _run_script("pegasus-submitdir", sys.argv[1:])


def pegasus_cwl_converter():
    _run_script("pegasus-cwl-converter", sys.argv[1:])


def pegasus_em():
    _run_script("pegasus-em", sys.argv[1:])


def pegasus_config():
    _run_script("pegasus-config", sys.argv[1:])


def pegasus_halt():
    _do_halt(sys.argv[1:])


def pegasus_configure_glite():
    _do_configure_glite(sys.argv[1:])


def pegasus_transfer():
    _run_script("pegasus-transfer", sys.argv[1:], search_worker=True)


def pegasus_s3():
    _run_script("pegasus-s3", sys.argv[1:], search_worker=True)


def pegasus_integrity():
    _run_script("pegasus-integrity", sys.argv[1:], search_worker=True)


def pegasus_checkpoint():
    _run_script("pegasus-checkpoint", sys.argv[1:], search_worker=True)


def pegasus_globus_online():
    _run_script("pegasus-globus-online", sys.argv[1:], search_worker=True)


def pegasus_globus_online_init():
    _run_script("pegasus-globus-online-init", sys.argv[1:], search_worker=True)
