"""
Unified CLI entry point for Pegasus Workflow Management System.

All Pegasus tools are accessible as subcommands of the ``pegasus`` command.
"""

import click
from Pegasus.cli.lazy_group import LazyGroup, make_subcommand_alias


@click.group(
    cls=LazyGroup,
    lazy_subcommands={
        # --- Already Click-based ---
        "status": "Pegasus.cli._status.status",
        "run": "Pegasus.cli._run.run",
        "analyzer": "Pegasus.cli._analyzer.analyzer_cmd",
        "statistics": "Pegasus.cli._statistics.statistics",
        "remove": "Pegasus.cli._remove.remove",
        # --- Java wrapper subcommands ---
        "plan": "Pegasus.cli._plan.plan",
        "version": "Pegasus.cli._version.version",
        "rc-client": "Pegasus.cli._rc_client.rc_client",
        "tc-converter": "Pegasus.cli._tc_converter.tc_converter",
        "rc-converter": "Pegasus.cli._rc_converter.rc_converter",
        "sc-converter": "Pegasus.cli._sc_converter.sc_converter",
        "aws-batch": "Pegasus.cli._aws_batch.aws_batch",
        # --- Python scripts (argparse/optparse/custom) ---
        "monitord": "Pegasus.cli._monitord.monitord",
        "dagman": "Pegasus.cli._dagman.dagman",
        "db-admin": "Pegasus.cli._db_admin.db_admin",
        "exitcode": "Pegasus.cli._exitcode.exitcode",
        "graphviz": "Pegasus.cli._graphviz.graphviz",
        "init": "Pegasus.cli._init.init",
        "metadata": "Pegasus.cli._metadata.metadata",
        "preflight-check": "Pegasus.cli._preflight_check.preflight_check",
        "service": "Pegasus.cli._service.service",
        "submitdir": "Pegasus.cli._submitdir.submitdir",
        "cwl-converter": "Pegasus.cli._cwl_converter.cwl_converter",
        "em": "Pegasus.cli._em.em",
        "config": "Pegasus.cli._config.config",
        # --- Worker scripts ---
        "transfer": "Pegasus.cli._transfer.transfer",
        "s3": "Pegasus.cli._s3.s3",
        "integrity": "Pegasus.cli._integrity.integrity",
        "checkpoint": "Pegasus.cli._checkpoint.checkpoint",
        "globus-online": "Pegasus.cli._globus_online.globus_online",
        "globus-online-init": "Pegasus.cli._globus_online.globus_online",
        # --- Converted shell scripts ---
        "halt": "Pegasus.cli._halt.halt",
        "configure-glite": "Pegasus.cli._configure_glite.configure_glite",
    },
    help="main CLI command for lazy example",
)
@click.version_option(package_name="pegasus-wms")
def cli():
    """Pegasus Workflow Management System.

    Scientific workflow management for automating, recovering, and debugging
    large-scale computational workflows.
    """
    pass


# ---------------------------------------------------------------------------
# Auto-generate pegasus-<subcommand> entry points from the lazy map.
#
# Each name below becomes a standalone console_scripts target in
# pyproject.toml, e.g.:
#
#   pegasus-run     = "Pegasus.cli.main:pegasus_run"
#   pegasus-analyze = "Pegasus.cli.main:pegasus_analyzer"
#   ...
# ---------------------------------------------------------------------------
_SUBCOMMANDS = cli.lazy_subcommands  # type: ignore[attr-defined]

for _cmd_name in _SUBCOMMANDS:
    # "rc-client" -> "pegasus_rc_client"
    _attr = "pegasus_" + _cmd_name.replace("-", "_")
    globals()[_attr] = make_subcommand_alias(cli, _cmd_name)


if __name__ == "__main__":
    cli()
