"""pegasus configure-glite — configure BLAHP/gLite support in HTCondor."""

import os
import subprocess
import sys

import click


@click.command("configure-glite")
@click.argument("args", nargs=-1, type=click.UNPROCESSED)
def configure_glite(args):
    """Configure BLAHP/gLite support in HTCondor.

    This wraps the pegasus-configure-glite shell script.
    """
    # Locate the shell script in package data
    from Pegasus.cli._java import get_pegasus_data_dir

    data_dir = get_pegasus_data_dir()

    # Try multiple locations for the script
    for candidate in [
        data_dir / "share" / "pegasus-configure-glite",
        data_dir / "bin" / "pegasus-configure-glite",
    ]:
        if candidate.exists():
            cmd = [str(candidate)] + list(args)
            result = subprocess.run(cmd)
            sys.exit(result.returncode)

    # Fallback: look in the bin directory relative to this file
    cli_dir = os.path.dirname(__file__)
    for parent_offset in range(1, 8):
        candidate = os.path.join(
            cli_dir, *[".."] * parent_offset, "bin", "pegasus-configure-glite"
        )
        candidate = os.path.normpath(candidate)
        if os.path.exists(candidate):
            cmd = [candidate] + list(args)
            result = subprocess.run(cmd)
            sys.exit(result.returncode)

    raise click.ClickException(
        "Cannot locate pegasus-configure-glite script. "
        "Ensure Pegasus is properly installed."
    )
