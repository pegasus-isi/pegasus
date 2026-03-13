"""
Base command classes for Pegasus CLI tools.

Provides :class:`Command`, :class:`LoggingCommand`, and :class:`CompoundCommand`
as the foundation for all ``pegasus-*`` command-line tools.
"""

import logging
import sys
from optparse import OptionParser

from Pegasus.tools import utils

log = logging.getLogger(__name__)


class Command:
    """Base class for all Pegasus CLI commands.

    Subclasses set :attr:`description`, :attr:`usage`, and :attr:`epilog` class
    attributes, then override :meth:`run` with their logic.
    """

    description = None
    epilog = None
    usage = "Usage: %prog [options] [args]"

    def __init__(self):
        self.parser = OptionParser(
            usage=self.usage, description=self.description, epilog=self.epilog
        )

    def parse(self, args):
        self.options, self.args = self.parser.parse_args(args)

    def run(self):
        pass

    def main(self, args=None):
        self.parse(args)
        self.run()


class LoggingCommand(Command):
    """A :class:`Command` subclass that configures logging verbosity via ``-v/--verbose``.

    Handles unhandled exceptions by printing them to stderr and exiting with code 1.
    Pass ``-v`` once for INFO logging, twice for DEBUG.
    """

    def __init__(self):
        Command.__init__(self)
        self.parser.add_option(
            "-v",
            "--verbose",
            action="count",
            default=0,
            dest="verbosity",
            help="Increase logging verbosity, repeatable",
        )

    def main(self, args=None):
        self.parse(args)

        verbosity = self.options.verbosity

        if verbosity == 0:
            log_level = logging.WARNING
        elif verbosity == 1:
            log_level = logging.INFO
        elif verbosity >= 2:
            log_level = logging.DEBUG

        utils.configureLogging(level=log_level)

        try:
            self.run()
        except Exception as e:
            # Only log stack grace if -v has been used
            if verbosity >= 1:
                log.exception(e)
            else:
                sys.stderr.write("%s\n" % e)
            exit(1)


class CompoundCommand(Command):
    """A :class:`Command` with multiple named sub-commands.

    Subclasses declare a :attr:`commands` list of ``(name, CommandClass)`` pairs
    and an optional :attr:`aliases` dict mapping alias strings to canonical names.
    The first non-option argument on the command line selects the sub-command to run.
    """

    usage = "%prog COMMAND [options] [args]"
    commands = []
    aliases = {}

    def __init__(self):
        Command.__init__(self)

        lines = ["\n\nCommands:"]
        for cmd, cmdclass in self.commands:
            lines.append(f"    {cmd:<20} {cmdclass.description}")

        self.parser.usage += "\n".join(lines)

    def main(self, args=None):
        if args is None:
            args = sys.argv[1:]

        command = None
        for arg in args:
            if arg[0] != "-":
                command = arg
                args.remove(command)
                break

        if command is None and ("-h" in args or "--help" in args):
            self.parser.print_help()
            exit(1)

        if command is None:
            self.parser.error("Specify COMMAND")
            exit(1)

        commandmap = dict(self.commands)

        if command in self.aliases:
            command = self.aliases[command]

        if command not in commandmap:
            sys.stderr.write("Invalid command: %s\n" % command)
            exit(1)

        cmdclass = commandmap[command]
        cmd = cmdclass()
        cmd.main(args)
