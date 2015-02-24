import os
import sys
from optparse import OptionParser

class Command:
    description = None
    usage = "Usage: %prog [options] [args]"

    def __init__(self):
        self.parser = OptionParser(usage=self.usage, description=self.description)

    def parse(self, args):
        self.options, self.args = self.parser.parse_args(args)

    def run(self):
        pass

    def main(self, args=None):
        self.parse(args)
        self.run()

class CompoundCommand(Command):
    "A Command with multiple sub-commands"
    description = None
    usage = "%prog COMMAND [options] [args]"
    commands = []

    def __init__(self):
        Command.__init__(self)

        lines = [
            "\n\nCommands:"
        ]
        for cmd, cmdclass in self.commands:
            lines.append("    %-10s %s" % (cmd, cmdclass.description))

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

        if command not in commandmap:
            sys.stderr.write("Invalid command: %s\n" % command)
            exit(1)

        cmdclass = commandmap[command]
        cmd = cmdclass()
        cmd.main(args)

