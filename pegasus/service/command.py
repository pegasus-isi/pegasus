import os
import sys
import urlparse
import requests
from optparse import OptionParser
from ConfigParser import ConfigParser

class Command:
    description = None
    usage = "Usage: %prog [options] [args]"

    def __init__(self):
        self.config = ConfigParser()
        self.config.read(os.path.expanduser("~/.pegasus/service.cfg"))

        self.parser = OptionParser(usage=self.usage, description=self.description)
        self.parser.add_option("-c", metavar="CONFIG", action="store", dest="config",
                               default="~/.pegasus/service.cfg",
                               help="Service config file")

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
    commands = {}

    def __init__(self):
        Command.__init__(self)

        lines = [
            "\n\nCommands:"
        ]
        for cmd, cmdclass in self.commands.items():
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

        if "-h" in args or "--help" in args:
            self.parser.print_help()
            exit(1)

        if command is None:
            self.parser.error("Specify COMMAND")
            exit(1)

        if command not in self.commands:
            sys.stderr.write("Invalid command: %s\n" % command)
            exit(1)

        cmdclass = self.commands[command]
        cmd = cmdclass()
        cmd.main(args)

class ClientCommand(Command):
    def __init__(self):
        Command.__init__(self)
        self.endpoint = self.config.get("client", "endpoint")
        self.username = self.config.get("client", "username")
        self.password = self.config.get("client", "password")

    def _request(self, method, path, **kwargs):
        headers = {
            'content-type': 'application/json',
            'accept': 'application/json'
        }
        defaults = {"auth": (self.username, self.password), "headers": headers}
        defaults.update(kwargs)
        url = urlparse.urljoin(self.endpoint, path)
        return requests.request(method, url, **defaults)

    def get(self, path, **kwargs):
        return self._request("get", path, **kwargs)

