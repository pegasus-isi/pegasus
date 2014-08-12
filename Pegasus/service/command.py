import os
import sys
import urlparse
import requests
from optparse import OptionParser

from Pegasus.service import app

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

class ClientCommand(Command):
    def __init__(self):
        Command.__init__(self)
        self.endpoint = app.config["ENDPOINT"]
        if self.endpoint is None:
            host = app.config["SERVER_HOST"]
            port = app.config["SERVER_PORT"]
            self.endpoint = "http://%s:%s/" % (host, port)
        self.username = app.config["USERNAME"]
        if not self.username:
            raise Exception("Specify USERNAME in configuration")
        self.password = app.config["PASSWORD"]
        if not self.password:
            raise Exception("Specify PASSWORD in configuration")

    def _request(self, method, path, **kwargs):
        headers = {
            'accept': 'application/json'
        }
        defaults = {"auth": (self.username, self.password), "headers": headers}
        defaults.update(kwargs)
        url = urlparse.urljoin(self.endpoint, path)
        return requests.request(method, url, **defaults)

    def get(self, path, **kwargs):
        return self._request("get", path, **kwargs)

    def post(self, path, **kwargs):
        return self._request("post", path, **kwargs)

    def delete(self, path, **kwargs):
        return self._request("delete", path, **kwargs)

    def put(self, path, **kwargs):
        return self._request("put", path, **kwargs)

