import sys

from pegasus.service.command import ClientCommand, CompoundCommand

def print_entry(lfn, pfn, **attrs):
    sys.stdout.write('%-20s %-60s' % (lfn, pfn))
    for k,v in attrs.items():
        sys.stdout.write(' %s="%s"' % (k,v))
    sys.stdout.write('\n')

class UpdateCommand(ClientCommand):
    description = "Update replica catalog"
    usage = "Usage: %prog update [options] RCFILE"

    def run(self):
        print self.endpoint, self.username, self.password
        print "post /replicas"

class LookupCommand(ClientCommand):
    description = "Lookup an lfn and get a list of PFNs"
    usage = "Usage: %prog lookup [options] LFN"

    def run(self):
        if len(self.args) == 0:
            self.parser.error("Specify LFN")
        elif len(self.args) > 1:
            self.parser.error("Invalid argument")

        lfn = self.args[0]

        response = self.get("/replicas/%s" % lfn)
        if response.status_code == 200:
            result = response.json()
            for r in result["pfns"]:
                print_entry(result["lfn"], r["pfn"], pool=r["pool"])
        else:
            # TODO Handle errors better
            print response.text
            exit(1)

class ShowCommand(ClientCommand):
    description = "Show the user's replica catalog"
    usage = "Usage: %prog show [options]"

    def run(self):
        response = self.get("/replicas")
        if response.status_code == 200:
            results = response.json()
            for r in results:
                print_entry(r["lfn"], r["pfn"], pool=r["pool"])
        else:
            # TODO Handle errors better
            print response.text
            exit(1)


class RCCommand(CompoundCommand):
    description = "Client for replica catalog"
    commands = {
        "update": UpdateCommand,
        "lookup": LookupCommand,
        "show": ShowCommand
    }

def main():
    "The entry point for pegasus-service-rc"
    RCCommand().main()

