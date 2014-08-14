import os
from unittest import TestLoader, TestSuite, TextTestRunner

try:
    import Pegasus
    import Pegasus.service
except ImportError:
    print "Unable to import Pegasus modules"
    print "Make sure dependencies are available"
    print "Set PYTHONPATH or run: python setup.py develop"
    exit(1)

def discoverTestModules(dirpath):
    modules = []
    for name in os.listdir(dirpath):
        path = os.path.join(dirpath, name)
        if os.path.isdir(path):
            modules += discoverTestModules(path)
        elif name.endswith(".py") and name.startswith("test_"):
            modules.append(path.replace(".py", "").replace("/", "."))
    return modules

loader = TestLoader()
alltests = TestSuite()

for module in discoverTestModules("Pegasus/test"):
    suite = loader.loadTestsFromName(module)
    alltests.addTest(suite)

runner = TextTestRunner(verbosity=2)
result = runner.run(alltests)

if result.wasSuccessful():
    exit(0)
else:
    exit(1)

