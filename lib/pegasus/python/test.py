import os
from unittest import TestSuite, TextTestRunner
from unittest.loader import TestLoader

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
modules = discoverTestModules("Pegasus/test")

for module in modules:
    tests = loader.loadTestsFromName(module)
    alltests.addTests(tests)

runner = TextTestRunner(verbosity=2)
result = runner.run(alltests)

if result.wasSuccessful():
    exit(0)
else:
    exit(1)

