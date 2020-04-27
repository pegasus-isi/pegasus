import os
import sys
from Pegasus.DAX3 import *

if len(sys.argv) != 2:
    print "Usage: python daxgen.py DAXFILE"
    exit(1)

daxfile = sys.argv[1]

dax = ADAG("pmc-only")

# Create a mapping for the test script
test = Executable(name="test", os="linux", arch="x86_64", installed=True)
test.PFN("file://%s/test.sh" % os.getcwd(), "local")
dax.addExecutable(test)

# Create input dir if it doesn't exist
if not os.path.exists("inputs"):
    os.mkdir("inputs")

# Create a bunch of parallel chains
for i in range(20):
    input = File(name="input_%d.txt" % i)
    inter = File(name="inter_%d.txt" % i)
    output = File(name="output_%d.txt" % i)
    
    # Create input file
    f = open("inputs/%s" % input.name, "w")
    f.write("Hello, World! #%d\n" % i)
    f.close()
    
    # Register mapping for input file
    input.PFN("file://%s/inputs/%s" % (os.getcwd(), input.name), "local")
    dax.addFile(input)
    
    parent = Job(name="test")
    parent.addArguments(input, inter)
    parent.uses(input, link="input", register=False, transfer=True)
    parent.uses(inter, link="output", register=False, transfer=False)
    dax.addJob(parent)
    
    child = Job(name="test")
    child.addArguments(inter, output)
    child.uses(inter, link="input", register=False, transfer=False)
    child.uses(output, link="output", register=False, transfer=True)
    dax.addJob(child)
    
    dax.depends(parent=parent, child=child)

f = open(daxfile, "w")
dax.writeXML(f)
f.close()
