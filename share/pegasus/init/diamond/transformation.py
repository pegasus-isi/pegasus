#!/usr/bin/env python3
import sys
import time

name = sys.argv[0]
inputs = []
outputs = []

args = sys.argv[1:]
args.reverse()

while len(args) > 0:
	arg = args.pop()
	if arg == "-i":
		inputs.append(args.pop())
	elif arg == "-o":
		outputs.append(args.pop())
	else:
		print "Invalid argument: %s" % args.pop()
		exit(1)

if len(inputs) == 0 or len(outputs) == 0:
	print "Usage: %s -i input... -o output..." % sys.argv[0]
	exit(1)

print "Sleeping for 30 seconds..."
time.sleep(30)

print "Generating output files..."
for output in outputs:
	o = open(output, "a")
	o.write("%s:\n" % name)
	for input in inputs:
		i = open(input, "r")
		data = i.read()
		i.close()
		o.write(data)
	o.close()


