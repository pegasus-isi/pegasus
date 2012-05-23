import resource

print "Memory limits:",resource.getrlimit(resource.RLIMIT_DATA)

# Make sure we malloc at least 1MB
x = 'c' * (1024*1024)
