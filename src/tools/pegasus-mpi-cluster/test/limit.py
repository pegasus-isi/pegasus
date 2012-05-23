import resource

print "RLIMIT_DATA:",resource.getrlimit(resource.RLIMIT_DATA)
print "RLIMIT_STACK:",resource.getrlimit(resource.RLIMIT_STACK)
print "RLIMIT_AS:",resource.getrlimit(resource.RLIMIT_AS)

# Make sure we allocate at least 1MB
x = 'c' * (1024*1024)
