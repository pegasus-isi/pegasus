ifndef prefix
prefix = $(HOME)
endif
bindir = $(prefix)/bin

CXX ?= mpicxx
CC ?= mpicxx
CXXFLAGS ?= -g -Wall
LDFLAGS ?= 
RM = rm -f
INSTALL = install
MAKE = make


OBJS += strlib.o
OBJS += tools.o
OBJS += failure.o
OBJS += engine.o
OBJS += dag.o
OBJS += master.o
OBJS += worker.o
OBJS += protocol.o
OBJS += log.o

PROGRAMS += pegasus-mpi-cluster

TESTS += test-strlib
TESTS += test-dag
TESTS += test-log
TESTS += test-engine

all: $(PROGRAMS)

pegasus-mpi-cluster: pegasus-mpi-cluster.o $(OBJS)
	$(LD) $(LDFLAGS) -o pegasus-mpi-cluster pegasus-mpi-cluster.o $(OBJS)

test-strlib: test-strlib.o $(OBJS)
test-dag: test-dag.o $(OBJS)
test-log: test-log.o $(OBJS)
test-engine: test-engine.o $(OBJS)

test: $(TESTS)
	for test in $^; do echo $$test; ./$$test; done

.PHONY: clean depends test install 

install: $(PROGRAMS)
	$(INSTALL) -d -m 755 $(bindir)
	$(INSTALL) -m 755 $(PROGRAMS) $(bindir)

clean:
	$(RM) *.o $(PROGRAMS) $(TESTS)

depends:
	mpiCC $(CXXFLAGS) -MM *.cpp > depends.mk

include depends.mk
