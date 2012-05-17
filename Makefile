ifndef prefix
prefix = $(PEGASUS_HOME)
endif
bindir = $(prefix)/bin

CXX = mpicxx
CC = $(CXX)
LD = $(CXX)
CXXFLAGS = -g -Wall
LDFLAGS = 
RM = rm -f
INSTALL = install
MAKE = make

OS=$(shell uname -s)
ifeq (Linux,$(OS))
  OPSYS = LINUX
endif
ifeq (Darwin,$(OS))
  OPSYS = DARWIN
endif
ifndef OPSYS
  $(error Unsupported OS: $(OS))
endif

CXXFLAGS += -D$(OPSYS)

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

.PHONY: clean depends test install 

ifeq ($(shell which $(CXX) || echo n),n)
$(warning To build pegasus-mpi-cluster set CXX to the path to your MPI C++ compiler wrapper)
all:
install:
else
all: $(PROGRAMS)
install: $(PROGRAMS)
	$(INSTALL) -m 0755 $(PROGRAMS) $(bindir)
endif

pegasus-mpi-cluster: pegasus-mpi-cluster.o $(OBJS)
test-strlib: test-strlib.o $(OBJS)
test-dag: test-dag.o $(OBJS)
test-log: test-log.o $(OBJS)
test-engine: test-engine.o $(OBJS)

test: $(TESTS) $(PROGRAMS)
	test/test.sh

distclean: clean
	$(RM) $(PROGRAMS)

clean:
	$(RM) *.o $(TESTS)

depends:
	g++ -MM *.cpp > depends.mk

include depends.mk
