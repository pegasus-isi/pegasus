GCCVERSION := $(shell gcc -dumpversion)
GCCMAJOR := $(shell echo $(GCCVERSION) | cut -d. -f1)
CC = gcc
LOADLIBES = -lm
CFLAGS = -Wall -O2 -ggdb -std=gnu99

# use PIE on newer gccs
ifeq ($(shell expr $(GCCMAJOR) \>= 14), 1)
    CFLAGS += -fpie
endif

ifndef ${prefix}
    prefix = $(CURDIR)/../../../
endif

.PHONY: install clean check distclean

all: pegasus-cluster

pegasus-cluster: pegasus-cluster.o tools.o parser.o report.o mysystem.o job.o statinfo.o
try-cpus: try-cpus.o

depends.mk: $(SRCS) Makefile
	$(CC) -MM *.c > $@

install: pegasus-cluster
	install -m 0755 $^ $(prefix)/bin

check: pegasus-cluster
	bash check.sh

clean:
	rm -f *.o core core.* check.? depends.mk

distclean: clean
	rm -f pegasus-cluster try-cpus

-include depends.mk
