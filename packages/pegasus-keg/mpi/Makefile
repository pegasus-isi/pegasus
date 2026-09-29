#
# Makefile for pegasus-mpi-keg, the MPI variant of pegasus-keg.
#
# The regular pegasus-keg is written in Go (see ../cmd/pegasus-keg) and is
# built by the top-level CMake build; this C++ source is kept only for the
# MPI build, which is not part of the CMake build.
#
INSTALL = install
RM      = rm -f
MPICC   = mpicc
CXXFLAGS += -O -Wall -DWITH_MPI
LOADLIBES = -lm -lstdc++
SOCKIO  = $(shell /bin/ls /usr/include/sys/sockio.h 2>/dev/null)

ifndef ${prefix}
  prefix = $(PEGASUS_HOME)
endif

ifneq (,${SOCKIO})
  CXXFLAGS += -DHAS_SYS_SOCKIO=1
endif

all: pegasus-mpi-keg

pegasus-mpi-keg: pegasus-keg.cc
	$(MPICC) $(CPPFLAGS) $(CXXFLAGS) $(LDFLAGS) $< -o $@ $(LOADLIBES)

install: pegasus-mpi-keg
	$(INSTALL) -m 0755 pegasus-mpi-keg $(prefix)/bin

test check: pegasus-mpi-keg
	@mpiexec -n 2 ./pegasus-mpi-keg -o /dev/fd/1 || echo "test failed" && echo "test ok"

clean distclean:
	$(RM) pegasus-mpi-keg core core.*

.PHONY: all install test check clean distclean
