#
# Makefile
#
INSTALL = install
RM      = rm -f
CXX	= g++ -ffor-scope
CXXFLAGS += -O -Wall
LD      = $(CXX)
LOADLIBES = -lm
GCCVERSION := $(shell gcc -dumpversion)
GCCMAJOR := $(shell echo $(GCCVERSION) | cut -d. -f1)
SYSTEM  = $(shell uname -s | tr '[a-z]' '[A-Z]' | tr -d '_ -/')
VERSION = $(shell uname -r)
MARCH	= $(shell uname -m | tr '[A-Z]' '[a-z]')
MAJOR   = $(firstword $(subst ., ,$(VERSION)))
SOCKIO  = $(shell /bin/ls /usr/include/sys/sockio.h 2>/dev/null)
EXTRA_OBJ = basic.o
EXTRA_INC = version.h
MUSLLIBC = $(shell gcc -dumpmachine | grep musl | wc -l)

ifndef ${prefix}
  prefix = $(PEGASUS_HOME)
endif

ifndef ${datadir}
  datadir = $(prefix)/share
endif

ifeq (DARWIN,${SYSTEM})
  CXXFLAGS += -DMACHINE_SPECIFIC=darwin
  EXTRA_OBJ += darwin.o
  EXTRA_INC += darwin.hh
else
  ifeq (LINUX,${SYSTEM})
    CXXFLAGS += -Wall -O2 -ggdb -DMACHINE_SPECIFIC=linux
    EXTRA_OBJ += linux.o
    EXTRA_INC += linux.hh
    CFLAGS += $(shell getconf LFS_CFLAGS 2>>/dev/null)
    LDFLAGS += $(shell getconf LFS_LDFLAGS 2>>/dev/null)
    # use PIE on newer gccs
    ifeq ($(shell expr $(GCCMAJOR) \>= 14), 1)
      CFLAGS += -fpie
    endif
  else
    ifeq (GNUKFREEBSD,${SYSTEM})
      CXXFLAGS = -Wall -O2 -ggdb -DMACHINE_SPECIFIC=gnukfreebsd
      EXTRA_OBJ += gnukfreebsd.o
      EXTRA_INC += gnukfreebsd.hh
    else
      $(error Unsupported system: $(SYSTEM))
    endif
  endif
endif

CXXFLAGS += -D${SYSTEM} -DMAJOR=${MAJOR}
ifneq (,${SOCKIO})
  CXXFLAGS += -DHAS_SYS_SOCKIO=1
endif

%.o : %.cc
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) $< -c -o $@

ifeq (0,$(strip ${MUSLLIBC}))
all: pegasus-keg
else
all:
endif

pegasus-keg: pegasus-keg.o $(EXTRA_OBJ)
	$(LD) $(LDFLAGS) $^ -o $@ $(LOADLIBES)

pegasus-keg.o: pegasus-keg.cc $(EXTRA_INC)
basic.o: basic.cc basic.hh
darwin.o: darwin.cc darwin.hh basic.hh
linux.o: linux.cc linux.hh basic.hh

version.h:
	$(CURDIR)/genversion.sh > $(CURDIR)/version.h

ifeq (0,$(strip ${MUSLLIBC}))
install: pegasus-keg
	$(INSTALL) -m 0755 pegasus-keg $(prefix)/bin
else
install:
endif

test check: pegasus-keg
	@./pegasus-keg -o /dev/fd/1 || echo "test failed" && echo "test ok"

clean:
	$(RM) pegasus-keg.o core core.* $(EXTRA_OBJ) $(EXTRA_COBJ) version.h

distclean: clean
	$(RM) pegasus-keg

# MPI version

MPICC	= mpicc
MPICCFLAGS = -DWITH_MPI
LD_MPI	= $(MPICC)
LD_MPIFLAGS = -lstdc++

install-mpi-keg: pegasus-mpi-keg
	$(INSTALL) -m 0755 pegasus-mpi-keg $(prefix)/bin

pegasus-mpi-keg: pegasus-mpi-keg.o $(EXTRA_OBJ)
	$(LD_MPI) $(LDFLAGS) $^ -o $@ $(LOADLIBES) $(LD_MPIFLAGS)

pegasus-mpi-keg.o : pegasus-keg.cc $(EXTRA_INC)
	$(MPICC) $(MPICCFLAGS) $(CPPFLAGS) $(CXXFLAGS) $< -c -o $@

test-mpi check-mpi: pegasus-mpi-keg
	@mpiexec -n 2 ./pegasus-mpi-keg -o /dev/fd/1 || echo "test failed" && echo "test ok"

clean-mpi:
	$(RM) pegasus-mpi-keg.o core core.* $(EXTRA_OBJ) $(EXTRA_COBJ)

distclean-mpi: clean-mpi
	$(RM) pegasus-mpi-keg

