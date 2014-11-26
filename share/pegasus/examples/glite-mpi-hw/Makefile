#
# Makefile 
#
INSTALL = install
RM      = rm -f
CXX     = mpicc
CXXFLAGS += -O
LD      = $(CXX)

ifndef ${prefix}
  prefix = .
endif

ifndef ${datadir}
  datadir = $(prefix)/share 
endif


%.o : %.c
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) $< -c -o $@

all: pegasus-mpi-hw

pegasus-mpi-hw: pegasus-mpi-hw.o 
	$(LD) $(LDFLAGS) $^ -o $@ $(LOADLIBES)

pegasus-mpi.hw.o: pegasus-mpi-hw.c

test check: pegasus-mpi-hw
	@./pegasus-mpi-hw -o /dev/fd/1 || echo "test failed" && echo "test ok"

clean:
	$(RM) pegasus-mpi-hw.o core core.* 

distclean: clean
	$(RM) pegasus-mpi-hw