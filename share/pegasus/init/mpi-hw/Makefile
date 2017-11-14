#
# Makefile 
#
INSTALL = install
BIN_DIR = ./bin
RM      = rm -f
CXX     = cc
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

install: pegasus-mpi-hw
	$(INSTALL) -D pegasus-mpi-hw ${BIN_DIR}

clean:
	$(RM) pegasus-mpi-hw.o core core.* 

distclean: clean
	$(RM) ${BIN_DIR}/pegasus-mpi-hw
