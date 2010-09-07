#
# Makefile
# $Id$
#
INSTALL	= install
RM	= rm -f
CC	= gcc
CXX	= g++
SYSTEM  = $(shell uname -s | tr '[a-z]' '[A-Z]' | tr -d '_ -/')
VERSION = $(shell uname -r)
MARCH	= $(shell uname -m | tr '[A-Z]' '[a-z]')
MAJOR   = $(firstword $(subst ., ,$(VERSION)))
LOADLIBES = -lm

ifndef ${prefix}
prefix	= $(PEGASUS_HOME)
endif

NROFF   = groff -mandoc
TEXT    = -Tutf8
HTML    = -Thtml

ifeq (SUNOS,${SYSTEM})
ifeq (5,${MAJOR})
CC	= cc
CXX	= CC
V7FLAGS	= -xtarget=generic
V9FLAGS	= -xtarget=ultra -xarch=v9
CFLAGS	= -dalign -ftrap=%none -fsimple -xlibmil $(EXTRACFLAGS)
CFLAGS := -DSOLARIS $(CFLAGS) -xO4 -D__EXTENSIONS__=1
LDFLAGS	+= $(EXTRACFLAGS)
INSTALL	= /usr/ucb/install
LOADLIBES += -lnsl
else
# old Solaris 1 not supported
endif
endif

ifeq (AIX,${SYSTEM})
CXX	= xlC
CC	= xlc
endif

ifeq (LINUX,${SYSTEM})
ifeq (ia64,${MARCH})
#
# Linux IA64
#
CFLAGS	= -Wall -O2 -DMARCH_IA64=1 -ggdb
# SDSC teragrid
MPICC_PBS     =  /usr/local/apps/mpich-vmi-intel/bin/mpicc
# UC teragrid
#MPICC_PBS     =  /soft/mpich-gm-1.2.5..10-intel-r2/bin/mpicc
# CALTECH teragrid
#MPICC_PBS     =  /usr/local/mpich/mpich-gm-1.2.5..10-intel-r2/bin/mpicc
# NCSA teragrid
#MPICC_PBS     =  /usr/local/mpich/mpich-gm-1.2.5..10-intel-r2/bin/mpicc
else
ifeq (x86_64,${MARCH})
#
# Linux AMD64
#
CFLAGS	= -Wall -O2 -m64 -ggdb
MPICC_PBS = /sys/software/mpich/bin/mpicc
else
#
# Linux IA32
#
CFLAGS	= -Wall -O2 -march=i686 -ggdb
#LDFLAGS += -static
#ISI dc/condor
MPICC_LSF     =  /cluster/mpich/mpich-1.2.5.3/bin/mpicc
MPICC_CONDOR  =  /nfs/asd2/pegasus/software/linux/mpi/mpich-1.2.5-gt3.0.2/bin/mpicc
endif
endif
endif

#
# === [3] ======================================================= rules section
# There is no need to change things below this line.
CFLAGS	+= -D${SYSTEM} -DMAJOR=${MAJOR}
LD	= $(CC) -D_FILE_OFFSET_BITS=64
CC	+= -D_FILE_OFFSET_BITS=64
CFLAGS	+= -D_LARGEFILE_SOURCE -D_FILE_OFFSET_BITS=64 -D_LARGEFILE64_SOURCE
GENDOC = seqexec.html seqexec.txt seqexec.ps

ifeq (,${MPICC})
MPICC	= $(MPICC_CONDOR)
endif

%.html : %.1 ;
	$(NROFF) $(HTML) $< > $@
%.ps : %.1 ;
	$(NROFF) $< > $@
%.txt : %.1 ;
	$(NROFF) $(TEXT) $< > $@
%.o : %.c
	$(CC) $(CFLAGS) $< -c -o $@

all: seqexec $(GENDOC)

#mpiexec.o: mpiexec.c mypopen.h mympi.h
#mympi.o: mympi.c mympi.h
job.o: job.c job.h
mypopen.o: mypopen.c mypopen.h
mysystem.o: mysystem.c tools.h report.h mysystem.h
parser.o: parser.c parser.h
report.o: report.c tools.h report.h
tools.o: tools.c tools.h
seqexec.o: seqexec.c tools.h parser.h report.h mysystem.h
newexec.o: newexec.c tools.h parser.h report.h mysystem.h job.h
try-cpus.o: try-cpus.c

seqexec: seqexec.o tools.o parser.o report.o mysystem.o 
	$(LD) $(LDFLAGS) $^ -o $@ $(LOADLIBES)

newexec: newexec.o tools.o parser.o report.o mysystem.o job.o 
	$(LD) $(LDFLAGS) $^ -o $@ $(LOADLIBES)

try-cpus: try-cpus.o
	$(LD) $(LDFLAGS) $^ -o $@ $(LOADLIBES)

mpiexec: mpiexec.c mypopen.c mympi.c
	$(MPICC) $(CFLAGS) -c -o mypopenMPI.o mypopen.c
	$(MPICC) $(CFLAGS) -c -o mympiMPI.o mympi.c
	$(MPICC) $(LDFLAGS) -o mpiexec mpiexec.c mypopenMPI.o mympiMPI.o $(LOADLIBES) -lpthread

mpiexec.condor: mpiexec.c mypopen.c
	$(MPICC_CONDOR) $(CFLAGS) -c -o mypopenMPI_condor.o mypopen.c ;\
	$(MPICC_CONDOR) $(CFLAGS) -c -o mympiMPI_condor.o mympi.c ;\
	$(MPICC_CONDOR) $(LDFLAGS) -o mpiexec.condor mpiexec.c mypopenMPI_condor.o mympiMPI_condor.o $(LOADLIBES) -lpthread

install: seqexec
	$(INSTALL) -m 0755 seqexec $(prefix)/bin

install.man: seqexec.1
	$(INSTALL) -m 0644 seqexec.1 $(prefix)/man/man1

install.doc: $(GENDOC)
	$(INSTALL) -m 0644 $(GENDOC) $(prefix)/man

install.all: install install.man install.doc

doc: $(GENDOC)

clean:
	$(RM) *.o $(GENDOC) core core.*

distclean: clean
	$(RM) seqexec newexec mpiexec try-cpus mpiexec.condor
