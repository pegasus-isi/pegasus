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

ifndef ${datadir}
datadir = $(prefix)/share 
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
CFLAGS	:= -Wall -O2 -ggdb
ifeq (ia64,${MARCH})
#
# Linux IA64
#
CFLAGS	+= -DMARCH_IA64=1
else
ifeq (x86_64,${MARCH})
#
# Linux AMD64
#
CFLAGS	+= -m64
else
ifeq (armv7l, ${MARCH})
# nothing to do
else
#
# Linux IA32
#
CFLAGS	+= -march=i686
# LDFLAGS	+= -static
endif
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
GENDOC = pegasus-cluster.html pegasus-cluster.txt pegasus-cluster.ps

%.html : %.1 ;
	$(NROFF) $(HTML) $< > $@
%.ps : %.1 ;
	$(NROFF) $< > $@
%.txt : %.1 ;
	$(NROFF) $(TEXT) $< > $@
%.o : %.c
	$(CC) $(CFLAGS) $< -c -o $@

all: pegasus-cluster $(GENDOC)

job.o: job.c job.h
mysystem.o: mysystem.c tools.h report.h mysystem.h
parser.o: parser.c parser.h tools.h
report.o: report.c tools.h report.h
pegasus-cluster.o: pegasus-cluster.c tools.h parser.h report.h mysystem.h \
  job.h statinfo.h
statinfo.o: statinfo.c statinfo.h
tools.o: tools.c tools.h
try-cpus.o: try-cpus.c

pegasus-cluster: pegasus-cluster.o tools.o parser.o report.o mysystem.o \
	job.o statinfo.o 
	$(LD) $(LDFLAGS) $^ -o $@ $(LOADLIBES)

try-cpus: try-cpus.o
	$(LD) $(LDFLAGS) $^ -o $@ $(LOADLIBES)

install: pegasus-cluster
	$(INSTALL) -m 0755 $^ $(prefix)/bin

install.man: pegasus-cluster.1
	$(INSTALL) -m 0644 $^ $(datadir)/man/man1

install.doc: $(GENDOC)
	$(INSTALL) -m 0644 $(GENDOC) $(datadir)/man

install.all: install install.man install.doc

doc: $(GENDOC)

check:	pegasus-cluster
	bash check.sh

clean:
	$(RM) *.o $(GENDOC) core core.* check.?

distclean: clean
	$(RM) pegasus-cluster try-cpus
