#
# Makefile 
#
INSTALL	= install	# use /usr/ucb/install on Solaris
STRIP	= strip
CC	= gcc
LD	= gcc
CXX	= g++
RM	= rm -f
SYSTEM  = $(shell uname -s | tr '[a-z]' '[A-Z]' | tr -d '_ -/')
VERSION = $(shell uname -r)
MARCH	= $(shell uname -m | tr '[A-Z]' '[a-z]')
TESTLIB = /bin/true	# overwritten for Linux
MAJOR   = $(firstword $(subst ., ,$(VERSION)))
MINOR	= $(strip $(word 2,$(subst ., ,$(VERSION))))
CFLAGS	+= -O
LOADLIBES += -lm -lpthread

ifndef ${prefix}
prefix = $(PEGASUS_HOME)
endif

NROFF   = groff -mandoc
TEXT    = -Tlatin1
HTML    = -Thtml

ifeq (SUNOS,${SYSTEM})
ifeq (5,${MAJOR})
# use these for the SUN CC compiler
CC	= cc -mt
LD	= $(CC)
## SPARCv7
V7FLAGS	= -xtarget=generic
V9FLAGS	= -xtarget=ultra -xarch=v9
CFLAGS  = -dalign -ftrap=%none -fsimple -xlibmil $(EXTRACFLAGS)
#EXTRACFLAGS = $(V7FLAGS)
CFLAGS := -DSOLARIS $(CFLAGS) -xO4 -D__EXTENSIONS__=1
LOADLIBES += -lnsl -lsocket -lthread
INSTALL = /usr/ucb/install
else
# old Solaris 1 not supported!
endif
# on Solaris use this link string for gcc:
# gcc -Wl,-Bstatic xx.o -lstdc++ -lm -lnsl -lsocket -Wl,-Bdynamic -ldl -o xx
endif

ifeq (IRIX64,${SYSTEM})
# The regular 64bit Irix stuff is just too slow, use n32!
SYSTEM        := IRIX
endif

ifeq (IRIX,${SYSTEM})
CC      = cc -n32 -mips3 -r4000
LD      = $(CC)
OPT_NORM = -O3 -IPA -LNO:opt=1
endif

ifeq (AIX,${SYSTEM})
CXX     = xlC_r
CC      = xlc_r
endif

ifeq (LINUX,${SYSTEM})
TESTLIB	= ./testlibc
ifeq (ia64,${MARCH})
CFLAGS	= -Wall -O2 -ggdb
else
ifeq (x86_64,${MARCH})
CFLAGS	= -Wall -m64 -O2 -ggdb
else
CFLAGS	= -Wall -march=i686 -O2 -ggdb # -DUSE_DEBUG=1
endif
endif
#LDFLAGS += -static
#LDFLAGS = -lefence
endif

ifeq (DARWIN,${SYSTEM})
TESTLIB	= /usr/bin/true
endif

#
# === [3] ======================================================= rules section
# There is no need to change things below this line.
CFLAGS += -D${SYSTEM} -DMAJOR=${MAJOR} -DMINOR=${MINOR}
CFLAGS += -D_REENTRANT # -D_POSIX_C_SOURCE=199506 -D__USE_XOPEN_EXTENDED
GENDOC = transfer.html transfer.txt transfer.ps

%.html : %.1 ;
	$(NROFF) $(HTML) $< > $@
%.ps : %.1 ;
	$(NROFF) $< > $@
%.txt : %.1 ;
	$(NROFF) $(TEXT) $< > $@
%.o : %.c
	$(CC) $(CFLAGS) $< -c -o $@

all : $(TESTLIB) transfer # $(GENDOC)
doc : $(GENDOC)

batch.o: batch.c batch.h
capabilities.o: capabilities.c capabilities.h mypopen.h
debug.o: debug.c debug.h
item.o: item.c item.h util.h
mypopen.o: mypopen.c mypopen.h
testlibc.o: testlibc.c
transfer.o: transfer.c capabilities.h batch.h item.h workq.h util.h mypopen.h
try-caps.o: try-caps.c mypopen.h capabilities.h
util.o: util.c util.h mypopen.h
workq.o: workq.c item.h util.h workq.h debug.h

OBJS	= debug.o item.o batch.o workq.o mypopen.o capabilities.o \
	  util.o transfer.o

testlibc: testlibc.o
	$(LD) $(LDFLAGS) $(CFLAGS) $^ -o $@ $(LOADLIBES)
transfer: $(TESTLIB) $(OBJS)
	$(TESTLIB)
	$(LD) $(LDFLAGS) $(CFLAGS) $(OBJS) -o $@ $(LOADLIBES)
try-caps: try-caps.o capabilities.o mypopen.o
	$(LD) $(LDFLAGS) $(CFLAGS) $^ -o $@ $(LOADLIBES)

$(GENDOC): transfer.1

install: transfer
	$(INSTALL) -m 0755 transfer $(prefix)/bin

install.man: transfer.1
	$(INSTALL) -m 0644 transfer.1 $(prefix)/man/man1

install.doc: $(GENDOC)
	$(INSTALL) -m 0644 $(GENDOC) $(prefix)/man

install.all: install install.man install.doc

clean:
	$(RM) *.o $(GENDOC) core

distclean: clean
	$(RM) transfer testlibc try-caps
