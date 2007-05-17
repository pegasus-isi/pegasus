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

ifeq (AIX,${SYSTEM})
CXX     = xlC_r
CC      = xlc_r
endif

ifeq (IRIX,${SYSTEM})
CC      = cc -n32 -mips3 -r4000
LD      = $(CC)
OPT_NORM = -O3 -IPA -LNO:opt=1
endif

ifeq (LINUX,${SYSTEM})
TESTLIB	= ./testlibc
ifeq (ia64,${MARCH})
CFLAGS	= -Wall -O2 -ggdb
else 
ifeq (x86_64,${MARCH})
CFLAGS  = -Wall -O2 -m64 -ggdb 
else
CFLAGS	= -Wall -O2 -march=i686 -ggdb
endif
endif
LOADLIBES := -Wl,-Bstatic -lstdc++ -Wl,-Bdynamic $(LOADLIBES)
#LDFLAGS += -static
endif

ifeq (DARWIN,${SYSTEM})
TESTLIB = /usr/bin/true
endif

#
# === [3] ======================================================= rules section
# There is no need to change things below this line.
CFLAGS += -D${SYSTEM} -DMAJOR=${MAJOR} -DMINOR=${MINOR} 
CFLAGS += -D_REENTRANT # -D_POSIX_C_SOURCE=199506 -D__USE_XOPEN_EXTENDED
GENDOC = T2.html T2.txt T2.ps

%.html : %.1 ;
	$(NROFF) $(HTML) $< > $@
%.ps : %.1 ;
	$(NROFF) $< > $@
%.txt : %.1 ;
	$(NROFF) $(TEXT) $< > $@
%.o : %.c
	$(CC) $(CFLAGS) $< -c -o $@

all : $(TESTLIB) T2 $(GENDOC)

dll.o: dll.c dll.h
item.o: item.c item.h xfer.h dll.h util.h
mypopen.o: mypopen.c mypopen.h
T2.o: T2.c error.h item.h xfer.h dll.h workq.h util.h mypopen.h
testlibc.o: testlibc.c
util.o: util.c util.h mypopen.h
workq.o: workq.c error.h item.h xfer.h dll.h util.h workq.h
xfer.o: xfer.c xfer.h dll.h

OBJS	= dll.o xfer.o item.o workq.o mypopen.o util.o T2.o

testlibc: testlibc.o
	$(LD) $(LDFLAGS) $(CFLAGS) $^ -o $@ $(LOADLIBES)
T2: $(TESTLIB) $(OBJS)
	$(TESTLIB)
	$(LD) $(LDFLAGS) $(CFLAGS) $(OBJS) -o $@ $(LOADLIBES)

$(GENDOC): T2.1

install: T2
	$(INSTALL) -m 0755 T2 $(prefix)/bin

install.doc: $(GENDOC)
	$(INSTALL) -m 0644 $(GENDOC) $(prefix)/man

install.man: T2.1
	$(INSTALL) -m 0644 T2.1 $(prefix)/man/man1

install.all: install install.man install.doc

clean:
	$(RM) *.o $(GENDOC) core

distclean: clean
	$(RM) T2 testlibc
