#
# Makefile 
#
INSTALL	= install
RM	= rm -f
CC	= gcc
CFLAGS	= -O
LD	= $(CC)
LOADLIBES = -lm
SYSTEM  = $(shell uname -s | tr '[a-z]' '[A-Z]' | tr -d '_ -/')
VERSION = $(shell uname -r)
MAJOR   = $(firstword $(subst ., ,$(VERSION)))

NROFF   = groff -mandoc
TEXT    = -Tlatin1
HTML    = -Thtml

ifeq (SUNOS,${SYSTEM})
ifeq (5,${MAJOR})
# use these for the SUN CC compiler
CC	= cc
LD	= $(CC)
## SPARCv7
#CFLAGS	= -dalign -ftrap=%none -fsimple -xlibmil -xtarget=generic
## SPARCv9
CFLAGS	= -dalign -ftrap=%none -fsimple -xlibmil -xtarget=ultra -xarch=v9
CFLAGS := -DSOLARIS $(CFLAGS) -xO4 -D__EXTENSIONS__=1
LOADLIBES += -lnsl -lsocket
INSTALL = /usr/ucb/install
else
# old Solaris 1 not supported!
endif
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

ifeq (LINUX,${SYSTEM})
## determine highest version of all installed libc's.
#LIBCVER = $(shell /bin/ls /lib/libc.so.? | \
#        awk -F'.' '{ if (m<$$3) m=$$3;} END { print m} ')
CFLAGS	= -Wall -O2 -march=pentium
EXTRA_LDFLAGS += -static
endif

#
# === [3] ======================================================= rules section
# There is no need to change things below this line.
CFLAGS += -D${SYSTEM} -DMAJOR=${MAJOR}
GENDOC = archstart.html archstart.txt archstart.ps
CFLAGS += # -ggdb

%.html : %.1 ;
	$(NROFF) $(HTML) $< > $@
%.ps : %.1 ;
	$(NROFF) $< > $@
%.txt : %.1 ;
	$(NROFF) $(TEXT) $< > $@
%.o : %.c
	$(CC) $(CFLAGS) $< -c -o $@

all : archstart $(GENDOC)

archstart: archstart.o
	$(LD) $(EXTRA_LDFLAGS) $(CFLAGS) $^ -o $@ $(LOADLIBES)

appinfo.o: appinfo.c

$(GENDOC): archstart.1

install:
	$(INSTALL) -s -m 0755 archstart $(PEGASUS_HOME)/bin
	$(INSTALL) -m 0644 $(GENDOC) $(PEGASUS_HOME)/man
	$(INSTALL) -m 0644 archstart.1 $(PEGASUS_HOME)/man/man1

doc: $(GENDOC)

clean:
	$(RM) *.o $(GENDOC) core

distclean: clean
	$(RM) archstart fail
