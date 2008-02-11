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
MARCH	= $(shell uname -m | tr '[A-Z]' '[a-z]')
MAJOR   = $(firstword $(subst ., ,$(VERSION)))
SOCKIO  = $(shell /bin/ls /usr/include/sys/sockio.h 2>/dev/null)
LFS_CFLAGS = -D_FILE_OFFSET_BITS=64 -D_LARGEFILE_SOURCE 
LFS_LDFLAGS = 

ifndef ${prefix}
prefix = $(PEGASUS_HOME)
endif

NROFF   = groff -mandoc
TEXT    = -Tlatin1
HTML    = -Thtml

ifeq (SUNOS,${SYSTEM})
ifeq (5,${MAJOR})
# use these for the SUN CC compiler
CC	= cc
LD	= $(CC)
## SPARCv7
LFS_CFLAGS = $(shell getconf LFS_CFLAGS 2>>/dev/null)
LFS_LDFLAGS = $(shell getconf LFS_LDFLAGS 2>>/dev/null)
V7FLAGS	= -xtarget=generic
V9FLAGS	= -xtarget=ultra -xarch=v9
CFLAGS  = -dalign -ftrap=%none -fsimple -xlibmil $(EXTRACFLAGS)
#EXTRACFLAGS = $(V7FLAGS)
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

ifeq (AIX,${SYSTEM})
CC	= xlc
CXX	= xlC
endif

ifeq (DARWIN,${SYSTEM})
#CFLAGS	+= -DDEBUG_WAIT=1 -DDEBUG_EVENTLOOP=1
CFLAGS	+= -DMUST_USE_SELECT_NOT_POLL=1
endif

ifeq (LINUX,${SYSTEM})
ifeq (ia64,${MARCH})
# old Intel-only IA64 architecture
CFLAGS	= -Wall -O2 -ggdb
else
ifeq (x86_64,${MARCH})
# new Intel/AMD 64bit architecture
CFLAGS	= -Wall -O2 -ggdb -m64
else
# regular 32bit x86 architecture
CFLAGS	= -Wall -O2 -march=i686 -ggdb
#EXTRA_LDFLAGS += -static
else
# UNKNOWN ARCHITECTURE -- MAKE NO ASSUMPTIONS
CFLAGS	= -Wall -O2 -ggdb
endif
endif
endif
#CFLAGS	+= -D__USE_POSIX=199309
#LOADLIBES += -Wl,-Bstatic -lefence -Wl,-Bdynamic 
LFS_CFLAGS = $(shell getconf LFS_CFLAGS 2>>/dev/null)
LFS_LDFLAGS = $(shell getconf LFS_LDFLAGS 2>>/dev/null)
endif

#
# === [3] ======================================================= rules section
# There is no need to change things below this line.
CFLAGS += -D${SYSTEM} -DMAJOR=${MAJOR} -DWITH_NEW_ARGS=1 # -DSOCKLEN=${SOCKLEN}
ifneq (,${SOCKIO})
CFLAGS += -DHAS_SYS_SOCKIO=1
endif
GENDOC = kickstart.html kickstart.txt kickstart.ps
CFLAGS += -DUSE_PARSE=1 # -ggdb

# add large file support
ifneq (,${LFS_CFLAGS})
CFLAGS += $(LFS_CFLAGS)
endif
ifneq (,${LFS_LDFLAGS})
LDFLAGS += $(LFS_LDFLAGS)
endif

%.html : %.1 ;
	$(NROFF) $(HTML) $< > $@
%.ps : %.1 ;
	$(NROFF) $< > $@
%.txt : %.1 ;
	$(NROFF) $(TEXT) $< > $@
%.o : %.c
	$(CC) $(CFLAGS) $< -c -o $@

all : kickstart $(GENDOC)

kickstart: getif.o rwio.o tools.o useinfo.o mynss.o statinfo.o event.o jobinfo.o limitinfo.o appinfo.o parse.o mysystem.o mylist.o invoke.o kickstart.o
	$(LD) $(EXTRA_LDFLAGS) $(CFLAGS) $^ -o $@ $(LOADLIBES)
show-if: show-if.o getif.o
	$(LD) $(EXTRA_LDFLAGS) $(CFLAGS) $^ -o $@ $(LOADLIBES)
try-parse: try-parse.o parse.o
	$(LD) $(EXTRA_LDFLAGS) $(CFLAGS) $^ -o $@ $(LOADLIBES)

appinfo.o: appinfo.c getif.h rwio.h tools.h useinfo.h jobinfo.h \
  statinfo.h appinfo.h mynss.h
bad1.o: bad1.c
event.o: event.c rwio.h tools.h event.h statinfo.h mysignal.h
fail.o: fail.c
getif.o: getif.c getif.h
invoke.o: invoke.c invoke.h
jobinfo.o: jobinfo.c getif.h tools.h useinfo.h jobinfo.h statinfo.h \
  parse.h
kickstart.o: kickstart.c appinfo.h statinfo.h jobinfo.h mysystem.h rwio.h \
  mylist.h invoke.h
limitinfo.o: limitinfo.c tools.h limitinfo.h
meminfo.o: meminfo.c meminfo.h
mylist.o: mylist.c mylist.h
mynss.o: mynss.c mynss.h
mysystem.o: mysystem.c tools.h appinfo.h statinfo.h jobinfo.h event.h \
  mysystem.h mysignal.h
parse.o: parse.c parse.h
rwio.o: rwio.c rwio.h
show-if.o: show-if.c getif.h
statinfo.o: statinfo.c mynss.h statinfo.h tools.h
test-post.o: test-post.c
test-pre.o: test-pre.c
test-ascii.o: test-ascii.c
try-parse.o: try-parse.c parse.h
tools.o: tools.c tools.h
useinfo.o: useinfo.c tools.h useinfo.h


$(GENDOC): kickstart.1

install: kickstart
	$(INSTALL) -m 0755 kickstart $(prefix)/bin

install.doc: $(GENDOC)
	$(INSTALL) -m 0644 $(GENDOC) $(prefix)/man

install.man: kickstart.1
	$(INSTALL) -m 0644 kickstart.1 $(prefix)/man/man1

install.all: install install.man install.doc

doc: $(GENDOC)

clean:
	$(RM) *.o $(GENDOC) core core.*

distclean: clean
	$(RM) kickstart fail test-post test-pre test-ascii show-if
