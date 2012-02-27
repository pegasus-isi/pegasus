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
MINOR   = $(strip $(word 2,$(subst ., ,$(VERSION))))
EXTRA_OBJ = machine/basic.o 
SOCKIO  = $(shell /bin/ls /usr/include/sys/sockio.h 2>/dev/null)
LFS_CFLAGS = -D_FILE_OFFSET_BITS=64 -D_LARGEFILE_SOURCE 
LFS_LDFLAGS = 

ifndef ${prefix}
prefix = $(PEGASUS_HOME)
endif

ifndef ${datadir}
datadir = $(prefix)/share 
endif

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
LOADLIBES += -lkstat -lnsl -lsocket
INSTALL = /usr/ucb/install
EXTRA_OBJ += machine/sunos-swap.o machine/sunos.o 
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
CFLAGS	+= -DMUST_USE_SELECT_NOT_POLL=1 -DHAS_REALPATH_EXT=1 -ggdb
LOADLIBES += -ggdb
EXTRA_OBJ += machine/darwin.o
endif

ifeq (CYGWINNT,$(findstring CYGWINNT,${SYSTEM}))
CFLAGS	+= -DSYS_NMLN=20 -DCYGWIN=1
CFLAGS	+= -DDEBUG_EVENTLOOP=1 -DDEBUG_WAIT=1
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
ifeq (i686,${MARCH})
# regular 32bit x86 architecture
CFLAGS	= -Wall -O2 -march=i686 -ggdb
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
EXTRA_OBJ += machine/linux.o 
CFLAGS += -DHAS_REALPATH_EXT=1
endif

#
# === [3] ======================================================= rules section
# There is no need to change things below this line.
CFLAGS += -D${SYSTEM} -DMAJOR=${MAJOR} -DWITH_NEW_ARGS=1 # -DSOCKLEN=${SOCKLEN}
ifneq (,${SOCKIO})
CFLAGS += -DHAS_SYS_SOCKIO=1
endif
CFLAGS += -DUSE_PARSE=1 # -ggdb

# add large file support
ifneq (,${LFS_CFLAGS})
NOLFS_CFLAGS := $(CFLAGS)
CFLAGS += $(LFS_CFLAGS)
endif
ifneq (,${LFS_LDFLAGS})
LDFLAGS += $(LFS_LDFLAGS)
endif

%.o : %.c
	$(CC) $(CFLAGS) $< -c -o $@

all : pegasus-kickstart

pegasus-kickstart: debug.o getif.o rwio.o tools.o useinfo.o mynss.o statinfo.o event.o jobinfo.o limitinfo.o $(EXTRA_OBJ) machine.o appinfo.o parse.o mysystem.o mylist.o invoke.o pegasus-kickstart.o
	$(LD) $(EXTRA_LDFLAGS) $(CFLAGS) $^ -o $@ $(LOADLIBES)
show-if: show-if.o getif.o debug.o
	$(LD) $(EXTRA_LDFLAGS) $(CFLAGS) $^ -o $@ $(LOADLIBES)
try-parse: try-parse.o parse.o
	$(LD) $(EXTRA_LDFLAGS) $(CFLAGS) $^ -o $@ $(LOADLIBES)

appinfo.o: appinfo.c getif.h rwio.h debug.h tools.h useinfo.h machine.h \
  jobinfo.h statinfo.h appinfo.h limitinfo.h mynss.h
debug.o: debug.c debug.h rwio.h
event.o: event.c rwio.h debug.h tools.h event.h statinfo.h mysignal.h
fail.o: fail.c
getif.o: getif.c debug.h getif.h
invoke.o: invoke.c invoke.h
jobinfo.o: jobinfo.c getif.h debug.h tools.h useinfo.h jobinfo.h \
  statinfo.h parse.h
pegasus-kickstart.o: pegasus-kickstart.c rwio.h debug.h appinfo.h \
  statinfo.h jobinfo.h limitinfo.h machine.h mysystem.h mylist.h \
  invoke.h tools.h
limitinfo.o: limitinfo.c debug.h tools.h limitinfo.h
machine.o: machine.c machine.h machine/basic.h debug.h
meminfo.o: meminfo.c meminfo.h debug.h
msave.o: msave.c machine.h debug.h tools.h
mylist.o: mylist.c mylist.h
mynss.o: mynss.c mynss.h
mysystem.o: mysystem.c debug.h tools.h appinfo.h statinfo.h jobinfo.h \
  limitinfo.h machine.h event.h mysystem.h mysignal.h
parse.o: parse.c debug.h parse.h
rwio.o: rwio.c rwio.h
show-if.o: show-if.c getif.h
statinfo.o: statinfo.c mynss.h debug.h statinfo.h tools.h
test-ascii.o: test-ascii.c
test-post.o: test-post.c
test-pre.o: test-pre.c
tools.o: tools.c tools.h
try-parse.o: try-parse.c parse.h
useinfo.o: useinfo.c tools.h useinfo.h
zio.o: zio.c zio.h

machine/basic.o: machine/basic.c machine/basic.h machine/../tools.h
machine/sunos.o: machine/sunos.c machine/basic.c machine/sunos.h \
  machine/sunos-swap.h machine/../tools.h
machine/sunos-swap.o: machine/sunos-swap.c machine/sunos-swap.h
	$(CC) $(NOLFS_CFLAGS) $< -c -o $@
machine/linux.o: machine/linux.c machine/basic.c machine/linux.h \
  machine/../tools.h
machine/darwin.o: machine/darwin.c machine/basic.c machine/darwin.h \
  machine/../tools.h

install: pegasus-kickstart
	$(INSTALL) -m 0755 pegasus-kickstart $(prefix)/bin

clean:
	$(RM) *.o machine/*.o core core.*

distclean: clean
	$(RM) pegasus-kickstart fail test-post test-pre test-ascii show-if
