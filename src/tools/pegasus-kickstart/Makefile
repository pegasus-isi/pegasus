INSTALL = install
RM = rm -f
CC = gcc
CFLAGS = -O
LD = $(CC)
LOADLIBES = -lm
SYSTEM = $(shell uname -s | tr '[a-z]' '[A-Z]' | tr -d '_ -/')
VERSION = $(shell uname -r)
MARCH = $(shell uname -m | tr '[A-Z]' '[a-z]')
MAJOR = $(firstword $(subst ., ,$(VERSION)))
MINOR = $(strip $(word 2,$(subst ., ,$(VERSION))))
EXTRA_OBJ = machine/basic.o
SOCKIO = $(shell /bin/ls /usr/include/sys/sockio.h 2>/dev/null)
LFS_CFLAGS = -D_FILE_OFFSET_BITS=64 -D_LARGEFILE_SOURCE 
LFS_LDFLAGS =

ifndef ${prefix}
prefix = $(PEGASUS_HOME)
endif

ifndef ${datadir}
datadir = $(prefix)/share
endif

ifeq (DARWIN,${SYSTEM})
CFLAGS += -Wall -DMUST_USE_SELECT_NOT_POLL=1 -DHAS_REALPATH_EXT=1 -ggdb
LOADLIBES += -ggdb
EXTRA_OBJ += machine/darwin.o
endif

ifeq (LINUX,${SYSTEM})
ifeq (ia64,${MARCH})
# old Intel-only IA64 architecture
CFLAGS = -Wall -O2 -ggdb
else
ifeq (x86_64,${MARCH})
# new Intel/AMD 64bit architecture
CFLAGS = -Wall -O2 -ggdb -m64
else
ifeq (i686,${MARCH})
# regular 32bit x86 architecture
CFLAGS = -Wall -O2 -march=i686 -ggdb
else
# UNKNOWN ARCHITECTURE -- MAKE NO ASSUMPTIONS
CFLAGS = -Wall -O2 -ggdb
endif
endif
endif
LFS_CFLAGS = $(shell getconf LFS_CFLAGS 2>>/dev/null)
LFS_LDFLAGS = $(shell getconf LFS_LDFLAGS 2>>/dev/null)
EXTRA_OBJ += machine/linux.o
CFLAGS += -DHAS_REALPATH_EXT=1
endif

CFLAGS += -D${SYSTEM} -DMAJOR=${MAJOR} -DWITH_NEW_ARGS=1
ifneq (,${SOCKIO})
CFLAGS += -DHAS_SYS_SOCKIO=1
endif
CFLAGS += -DUSE_PARSE=1

# add large file support
ifneq (,${LFS_CFLAGS})
NOLFS_CFLAGS := $(CFLAGS)
CFLAGS += $(LFS_CFLAGS)
endif
ifneq (,${LFS_LDFLAGS})
LDFLAGS += $(LFS_LDFLAGS)
endif

.PHONY: test

%.o : %.c
	$(CC) $(CFLAGS) $< -c -o $@

all : pegasus-kickstart

pegasus-kickstart: debug.o getif.o rwio.o tools.o useinfo.o mynss.o statinfo.o jobinfo.o limitinfo.o $(EXTRA_OBJ) machine.o appinfo.o parse.o mysystem.o mylist.o invoke.o pegasus-kickstart.o procinfo.o syscall.o
	$(LD) $(EXTRA_LDFLAGS) $(CFLAGS) $^ -o $@ $(LOADLIBES)
show-if: show-if.o getif.o debug.o rwio.o
	$(LD) $(EXTRA_LDFLAGS) $(CFLAGS) $^ -o $@ $(LOADLIBES)
try-parse: try-parse.o parse.o debug.o rwio.o
	$(LD) $(EXTRA_LDFLAGS) $(CFLAGS) $^ -o $@ $(LOADLIBES)

appinfo.o: appinfo.c getif.h rwio.h debug.h tools.h useinfo.h machine.h \
  jobinfo.h statinfo.h appinfo.h limitinfo.h mynss.h
debug.o: debug.c debug.h rwio.h
fail.o: fail.c
getif.o: getif.c debug.h getif.h rwio.h
invoke.o: invoke.c invoke.h
jobinfo.o: jobinfo.c getif.h debug.h tools.h useinfo.h jobinfo.h \
  statinfo.h parse.h procinfo.h
pegasus-kickstart.o: pegasus-kickstart.c rwio.h debug.h appinfo.h \
  statinfo.h jobinfo.h limitinfo.h machine.h mysystem.h mylist.h \
  invoke.h tools.h version.h ptrace.h
limitinfo.o: limitinfo.c debug.h tools.h limitinfo.h
machine.o: machine.c machine.h machine/basic.h debug.h
mylist.o: mylist.c mylist.h
mynss.o: mynss.c mynss.h
mysystem.o: mysystem.c debug.h tools.h appinfo.h statinfo.h jobinfo.h \
  limitinfo.h machine.h mysystem.h mysignal.h procinfo.h
parse.o: parse.c debug.h parse.h
procinfo.o: procinfo.c procinfo.h ptrace.h tools.h
rwio.o: rwio.c rwio.h
show-if.o: show-if.c getif.h
statinfo.o: statinfo.c mynss.h debug.h statinfo.h tools.h
test-ascii.o: test-ascii.c
test-post.o: test-post.c
test-pre.o: test-pre.c
tools.o: tools.c tools.h
try-parse.o: try-parse.c parse.h rwio.h
useinfo.o: useinfo.c tools.h useinfo.h
zio.o: zio.c zio.h

machine/basic.o: machine/basic.c machine/basic.h machine/../tools.h
machine/linux.o: machine/linux.c machine/basic.c machine/linux.h \
  machine/../tools.h
machine/darwin.o: machine/darwin.c machine/basic.c machine/darwin.h \
  machine/../tools.h

version.h:
	$(CURDIR)/../../../release-tools/getversion --header > $(CURDIR)/version.h

install: pegasus-kickstart
	$(INSTALL) -m 0755 pegasus-kickstart $(prefix)/bin

clean:
	$(RM) *.o machine/*.o core core.* version.h

distclean: clean
	$(RM) pegasus-kickstart fail test-post test-pre test-ascii show-if

test: pegasus-kickstart
	cd $(CURDIR)/test && ./test.sh
