INSTALL = install
RM = rm -f
CC = gcc
CFLAGS = -Wall -O2 -ggdb
LD = $(CC)
LDLIBS = -lm
SYSTEM = $(shell uname -s | tr '[a-z]' '[A-Z]' | tr -d '_ -/')
TARGET = pegasus-kickstart

ifndef ${prefix}
    prefix = $(CURDIR)/../../../
endif

ifeq (DARWIN,${SYSTEM})
    SYSTEM_OBJ = machine/darwin.o
endif

ifeq (LINUX,${SYSTEM})
    CFLAGS += $(shell getconf LFS_CFLAGS 2>>/dev/null)
    LDFLAGS += $(shell getconf LFS_LDFLAGS 2>>/dev/null)
    SYSTEM_OBJ = machine/linux.o syscall.o
    TARGET += libinterpose.so
endif

CFLAGS += -D${SYSTEM}

.PHONY: install clean test

%.o : %.c
	$(CC) $(CFLAGS) $< -c -o $@

all: $(TARGET)

pegasus-kickstart: getif.o utils.o useinfo.o statinfo.o jobinfo.o \
  limitinfo.o machine.o machine/basic.o $(SYSTEM_OBJ) appinfo.o parse.o \
  mysystem.o mylist.o invoke.o pegasus-kickstart.o procinfo.o
	$(LD) $(LDFLAGS) $^ $(LDLIBS) -o $@

appinfo.o: appinfo.c getif.h utils.h useinfo.h machine.h \
  jobinfo.h statinfo.h appinfo.h limitinfo.h
getif.o: getif.c getif.h
invoke.o: invoke.c invoke.h
jobinfo.o: jobinfo.c getif.h utils.h useinfo.h jobinfo.h \
  statinfo.h parse.h procinfo.h
pegasus-kickstart.o: pegasus-kickstart.c appinfo.h \
  statinfo.h jobinfo.h limitinfo.h machine.h mysystem.h mylist.h \
  invoke.h utils.h version.h ptrace.h
limitinfo.o: limitinfo.c utils.h limitinfo.h
machine.o: machine.c machine.h machine/basic.h
mylist.o: mylist.c mylist.h
mysystem.o: mysystem.c utils.h appinfo.h statinfo.h jobinfo.h \
  limitinfo.h machine.h mysystem.h procinfo.h
parse.o: parse.c parse.h
statinfo.o: statinfo.c statinfo.h utils.h
utils.o: utils.c utils.h
useinfo.o: useinfo.c utils.h useinfo.h
machine/basic.o: machine/basic.c machine/basic.h utils.h
machine/linux.o: machine/linux.c machine/basic.h machine/linux.h utils.h
machine/darwin.o: machine/darwin.c machine/basic.h machine/darwin.h utils.h
procinfo.o: procinfo.c procinfo.h ptrace.h utils.h
syscall.o: syscall.c syscall.h syscall_32.h syscall_64.h ptrace.h procinfo.h

libinterpose.so: interpose.c
	$(CC) $(CFLAGS) -std=gnu99 -shared -fPIC -o libinterpose.so interpose.c -ldl

version.h:
	$(CURDIR)/../../../release-tools/getversion --header > $(CURDIR)/version.h

install: $(TARGET)
	$(INSTALL) -m 0755 pegasus-kickstart $(prefix)/bin
ifeq (LINUX,${SYSTEM})
	$(INSTALL) -m 0755 libinterpose.so $(prefix)/lib/pegasus
endif

clean:
	$(RM) *.o *.so machine/*.o core core.* version.h $(TARGET)

test: $(TARGET)
	cd $(CURDIR)/test && ./test.sh

