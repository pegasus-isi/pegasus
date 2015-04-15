INSTALL = install
RM = rm -f
CC = gcc
CFLAGS = -Wall -O2 -ggdb -std=gnu99
LD = $(CC)
LDLIBS = -lm -lpthread -lcurl
SYSTEM = $(shell uname -s | tr '[a-z]' '[A-Z]' | tr -d '_ -/')
ARCH = $(shell uname -m)
TARGET = pegasus-kickstart

ifndef ${prefix}
    prefix = $(CURDIR)/../../../
endif

libdir = ${prefix}/$(shell $(CURDIR)/../../../release-tools/determine-lib-dir)/pegasus

ifeq (DARWIN,${SYSTEM})
    SYSTEM_OBJ = machine/darwin.o
endif

ifeq (LINUX,${SYSTEM})
ifeq (x86_64,${ARCH})
    TARGET += libinterpose.so
endif
    CFLAGS += $(shell getconf LFS_CFLAGS 2>>/dev/null)
    LDFLAGS += $(shell getconf LFS_LDFLAGS 2>>/dev/null)
    SYSTEM_OBJ = machine/linux.o syscall.o
endif

CFLAGS += -D${SYSTEM}

.PHONY: install clean test

%.o : %.c
	$(CC) $(CPPFLAGS) $(CFLAGS) $< -c -o $@

all: $(TARGET)

pegasus-kickstart: getif.o utils.o useinfo.o statinfo.o jobinfo.o \
  limitinfo.o machine.o machine/basic.o $(SYSTEM_OBJ) appinfo.o parse.o \
  mysystem.o mylist.o invoke.o pegasus-kickstart.o procinfo.o monitoring.o
	$(LD) $(LDFLAGS) $^ $(LDLIBS) -o $@

appinfo.o: appinfo.c getif.h utils.h useinfo.h machine.h jobinfo.h \
	  statinfo.h procinfo.h ptrace.h appinfo.h limitinfo.h error.h
getif.o: getif.c getif.h utils.h error.h
invoke.o: invoke.c invoke.h error.h
jobinfo.o: jobinfo.c getif.h utils.h useinfo.h jobinfo.h statinfo.h \
  procinfo.h ptrace.h parse.h error.h
limitinfo.o: limitinfo.c utils.h limitinfo.h error.h
machine.o: machine.c machine.h machine/basic.h
mylist.o: mylist.c mylist.h
mysystem.o: mysystem.c utils.h appinfo.h statinfo.h jobinfo.h procinfo.h \
  ptrace.h limitinfo.h machine.h mysystem.h error.h monitoring.h
parse.o: parse.c parse.h utils.h error.h
pegasus-kickstart.o: pegasus-kickstart.c error.h appinfo.h statinfo.h \
  jobinfo.h procinfo.h ptrace.h limitinfo.h machine.h mysystem.h \
  mylist.h invoke.h utils.h version.h
procinfo.o: procinfo.c procinfo.h ptrace.h utils.h syscall.h error.h
statinfo.o: statinfo.c statinfo.h utils.h error.h
syscall.o: syscall.c syscall.h syscall_32.h syscall_64.h ptrace.h \
  procinfo.h error.h
useinfo.o: useinfo.c utils.h useinfo.h
utils.o: utils.c utils.h
machine/basic.o: machine/basic.c machine/basic.h utils.h error.h
machine/darwin.o: machine/darwin.c machine/basic.h machine/darwin.h \
  utils.h error.h
machine/linux.o: machine/linux.c machine/basic.h machine/linux.h \
  utils.h error.h

monitoring.o: monitoring.c monitoring.h

libinterpose.so: interpose.c
	$(CC) $(CFLAGS) -std=gnu99 -shared -fPIC -o libinterpose.so interpose.c -ldl -lpthread

version.h:
	$(CURDIR)/../../../release-tools/getversion --header > $(CURDIR)/version.h

install: $(TARGET)
	$(INSTALL) -m 0755 pegasus-kickstart $(prefix)/bin
ifeq (LINUX,${SYSTEM})
ifeq (x86_64,${ARCH})
	mkdir -p $(libdir)
	$(INSTALL) -m 0755 libinterpose.so $(libdir)
endif
endif

clean:
	$(RM) *.o *.so machine/*.o core core.* version.h

distclean:
	$(RM) $(TARGET)

test: $(TARGET)
	cd $(CURDIR)/test && ./test.sh
