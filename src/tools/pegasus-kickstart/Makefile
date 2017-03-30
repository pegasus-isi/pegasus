INSTALL = install
RM = rm -f
CC = gcc
CFLAGS = -Wall -O2 -ggdb -std=gnu99
LD = $(CC)
LDLIBS = -lm
SYSTEM = $(shell uname -s | tr '[a-z]' '[A-Z]' | tr -d '_ -/')
ARCH = $(shell uname -m)
TARGET = pegasus-kickstart

ifndef ${prefix}
    prefix = $(CURDIR)/../../../
endif

libdir = ${prefix}/$(shell $(CURDIR)/../../../release-tools/determine-lib-dir)/pegasus

OBJS =getif.o
OBJS+=utils.o
OBJS+=useinfo.o
OBJS+=statinfo.o
OBJS+=jobinfo.o
OBJS+=limitinfo.o
OBJS+=machine.o
OBJS+=machine/basic.o
OBJS+=appinfo.o
OBJS+=parse.o
OBJS+=mysystem.o
OBJS+=mylist.o
OBJS+=invoke.o
OBJS+=pegasus-kickstart.o
OBJS+=procinfo.o
OBJS+=checksum.o

ifeq (DARWIN,${SYSTEM})
    OBJS += machine/darwin.o
endif

ifeq (LINUX,${SYSTEM})
ifeq (x86_64,${ARCH})
    TARGET += libinterpose.so
    PAPI_SO=$(shell /sbin/ldconfig -p | grep libpapi.so)
    PAPI_H=$(shell ls /usr/include/papi.h)
    ifneq ($(PAPI_SO),)
        ifneq ($(PAPI_H),)
            CFLAGS += -DHAS_PAPI
            LI_LDFLAGS += -lpapi
        endif
    endif
endif
    CFLAGS += $(shell getconf LFS_CFLAGS 2>>/dev/null)
    LDFLAGS += $(shell getconf LFS_LDFLAGS 2>>/dev/null)
    OBJS += machine/linux.o syscall.o
endif

CFLAGS += -D${SYSTEM}

SRCS=$(OBJS:.o=.c)

.PHONY: install clean test

%.o : %.c
	$(CC) $(CPPFLAGS) $(CFLAGS) $< -c -o $@

all: $(TARGET)

pegasus-kickstart: $(OBJS)
	$(LD) $(LDFLAGS) $^ $(LDLIBS) -o $@

libinterpose.so: interpose.c
	$(CC) $(CFLAGS) -pthread -shared -fPIC -o libinterpose.so interpose.c -ldl $(LI_LDFLAGS)

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

depends.mk: $(SRCS) version.h
	$(CC) -MM $(SRCS) > $@

clean:
	$(RM) *.o *.so machine/*.o core core.* version.h depends.mk

distclean:
	$(RM) $(TARGET)

test: $(TARGET)
	cd $(CURDIR)/test && ./test.sh

-include depends.mk
