INSTALL = install
RM = rm -f
CC = gcc
CFLAGS = -Wall -O2 -ggdb -std=gnu99
LD = $(CC)
LDLIBS = -lm
LI_LDFLAGS = -ldl -lcurl -pthread
SYSTEM = $(shell uname -s | tr '[a-z]' '[A-Z]' | tr -d '_ -/')
ARCH = $(shell uname -m)
MUSLLIBC = $(shell gcc -dumpmachine | grep musl | wc -l)
TARGET = pegasus-kickstart-cpu

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
OBJS+=procinfo.o
OBJS+=sha2.o
OBJS+=checksum.o
OBJS+=log.o

ifeq (DARWIN,${SYSTEM})
    OBJS += machine/darwin.o
endif

ifeq (LINUX,${SYSTEM})
ifeq (0,${MUSLLIBC})
ifeq (x86_64,${ARCH})
    TARGET += libinterpose.so
    # TODO Replace this with pkg-config, which is supported in PAPI 5.4.1
    PAPI_SO=$(shell /sbin/ldconfig -p | grep libpapi.so)
    PAPI_H=$(shell ls /usr/include/papi.h)
    ifneq ($(PAPI_SO),)
        ifneq ($(PAPI_H),)
            CFLAGS += -DHAS_PAPI
            LI_LDFLAGS += -lpapi
        endif
    endif
endif
endif
    TARGET += pegasus-monitor-cpu
    LDLIBS += -lcurl -pthread
    CFLAGS += $(shell getconf LFS_CFLAGS 2>>/dev/null)
    LDFLAGS += $(shell getconf LFS_LDFLAGS 2>>/dev/null)
    OBJS += machine/linux.o
    OBJS += syscall.o
    OBJS += procfs.o
    OBJS += monitoring_helpers.o
    OBJS += monitoring.o
    NVIDIA_DRIVER_BRANCH = $(shell nvidia-smi | grep Driver | cut -f 3 -d' ' | cut -f 1 -d '.')
ifneq ($(NVIDIA_DRIVER_BRANCH),)
		    TARGET += pegasus-monitor-cuda
		    TARGET += pegasus-kickstart-cuda
				CUDA_CFLAGS += -DHAS_CUDA
        CUDA_OBJS += nvidia/nvml_wrapper.o
        CUDA_OBJS += nvidia_monitoring.o
        CUDA_LDLIBS += -lnvidia-ml -L /usr/lib/nvidia-440
endif
endif


CFLAGS += -D${SYSTEM}

SRCS=$(OBJS:.o=.c)

.PHONY: install clean test

%.o : %.c
	$(CC) $(CFLAGS) $< -c -o $@

%-cuda.o : %.c
	$(CC) $(CUDA_CFLAGS) $(CFLAGS) $< -c -o $@

all: $(TARGET)

pegasus-kickstart-cpu: pegasus-kickstart.o $(OBJS)
	$(LD) $(LDFLAGS) $^ $(LDLIBS) -o $@

pegasus-kickstart-cuda: pegasus-kickstart-cuda.o $(OBJS) $(CUDA_OBJS)
	$(LD) $(LDFLAGS) $^ $(LDLIBS) $(CUDA_LDLIBS) -o $@

pegasus-monitor-cpu: pegasus-monitor.o monitoring_helpers.o monitoring.o procfs.o log.o
	$(LD) $(LDFLAGS) $^ $(LDLIBS) -o $@

pegasus-monitor-cuda: pegasus-monitor-cuda.o monitoring_helpers.o monitoring.o nvidia_monitoring.o nvidia/nvml_wrapper.o procfs.o log.o
	$(LD) $(LDFLAGS) $^ $(LDLIBS) $(CUDA_LDLIBS) -o $@

libinterpose.so: interpose.c interpose_monitoring.c procfs.c log.c monitoring.c
	$(CC) $(CFLAGS) -shared -o $@ $^ -fPIC -fvisibility=hidden $(LI_LDFLAGS)

version.h:
	$(CURDIR)/../../../release-tools/getversion --header > $(CURDIR)/version.h

install: $(TARGET)
	$(INSTALL) -m 0755 pegasus-kickstart-cpu $(prefix)/bin
ifeq (LINUX,${SYSTEM})
	$(INSTALL) -m 0755 pegasus-monitor-cpu $(prefix)/bin
ifneq ($(NVIDIA_DRIVER_BRANCH),)
	  $(INSTALL) -m 0755 pegasus-kickstart-cuda $(prefix)/bin
	  $(INSTALL) -m 0755 pegasus-monitor-cuda $(prefix)/bin
endif
ifeq (0,${MUSLLIBC})
ifeq (x86_64,${ARCH})
	mkdir -p $(libdir)
	$(INSTALL) -m 0755 libinterpose.so $(libdir)
endif
endif
endif

depends.mk: $(SRCS) version.h
	$(CC) -MM $(SRCS) > $@

clean:
	$(RM) *.o *.so machine/*.o nvidia/*.o core core.* version.h depends.mk

distclean: clean
	$(RM) $(TARGET)

test: $(TARGET)
	cd $(CURDIR)/test && ./test.sh

-include depends.mk
