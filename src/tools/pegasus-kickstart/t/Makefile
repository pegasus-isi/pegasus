CC	= gcc
LD	= $(CC)
SYSTEM  = $(shell uname -s | tr '[a-z]' '[A-Z]' | tr -d '_ -/')
MARCH   = $(shell uname -m | tr '[A-Z]' '[a-z]')
LFS_CFLAGS = -D_FILE_OFFSET_BITS=64 -D_LARGEFILE_SOURCE
LFS_LDFLAGS =

ifeq (DARWIN,${SYSTEM})
SDK=$(lastword $(sort $(wildcard /Developer/SDKs/MacOSX10.*.sdk)))
ifneq (,${SDK})
CFLAGS	:= -g -pipe -arch x86_64 -arch i686 -nostdinc \
        -B${SDK}/usr/include/gcc -B${SDK}/usr/lib/gcc \
        -isystem${SDK}/usr/include -F${SDK}/System/Library/Frameworks
LDFLAGS	:= -arch x86_64 -arch i686 -Wl,-syslibroot,${SDK}
CFLAGS	+= -DMACHINE_SPECIFIC=darwin
endif
endif

ifeq (SUNOS,${SYSTEM})
CC	= cc
V7FLAGS	= -xtarget=generic
V9FLAGS	= -xtarget=ultra -xarch=v9
CFLAGS	:= -DSOLARIS '-library=%none,Cstd,Crun' -dalign -ftrap=%none -fsimple -xlibmil $(EXTRACFLAGS) -xO4 -D__EXTENSIONS__=1
LD	:= $(CC) $(EXTRACFLAGS)
LFS_CFLAGS = $(shell getconf LFS_CFLAGS 2>>/dev/null)
LFS_LDFLAGS = $(shell getconf LFS_LDFLAGS 2>>/dev/null)
CFLAGS	+= -DMACHINE_SPECIFIC=sunos
endif

ifeq (LINUX,${SYSTEM})
CFLAGS	= -Wall -ggdb -O
ifeq (ia64,${MARCH})
CFLAGS	+= -m64	# is this true?
endif
ifeq (x86_64,${MARCH})
CFLAGS	+= -m64
endif
ifeq (armv7l,${MARCH})
endif
ifeq (i386,$(MARCH,S))
CFLAGS	+= -m32
endif
ifeq (i686,$(MARCH,S))
CFLAGS	+= -m32
endif
LFS_CFLAGS = $(shell getconf LFS_CFLAGS 2>>/dev/null)
LFS_LDFLAGS = $(shell getconf LFS_LDFLAGS 2>>/dev/null)
CFLAGS	+= -DMACHINE_SPECIFIC=linux
endif

#
# no changes 
#
CFLAGS	+= -D${SYSTEM} $(LFS_CFLAGS) -DMARCH=${MARCH}
LDFLAGS	+= $(LFS_LDFLAGS)
OBJS	= alarmme.o dienow.o hello.o forkme.o \
	grandfather.o threadme.o threadach.o 
APPS	= $(OBJS:%.o=%)

%.o : %.c
	$(CC) $(CFLAGS) $< -c -o $@

%   : %.o
	$(LD) $(LDFLAGS) $< -o $@ $(LOADLIBES)

all:	$(APPS)

debug.o: debug.c debug.h

alarmme: alarmme.o
	$(LD) $(LDFLAGS) $< -o $@ $(LOADLIBES)

dienow: dienow.o
	$(LD) $(LDFLAGS) $< -o $@ $(LOADLIBES)

hello: hello.o
	$(LD) $(LDFLAGS) $< -o $@ $(LOADLIBES)

forkme: forkme.o
	$(LD) $(LDFLAGS) $< -o $@ $(LOADLIBES)

grandfather: grandfather.o
	$(LD) $(LDFLAGS) $< -o $@ $(LOADLIBES)

threadme: threadme.o
	$(LD) $(LDFLAGS) $^ -o $@ $(LOADLIBES) -lpthread

threadach: threadach.o
	$(LD) $(LDFLAGS) $^ -o $@ $(LOADLIBES) -lpthread

clean:
	$(RM) $(OBJS)

distclean: clean
	$(RM) $(APPS)
