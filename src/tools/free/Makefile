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
MAJOR   = $(firstword $(subst ., ,$(VERSION)))
CFLAGS	+= -O
LOADLIBES += -lm

ifndef ${prefix}
prefix = $(PEGASUS_HOME)
endif

ifeq (SUNOS,${SYSTEM})
ifeq (5,${MAJOR})
# use these for the SUN CC compiler
CC	= cc
LD	= $(CC)
## SPARCv7
CFLAGS = -dalign -ftrap=%none -fsimple -xlibmil -xtarget=generic
## SPARCv9
#CFLAGS  = -dalign -ftrap=%none -fsimple -xlibmil -xtarget=ultra -xarch=v9
CFLAGS := -DSOLARIS $(CFLAGS) -xO4 -D__EXTENSIONS__=1
LOADLIBES += -lnsl -lsocket
INSTALL = /usr/ucb/install
PRODUCT	= sol-free
SOURCE	= $(PRODUCT)
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

ifeq (IRIX,${SYSTEM})
CC      = cc -n32 -mips3 -r4000
LD      = $(CC)
OPT_NORM = -O3 -IPA -LNO:opt=1
endif

ifeq (LINUX,${SYSTEM})
## determine highest version of all installed libc's.
#LIBCVER = $(shell /bin/ls /lib/libc.so.? | \
#        awk -F'.' '{ if (m<$$3) m=$$3;} END { print m} ')
ifeq (ia64,${MARCH})
CFLAGS	= -Wall -O2 -ggdb
PRODUCT	= lnx-ia64-free
SOURCE	= lnx-free
else
CFLAGS	= -Wall -O2 -march=i686 -ggdb
#LDFLAGS += -static
PRODUCT	= lnx-free
SOURCE	= $(PRODUCT)
endif
endif

ifeq (AIX,${SYSTEM})
CC	= xlc
PRODUCT	= aix-free
SOURCE	= $(PRODUCT)
endif


#
# === [3] ======================================================= rules section
# There is no need to change things below this line.
CFLAGS += -D${SYSTEM} -DMAJOR=${MAJOR} # -DSOCKLEN=${SOCKLEN}

%.o : %.c
	$(CC) $(CFLAGS) $< -c -o $@

all : $(PRODUCT)

$(PRODUCT): $(SOURCE).o util.o
	$(LD) $(LDFLAGS) $(CFLAGS) $^ -o $@ $(LOADLIBES)

$(SOURCE).o: $(SOURCE).c
util.o: util.c util.h

install: $(PRODUCT)
	$(INSTALL) -m 0755 $(PRODUCT) $(prefix)/bin

install.man:
	@echo "no manpage to install"

install.doc:
	@echo "no documentation to install"

install.all: install # install.man install.doc

clean:
	$(RM) $(PRODUCT).o util.o

distclean: clean
	$(RM) $(PRODUCT)
