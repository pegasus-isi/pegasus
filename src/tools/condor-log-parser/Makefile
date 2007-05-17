#
# Makefile 
#
INSTALL = install
STRIP   = strip
RM      = rm -f
CXX	= g++ -ffor-scope 
CXXFLAGS = -O
LD      = $(CXX)
LOADLIBES = -lm
SYSTEM  = $(shell uname -s | tr '[a-z]' '[A-Z]' | tr -d '_ -/')
VERSION = $(shell uname -r)
MARCH	= $(shell uname -m | tr '[A-Z]' '[a-z]')
MAJOR   = $(firstword $(subst ., ,$(VERSION)))
CONDOR	= condor_compile
CONDOR_LOCATION = $(shell condor_config_val RELEASE_DIR)

ifndef ${prefix}
prefix = $(PEGASUS_HOME)
endif

NROFF   = groff -mandoc
TEXT    = -Tlatin1
HTML    = -Thtml

ifeq (SUNOS,${SYSTEM})
ifeq (5,${MAJOR})
# use these for the SUN CC compiler
# on Solaris use this link string with gcc 
# gcc -Wl,-Bstatic keg.o -lstdc++ -lm -lnsl -lsocket -Wl,-Bdynamic -ldl -o keg
CXX     = CC 
## SPARCv7
V7FLAGS	= -xtarget=generic
V9FLAGS	= -xtarget=ultra -xarch=v9
CXXFLAGS  = '-library=%none,Cstd,Crun' -dalign -ftrap=%none -fsimple -xlibmil $(EXTRACXXFLAGS)
#EXTRACXXFLAGS = $(V7FLAGS)
LD       := $(CXX) $(EXTRACXXFLAGS)
CXXFLAGS := -DSOLARIS $(CXXFLAGS) -xO4 -D__EXTENSIONS__=1
LOADLIBES = -lnsl -lsocket -lm -lc
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
CXX     = CC -n32 -mips3 -r4000
LD      = $(CXX)
OPT_NORM = -O3 -IPA -LNO:opt=1
endif

ifeq (AIX,${SYSTEM})
CXX	= xlC
CC	= xlc
endif

ifeq (LINUX,${SYSTEM})
ifeq (ia64,${MARCH})
CXXFLAGS = -Wall -O2 -DMARCH_IA64=1 -ggdb
else
ifeq (x86_64,${MARCH})
CXXFLAGS = -Wall -O2 -m64 -ggdb
else
CXXFLAGS  = -Wall -O2 -march=i686 -ggdb
#LDFLAGS += -static
LOADLIBES := -Wl,-Bstatic -lstdc++ -Wl,-Bdynamic $(LOADLIBES)
LOADLIBES += -ldl
endif
endif
LD	= gcc
endif


ifeq (,${CONDOR_LOCATION})
all:
	@echo "ERROR! You must set your CONDOR_LOCATION to point to the"
	@echo "base directory of your (full) Condor installation. Exiting."
	@exit 1
else
CXXFLAGS += -I${CONDOR_LOCATION}/include 
LOADLIBES := ${CONDOR_LOCATION}/lib/libcondorapi.a $(LOADLIBES)
endif


#
# === [3] ======================================================= rules section
# There is no need to change things below this line.
CXXFLAGS += -D${SYSTEM} -DMAJOR=${MAJOR}

all : condor-log-parser

userlog-reader.o : userlog-reader.C
	$(CXX) $(CXXFLAGS) $< -c -o $@

condor-log-parser: userlog-reader.o
	$(LD) $(LDFLAGS) $^ -o $@ $(LOADLIBES)

install: condor-log-parser
	$(INSTALL) -s -m 0755 condor-log-parser $(prefix)/bin

install.man:
	@echo "no manual pages available"

install.doc:
	@echo "no documentation available"

install.all: install

clean:
	$(RM) userlog-reader.o core

distclean: clean
	$(RM) condor-log-parser
