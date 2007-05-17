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
LOADLIBES_VANILLA = 
LOADLIBES_CONDOR  =
SYSTEM  = $(shell uname -s | tr '[a-z]' '[A-Z]' | tr -d '_ -/')
VERSION = $(shell uname -r)
MARCH	= $(shell uname -m | tr '[A-Z]' '[a-z]')
MAJOR   = $(firstword $(subst ., ,$(VERSION)))
CONDOR	= condor_compile
SOCKIO  = $(shell /bin/ls /usr/include/sys/sockio.h 2>/dev/null)
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
LOADLIBES_VANILLA = -lnsl -lsocket
LOADLIBES_CONDOR  = -lnsl -lsocket
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
LD	= gcc -m64
else
CXXFLAGS  = -Wall -O2 -march=i686 -ggdb
#LDFLAGS += -static
endif
LOADLIBES_VANILLA = -Wl,-Bstatic -lstdc++ -Wl,-Bdynamic
LOADLIBES_CONDOR  = -lstdc++
endif
LD	= gcc
endif

#
# === [3] ======================================================= rules section
# There is no need to change things below this line.
CXXFLAGS += -D${SYSTEM} -DMAJOR=${MAJOR} # -DSOCKLEN=${SOCKLEN}
ifneq (,${SOCKIO})
CXXFLAGS += -DHAS_SYS_SOCKIO=1
endif
GENDOC = keg.html keg.txt keg.ps
CONDOR_LD = $(CONDOR) $(LD)
CONDOR_CXX = $(CONDOR) $(CXX)

%.html : %.1 ;
	$(NROFF) $(HTML) $< > $@
%.ps : %.1 ;
	$(NROFF) $< > $@
%.txt : %.1 ;
	$(NROFF) $(TEXT) $< > $@
%.o : %.cc
	$(CXX) $(CXXFLAGS) $< -c -o $@

TARGET	= keg
INST_TG = install-keg
ifneq (,${CONDOR_LOCATION})
ifneq (ia64,${MARCH})
ifneq (x86_64,${MARCH})
TARGET += keg.condor
INST_TG += install-condor
endif
endif
endif

all : $(TARGET) $(GENDOC)

keg: keg.o
	$(LD) $(LDFLAGS) $^ -o $@ $(LOADLIBES_VANILLA) $(LOADLIBES)

keg.condor: keg.condor.o
	$(CONDOR_LD) $(LDFLAGS) $^ -o $@ $(LOADLIBES_CONDOR) $(LOADLIBES)

keg.condor.o: keg.cc
	$(CONDOR_CXX) $(CXXFLAGS) $< -c -o $@

$(GENDOC): keg.1

install-keg: keg
	$(INSTALL) -m 0755 keg $(prefix)/bin

install-condor: keg.condor
	$(INSTALL) -m 0755 keg.condor $(prefix)/bin

install: $(INST_TG)

install.doc: $(GENDOC)
	$(INSTALL) -m 0644 $(GENDOC) $(prefix)/man

install.man: keg.1
	$(INSTALL) -m 0644 keg.1 $(prefix)/man/man1

install.all: install install.man install.doc

doc: $(GENDOC)

man: $(GENDOC)

test check: keg
	@./keg -o /dev/fd/1 || echo "test failed" && echo "test ok"

clean:
	$(RM) keg.o keg.condor.o $(GENDOC) core

distclean: clean
	$(RM) keg keg.condor
