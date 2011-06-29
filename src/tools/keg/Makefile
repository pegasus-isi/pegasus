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
CONDOR_LOCATION = $(shell condor_config_val RELEASE_DIR 2>/dev/null)
EXTRA_OBJ = basic.o
EXTRA_INC = 
LFS_CFLAGS = -D_FILE_OFFSET_BITS=64 -D_LARGEFILE_SOURCE
LFS_LDFLAGS =


ifndef ${prefix}
prefix = $(PEGASUS_HOME)
endif

NROFF   = groff -mandoc
TEXT    = -Tlatin1
HTML    = -Thtml

ifeq (DARWIN,${SYSTEM})
#SDK=/Developer/SDKs/MacOSX10.6.sdk
SDK=$(lastword $(sort $(wildcard /Developer/SDKs/MacOSX10.*.sdk)))
ifneq (,${SDK})
CXXFLAGS = -g -pipe -arch x86_64 -arch i686 -nostdinc \
	-B${SDK}/usr/include/gcc -B${SDK}/usr/lib/gcc \
	-isystem${SDK}/usr/include -F${SDK}/System/Library/Frameworks
LDFLAGS = -arch x86_64 -arch i686 -Wl,-syslibroot,${SDK}
endif
EXTRA_OBJ += darwin.o
EXTRA_INC = darwin.hh
CXXFLAGS += -DMACHINE_SPECIFIC=darwin
endif

ifeq (SUNOS,${SYSTEM})
ifeq (5,${MAJOR})
# use these for the SUN CC compiler
# on Solaris use this link string with gcc 
# gcc -Wl,-Bstatic keg.o -lstdc++ -lm -lnsl -lsocket -Wl,-Bdynamic -ldl -o keg
CXX     = CC 
## SPARCv7
LFS_CFLAGS = $(shell getconf LFS_CFLAGS 2>>/dev/null)
LFS_LDFLAGS = $(shell getconf LFS_LDFLAGS 2>>/dev/null)
V7FLAGS	= -xtarget=generic
V9FLAGS	= -xtarget=ultra -xarch=v9
CXXFLAGS  = '-library=%none,Cstd,Crun' -dalign -ftrap=%none -fsimple -xlibmil $(EXTRACXXFLAGS)
#EXTRACXXFLAGS = $(V7FLAGS)
LD       := $(CXX) $(EXTRACXXFLAGS)
CXXFLAGS := -DSOLARIS $(CXXFLAGS) -xO4 -D__EXTENSIONS__=1
CXXFLAGS += -DMACHINE_SPECIFIC=sunos
EXTRA_OBJ += sunos.o sunos-swap.o
EXTRA_INC = sunos.hh
LOADLIBES_VANILLA = -lkstat -lnsl -lsocket
LOADLIBES_CONDOR  = -lkstat -lnsl -lsocket
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
LD	= $(CXX)
else
ifeq (x86_64,${MARCH})
CXXFLAGS = -Wall -O2 -m64 -ggdb
LD	= gcc -m64
else
ifeq (armv7l, ${MARCH})
CXXFLAGS = -Wall -O2 -ggdb
else
CXXFLAGS  = -Wall -O2  -march=i686 -ggdb
#LDFLAGS += -static
endif
endif
LOADLIBES_VANILLA = -Wl,-Bstatic -lstdc++ -Wl,-Bdynamic
LOADLIBES_CONDOR  = -lstdc++
LD	= gcc
endif
CXXFLAGS += -DMACHINE_SPECIFIC=linux
EXTRA_OBJ += linux.o
EXTRA_INC = linux.hh
LFS_CFLAGS = $(shell getconf LFS_CFLAGS 2>>/dev/null)
LFS_LDFLAGS = $(shell getconf LFS_LDFLAGS 2>>/dev/null)
endif

#
# === [3] ======================================================= rules section
# There is no need to change things below this line.
CXXFLAGS += -D${SYSTEM} -DMAJOR=${MAJOR} # -DSOCKLEN=${SOCKLEN}
ifneq (,${SOCKIO})
CXXFLAGS += -DHAS_SYS_SOCKIO=1
endif
##ifneq (,${STATVFS})
##CXXFLAGS += -DHAS_SYS_STATVFS=1
##endif
GENDOC = keg.html keg.txt keg.ps
CONDOR_LD = $(CONDOR) $(LD)
CONDOR_CXX = $(CONDOR) $(CXX)

SVNVERSION = $(shell svnversion . 2>> /dev/null)
ifneq (,${SVNVERSION})
CXXFLAGS += -DHAS_SVNVERSION=\"${SVNVERSION}\"
endif

# add large file support
ifneq (,${LFS_CFLAGS})
NOLFS_CFLAGS := $(CXXFLAGS)
CXXFLAGS += $(LFS_CFLAGS)
endif
ifneq (,${LFS_LDFLAGS})
LDFLAGS += $(LFS_LDFLAGS)
endif

%.html : %.1 ;
	$(NROFF) $(HTML) $< > $@
%.ps : %.1 ;
	$(NROFF) $< > $@
%.txt : %.1 ;
	$(NROFF) $(TEXT) $< > $@
%.o : %.cc
	$(CXX) $(CXXFLAGS) $< -c -o $@
%.co : %.cc
	$(CONDOR_CXX) $(CXXFLAGS) $< -c -o $@

TARGET	= keg
INST_TG = install-keg
ifneq (,${CONDOR_LOCATION})
ifneq (ia64,${MARCH})
ifneq (x86_64,${MARCH})
ifneq (DARWIN,${SYSTEM})
TARGET += keg.condor
INST_TG += install-condor
endif
endif
endif
endif

all : $(TARGET) $(GENDOC)
EXTRA_COBJ = $(EXTRA_OBJ:.o=.co)

keg.o: keg.cc $(EXTRA_INC)

keg: keg.o $(EXTRA_OBJ)
	$(LD) $(LDFLAGS) $^ -o $@ $(LOADLIBES_VANILLA) $(LOADLIBES)

keg.condor: keg.co $(EXTRA_COBJ)
	$(CONDOR_LD) $(LDFLAGS) $^ -o $@ $(LOADLIBES_CONDOR) $(LOADLIBES)

keg.co: keg.cc $(EXTRA_INC)
	$(CONDOR_CXX) $(CXXFLAGS) $< -c -o $@

basic.o: basic.cc basic.hh
basic.co: basic.cc basic.hh
darwin.o: darwin.cc darwin.hh basic.hh
darwin.co: darwin.cc darwin.hh basic.hh
linux.o: linux.cc linux.hh basic.hh
linux.co: linux.cc linux.hh basic.hh
sunos.o: sunos.cc sunos.hh basic.hh
sunos.co: sunos.cc sunos.hh basic.hh
sunos-swap.o: sunos-swap.cc sunos-swap.hh
	$(CXX) $(NOLFS_CFLAGS) $< -c -o $@
sunos-swap.co: sunos-swap.cc sunos-swap.hh
	$(CONDOR_CXX) $(NOLFS_CFLAGS) $< -c -o $@

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
	$(RM) keg.o keg.co $(GENDOC) core $(EXTRA_OBJ) $(EXTRA_COBJ)

distclean: clean
	$(RM) keg keg.condor
