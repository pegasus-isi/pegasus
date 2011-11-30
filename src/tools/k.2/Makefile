#
# Makefile 
#
INSTALL	= install
CC	= gcc
CFLAGS += -DHAS_BOOL -DHAS_MUTABLE
CXX	= g++
LD	= $(CXX)
CXXFLAGS += -DHAS_BOOL -DHAS_MUTABLE
RM	= rm -f
MV	= mv
TOUCH	= touch
FLEX	= flex -B -l
BISON	= bison -y
# uname-s is a perl script doing uname -s | tr various
SYSTEM	= $(shell ./uname-s)
VERSION = $(shell uname -r)
MARCH	= $(shell uname -m | tr '[A-Z]' '[a-z]')
MAJOR	= $(firstword $(subst ., ,$(VERSION)))
MINOR	= $(strip $(word 2,$(subst ., ,$(VERSION))))
SOCKIO  = $(shell /bin/ls /usr/include/sys/sockio.h 2>/dev/null)
PERL	= perl
LOADLIBES = -lm

ifndef ${prefix}
prefix = $(PEGASUS_HOME)
endif

NROFF   = groff -mandoc
TEXT    = -Tlatin1
HTML    = -Thtml

OPENSSL_BASEDIR = /usr/share/ssl
OPENSSL_INCLUDE = -I$(OPENSSL_BASEDIR)/include
OPENSSL_LIBRARY = -L$(OPENSSL_BASEDIR)/lib -lcrypto

#ZLIB_BASEDIR = /usr
#ZLIB_INCLUDE = -I$(ZLIB_BASDIR)/include
#ZLIB_LIBRARY = -L$(ZLIB_BASEDIR)/lib -lz

ifeq (SUNOS,${SYSTEM})
ifeq (5,${MAJOR})
# use these for the SUN CC compiler
CC	= cc
CXX	= CC
LD	= $(CXX)
## SPARCv7
V7FLAGS	= -xtarget=generic
V9FLAGS	= -xtarget=ultra -xarch=v9
CFLAGS  = -dalign -ftrap=%none -fsimple -xlibmil $(EXTRACFLAGS)
CFLAGS := -DSOLARIS $(CFLAGS) -xO2 -D__EXTENSIONS__=1
#CFXXLAGS  += -dalign -ftrap=%none -fsimple -xlibmil $(EXTRACFLAGS)
CXXFLAGS  += -dalign $(EXTRACFLAGS) -xO2 -D__EXTENSIONS__=1
LOADLIBES += -lnsl -lsocket
INSTALL = /usr/ucb/install
OPENSSL_BASEDIR = /usr/local/ssl
OPENSSL_INCLUDE = -I$(OPENSSL_BASEDIR)/include
OPENSSL_LIBRARY = -Bstatic -L$(OPENSSL_BASEDIR)/lib -lcrypto -Bdynamic
else
# old Solaris 1 not supported!
endif
endif

ifeq (LINUX,${SYSTEM})
ifeq (ia64,${MARCH})
# TeraGrid
OPENSSL_BASEDIR = /usr
OPENSSL_INCLUDE = -I$(OPENSSL_BASEDIR)/include
OPENSSL_LIBRARY = -Wl,-Bstatic -L$(OPENSSL_BASEDIR)/lib -lcrypto -Wl,-Bdynamic
CXXFLAGS += -Wall -O2 -DHAS_RUSAGE_WHO -DHAS_SETENV -DHAS_UNSETENV -frtti
CXXFLAGS += -fexceptions -ggdb
LOADLIBES := -Wl,-Bstatic -lstdc++ -Wl,-Bdynamic $(LOADLIBES)
LD	= $(CC)
else
ifeq (x86_64,${MARCH})
OPENSSL_BASEDIR = /usr
OPENSSL_INCLUDE = -I$(OPENSSL_BASEDIR)/include
OPENSSL_LIBRARY = -Wl,-Bstatic -L$(OPENSSL_BASEDIR)/lib64 -lcrypto -Wl,-Bdynamic
CXXFLAGS += -Wall -O2 -m64 -DHAS_RUSAGE_WHO -DHAS_SETENV
CXXFLAGS += -DHAS_UNSETENV -frtti -fexceptions -ggdb
LOADLIBES := -Wl,-Bstatic -lstdc++ -Wl,-Bdynamic $(LOADLIBES)
LD	= $(CC) -m64
else
OPENSSL_LIBRARY = -Wl,-Bstatic -L$(OPENSSL_BASEDIR)/lib -lcrypto -Wl,-Bdynamic
CXXFLAGS += -Wall -O2 -march=$(MARCH) -DHAS_RUSAGE_WHO -DHAS_SETENV
CXXFLAGS += -DHAS_UNSETENV -frtti -fexceptions -ggdb
LOADLIBES := -Wl,-Bstatic -lstdc++ -Wl,-Bdynamic $(LOADLIBES)
LD	= gcc
#EXTRA_LDFLAGS += -static
endif
endif
endif

ifeq (IRIX64,${SYSTEM})
# The regular 64bit Irix stuff is just too slow, use n32!
SYSTEM        := IRIX
endif

ifeq (IRIX,${SYSTEM})
CC      = cc -n32 -mips3 -r4000
LD      = $(CXX)
OPT_NORM = -O3 -IPA -LNO:opt=1
endif

ifeq (AIX,${SYSTEM})
#CC	= xlc
#CXX	= xlC
CXXFLAGS += -DHAS_SETENV
OPENSSL_BASEDIR = /usr/share/apps/openssl
OPENSSL_INCLUDE = -I$(OPENSSL_BASEDIR)/include
OPENSSL_LIBRARY = -Bstatic -L$(OPENSSL_BASEDIR)/lib -lcrypto -Bdynamic
ZLIB_BASEDIR = /usr/share/apps/zlib
ZLIB_INCLUDE = -I$(ZLIB_BASDIR)/include
ZLIB_LIBRARY = -L$(ZLIB_BASEDIR)/lib -lz
endif

ifeq (DARWIN,${SYSTEM})
CXXFLAGS += -DHAS_UNSETENV -DHAS_SETENV
OPENSSL_BASEDIR = /usr
OPENSSL_INCLUDE = -I$(OPENSSL_BASEDIR)/include
OPENSSL_LIBRARY = -lcrypto -Bdynamic
ZLIB_BASEDIR = /usr
ZLIB_INCLUDE = -I$(ZLIB_BASDIR)/include
ZLIB_LIBRARY = -lz
endif


#
# === [3] ======================================================= rules section
# There is no need to change things below this line.
CXXFLAGS += -I. -D${SYSTEM} -DMAJOR=${MAJOR} # -DSOCKLEN=${SOCKLEN}
ifneq (,${SOCKIO})
CXXFLAGS += -DHAS_SYS_SOCKIO=1
endif
LDFLAGS += $(EXTRA_LDFLAGS)

#CXXFLAGS  += $(ZLIB_INCLUDE)
#LOADLIBES += $(ZLIB_LIBRARY)
ifeq (LINUX,${SYSTEM})
CXXFLAGS  += $(OPENSSL_INCLUDE)
#LOADLIBES := -Wl,-Bstatic $(OPENSSL_LIBRARY) -Wl,-Bdynamic $(LOADLIBES)
LOADLIBES := $(OPENSSL_LIBRARY) $(LOADLIBES)
else
CXXFLAGS  += $(OPENSSL_INCLUDE)
LOADLIBES += $(OPENSSL_LIBRARY)
endif

GENDOC = k.2.html k.2.txt k.2.ps

CXXOBJS = quote.o scan.o scan.tab.o appinfo.o jobinfo.o stagejob.o \
	mysignal.o getif.o statinfo.o useinfo.o uname.o xml.o time.o \
	null.o

#CXXFLAGS += -DUSE_PARSE=1 -DUSE_SIGCHILD=1 # -ggdb
#CXXFLAGS += -ggdb

%.html : %.1 ;
	$(NROFF) $(HTML) $< > $@
%.ps : %.1 ;
	$(NROFF) $< > $@
%.txt : %.1 ;
	$(NROFF) $(TEXT) $< > $@
%.o : %.c
	$(CC) $(CFLAGS) -c $< -o $@
%.o : %.cc
	$(CXX) $(CXXFLAGS) -c $< -o $@
%   : %.o
	$(CXX) $(CXXFLAGS) $(LDFLAGS) $^ -o $@ $(LOADLIBES)

all: k.2

.PHONY: miniclean clean realclean

ifeq (g++,${CXX})
depend.mak: *.cc
	$(RM) depend.mak ; touch depend.mak
	$(CXX) $(OPENSSL_INCLUDE) -MM -E *.cc >> depend.mak
endif

k.2: k.2.o $(CXXOBJS)
	$(LD) $(CXXFLAGS) $(LDFLAGS) $^ -o $@ $(LOADLIBES)

justparse: justparse.o scan.o # scan.tab.o
	$(CXX) $(CXXFLAGS) $(LDFLAGS) $^ -o $@ $(LOADLIBES) 

scan.c: scan.l scan.tab.h
	$(FLEX) -t $< > $@

scan.o: scan.c scan.tab.h scan.y
	$(CXX) $(CXXFLAGS) $(LEXCFLG) -c $< -o $@

scan.tab.o: scan.tab.c scan.tab.h
	$(CXX) $(CXXFLAGS) $(YACCCFLG) -c $< -o $@

scan.tab.c scan.tab.h: scan.y
	$(BISON) -d -o scan.tab.c $<
	$(PERL) -i fixbison.pl scan.tab.c

install: k.2
	$(INSTALL) -m 0755 k.2 $(prefix)/bin

install.doc: $(GENDOC)
	$(INSTALL) -m 0644 $(GENDOC) $(prefix)/man

install.man: k.2.1
	$(INSTALL) -m 0644 k.2.1 $(prefix)/man/man1

install.all: install install.man install.doc

gendoc: $(GENDOC)
$(GENDOC): k.2.1

miniclean:
	$(RM) scan.tab.[cho] scan.[co]

clean: miniclean
	$(RM) *.o core $(GENDOC)

distclean: clean
	$(RM) k.2 justparse

-include depend.mak
