#
# Makefile 
#
INSTALL	= install	# use /usr/ucb/install on Solaris
STRIP	= strip
CC	= gcc
LD	= gcc
CXX	= g++
RM	= rm -f
CFLAGS	+= -Wall -O2 -march=pentium
CXXFLAGS += $(CFLAGS) -fno-exceptions -fno-rtti
LOADLIBES += -lm

# on Solaris use this link string:
# gcc -Wl,-Bstatic keg.o -lstdc++ -lm -lnsl -lsocket -Wl,-Bdynamic -ldl -o keg

set-classpath: set-classpath.o
	$(CXX) -static $(CXXFLAGS) $(LDFLAGS) $^ -o $@ $(LOADLIBES)
	$(STRIP) $@

set-classpath.o: set-classpath.cc
	$(CXX) $(CXXFLAGS) $< -c -o $@

install:
	$(INSTALL) -s -m 0755 set-classpath $(PEGASUS_HOME)/bin

clean:
	$(RM) set-classpath.o

distclean: clean
	$(RM) set-classpath
