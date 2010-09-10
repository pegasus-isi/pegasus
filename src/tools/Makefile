#
# build all C/C++ sources 
#
# $Id$
#
TARGET	= kickstart keg cluster invoke # free
CONDOR_VERSION	= $(shell condor_version | awk '{ print $2; exit }')
MARCH   = $(shell uname -m | tr '[A-Z]' '[a-z]')

ifneq (,${CONDOR_VERSION})
ifneq (ia64,${MARCH})
ifneq (x86_64,${MARCH})

#commented out on 10/15/2008 by Gaurang. No longer use condor-log-parser
#TARGET += condor-log-parser
endif
endif
endif

ifndef ${prefix}
prefix = $(PEGASUS_HOME)
endif

all:
	for i in $(TARGET); do \
	    test -d $$i && $(MAKE) -C $$i all; \
	done

clean:
	for i in $(TARGET); do \
	    test -d $$i && $(MAKE) -C $$i clean; \
	done

distclean:
	for i in $(TARGET); do \
	    test -d $$i && $(MAKE) -C $$i distclean; \
	done

install: 
	for i in $(TARGET); do \
	    test -d $$i && $(MAKE) -C $$i prefix=$(prefix) install; \
	done

install.man: 
	for i in $(TARGET); do \
	    test -d $$i && $(MAKE) -C $$i prefix=$(prefix) install.man; \
	done

install.doc: 
	for i in $(TARGET); do \
	    test -d $$i && $(MAKE) -C $$i prefix=$(prefix) install.doc; \
	done

install.all:
	for i in $(TARGET); do \
	    test -d $$i && $(MAKE) -C $$i prefix=$(prefix) install.all; \
	done
