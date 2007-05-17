#
# build all C/C++ sources 
#
# $Id: Makefile,v 1.8 2006/07/20 22:38:42 benc Exp $
#
TARGET	= kickstart k.2 keg transfer T2 cluster invoke # free
CONDOR_VERSION	= $(shell condor_version | awk '{ print $2; exit }')
MARCH   = $(shell uname -m | tr '[A-Z]' '[a-z]')

ifneq (,${CONDOR_VERSION})
ifneq (ia64,${MARCH})
ifneq (x86_64,${MARCH})
TARGET += condor-log-parser
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
