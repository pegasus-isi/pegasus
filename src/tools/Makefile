#
# build all C/C++ sources 
#
# $Id$
#
TARGET	= kickstart pegasus-keg cluster invoke # free
CONDOR_VERSION	= $(shell condor_version | awk '{ print $2; exit }')
MARCH   = $(shell uname -m | tr '[A-Z]' '[a-z]')

ifndef ${prefix}
prefix = $(PEGASUS_HOME)
endif

.ONESHELL:
all clean distclean install install.man install.doc install.all:
	for i in $(TARGET); do \
	    if [ -d $$i ]; then \
		$(MAKE) -C $$i $@ || exit 1; \
	    fi; \
	done
