TARGET	= pegasus-cluster pegasus-keg pegasus-kickstart
CONDOR_VERSION	= $(shell condor_version | awk '{ print $2; exit }')
MARCH   = $(shell uname -m | tr '[A-Z]' '[a-z]')

ifndef ${prefix}
prefix = $(PEGASUS_HOME)
endif

.ONESHELL:
all install:
	for i in $(TARGET); do \
	    if [ -d $$i ]; then \
		$(MAKE) -C $$i $@ || exit 1; \
	    fi; \
	done

.ONESHELL:
clean distclean:
	for i in $(TARGET); do \
	    if [ -d $$i ]; then \
		$(MAKE) -C $$i $@ || true; \
	    fi; \
	done
