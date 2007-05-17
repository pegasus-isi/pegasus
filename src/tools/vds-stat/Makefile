# $Id$

INSTALL = install
RM      = rm -f
CC      = gcc
CFLAGS  = -Wall -std=c99 `xml2-config --cflags` -D_GNU_SOURCE
LD      = $(CC)
EXTRA_LDFLAGS= `xml2-config --libs` -ll -lm


ifndef ${prefix}
prefix = $(PEGASUS_HOME)
endif

all: vds-format-kickstart

vds-format-kickstart: vds-format-kickstart.o vds-format-kickstart-help.o
	$(LD) $^ -o $@ $(LOADLIBES) $(EXTRA_LDFLAGS)

%.o : %.c
	$(CC) $(CFLAGS) $< -c -o $@

clean:
	$(RM) *.o vds-format-kickstart

install: vds-format-kickstart
	$(INSTALL) -m 0755 vds-stat $(prefix)/bin
	$(INSTALL) -m 0755 vds-format-kickstart $(prefix)/bin

# At some point, we'll have a man page and other documentation... hopefully...
# so here are the build targets for when we do.

#install.doc: $(GENDOC)
#	$(INSTALL) -m 0644 $(GENDOC) $(prefix)/man

install.man: vds-stat.1
	$(INSTALL) -m 0644 vds-stat.1 $(prefix)/man/man1

install.all: install install.man #install.doc

