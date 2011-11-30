#
# Makefile 
#
INSTALL	= install
NROFF   = groff -mandoc
TEXT    = -Tlatin1
HTML    = -Thtml

GENDOC = replanner.html replanner.txt replanner.ps

%.html : %.1 ;
	$(NROFF) $(HTML) $< > $@
%.ps : %.1 ;
	$(NROFF) $< > $@
%.txt : %.1 ;
	$(NROFF) $(TEXT) $< > $@

install: $(GENDOC)
	$(INSTALL) -m 0755 replanner $(PEGASUS_HOME)/bin
	$(INSTALL) -m 0644 $(GENDOC) $(PEGASUS_HOME)/man
	$(INSTALL) -m 0644 replanner.1 $(PEGASUS_HOME)/man/man1

doc: $(GENDOC)

clean:
	$(RM) $(GENDOC) core

distclean: clean

