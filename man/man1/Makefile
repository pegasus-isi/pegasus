#
# make documentation
#
PS2PDF	= ps2pdf14
NROFF	= groff -mandoc
TEXT	= -Tlatin1
HTML	= -Thtml
# use /usr/ucb/install on Solaris
INSTALL	= install

SRC	= exitcode.1 \
	gencdag.1 \
	sc-client.1 \
	rc-client.1 \
	show-properties.1 \
	tc-client.1 \
	test-version.1 \
	pegasus-check-sites.1 \
	pegasus-get-sites.1 \
	pegasus-plan.1 \
	pegasus-submit-dag.1 \
	pegasus-verify.1 \
	pegasus-version.1

OUTPS	= $(SRC:.1=.ps)
OUTPDF	= $(SRC:.1=.pdf)
OUTTXT	= $(SRC:.1=.txt)
OUTHTML	= $(SRC:.1=.html)

%.html : %.1 ;
	$(NROFF) $(HTML) $< > $@
%.ps : %.1 ;
	$(NROFF) $< > $@
%.pdf : %.1 ;
	$(NROFF) $< | $(PS2PDF) > $@
%.txt : %.1
	$(NROFF) $(TEXT) $< > $@

all : all-ps all-txt all-html

all-ps : $(OUTPS)
all-txt : $(OUTTXT)
all-html : $(OUTHTML)

install: all
	@test "$VDS_HOME" || exit 1
#	$(INSTALL) -m 0644 $(SRC) $(VDS_HOME)/man/man1
	$(INSTALL) -m 0644 $(OUTPS) $(VDS_HOME)/man
	$(INSTALL) -m 0644 $(OUTTXT) $(VDS_HOME)/man
	$(INSTALL) -m 0644 $(OUTHTML) $(VDS_HOME)/man

clean : 
	rm -f $(OUTPS) $(OUTTXT) $(OUTHTML)
