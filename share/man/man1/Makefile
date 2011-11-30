#
# make documentation
#
PS2PDF	= ps2pdf14
NROFF	= groff -mandoc
TEXT	= -Tlatin1
HTML	= -Thtml
# use /usr/ucb/install on Solaris
INSTALL	= install

SRC	= pegasus-analyzer.1 \
	pegasus-cleanup.1 \
	pegasus-create-dir.1 \
	pegasus-dax-validator.1 \
	pegasus-exitcode.1 \
	pegasus-monitord.1 \
	pegasus-plan.1 \
	pegasus-plots.1 \
	pegasus-rc-client.1 \
	pegasus-remove.1 \
	pegasus-run.1 \
	pegasus-s3.1 \
	pegasus-sc-client.1 \
	pegasus-sc-converter.1 \
	pegasus-statistics.1 \
	pegasus-status.1 \
	pegasus-tc-client.1 \
	pegasus-tc-converter.1 \
	pegasus-transfer.1 \
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
	@test "$PEGASUS_HOME" || exit 1
#	$(INSTALL) -m 0644 $(SRC) $(PEGASUS_HOME)/man/man1
	$(INSTALL) -m 0644 $(OUTPS) $(PEGASUS_HOME)/man
	$(INSTALL) -m 0644 $(OUTTXT) $(PEGASUS_HOME)/man
	$(INSTALL) -m 0644 $(OUTHTML) $(PEGASUS_HOME)/man

clean : 
	rm -f $(OUTPS) $(OUTTXT) $(OUTHTML)
