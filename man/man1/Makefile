#
# make documentation
#
PS2PDF	= ps2pdf14
NROFF	= groff -mandoc
TEXT	= -Tlatin1
HTML	= -Thtml
# use /usr/ucb/install on Solaris
INSTALL	= install

SRC	= d2d.1 deletevdc.1 exitcode.1 gencdag.1 gendax.1 \
	genmdag.1 genpoolconfig.1 insertvdc.1 rls-client.1 \
	searchvdc.1 show-properties.1 shplanner.1 shrc.1 \
	tc-client.1 test-version.1 updatevdc.1 vdlc.1 \
	vdlt2vdlx.1 vdlx2vdlt.1 vds-check-sites.1 vds-get-sites.1 \
	vds-plan.1 vds-submit-dag.1 vds-verify.1 vds-version.1

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
