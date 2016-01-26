ifndef ${prefix}
prefix = ../..
endif

ifndef ${datadir}
datadir = $(prefix)/share
endif

VERSION = $(shell ../../release-tools/getversion)

ASCIIDOC = asciidoc -a manversion=$(VERSION) -a mansource="Pegasus" -a manmanual="Pegasus Manual" -d manpage -b docbook
XSLTPROC = xsltproc -xinclude

DOCBOOK2MAN = $(XSLTPROC) docbook-man.xsl

TXTFILES = $(shell ls *.txt)
MANPAGES = $(TXTFILES:.txt=.1)

.PHONY: manpages

.PRECIOUS: %.xml

all: manpages

manpages: $(MANPAGES)

%.1 : %.xml
	$(DOCBOOK2MAN) $<

%.xml : %.txt
	$(ASCIIDOC) -o $@ $<
	BASENAME=`echo $< | sed 's/.txt//'`; \
	perl -p -i.bak -e "s/id=\"(_[^\"]+)\"/id=\"$$BASENAME\1\"/" $@ ; \
	perl -p -i.bak -e "s/<refentry lang.*/<refentry lang='en' id='cli-$$BASENAME'>/" $@ ; \
	rm $@.bak

clean:
	rm -f pegasus-*.xml *.1

install: manpages
	mkdir -p $(datadir)/man/man1
	for MPAGE in $(MANPAGES); do \
		cp $$MPAGE $(datadir)/man/man1/; \
	done

install-docbook: $(XMLMANPAGES)
	for XML in `ls *.xml`; do \
		cp $$XML ../docbook/manpage.$$XML; \
	done

