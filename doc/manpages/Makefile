ifndef ${prefix}
prefix = ../..
endif

ifndef ${datadir}
datadir = $(prefix)/share
endif

ASCIIDOC = asciidoc -d manpage -b docbook
XSLTPROC = xsltproc -xinclude
FOP = fop

DOCBOOK2FO = $(XSLTPROC) docbook-pdf.xsl
DOCBOOK2MAN = $(XSLTPROC) docbook-man.xsl
DOCBOOK2HTML = $(XSLTPROC) docbook-html.xsl

TXTFILES = $(shell ls *.txt)
MANPAGES = $(TXTFILES:.txt=.1)
XMLMANPAGES = $(TXTFILES:.txt=.xml)
PDFMANPAGES = $(TXTFILES:.txt=.pdf)
HTMLMANPAGES = $(TXTFILES:.txt=.html)

.PHONY: manpages pdfmanpages htmlmanpages

.PRECIOUS: %.xml

all: manpages

manpages: $(MANPAGES)
pdfmanpages: $(PDFMANPAGES)
htmlmanpages: $(HTMLMANPAGES)

manpages.fo : manpages.xml $(XMLMANPAGES)
manpages.pdf : manpages.fo
manpages.html : manpages.xml $(XMLMANPAGES)

%.html : %.xml
	$(DOCBOOK2HTML) $< > $@

%.1 : %.xml
	$(DOCBOOK2MAN) $<

%.pdf : %.fo
	$(FOP) $< $@

%.fo : %.xml
	$(DOCBOOK2FO) $< > $@

%.xml : %.txt
	$(ASCIIDOC) -o $@ $<
	BASENAME=`echo $< | sed 's/.txt//'`; \
	sed -i.bak -E "s/id=\"(_[^\"]+)\"/id=\"$$BASENAME\1\"/" $@ ; \
	sed -i.bak -E "s/<refentry lang.*/<refentry lang='en' id='cli-$$BASENAME'>/" $@ ; \
	rm $@.bak

clean:
	rm -f pegasus-*.xml *.fo *.pdf *.1 *.html

install: manpages
	mkdir -p $(datadir)/man/man1
	for MPAGE in $(MANPAGES); do \
		cp $$MPAGE $(datadir)/man/man1/; \
	done

install-docbook: $(XMLMANPAGES)
	for XML in `ls *.xml`; do \
		cp $$XML ../docbook/manpage.$$XML; \
	done

