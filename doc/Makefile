VERSION = $(shell python ../setup.py -V)
ASCIIDOC = asciidoc --attribute VERSION=$(VERSION)
XSLTPROC = xsltproc
FOP = fop

MANPAGES = pegasus-service-server.man
MANPAGES += pegasus-service-admin.man
MANPAGES += pegasus-service-catalogs.man
MANPAGES += pegasus-service-ensemble.man

.PHONY: clean $(MANPAGES)

all: manual.html manual.pdf $(MANPAGES)

$(MANPAGES): %.man : %.pdf %.1 %.html

%.xml: %.asciidoc
	$(ASCIIDOC) -o $@ -b docbook $^

%.html: %.xml
	$(XSLTPROC) docbook-html.xsl $^ > $@

%.1: %.xml
	$(XSLTPROC) docbook-man.xsl $^ > $@

%.fo: %.xml
	$(XSLTPROC) docbook-pdf.xsl $^ > $@

%.pdf: %.fo
	$(FOP) -fo $^ -pdf $@

clean:
	rm -f *.html *.fo *.pdf *.xml *.1

