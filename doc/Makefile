VERSION = $(shell python ../setup.py -V)
ASCIIDOC = asciidoc --attribute VERSION=$(VERSION)
XSLTPROC = xsltproc
FOP = fop

all: manual.html manual.pdf pegasus-service.pdf pegasus-service.1 pegasus-service.html

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

