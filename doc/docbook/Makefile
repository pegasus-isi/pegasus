PEGASUS_HOME=$(shell cd ../.. && pwd)
VERSION=$(shell ../../release-tools/getversion --majorminor)
XML=title.xml basic_properties.xml reference_properties.xml $(shell ls *.xml)

SAMPLEPROPS=../../libexec/docbook-sample-props
PROP2DOC=env PEGASUS_HOME=$(PEGASUS_HOME) $(SAMPLEPROPS)
BASICPROPS=../../etc/basic.properties
ADVANCEDPROPS=../../etc/advanced.properties

ifndef ${prefix}
prefix = html
endif

.PHONY: title.xml

all: ${prefix}/index.php ${prefix}/pegasus-user-guide.pdf

title.xml:
	echo "<title>Pegasus $(VERSION) User Guide</title>" > title.xml

basic_properties.xml: $(BASICPROPS) $(SAMPLEPROPS)
	$(PROP2DOC) $(BASICPROPS) > basic_properties.xml

reference_properties.xml: $(ADVANCEDPROPS) $(SAMPLEPROPS)
	$(PROP2DOC) $(ADVANCEDPROPS) > reference_properties.xml

${prefix}/index.php: $(XML) pegasus-php-style.xsl
	mkdir -p ${prefix}/images
	xsltproc --noout --stringparam base.dir ${prefix}/ --xinclude pegasus-php-style.xsl pegasus-book.xml
	cp images/*.png images/*.jpg ${prefix}/images/

pegasus-user-guide.fo: $(XML) pegasus-pdf-style.xsl
	xsltproc --xinclude pegasus-pdf-style.xsl pegasus-book.xml > pegasus-user-guide.fo

${prefix}/pegasus-user-guide.pdf: pegasus-user-guide.fo
	fop pegasus-user-guide.fo -pdf pegasus-user-guide.pdf
	cp pegasus-user-guide.pdf ${prefix}/

clean:
	rm -rf html *.fo ./*~ title.xml basic_properties.xml reference_properties.xml

