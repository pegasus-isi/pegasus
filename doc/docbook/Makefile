PEGASUS_HOME=$(shell cd ../.. && pwd)
VERSION=$(shell ../../release-tools/getversion)

XML  = about.xml
XML += creating_workflows.xml
XML += example_workflows.xml
XML += execution_environments.xml
XML += funding_citing_usage_stats.xml
XML += glossary.xml
XML += installation.xml
XML += monitoring_debugging_stats.xml
XML += pegasus-book.xml
XML += data_management.xml
XML += reference_api.xml
XML += reference_configuration.xml
XML += running_workflows.xml
XML += submit_directory.xml
XML += tutorial.xml
XML += tutorial_vm.xml
XML += useful_tips.xml
XML += service.xml

# Generated stuff
XML += title.xml 
XML += reference_command_lines.xml

# Generated manpages
MANPAGES=$(shell ls ../manpages | grep txt)
XMLMANPAGES=$(addprefix manpage.,$(MANPAGES:.txt=.xml))
TXTMANPAGES=$(addprefix ../manpages/,$(MANPAGES))
XML += $(XMLMANPAGES)

ifndef ${prefix}
prefix = $(PEGASUS_HOME)/share/pegasus/doc
endif

TARGETS = wordpress pegasus-user-guide.pdf

all: $(TARGETS)

$(XMLMANPAGES): $(TXTMANPAGES)
	make -C ../manpages install-docbook

reference_command_lines.xml: $(XMLMANPAGES)
	./generate_cli_reference.sh > reference_command_lines.xml

title.xml: ../../build.properties
	echo "<title>Pegasus $(VERSION) User Guide</title>" > title.xml

wordpress: $(XML) pegasus-wordpress-style.xsl pegasus-user-guide.pdf
	mkdir -p ./wordpress/images
	xsltproc --noout --stringparam base.dir ./wordpress/ --xinclude pegasus-wordpress-style.xsl pegasus-book.xml
	cp images/*.png images/*.jpg images/*.svg ./wordpress/images/
	cp pegasus-user-guide.pdf wordpress/
	touch wordpress

pegasus-user-guide.fo: $(XML) pegasus-pdf-style.xsl
	xsltproc --xinclude pegasus-pdf-style.xsl pegasus-book.xml > pegasus-user-guide.fo

pegasus-user-guide.pdf: pegasus-user-guide.fo
	fop pegasus-user-guide.fo -pdf pegasus-user-guide.pdf 2>&1 \
        | grep -v "The following feature isn't implemented by Apache FOP, yet: table-layout=\"auto\"" \
        | grep -v "function may only be used when fo:table has table-layout=\"fixed\"" \
        | grep -v "org.apache.fop.events.LoggingEventListener processEvent"

install: $(TARGETS)
	mkdir -p ${prefix}
	cp -R wordpress ${prefix}/
	cp pegasus-user-guide.pdf ${prefix}/

clean:
	rm -rf wordpress pegasus-user-guide.pdf pegasus-user-guide.fo ./*~ title.xml reference_command_lines.xml manpage.*.xml

