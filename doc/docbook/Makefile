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
XML += reference.xml
XML += reference_api.xml
XML += reference_command_lines.xml
XML += reference_hierarchical_workflow.xml
XML += reference_job_clustering.xml
XML += reference_monitoring.xml
XML += reference_notifications.xml
XML += reference_profiles.xml
XML += reference_replica_selection.xml
XML += reference_transfers.xml
XML += running_workflows.xml
XML += submit_directory.xml
XML += tutorial.xml
XML += tutorial_vm.xml
XML += useful_tips.xml

# Generated stuff
XML += title.xml basic_properties.xml reference_properties.xml

# Generated manpages
MANPAGES=$(shell ls ../manpages | grep txt)
XMLMANPAGES=$(addprefix manpage.,$(MANPAGES:.txt=.xml))
TXTMANPAGES=$(addprefix ../manpages/,$(MANPAGES))
XMLMANPAGES += manpage.manpages.xml
XML += $(XMLMANPAGES)

SAMPLEPROPS=../../libexec/docbook-sample-props
PROP2DOC=env PEGASUS_HOME=$(PEGASUS_HOME) $(SAMPLEPROPS)
BASICPROPS=../../etc/basic.properties
ADVANCEDPROPS=../../etc/advanced.properties

ifndef ${prefix}
prefix = $(PEGASUS_HOME)/share/pegasus/doc
endif

TARGETS = html pegasus-user-guide.pdf

all: $(TARGETS)

$(XMLMANPAGES): $(TXTMANPAGES)
	make -C ../manpages install-docbook

title.xml: ../../build.properties
	echo "<title>Pegasus $(VERSION) User Guide</title>" > title.xml

basic_properties.xml: $(BASICPROPS) $(SAMPLEPROPS)
	$(PROP2DOC) $(BASICPROPS) > basic_properties.xml

reference_properties.xml: $(ADVANCEDPROPS) $(SAMPLEPROPS)
	$(PROP2DOC) $(ADVANCEDPROPS) > reference_properties.xml

html: $(XML) pegasus-php-style.xsl
	mkdir -p ./html/images
	xsltproc --noout --stringparam base.dir ./html/ --xinclude pegasus-php-style.xsl pegasus-book.xml
	cp images/*.png images/*.jpg ./html/images/
	touch html

pegasus-user-guide.fo: $(XML) pegasus-pdf-style.xsl
	xsltproc --xinclude pegasus-pdf-style.xsl pegasus-book.xml > pegasus-user-guide.fo

pegasus-user-guide.pdf: pegasus-user-guide.fo
	fop pegasus-user-guide.fo -pdf pegasus-user-guide.pdf

install: $(TARGETS)
	mkdir -p ${prefix}
	cp -R html ${prefix}/
	cp pegasus-user-guide.pdf ${prefix}/

clean:
	rm -rf html pegasus-user-guide.pdf pegasus-user-guide.fo ./*~ title.xml basic_properties.xml reference_properties.xml manpage.*.xml

