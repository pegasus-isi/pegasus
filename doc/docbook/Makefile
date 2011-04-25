
ifndef ${prefix}
prefix = html
endif

all: ${prefix}/index.php ${prefix}/pegasus-user-guide.pdf ${prefix}/basic-properties.pdf ${prefix}/advanced-properties.pdf

configuration.xml: ../../etc/basic.properties ../../libexec/docbook-sample-props
	export PEGASUS_HOME=`dirname \`pwd ../../\`` \
        && ../../libexec/docbook-sample-props ../../etc/basic.properties > configuration.xml 

advanced_concepts_properties.xml: ../../etc/advanced.properties ../../libexec/docbook-sample-props
	export PEGASUS_HOME=`dirname \`pwd ../../\`` \
	    && ../../libexec/docbook-sample-props ../../etc/advanced.properties > advanced_concepts_properties.xml 

${prefix}/index.php: *.xml pegasus-style.xsl configuration.xml advanced_concepts_properties.xml
	mkdir -p ${prefix}/images
	xsltproc --noout \
		      --stringparam base.dir ${prefix}/ \
		      --xinclude \
	         pegasus-style.xsl \
	         pegasus-book.xml
	cp images/*.png images/*.jpg ${prefix}/images/

pegasus-user-guide.fo: *.xml pegasus-pdf-style.xsl
	xsltproc --xinclude pegasus-pdf-style.xsl pegasus-book.xml > pegasus-user-guide.fo

basic-properties.fo: *.xml pegasus-pdf-style.xsl
	xsltproc --xinclude pegasus-pdf-style.xsl basic-properties-book.xml > basic-properties.fo

advanced-properties.fo: *.xml pegasus-pdf-style.xsl
	xsltproc --xinclude pegasus-pdf-style.xsl advanced-properties-book.xml > advanced-properties.fo

${prefix}/pegasus-user-guide.pdf: pegasus-user-guide.fo
	fop pegasus-user-guide.fo -pdf pegasus-user-guide.pdf
	cp pegasus-user-guide.pdf ${prefix}/

${prefix}/basic-properties.pdf: basic-properties.fo
	fop basic-properties.fo -pdf basic-properties.pdf
	cp basic-properties.pdf ${prefix}/

${prefix}/advanced-properties.pdf: advanced-properties.fo
	fop advanced-properties.fo -pdf advanced-properties.pdf
	cp advanced-properties.pdf ${prefix}/

clean:
	  rm -rf html *.fo *.pdf ./*~

