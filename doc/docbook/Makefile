
ifndef ${prefix}
prefix = html
endif

all: ${prefix}/index.php ${prefix}/pegasus-user-guide.pdf

pegasus-user-guide.fo: *.xml
	xsltproc --xinclude pegasus-pdf-style.xsl pegasus-book.xml > pegasus-user-guide.fo

${prefix}/pegasus-user-guide.pdf: pegasus-user-guide.fo
	fop pegasus-user-guide.fo -pdf pegasus-user-guide.pdf
	cp pegasus-user-guide.pdf ${prefix}/

configuration.xml: ../../etc/basic.properties
	export PEGASUS_HOME=`dirname \`pwd ../../\`` \
        && ../../libexec/docbook-sample-props ../../etc/basic.properties > configuration.xml 

advanced_concepts_properties.xml: ../../etc/advanced.properties
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

clean:
	  rm -rf html *.fo *.pdf ./*~

