

all: html/basic.properties.html html/index.php

html:
	mkdir -p html/images

html/basic.properties.html: html/index.php ../../etc/basic.properties ../../etc/advanced.properties
	export PEGASUS_HOME=`dirname \`pwd ../../\`` \
        && export HTML_SNIPPET_ONLY="1" \
        && ../../libexec/html-sample-props ../../etc/basic.properties > html/basic.properties.html \
	    && ../../libexec/html-sample-props ../../etc/advanced.properties > html/advanced.properties.html 
	perl -p -i -e 's/PHP_BASIC_PROPERTIES/<div class="titlepage"><?php include("basic.properties.html");?><\/div>/' html/configuration.php

html/index.php: html pegasus-book.xml pegasus-style.xsl
	mkdir -p html
	xsltproc --noout \
		      --stringparam base.dir html/ \
		      --xinclude \
	         pegasus-style.xsl \
	         pegasus-book.xml
	(cd images && cp *.png *.jpg ../html/images/)

clean:
	  rm -rf html ./*~

