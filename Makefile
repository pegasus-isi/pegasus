

all: html/index.php

pegasus-user-guide.fo: *.xml
	xsltproc --xinclude pegasus-pdf-style.xsl pegasus-book.xml > pegasus-user-guide.fo

pegasus-user-guide.pdf: pegasus-user-guide.fo html
	fop pegasus-user-guide.fo -pdf pegasus-user-guide.pdf
	cp pegasus-user-guide.pdf html/

html:
	mkdir -p html/images

html/index.php: html *.xml pegasus-style.xsl
	mkdir -p html
	xsltproc --noout \
		      --stringparam base.dir html/ \
		      --xinclude \
	         pegasus-style.xsl \
	         pegasus-book.xml
	(cd images && cp *.png *.jpg ../html/images/)

clean:
	  rm -rf html *.fo *.pdf ./*~

