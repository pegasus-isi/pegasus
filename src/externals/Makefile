ifndef ${prefix}
prefix = $(PEGASUS_HOME)
endif

ifndef ${libdir}
libdir = ${prefix}/lib
endif

BOTO=boto-2.0b3-24047c8

all: ${libdir}/python/pysqlite2 ${libdir}/python/sqlalchemy ${libdir}/python/boto

${libdir}/python/pysqlite2:
	@rm -rf pysqlite-2.6.3
	tar xzf pysqlite-2.6.3.tar.gz
	(cd pysqlite-2.6.3 && \
	  python setup.py build_static && \
	  cp -R build/lib*/pysqlite2 ${libdir}/python/ \
	)
	@rm -rf pysqlite-2.6.3

${libdir}/python/sqlalchemy:
	@rm -rf SQLAlchemy-0.6.4
	tar xzf SQLAlchemy-0.6.4.tar.gz
	(cd SQLAlchemy-0.6.4 && \
	  python setup.py build && \
	  cp -R build/lib*/sqlalchemy ${libdir}/python/ \
	)
	@rm -rf SQLAlchemy-0.6.4

${libdir}/python/boto:
	@rm -rf $(BOTO)
	tar xzf $(BOTO).tar.gz
	(cd $(BOTO) && \
	  python setup.py build && \
	  rm -rf ${prefix}/lib/python/boto && \
	  cp -R build/lib*/boto ${libdir}/python/ \
	)
	@rm -rf $(BOTO)
	
clean:
	rm -rf touch.pysqlite pysqlite-2.6.0
	rm -rf touch.sqlalchemy SQLAlchemy-0.6.4
	rm -rf touch.boto $(BOTO) .boto*

