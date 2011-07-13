ifndef ${prefix}
prefix = $(PEGASUS_HOME)
endif

BOTO=boto-2.0b3-24047c8

all: touch.pysqlite touch.sqlalchemy touch.boto

touch.pysqlite:
	@rm -rf pysqlite-2.6.3
	tar xzf pysqlite-2.6.3.tar.gz
	(cd pysqlite-2.6.3 && \
	  python setup.py build_static && \
	  cp -R build/lib*/pysqlite2 ${prefix}/lib/python/ \
	)
	@rm -rf pysqlite-2.6.3
	touch touch.pysqlite

touch.sqlalchemy:
	@rm -rf SQLAlchemy-0.6.4
	tar xzf SQLAlchemy-0.6.4.tar.gz
	(cd SQLAlchemy-0.6.4 && \
	  python setup.py build && \
	  cp -R build/lib*/sqlalchemy ${prefix}/lib/python/ \
	)
	@rm -rf SQLAlchemy-0.6.4
	touch touch.sqlalchemy

touch.boto:
	@rm -rf $(BOTO)
	tar xzf $(BOTO).tar.gz
	(cd $(BOTO) && \
	  python setup.py build && \
	  rm -rf ${prefix}/lib/python/boto && \
	  cp -R build/lib*/boto ${prefix}/lib/python/ \
	)
	@rm -rf $(BOTO)
	touch touch.boto
	
clean:
	rm -rf touch.pysqlite pysqlite-2.6.0
	rm -rf touch.sqlalchemy SQLAlchemy-0.6.4
	rm -rf touch.boto $(BOTO)

