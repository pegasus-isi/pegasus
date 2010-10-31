ifndef ${prefix}
prefix = $(PEGASUS_HOME)
endif

all: touch.pysqlite touch.sqlalchemy

touch.pysqlite:
	@rm -rf pysqlite-2.6.0
	tar xzf pysqlite-2.6.0.tar.gz
	(cd pysqlite-2.6.0 && \
	  python setup.py build_static && \
	  cp -R build/lib*/pysqlite2 ${prefix}/lib/python/ \
	)
	@rm -rf pysqlite-2.6.0
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
	
clean:
	@rm -rf touch.pysqlite pysqlite-2.6.0
	@rm -rf touch.sqlalchemy SQLAlchemy-0.6.4

