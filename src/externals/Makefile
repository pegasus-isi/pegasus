ifndef ${prefix}
prefix = $(PEGASUS_HOME)
endif

ifndef ${libdir}
libdir = ${prefix}/lib
endif

PySQLite=pysqlite-2.6.0
BOTO=boto-2.0b3-24047c8
SQLAlchemy=SQLAlchemy-0.7.6
Jinja=Jinja2-2.6
Flask=Flask-0.9
need_pysqlite2=$(shell if python -V 2>&1 | grep -qE 'ython 2\.[3-4]'; then echo 1; else echo 0; fi)

all: ${libdir}/python/pysqlite2 ${libdir}/python/sqlalchemy ${libdir}/python/boto ${libdir}/python/jinja2 ${libdir}/python/flask

${libdir}/python/pysqlite2:
ifeq ($(need_pysqlite2), 1)
	@rm -rf $(PySQLite)
	tar xzf $(PySQLite).tar.gz
	(cd $(PySQLite) && \
	    python setup.py build_static && \
	    cp -R build/lib*/pysqlite2 ${libdir}/python/ \
	)
	@rm -rf $(PySQLite)
else
	mkdir -p ${libdir}/python/pysqlite2
endif

${libdir}/python/sqlalchemy:
	@rm -rf $(SQLAlchemy)
	tar xzf $(SQLAlchemy).tar.gz
	(cd $(SQLAlchemy) && \
	  python setup.py build && \
	  cp -R build/lib*/sqlalchemy ${libdir}/python/ \
	)
	@rm -rf $(SQLAlchemy)

${libdir}/python/boto:
	@rm -rf $(BOTO)
	tar xzf $(BOTO).tar.gz
	(cd $(BOTO) && \
	  python setup.py build && \
	  rm -rf ${prefix}/lib/python/boto && \
	  cp -R build/lib*/boto ${libdir}/python/ \
	)
	@rm -rf $(BOTO)

${libdir}/python/jinja2:
	@rm -rf $(Jinja)
	tar xzf $(Jinja).tar.gz
	(cd $(Jinja) && \
	  python setup.py build && \
	  rm -rf ${prefix}/lib/python/jinja2 && \
	  cp -R build/lib*/jinja2 ${libdir}/python/ \
	)
	@rm -rf $(Jinja)

${libdir}/python/flask:
	@rm -rf $(Flask)
	tar xzf $(Flask).tar.gz
	(cd $(Flask) && \
	  python setup.py build && \
	  rm -rf ${prefix}/lib/python/flask && \
	  cp -R build/lib*/flask ${libdir}/python/ \
	)
	@rm -rf $(Flask)

clean:
	rm -rf touch.pysqlite $(PySQLite)
	rm -rf touch.sqlalchemy $(SQLAlchemy)
	rm -rf touch.boto $(BOTO) .boto*
	rm -rf touch.flask $(Flask)

