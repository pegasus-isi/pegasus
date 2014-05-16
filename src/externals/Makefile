ifndef ${libdir}
libdir = $(CURDIR)/../../lib/pegasus
endif

PySQLite=pysqlite-2.6.0
BOTO=boto-2.5.2
SQLAlchemy=SQLAlchemy-0.7.6
SleekXMPP=SleekXMPP-1.2.5
need_pysqlite2=$(shell if python -V 2>&1 | grep -qE 'ython 2\.[3-4]'; then echo 1; else echo 0; fi)

all: ${libdir}/python/pysqlite2 ${libdir}/python/sqlalchemy ${libdir}/python/boto ${libdir}/python/sleekxmpp

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
	 cp -R build/lib*/boto ${libdir}/python/ \
	)
	@rm -rf $(BOTO)

${libdir}/python/sleekxmpp:
	@rm -rf $(SleekXMPP)
	tar xzf $(SleekXMPP).tar.gz
	(cd $(SleekXMPP) && \
	 python setup.py build && \
	 cp -R build/lib*/sleekxmpp ${libdir}/python/ \
	)
	@rm -rf $(SleekXMPP)

clean:
	rm -rf $(PySQLite)
	rm -rf $(SQLAlchemy)
	rm -rf $(BOTO)
	rm -rf $(SleekXMPP)

