
ifndef $(prefix)
    prefix=$(shell cd $(CURDIR)/../.. && pwd)
endif

EXTDIR=$(prefix)/lib/pegasus/externals
LIBDIR=$(EXTDIR)/python

BUILDPKG=$(CURDIR)/buildpackage.sh

# We always use SQLAlchemy
PACKAGES=sqlalchemy

ifneq ($(shell python -V 2>&1 | grep -E 'ython 2\.[3-6]'),)
    PACKAGES+=ordereddict.py
endif

ifneq ($(shell python -V 2>&1 | grep -E 'ython 2\.[3-4]'),)
    # Install alternative dependencies for python 2.4
    PACKAGES+=pysqlite2
else
    # For other python >= 2.5, install the service dependencies
    PACKAGES+=flask
    PACKAGES+=flask_cache
    PACKAGES+=flask_sqlalchemy.py
    PACKAGES+=jinja2
    PACKAGES+=markupsafe
    PACKAGES+=wtforms
    PACKAGES+=werkzeug
    PACKAGES+=requests
    PACKAGES+=itsdangerous.py
    PACKAGES+=boto
    PACKAGES+=pam.py
    PACKAGES+=OpenSSL
    PACKAGES+=plex
    ifeq ($(shell which pg_config),)
        $(warning WARNING: pg_config not found: skipping python postgresql library)
    else
        PACKAGES+=psycopg2
    endif
    ifeq ($(shell which mysql_config),)
        $(warning WARNING: mysql_config not found: skipping python mysql library)
    else
        PACKAGES+=MySQLdb
    endif
endif

all: $(addprefix $(LIBDIR)/,$(PACKAGES))

$(LIBDIR)/flask: Flask-0.10.tar.gz
	$(BUILDPKG) $^ $(LIBDIR) flask

$(LIBDIR)/flask_cache: Flask-Cache-0.13.1.tar.gz
	$(BUILDPKG) $^ $(LIBDIR) flask_cache

$(LIBDIR)/flask_sqlalchemy.py: Flask-SQLAlchemy-0.16.tar.gz
	$(BUILDPKG) $^ $(LIBDIR) flask_sqlalchemy.py

$(LIBDIR)/jinja2: Jinja2-2.7.tar.gz
	$(BUILDPKG) $^ $(LIBDIR) jinja2

$(LIBDIR)/markupsafe: MarkupSafe-0.18.tar.gz
	$(BUILDPKG) $^ $(LIBDIR) markupsafe

$(LIBDIR)/wtforms: WTForms-1.0.3.tar.gz
	$(BUILDPKG) $^ $(LIBDIR) wtforms

$(LIBDIR)/werkzeug: Werkzeug-0.9.3.tar.gz
	$(BUILDPKG) $^ $(LIBDIR) werkzeug

$(LIBDIR)/requests: requests-1.2.3.tar.gz
	$(BUILDPKG) $^ $(LIBDIR) requests

$(LIBDIR)/itsdangerous.py: itsdangerous-0.21.tar.gz
	$(BUILDPKG) $^ $(LIBDIR) itsdangerous.py

$(LIBDIR)/boto: boto-2.5.2.tar.gz
	$(BUILDPKG) $^ $(LIBDIR) boto

ifneq ($(shell python -V 2>&1 | grep -E 'ython 2\.[3-4]'),)
# We use an old version of sqlalchemy for python 2.3 and 2.4
$(LIBDIR)/sqlalchemy: SQLAlchemy-0.7.6.tar.gz
	$(BUILDPKG) $^ $(LIBDIR) sqlalchemy
else
$(LIBDIR)/sqlalchemy: SQLAlchemy-0.8.0.tar.gz
	$(BUILDPKG) $^ $(LIBDIR) sqlalchemy
endif

$(LIBDIR)/pam.py: pam-0.1.4.tar.gz
	$(BUILDPKG) $^ $(LIBDIR) pam.py

$(LIBDIR)/OpenSSL: pyOpenSSL-0.13.tar.gz
	$(BUILDPKG) $^ $(LIBDIR) OpenSSL

$(LIBDIR)/plex: plex-2.0.0dev.tar.gz
	$(BUILDPKG) $^ $(LIBDIR) plex

$(LIBDIR)/MySQLdb: MySQL-python-1.2.5.tar.gz
	$(BUILDPKG) $^ $(LIBDIR) MySQLdb

$(LIBDIR)/psycopg2: psycopg2-2.6.tar.gz
	$(BUILDPKG) $^ $(LIBDIR) psycopg2

$(LIBDIR)/pysqlite2: pysqlite-2.6.0.tar.gz
	$(BUILDPKG) $^ $(LIBDIR) pysqlite2

$(LIBDIR)/ordereddict.py: ordereddict-1.1.tar.gz
	$(BUILDPKG) $^ $(LIBDIR) ordereddict.py

clean:
	rm -rf $(EXTDIR)

