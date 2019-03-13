SHELL := /bin/bash

ifndef $(prefix)
    prefix:=$(shell cd $(CURDIR)/../.. && pwd)
endif

PYTHON:=$(shell $(CURDIR)/../../release-tools/get-system-python)
OSID:=$(shell $(CURDIR)/../../release-tools/getosid)

EXTDIR=$(prefix)/lib/pegasus/externals
LIBDIR=$(EXTDIR)/python

PACKAGES=sqlalchemy

ifneq ($(shell $(PYTHON) -V 2>&1 | grep -E 'ython 2\.[3-6]'),)
    PACKAGES+=ordereddict.py
endif

PACKAGES+=argparse
PACKAGES+=click
PACKAGES+=flask
PACKAGES+=flask_cache
PACKAGES+=flask_sqlalchemy.py
PACKAGES+=jinja2
PACKAGES+=markupsafe
PACKAGES+=werkzeug
PACKAGES+=requests
PACKAGES+=itsdangerous.py
PACKAGES+=boto
PACKAGES+=pam.py
#PACKAGES+=OpenSSL
PACKAGES+=six
#PACKAGES+=enum34
#PACKAGES+=cffi
#PACKAGES+=cryptography
PACKAGES+=plex
PACKAGES+=future
PACKAGES+=chardet
PACKAGES+=idna
PACKAGES+=certifi
PACKAGES+=urllib3
PACKAGES+=pyjwt
ifeq ($(shell python --version 2>&1 | grep " 2.6"),)
    PACKAGES+=globus_sdk
else
    $(warning WARNING: globus_sdk does not work with Python 2.6: skipping globus_sdk library)
endif

#ifeq ($(shell which mysql_config),)
#    $(warning WARNING: mysql_config not found: skipping python mysql library)
#else
#    PACKAGES+=MySQLdb
#endif
PACKAGES+=influxdb
PACKAGES+=pika
PACKAGES+=dateutil
PACKAGES+=six
PACKAGES+=chardet
PACKAGES+=idna
PACKAGES+=certifi
PACKAGES+=urllib3
PACKAGES+=pytz
PACKAGES+=pyjwt
PACKAGES+=globus_sdk

PYOPENSSL_ARGS=
ifeq ($(OSID),macos10)
    OPENSSL:=$(shell brew --prefix openssl)
    ifneq ($(OPENSSL),)
        PYOPENSSL_ARGS=-I $(OPENSSL)/include -L $(OPENSSL)/lib
    endif
endif

all: $(addprefix $(LIBDIR)/,$(PACKAGES))

$(LIBDIR)/%:
	$(eval TARNAME := $(subst .tar.gz,,$<))
	@echo Building $< &&\
	mkdir -p $(LIBDIR) &&\
	tar xzf $< &&\
	pushd $(TARNAME) > /dev/null &&\
	if [ -d "src/OpenSSL" ]; then $(PYTHON) setup.py build_ext $(PYOPENSSL_ARGS) > /dev/null; fi &&\
	if [ -d "src/cryptography" ]; then $(PYTHON) setup.py build_ext $(PYOPENSSL_ARGS) > /dev/null; fi &&\
	$(PYTHON) setup.py install_lib -d $(LIBDIR) > /dev/null &&\
	popd > /dev/null &&\
	rm -rf $(TARNAME) &&\
	touch $@

$(LIBDIR)/argparse: argparse-1.4.0.tar.gz
$(LIBDIR)/click: click-6.7.tar.gz
$(LIBDIR)/sqlalchemy: SQLAlchemy-0.8.0.tar.gz
$(LIBDIR)/flask: Flask-0.12.4.tar.gz
$(LIBDIR)/flask_cache: Flask-Cache-0.13.1.tar.gz
$(LIBDIR)/flask_sqlalchemy.py: Flask-SQLAlchemy-0.16.tar.gz
$(LIBDIR)/jinja2: Jinja2-2.8.1.tar.gz
$(LIBDIR)/markupsafe: MarkupSafe-1.0.tar.gz
$(LIBDIR)/werkzeug: Werkzeug-0.14.1.tar.gz
$(LIBDIR)/requests: requests-2.18.4.tar.gz
$(LIBDIR)/itsdangerous.py: itsdangerous-0.24.tar.gz
$(LIBDIR)/boto: boto-2.48.0.tar.gz
$(LIBDIR)/pam.py: pam-0.1.4.tar.gz
$(LIBDIR)/OpenSSL: pyOpenSSL-16.2.0.tar.gz
$(LIBDIR)/enum34: enum34-1.1.6.tar.gz
$(LIBDIR)/cffi: cffi-1.10.0.tar.gz
$(LIBDIR)/cryptography: cryptography-2.0.3.tar.gz
$(LIBDIR)/plex: plex-2.0.0dev.tar.gz
$(LIBDIR)/ordereddict.py: ordereddict-1.1.tar.gz
$(LIBDIR)/future: future-0.16.0.tar.gz
$(LIBDIR)/influxdb: influxdb-4.1.1.tar.gz
$(LIBDIR)/pika: pika-0.11.2.tar.gz
$(LIBDIR)/dateutil: python-dateutil-2.6.1.tar.gz
$(LIBDIR)/six: six-1.11.0.tar.gz
$(LIBDIR)/chardet: chardet-3.0.4.tar.gz
$(LIBDIR)/idna: idna-2.6.tar.gz
$(LIBDIR)/certifi: python-certifi-2017.11.05.tar.gz
$(LIBDIR)/urllib3: urllib3-1.22.tar.gz
$(LIBDIR)/pytz: pytz-2017.3.tar.gz
$(LIBDIR)/pyjwt: pyjwt-1.5.3.tar.gz
$(LIBDIR)/globus_sdk: globus-sdk-python-1.4.1.tar.gz


clean:
	rm -rf $(EXTDIR)

