SHELL := /bin/bash

ifndef $(prefix)
    prefix:=$(shell cd $(CURDIR)/../.. && pwd)
endif

PYTHON:=$(shell $(CURDIR)/../../release-tools/get-system-python)
OSID:=$(shell $(CURDIR)/../../release-tools/getosid)

EXTDIR=$(prefix)/lib/pegasus/externals
LIBDIR=$(EXTDIR)/python

PACKAGES=setuptools
PACKAGES+=sqlalchemy
PACKAGES+=attrs
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
PACKAGES+=pamela
PACKAGES+=six
PACKAGES+=chardet
PACKAGES+=idna
PACKAGES+=certifi
PACKAGES+=urllib3
PACKAGES+=pyjwt
PACKAGES+=pika
ifeq ($(shell python --version 2>&1 | grep " 2.6"),)
    PACKAGES+=globus_sdk
else
    $(warning WARNING: globus_sdk does not work with Python 2.6: skipping globus_sdk library)
endif

PYOPENSSL_ARGS=
ifeq ($(OSID),macos10)
    OPENSSL:=$(shell brew --prefix openssl)
    ifneq ($(OPENSSL),)
        PYOPENSSL_ARGS=-I $(OPENSSL)/include -L $(OPENSSL)/lib
    endif
endif

all: $(addprefix $(LIBDIR)/,$(PACKAGES))

$(LIBDIR)/%:
	$(eval PACKDIR:=$(shell echo $< | sed -E 's/\.tar\.gz|\.zip//'))
	@echo Building $< &&\
	export LANG=en_US.UTF-8 && \
	mkdir -p $(LIBDIR) &&\
	if (echo "$<" | grep tar.gz) >/dev/null; then tar xzf $< ; else unzip -q -o $< ; fi &&\
	pushd $(PACKDIR) > /dev/null &&\
	$(PYTHON) setup.py install_lib -d $(LIBDIR) > /dev/null &&\
	popd > /dev/null &&\
	rm -rf $(PACKDIR) &&\
	touch $@

$(LIBDIR)/attrs: attrs-19.3.0.tar.gz
$(LIBDIR)/boto: boto-2.48.0.tar.gz
$(LIBDIR)/certifi: python-certifi-2017.11.05.tar.gz
$(LIBDIR)/chardet: chardet-3.0.4.tar.gz
$(LIBDIR)/click: click-6.7.tar.gz
$(LIBDIR)/flask_cache: Flask-Cache-0.13.1.tar.gz
$(LIBDIR)/flask: Flask-0.12.4.tar.gz
$(LIBDIR)/flask_sqlalchemy.py: Flask-SQLAlchemy-0.16.tar.gz
$(LIBDIR)/globus_sdk: globus-sdk-python-1.4.1.tar.gz
$(LIBDIR)/idna: idna-2.6.tar.gz
$(LIBDIR)/itsdangerous.py: itsdangerous-0.24.tar.gz
$(LIBDIR)/jinja2: Jinja2-2.8.1.tar.gz
$(LIBDIR)/markupsafe: MarkupSafe-1.0.tar.gz
$(LIBDIR)/pamela: pamela-1.0.0.tar.gz
$(LIBDIR)/pika: pika-1.1.0.tar.gz
$(LIBDIR)/pyjwt: pyjwt-1.5.3.tar.gz
$(LIBDIR)/requests: requests-2.18.4.tar.gz
$(LIBDIR)/setuptools: setuptools-45.3.0.zip
$(LIBDIR)/six: six-1.12.0.tar.gz
$(LIBDIR)/sqlalchemy: SQLAlchemy-1.3.10.tar.gz
$(LIBDIR)/urllib3: urllib3-1.22.tar.gz
$(LIBDIR)/werkzeug: Werkzeug-0.14.1.tar.gz

clean:
	rm -rf $(EXTDIR)

