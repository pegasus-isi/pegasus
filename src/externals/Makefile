SHELL := /bin/bash

ifndef $(prefix)
    prefix:=$(shell cd $(CURDIR)/../.. && pwd)
endif

PYTHON:=$(shell $(CURDIR)/../../release-tools/get-system-python)
OSID:=$(shell $(CURDIR)/../../release-tools/getosid)

EXTDIR=$(prefix)/lib/pegasus/externals
LIBDIR=$(EXTDIR)/python

PACKAGES+=flask
PACKAGES+=flask_caching
PACKAGES+=jinja2
PACKAGES+=markupsafe
PACKAGES+=pyjwt
#PACKAGES+=yaml
PACKAGES+=sqlalchemy
PACKAGES+=werkzeug
PACKAGES+=attrs
PACKAGES+=boto3
PACKAGES+=botocore
PACKAGES+=certifi
PACKAGES+=chardet
PACKAGES+=click
PACKAGES+=docutils
PACKAGES+=globus_sdk
PACKAGES+=idna
PACKAGES+=itsdangerous.py
PACKAGES+=jmespath
PACKAGES+=pamela
PACKAGES+=pika
PACKAGES+=pycparser
PACKAGES+=dateutil
PACKAGES+=requests
PACKAGES+=s3transfer
PACKAGES+=six
PACKAGES+=urllib3


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

$(LIBDIR)/flask: Flask-1.1.1.tar.gz
$(LIBDIR)/flask_caching: Flask-Caching-1.8.0.tar.gz
$(LIBDIR)/jinja2: Jinja2-2.11.1.tar.gz
$(LIBDIR)/markupsafe: MarkupSafe-1.1.1.tar.gz
$(LIBDIR)/pyjwt: PyJWT-1.7.1.tar.gz
#$(LIBDIR)/yaml: PyYAML-5.3.1.tar.gz
$(LIBDIR)/sqlalchemy: SQLAlchemy-1.3.15.tar.gz
$(LIBDIR)/werkzeug: Werkzeug-1.0.0.tar.gz
$(LIBDIR)/attrs: attrs-19.3.0.tar.gz
$(LIBDIR)/boto3: boto3-1.12.37.tar.gz
$(LIBDIR)/botocore: botocore-1.15.37.tar.gz
$(LIBDIR)/certifi: certifi-2019.11.28.tar.gz
$(LIBDIR)/chardet: chardet-3.0.4.tar.gz
$(LIBDIR)/click: click-7.1.1.tar.gz
$(LIBDIR)/docutils: docutils-0.15.2.tar.gz
$(LIBDIR)/globus_sdk: globus-sdk-1.4.1.tar.gz
$(LIBDIR)/idna: idna-2.9.tar.gz
$(LIBDIR)/itsdangerous.py: itsdangerous-1.1.0.tar.gz
$(LIBDIR)/jmespath: jmespath-0.9.5.tar.gz
$(LIBDIR)/pamela: pamela-1.0.0.tar.gz
$(LIBDIR)/pika: pika-1.1.0.tar.gz
$(LIBDIR)/pycparser: pycparser-2.20.tar.gz
$(LIBDIR)/dateutil: python-dateutil-2.8.1.tar.gz
$(LIBDIR)/requests: requests-2.23.0.tar.gz
$(LIBDIR)/s3transfer: s3transfer-0.3.3.tar.gz
$(LIBDIR)/six: six-1.14.0.tar.gz
$(LIBDIR)/urllib3: urllib3-1.25.8.tar.gz

clean:
	rm -rf $(EXTDIR)

