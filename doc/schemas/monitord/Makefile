# Makefile for monitoring event Yang schemas
#
# Prerequisites:
# 	pyang - http://code.google.com/p/pyang/
#

# Schema directory is by default relative to
# this directory
schemadir := ../../../share/pegasus/schema

# Python Yang (pyang) setup
# Yang should be installed by user
#PYANG_HOME := $(HOME)/local
#yang2dsdl := PYANG_XSLT_DIR=$(PYANG_HOME)/share/yang/xslt	\
	     PYANG_RNG_LIBDIR=$(PYANG_HOME)/share/yang/schema \
	     yang2dsdl

yang2dsdl := yang2dsdl

# Pick a schema version
schema_ver=2011-05-04

default: validate

# Transform BP log to XML
transform: output-file.xml

%.xml: %.bp
	nl_load xml pretty=true netconf=true < output-file.bp > output-file.xml

# Validate XML
validate: transform
	$(yang2dsdl) -v output-file.xml $(schemadir)/stampede-schema.yang 

# Clean up
clean:
	/bin/rm -f *.dsrl *.rng *.sch *.xml 
