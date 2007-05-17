#! /bin/sh

# $Revision$

#
# setwordtemplate $file
#
# Updates the argument file to point to a new word template
# (if it had an exsting one)
# FIXME: indicate an error if the attachedTemplate element doesnt exist
#

wordtemplate=VDSUGTemplate.dot

# set "attached template" to the local version of the UG word .dot template:

sed -i -e "s@<w:attachedTemplate w:val=\"[^\"]\+@<w:attachedTemplate w:val=\"$wordtemplate@" $1
