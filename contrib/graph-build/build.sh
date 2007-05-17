#!/bin/sh
#
# Constructs the most common use case
#
perl build.pl $VDS_HOME/build.xml > build.dot && dot -Tpng build.dot > build.png
