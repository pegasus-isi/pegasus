#!/bin/sh
#
# Constructs the most common use case
#
dir=`dirname $0`;
antfile=$1;
perl $dir/ant2dot.pl $antfile > build.dot && dot -Tpng build.dot > build.png
