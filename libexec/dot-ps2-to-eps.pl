#!/usr/bin/env perl
#
# This program fixes the ps2 type output of graphviz's dot and
# attempts to make it a better-behaved eps file.
#
# $Id$
#
# Usage: dot -Tps2 foo.dot | $0 > foo.eps
#
use 5.006;
use strict;

# 
# skip first line - original PS header written by dot
#
my $head = <STDIN>;

#
# write our own fake PS header with EPS extensions
#
print "%!PS-Adobe-3.0 EPSF-2.0\n";

#
# read rest of file for bounding boxes and page sizes
#
my (@bb,$bb);
while ( <STDIN> ) {
    if ( /\%\%PageBoundingBox: ([0-9.]+) ([0-9.]+) ([0-9.]+) ([0-9.]+)/ ) {
	@bb = ( $1, $2, $3, $4 );
	$bb = "$1 $2 $3 $4";
    } elsif ( m{/PageSize} ) {
	s{/PageSize \[\S+ [0-9.]+\]}{/PageSize \[$bb[2] $bb[3]\]};
    } elsif ( m{^\S+ \S+ \S+ \S+ boxprim clip newpath}o ) {
	$_ = "$bb boxprim clip newpath\n";
    }

    print ;
}
