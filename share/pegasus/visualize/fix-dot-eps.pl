#!/usr/bin/env perl
#
# This program fixes the ps2 type output of graphviz's dot and
# tries to make it well-behaved eps
#
use 5.006;
use strict;

my $head = <STDIN>;		# ignore original header
print "%!PS-Adobe-3.0 EPSF-2.0\n"; # write fake EPSF header

my (@bb,$bb);
while ( <> ) {
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
