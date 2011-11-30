#!/usr/bin/env perl
use 5.005;
use strict;

my $handle = shift || die "need a pool handle to look for.\n";
my $source = shift || die "need the location of the pool config file\n";
open( IN, "<$source" ) || die "open($source): $!\n";

my $comment = '#.*';
my (@x,$found);
while ( <IN> ) {
    s/\r*\n*$//;
    s/$comment//;
    next unless 1 <= length ;
    @x = split /\s+/;
    if ( $x[0] eq $handle && $x[1] eq 'transfer' ) {
	$x[3] .= '/' unless $x[3] =~ m:/$:;	# 
	print "$x[3]\n";
	$found=1;
	last;
    }
}
close(IN);
exit( 1 - $found );
