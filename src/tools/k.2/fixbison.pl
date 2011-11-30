#!/usr/bin/env perl
#
# fix bug in bison 1.875 generated output
#
require 5.005;
use strict;

my $state;
while ( <> ) {
    if ( ! $state &&
	 m{Suppress GCC warning that yyerrlab1 is unused when no action} ) {
	$state=1;
	$_ = "#if 0\n$_";
    } elsif ( $state == 1 &&
	      m'#endif' ) {
	undef $state;
	$_ .= "#endif\n";
    }
    print;
}
