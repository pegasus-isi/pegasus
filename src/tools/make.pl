#!/usr/bin/env perl
#
# prefer gmake over make -- ant can be so difficult!
#
require 5.005;
use strict;
use File::Spec;

sub find_exec($) {
    # purpose: determine location of given binary in $PATH
    # returns: fully qualified path to binary, undef if not found
    my $program = shift;
    local($_);
    foreach ( File::Spec->path ) {
        my $fs = File::Spec->catfile( $_, $program );
        return $fs if -x $fs;
    }
    undef;
}

my $make = find_exec('gmake') || find_exec('make');
exec { $make } $make, @ARGV if defined $make;
exit 127;
