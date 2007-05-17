#
# Provides a simple scanning facility
#
# This file or a portion of this file is licensed under the terms of
# the Globus Toolkit Public License, found in file GTPL, or at
# http://www.globus.org/toolkit/download/license.html. This notice must
# appear in redistributions of this file, with or without modification.
#
# Redistributions of this Software, with or without modification, must
# reproduce the GTPL in: (1) the Software, or (2) the Documentation or
# some other similar material which is provided with the Software (if
# any).
#
# Copyright 1999-2004 University of Chicago and The University of
# Southern California. All rights reserved.
#
# Author: Jens-S. Vöckler voeckler@cs.uchicago.edu
# Revision : $Revision$
#
package GriPhyN::Scanner;
use 5.006;
use strict;
use warnings;
use Carp;

require Exporter;
our @ISA = qw(Exporter);

# declarations of methods here. Use the commented body to unconfuse emacs
our @classes = qw(EOF VERBATIM INTEGER IDENTIFIER STRING SPECIAL);
our %classes = ( 'EOF' => 0,
		 'VERBATIM' => 1,
		 'INTEGER' => 2,
		 'IDENTIFIER' => 3,
		 'STRING' => 4,
		 'SPECIAL' => 5 );

# Items to export into callers namespace by default. Note: do not export
# names by default without a very good reason. Use EXPORT_OK instead.
# Do not simply export all your public functions/methods/constants.
our $VERSION = '0.1';
$VERSION=$1 if ( '$Revision$' =~ /Revision:\s+([0-9.]+)/o );

our %EXPORT_TAGS = ();
our @EXPORT_OK = qw($VERSION %classes @classes);
our @EXPORT = qw();

# Preloaded methods go here.

#
# c'tor
#
sub new {
    my $proto = shift;
    my $class = ref($proto) || $proto || __PACKAGE__;
    my $file = shift || croak "Need a filename argument";

    local (*FH);
    open( FH, "<$file" ) || croak "open $file: $!";

    # done
    bless { 
	'FH' => *FH, 
	'_open' => 1,
	lineno => 1,
	filename => $file, 
	lookahead => undef }, $class;
}

sub new_from_fh {
    my $proto = shift;
    my $class = ref($proto) || $proto || __PACKAGE__;
    croak "Need a filehandle argument" if ( @_ == 0 );

    # done
    bless { 
	'FH' => $_[0], 
        '_open' => 0,
        lineno => 0,
	filename => undef,
        lookahead => undef }, $class;
}

sub DESTROY {
    my $self = shift;
    close( $self->{FH} ) if ( $self->{'_open'} );
}

sub lookahead {
    my $self = shift;
    my $oldv = $self->{lookahead};
    $self->{lookahead} = shift if @_;
    $oldv;
}

sub lineno {
    my $self = shift;
    my $oldv = $self->{lineno};
    $self->{lineno} = shift if @_;
    $oldv;
}

sub _uscwarn {
    my $self = shift;
    if ( defined $self->{filename} ) {
	warn( 'ERROR: ', $self->{filename}, ':', $self->{lineno}-1, 
	      ": Unterminated string constant\n" );
    } else {
	warn( 'ERROR: line ', $self->{lineno}-1, 
	      ": Unterminated string constant\n" );
    }
}

sub _next {
    my $self = shift;
    my $rsize;
    local($_) = undef;

    if ( ($rsize = read( $self->{FH}, $_, 1 )) == 1 ) {
	# normal operation
	++$self->{lineno} if ( $_ eq "\n" );
    } else {
	unless ( defined $rsize ) {
	    if ( defined $self->{filename} ) {
		warn 'ERROR: While reading ', $self->{filename}, ": $!\n";
	    } else {
		warn "ERROR: While reading: $!\n";
	    }
	}
	undef $_;
    }

    $_;
}

sub next {
    # purpose: returns a common token, ignoring whitespaces and comments
    # paramtr: -
    # returns: always a hash, which at minimum has a key 'class'
    #          ${class}         ${value}
    #          0: EOF            -
    #          1: VERBATIM       single-character token
    #          2: INTEGER        [0-9]+
    #          3: IDENTIFIER     [A-Za-z_][A-Za-z0-9_]*
    #          4: STRING         "..." permits backslash escapes
    #          5: SPECIAL        site => [ eno, raw, gk, cpus ] // cheat
    my $self = shift;
    my $accu;
    local $_;

  REDO:
    if ( defined $self->{lookahead} ) {
	$_ = $self->{lookahead};
	$self->{lookahead} = undef;
    } else {
	$_ = $self->_next;
    }
    return ( class => $classes{EOF} ) unless defined $_;

    if ( /\s/ ) {
	goto REDO;
    } elsif ( $_ eq '#' ) {
	my $collect = '';
	while ( defined $_ && $_ ne "\n" ) {
	    $collect .= $_;
	    $_ = $self->_next;
	}
	if ( $collect =~ /^\# \[(\d+)\] (\S+) \@ (\S+) : (\d+)/o ) {
	    my $raw = $2;
	    $raw =~ tr/-/_/;
	    return ( class => $classes{SPECIAL}, value => $raw, 
		     extra => [ $1, $2, $3, $4 ] );
	}
	goto REDO;
    } elsif ( /\d/ ) {
	$accu = $_;
	for (;;) {
	    if ( defined ($_=$self->_next) && /\d/ ) {
		$accu .= $_;
	    } else { 
		$self->{lookahead} = $_;
		last;
	    }
	}
	return ( class => $classes{INTEGER}, value => $accu )
    } elsif ( /[A-Za-z_]/ ) {
	$accu = $_;
	for (;;) {
	    if ( defined ($_ = $self->_next) && /[A-Za-z0-9_]/ ) {
		$accu .= $_;
	    } else {
		$self->{lookahead} = $_;
		last;
	    }
	}
	return ( class => $classes{IDENTIFIER}, value => $accu )
    } elsif ( $_ eq '"' ) {
	$accu = '';
	for (;;) {
	    my $state = 0;
	    if ( defined ($_ = $self->_next) ) {
		if ( $state == 0 ) {
		    # regular state
		    if ( $_ eq "\n" ) {
			$self->_uscwarn;
			last;
		    } elsif ( $_ eq "\\" ) {
			$state = 1;
		    } elsif ( $_ eq '"' ) {
			last;
		    } else {
			$accu .= $_;
		    }
		} else {
		    # backslash escapes
		    $accu .= $_;
		    $state = 0;
		}
	    } else {
		$self->_uscwarn;
		last;
	    }
	}
	return ( class => $classes{STRING}, value => $accu )
    } else {
	return ( class => $classes{VERBATIM}, value => $_ )
    }
}

# must
1;
