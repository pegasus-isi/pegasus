package GriPhyN::TC::old;
#
# old-style multi-column formatted tc.data file
# VDS property vds.tc.mode OldFile
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
# Revision : $Revision: 1.3 $
#
# $Id: old.pm,v 1.3 2005/11/17 16:34:41 voeckler Exp $
#
use 5.006;
use strict;
use Exporter;
use GriPhyN::TC;
use vars qw/$VERSION/;

our @ISA = qw(Exporter GriPhyN::TC);
our @EXPORT = qw();
our @EXPORT_OK = qw($VERSION);

$VERSION=$1 if ( '$Revision: 1.3 $' =~ /Revision:\s+([0-9.]+)/o );

#
# --- start -----------------------------------------------------
#
use Carp;
use File::Spec;
use GriPhyN::WF qw/parse_properties/;
use GriPhyN::Log qw/log/;

our $prefix = '[' . __PACKAGE__ . '] ';

#
# ctor
#
sub new {
    my $proto = shift;
    my $class = ref($proto) || $proto || __PACKAGE__;
    my $self = bless GriPhyN::TC->new( style => 'old', @_ ), $class;
    $self->{'_permitted'}->{file} = 1;
    
    if ( exists $self->{file} && ! defined $self->{file} ) {
	# special case file => undef
	$self->{pool} = { };
    } else {
	# normal case
	my $file = $self->file || 
	    croak $prefix, "no location specified (missing property ...file)"; 
	$self->parse_old($file);
    }

    # return handle to self
    $self;
}

sub DESTROY {
    # empty
}

# Autoload methods go after =cut, and are processed by the autosplit program.

sub parse_old {
    my $self = shift;
    my $fn = shift;

    my ($x,@x,%result,%seen);
    if ( open( IN, "<$fn" ) ) {
	log( $prefix, "reading from $fn" ) if ( $main::DEBUG & 0x10 );
	while ( <IN> ) {
	    s/[ \t\r\n]*$//;	# trim rear
	    if ( /^\# \[(\d+)\] (\S+) \@ (\S+) : (\d+)/o ) {
		# special comment
		my $raw = $2;
		$raw =~ tr/-/_/;
		$self->{pool}->{$raw}->{special} = [ $1, $2, $3, $4 ];
	    }
	    s/\#.*//;		# remove comments
	    s/^\s*//;		# trim front
	    next if length($_) < 10; # skip virtually empty lines

	    @x = split( /\s+/, $_, 4);
	    if ( @x >= 4 ) {
		$x = [ $x[2], { }, 'INSTALLED', 'null' ];
		if ( lc($x[3]) ne 'null' ) {
		    foreach my $env ( split(';',$x[3]) ) {
			my ($k,$v) = split '=', $env, 2;
			$x->[1]{env}{$k} = $v;
		    }
		}
		
		$self->{pool}->{$x[0]}->{xform}->{$x[1]} = $x;
		GriPhyN::TC::_add( @{$self->{xform}->{$x[1]}}, $x[0] );
	    } elsif ( @x == 3 ) { 
		$x = [ $x[2], { } ];
		$self->{pool}->{$x[0]}->{xform}->{$x[1]} = $x;
		GriPhyN::TC::_add( @{$self->{xform}->{$x[1]}}, $x[0] );
	    } else {
		carp( $prefix, "$fn:$.: invalid number of columns\n" );
		next;
	    }

#	    # logging
#	    if ( @x >= 3 && ($main::DEBUG & 0x400) > 0 ) {
#		unless ( exists $seen{$x[0]} ) {
#		    log( "TC: seen site $x[0]" );
#		    $seen{$x[0]} = 1;
#		}
#	    }
	}
	close(IN);
    } else {
	croak( $prefix, "open textual TC file $fn: $!\n" );
    }

    %result;
}


sub show_site {
    # purpose: dumps one site entry into a stream
    # paramtr: $fh (IN): filehandle open for writing
    #          $site (IN): site handle to show
    # returns: -
    my $self = shift;
    my $fh = shift || croak $prefix, "need an open filehandle";
    croak $prefix, "need an open filehandle" unless ref $fh;
    my $site = shift || croak $prefix, "need a site parameter";
    return unless exists $self->{pool}->{$site};
    my $sref = $self->{pool}->{$site};

    # preamble
    if ( exists $sref->{special} ) {
	print $fh "#\n";
	printf $fh "# [%u] %s @ %s : %u\n", @{$sref->{special}};
	print $fh "#\n";
    }

    # entries
    foreach my $tr ( sort keys %{ $sref->{xform} } ) {
	print $fh "$site\t$tr\t", $sref->{xform}->{$tr}->[0]; # app
	if ( defined $sref->{xform}->{$tr}->[1] && 
	     exists $sref->{xform}->{$tr}->[1]->{env} ) {
	    my $flag = 0;
	    foreach my $key ( keys %{ $sref->{xform}->{$tr}->[1]->{env} } ) {
		print $fh ( $flag ? ';' : ' ' );
		$flag = 1;
		print $fh $key, '=', $sref->{xform}->{$tr}->[1]->{env}->{$key};
	    }
	}
	print $fh "\n";
    }
}

#
# return 'true' to package loader
#
1;
__END__
