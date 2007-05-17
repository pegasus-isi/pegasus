package GriPhyN::SC::old;
#
# old-style multi-column formatted pool.config file
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
# $Id: old.pm,v 1.3 2005/09/29 22:41:40 griphyn Exp $
#
use 5.006;
use strict;
use vars qw($VERSION);
use Exporter;

our @ISA = qw(Exporter GriPhyN::SC);
our @EXPORT = qw();
our @EXPORT_OK = qw($VERSION);

$VERSION=$1 if ( '$Revision: 1.3 $' =~ /Revision:\s+([0-9.]+)/o );

#
# --- start -----------------------------------------------------
#
use Data::Dumper;
use Carp;
use File::Spec;
use GriPhyN::Log qw(log);
use GriPhyN::WF qw/parse_properties/;

our $prefix = '[' . __PACKAGE__ . '] ';

sub add_vector(\@$) {
    my $vref = shift;
    my $what = shift;

    push( @$vref, $what ) if ( (grep { /$what/ } @$vref) == 0 );
}

sub add_gridftp(\@\@$$) {
    my $gftp = shift;
    my $vref = shift;
    my $uri = shift;
    my $jm = shift;
    push( @$vref, [ GriPhyN::SC::break_uri($uri), undef, 
		    GriPhyN::SC::major_from_contact($jm), undef ] )
	if ( (grep { /$uri/ } @$gftp) == 0 );
    push( @$gftp, $uri );
}

sub parse_old($) {
    my $fn = shift;

    my ($x,@x,%result,@gftp);
    if ( open( IN, "<$fn" ) ) {
	log( $prefix, "reading from $fn" ) if ( $main::DEBUG & 0x10 );
	my $lastsite = '';
	while ( <IN> ) {
	    s/[ \t\r\n]*$//;	# trim rear
	    if ( /^\# \[(\d+)\] (\S+) \@ (\S+) : (\d+)/o ) {
		# special comment
		my $raw = $2;
		$raw =~ tr/-/_/;
		$result{$raw}->{ncpus} = $4;
		$result{$raw}->{special} = [ $1, $2, $3, $4 ];
	    }
	    s/\#.*//;		# remove comments
	    s/^\s*//;		# trim front
	    next if length($_) < 10; # skip virtually empty lines

	    @x = split ;
	    if ( @x == 7 ) {
		undef $x[6] if lc($x[6]) eq 'null';
		undef $x[5] if lc($x[5]) eq 'null';

		$result{$x[0]} = { } unless exists $result{$x[0]};
		@gftp = () if $x[0] ne $lastsite;
		$lastsite = $x[0];

		$x = $result{$x[0]}; # start position
		$x->{gridshell} = $x[5] if defined $x[5];
                add_vector( @{$x->{lrc}}, $x[6] ) if defined $x[6];
		$x->{workdir} = $x[4];
                add_gridftp( @gftp, @{$x->{gridftp}}, $x[3], $x[2] );
		push( @{$x->{contact}->{$x[1]}}, 
		      [ $x[2], undef, 
			GriPhyN::SC::major_from_contact($x[2]) ] );
	    } else {
		carp( $prefix, "$fn:$.: invalid number of columns\n" );
	    }
	}
	close(IN);
    } else {
	croak $prefix, "open textual SC file $fn: $!\n";
    }

    GriPhyN::SC::sanitize( %result );
    %result;
}

#
# ctor
#
sub new {
    my $proto = shift;
    my $class = ref($proto) || $proto || __PACKAGE__;
    my $self = bless GriPhyN::SC->new( style => 'old', @_ ), $class;
    $self->{'_permitted'}->{file} = 1;

    if ( exists $self->{file} && ! defined $self->{file} ) {
	# special case file => undef
	$self->{pool} = { };
    } else {
	# normal case
	my $file = $self->file ||
	    croak $prefix, "no location specified (missing property ...file)"; 
	$self->{pool} = { parse_old($file) };
    }

    # return handle to self
    $self;
}

sub DESTROY {
    # empty
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
    foreach my $universe ( reverse sort keys %{ $sref->{contact} } ) {
	foreach my $jm ( @{$sref->{contact}{$universe}} ) {
	    foreach my $ftp (  @{ $sref->{gridftp} } ) {
		printf $fh "$site\t%-9s %s", $universe, $jm->[0];
		print $fh "\t", $ftp->[0], $ftp->[1];
		print $fh "\t", $sref->{workdir};
		print $fh "\t", $sref->{gridshell} || 'null';
		if ( $universe eq 'vanilla' ) {
		    print $fh "\t", $sref->{lrc}[0] || 'null';
		} else {
		    print $fh "\tnull";
		}
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
