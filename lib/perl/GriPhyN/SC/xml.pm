package GriPhyN::SC::xml;
#
# XML formatted pool.config file
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
# Author: Jens-S. Vöckler voeckler at cs dot uchicago dot edu
# Revision : $Revision$
#
# $Id$
#
use 5.006;
use strict;
use vars qw($VERSION);
use Exporter;

our @ISA = qw(Exporter GriPhyN::SC);
our @EXPORT = qw();
sub parse_xml($);		# { }
our @EXPORT_OK = qw($VERSION parse_xml);

$VERSION=$1 if ( '$Revision$' =~ /Revision:\s+([0-9.]+)/o );

#
# --- start -----------------------------------------------------
#
use Carp;
use GriPhyN::Log qw(log);
use XML::Parser::Expat;

our $prefix = '[' . __PACKAGE__ . '] ';

sub parse_xml($) {
    # purpose: parse XML file
    # paramtr: $fn (IN): filename
    # returns: deep structure
    my $fn = shift;
    my (%result,@stack,$site,$profns,$profid);

    my $parser = new XML::Parser::Expat( 'ProtocolEncoding' => 'ISO-8859-1',
					 'Namespaces' => 1 );
    $parser->setHandlers( 'Start' => sub { 
	my $self = shift;
	my $element = shift;	# name of element
	my %attr = @_;		# attributes
	push( @stack, $element );

	if ( @stack == 1 ) {
	    # <config>
	    die( "Illegal XML root element $element\n" )
		if ( $element ne 'config' );
	} elsif ( @stack == 2 ) {
	    # <pool ...>
	    die( "Illegal XML element $element under <config>\n" )
		if ( $element ne 'pool' );

	    $site = $attr{handle};
	    if ( exists $result{$site} ) {
		# permitted: ncpus only
		my @keyset = keys %{$result{$site}};
		log( "site $site already exists, merging info!\n" )
		    if ( ! ( @keyset == 2 && ( $keyset[0] eq 'ncpus' || 
					       $keyset[0] eq 'special' ) ) );
	    } else {
		# new (empty) entry
		#$result{$site} = { };
		%{$result{$site}} = 7; # dimen hashsize
	    }
	    $result{$site}->{gridshell} = $attr{gridlaunch}
	        if ( exists $attr{gridlaunch} &&
		     length($attr{gridlaunch}) > 0 );
	    # use defaults
	    $result{$site}->{sysinfo} = $attr{sysinfo} || 'INTEL32::LINUX';
	} else {
	    # inside pool
	    if ( $element eq 'workdirectory' ) {
		# ignore -- value is text
	    } elsif ( $element eq 'lrc' ) {
		push( @{$result{$site}->{lrc}}, $attr{url} )
		    if ( exists $attr{url} && length($attr{url}) > 0 );
	    } elsif ( $element eq 'gridftp' ) {
		push( @{$result{$site}->{gridftp}}, 
		      [ $attr{url}, $attr{storage}, 
			$attr{major}, $attr{minor}, 
			$attr{patch} || $attr{plevel} ] )
		    if ( exists $attr{url}  && exists $attr{storage} &&
			 length($attr{url}) && length($attr{storage}) );
	    } elsif ( $element eq 'jobmanager' ) {
		push( @{$result{$site}->{contact}->{$attr{universe}}},
		      [ $attr{url}, $attr{type}, # may be undef
			$attr{major}, $attr{minor}, 
			$attr{patch} || $attr{plevel} ] )
		    if ( exists $attr{universe}  && exists $attr{url} &&
			 length($attr{universe}) && length($attr{url}) )
	    } elsif ( $element eq 'profile' ) {
		$profns = $attr{namespace};
		$profid = $attr{key}; 
	    } else {
		warn "Warning: Ignoring unknown element $element\n";
	    }
	}
	1;
    }, 'End' => sub { 
	my $self = shift;
	my $element = shift;
	if ( @stack == 2 ) {
	    # log( "seen site $site" );
	    undef $site;
	} elsif ( @stack == 3 && $stack[2] eq 'profile' ) {
	    undef $profns;
	    undef $profid;
	}
	pop( @stack ) eq $element;
    }, 'Char' => sub { 
	my $self = shift;
	my $text = shift;
	
	if ( $text =~ /^\s+$/ ) {
	    # ignore
	} else {
	    if ( @stack == 3 ) {
		if ( $stack[2] eq 'workdirectory' ) {
		    $result{$site}->{workdir} .= $text
			if ( length($text) );
		} elsif ( $stack[2] eq 'profile' ) {
		    $result{$site}->{profile}->{$profns}->{$profid} .= $text
			if ( defined $profns && defined $profid &&
			     length($text) );
		} else {
		    warn "Warning: Ignoring \"$text\" inside $stack[2]\n";
		}
	    }
	}
	1;
    }, 'Comment' => sub {
	my $self = shift;
	my $text = shift;
	if ( $text =~ /\[(\d+)\] (\S+) \@ (\S+) : (\d+)/ ) {
	    # our own special comment
	    my $raw = $2;
	    $raw =~ tr/-/_/;
	    $result{$raw}->{special} = [ $1, $2, $3, $4 ];
	    $result{$raw}->{ncpus} = $4;
	}
	1;
    }, 'XMLDecl' => sub {
	# ignore
	1;
    }, 'Default' => sub {
	my $self = shift;
	my $text = shift;
	if ( $text =~ /^\s*$/ ) {
	    # ignore 
	} else {
	    log( "unknown xml \"$text\", ignoring\n" );
	}
	1;
    } );

    local(*XML);
    open( XML, "<$fn" ) || croak $prefix, " open XML SC file $fn: $!";
    log( "reading from $fn" ) if ( $main::DEBUG & 0x10 );
    $parser->parse(*XML) unless eof(XML);
    close(XML);

    GriPhyN::SC::sanitize( %result );
    %result;
}

#
# ctor
#
sub new {
    my $proto = shift;
    my $class = ref($proto) || $proto || __PACKAGE__;
    my $self = bless GriPhyN::SC->new( style => 'xml', @_ ), $class;
    $self->{'_permitted'}->{file} = 1;
    
     if ( exists $self->{file} && ! defined $self->{file} ) {
	# special case file => undef
	$self->{pool} = { };
    } else {
	# normal case
	my $file = $self->file || 
	    croak $prefix, "no location specified (missing property ...file)"; 
	$self->{pool} = { parse_xml($file) };
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
	printf $fh "  <!-- [%u] %s @ %s : %u -->\n", @{$sref->{special}};
    }

    # site entry
    print $fh "  <pool handle=\"$site\"";
    print $fh " sysinfo=\"", $sref->{sysinfo}, "\"" if exists $sref->{sysinfo};
    print $fh " gridlaunch=\"", $sref->{gridshell}, "\">\n";
    foreach my $ns ( sort keys %{ $sref->{profile} } ) {
	foreach my $key ( sort keys %{ $sref->{profile}->{$ns} } ) {
	    print $fh "    <profile namespace=\"$ns\" key=\"$key\">";
	    print $fh $sref->{profile}->{$ns}->{$key}, "</profile>\n";
	}
    }
    foreach my $lrc ( @{ $sref->{lrc} } ) {
	print $fh "    <lrc url=\"$lrc\"/>\n";
    }
    foreach my $ftp ( @{ $sref->{gridftp} } ) {
	print $fh "    <gridftp url=\"", $ftp->[0], "\"";
	print $fh " storage=\"", $ftp->[1], "\"";
	printf $fh " major=\"%u\"", $ftp->[2];
	printf $fh " minor=\"%u\"", $ftp->[3];
	printf $fh " patch=\"%u\"", $ftp->[4]
	    if ( defined $ftp->[4] );
	print $fh "/>\n";
    }
    foreach my $universe ( reverse sort keys %{ $sref->{contact} } ) {
	foreach my $jm ( @{$sref->{contact}->{$universe}} ) {
	    print $fh "    <jobmanager universe=\"", $universe, "\"";
	    print $fh " url=\"", $jm->[0], "\"";
	    printf $fh " major=\"%u\"", $jm->[2];
	    printf $fh " minor=\"%u\"", $jm->[3];
	    printf $fh " patch=\"%u\"", $jm->[4]
		if ( defined $jm->[4] );
	    print $fh " total-nodes=\"", $sref->{ncpus}+0, "\""
		if ( exists $sref->{ncpus} );
	    print $fh "/>\n";
	}
    }
    print $fh "    <workdirectory>", $sref->{workdir}, "</workdirectory>\n";
    print $fh "  </pool>\n\n";
}

sub show_preamble {
    my $self = shift;
    my $fh = shift || croak $prefix, "need an open filehandle";
    croak $prefix, "need an open filehandle" unless ref $fh;

    print $fh '<?xml version="1.0" encoding="ISO-8859-1"?>', "\n";
    print $fh '<!-- generated: ', $self->isodate(), " -->\n";
    print $fh '<!-- gen. user: ', scalar getpwuid($>), " -->\n";
    print $fh '<!-- generator: ', ref $self, " $VERSION -->\n";

    print $fh '<config xmlns="http://www.griphyn.org/chimera/GVDS-PoolConfig" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.griphyn.org/chimera/GVDS-PoolConfig http://www.griphyn.org/chimera/sc-1.4.xsd" version="1.4">', "\n";
}

sub show_postamble {
    my $self = shift;
    my $fh = shift || croak $prefix, "need an open filehandle";
    croak $prefix, "need an open filehandle" unless ref $fh;
    
    # XML postable
    print $fh "</config>\n";
}

#
# return 'true' to package loader
#
1;
__END__
