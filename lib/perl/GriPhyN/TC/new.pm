package GriPhyN::TC::new;
#
# new-style multi-column formatted tc.data file
# VDS property vds.tc.mode File
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
# Revision : $Revision: 1.3 $
#
# $Id: new.pm,v 1.3 2005/08/09 22:12:31 griphyn Exp $
#
use 5.006;
use strict;
use Exporter;
use GriPhyN::TC;
use vars qw/$VERSION/;

our @ISA = qw(Exporter GriPhyN::TC);
our @EXPORT = qw();
sub parse_profile($);		# { }
our @EXPORT_OK = qw($VERSION parse_profile);

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
    my $self = bless GriPhyN::TC->new( style => 'new', @_ ), $class;
    $self->{'_permitted'}->{file} = 1;
    
    if ( exists $self->{file} && ! defined $self->{file} ) {
	# special case file => undef
	$self->{pool} = { };
    } else {
	# normal case
	my $file = $self->file || 
	    croak $prefix, "no location specified (missing property ...file)"; 
	$self->parse_new($file);
    }

    # return handle to self
    $self;
}

sub DESTROY {
    # empty
}

# Autoload methods go after =cut, and are processed by the autosplit program.

#
# see org.griphyn.common.util.ParseProfile.java for STD
#
my @c_state = 
    ( [  8,  0,  9,  9,  1,  9,  9,  9,  9 ], # 0: recognize ns
      [ 10,  9,  9,  9,  2,  9,  9,  9,  9 ], # 1: found colon
      [  8,  2,  9,  9,  9,  9,  9,  3,  9 ], # 2: recognize key
      [ 10,  6,  9,  9,  9,  9,  4,  9,  6 ], # 3: seen equals
      [ 10,  4,  4,  4,  4,  5,  7,  4,  4 ], # 4: quoted value
      [ 10,  4,  4,  4,  4,  4,  4,  4,  4 ], # 5: backslashed qv
      [  8,  6,  2,  0,  9,  9,  9,  9,  6 ], # 6: unquoted value
      [  8,  9,  2,  0,  9,  9,  9,  9,  9 ]  # 7: closed quote
    );

my @c_action = 
    ( [  0,  1,  0,  0,  0,  0,  0,  0,  0 ], # 0: recognize ns
      [  0,  0,  0,  0,  0,  0,  0,  0,  0 ], # 1: found colon
      [  0,  2,  0,  0,  0,  0,  0,  0,  0 ], # 2: recognize key
      [  0,  3,  0,  0,  0,  0,  0,  0,  3 ], # 3: seen equals
      [  0,  3,  3,  3,  3,  0,  0,  3,  3 ], # 4: quoted value
      [  0,  3,  3,  3,  3,  3,  3,  3,  3 ], # 5: backslashed qv
      [  4,  3,  5,  4,  0,  0,  0,  0,  3 ], # 6: unquoted value
      [  4,  0,  5,  4,  0,  0,  0,  0,  0 ]  # 7: closed quote
    );
	       
sub parse_profile($) {
    # purpose: Uses state machinery to correctly parse new textual profiles
    # paramtr: $s (IN): string that contains all profiles to parse
    # returns: a hash, possibly empty, with the parsed profiles. 
    my $s = shift;
    my %result = ();
    return %result if ( lc($s) eq 'null' || length($s) == 0 );

    my ($ns,$k,$v,$ch,$cc,$action);
    $ns = $k = $v = '';

    my $i = 0;
    my $state = 0;
    while ( $state < 8 ) {
	$ch = ( $i < length($s) ? substr($s,$i,1) : "\0" );
	$i++;

	if    ( $ch eq "\0" ) { $cc = 0; }
	elsif ( $ch =~ /\w/ ) { $cc = 1; }
	elsif ( $ch eq ',' )  { $cc = 2; }
	elsif ( $ch eq ';' )  { $cc = 3; }
	elsif ( $ch eq ':' )  { $cc = 4; }
	elsif ( $ch eq "\\" ) { $cc = 5; }
	elsif ( $ch eq '"' )  { $cc = 6; }
	elsif ( $ch eq '=' )  { $cc = 7; }
	else                  { $cc = 8; }
	
	$action = $c_action[$state]->[$cc];
	if    ( $action == 1 ) { $ns .= $ch }
	elsif ( $action == 2 ) { $k .= $ch }
	elsif ( $action == 3 ) { $v .= $ch }
	elsif ( $action == 4 ) { $result{$ns}{$k} = $v; $ns = $k = $v = '' }
	elsif ( $action == 5 ) { $result{$ns}{$k} = $v; $k = $v = '' }
	$state = $c_state[$state]->[$cc];
    }

    die "ERROR: $.: Illegal character $ch at $i\n" if ( $state == 9 );
    die "ERROR: $.: Premature end of string\n" if ( $state == 10 );

    %result;
}

sub parse_new {
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

	    @x = split( /\s+/, $_, 6 );
	    if ( @x >= 5 ) {
		$x[3] = 'INSTALLED' if ( lc($x[3]) eq 'null' );
		$x[4] = 'INTEL32::LINUX' if ( lc($x[4]) eq 'null' );

		$x = [ $x[2], { }, $x[3], $x[4] ];
#		if ( lc($x[3]) ne 'installed' ) {
#		    log( $prefix, "$fn:$.: ignoring !INSTALLED" );
#		    next;
#		}

		$x->[1] = { parse_profile($x[5]) }
		    if ( defined $x[5] && 
			 length($x[5]) && lc($x[5]) ne 'null' );

#		if ( length($x[5]) && lc($x[5]) ne 'null' ) {
#		    foreach my $prof ( split /;/, $x[5] ) {
#			next unless length($prof);
#			my ($ns,$rest) = split /::/, $prof, 2;
#			foreach my $kv ( split /,/, $rest ) {
#			    next unless length($kv);
#			    my ($k,$v) = split /=/, $kv, 2;
#			    $x->[1]{$ns}{$k} = $v;
#			}
#		    }
#		}
		
		$self->{pool}->{$x[0]}->{xform}->{$x[1]} = $x;
		# only remember installed applications in xform
		GriPhyN::TC::_add( @{$self->{xform}->{$x[1]}}, $x[0] )
		    if ( uc($x[3]) eq 'INSTALLED' );
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
	printf $fh "$site\t%-24s", $tr;
	print $fh "\t", $sref->{xform}->{$tr}->[0]; # app
	print $fh "\t", $sref->{xform}->{$tr}->[2] || 'INSTALLED'; # INSTALLED 
	print $fh " ", $sref->{xform}->{$tr}->[3] || 'null'; # OSARCH
	print $fh " ";
	if ( defined $sref->{xform}->{$tr}->[1] ) {
	    # profiles to print
	    my $flag = 0;
	    foreach my $ns ( keys %{ $sref->{xform}->{$tr}->[1] } ) {
		print $fh ';' if $flag;
		$flag++;
		my $flag2 = 0;
		foreach my $key ( keys %{ $sref->{xform}{$tr}[1]{$ns} } ) {
		    if ( $flag2 ) { print $fh ',' }
		    else { print $fh $ns, '::' }
		    $flag2++;
		    print $fh $key, '="',
		              $sref->{xform}->{$tr}->[1]->{$ns}->{$key}, '"';
		}
	    }
	    print $fh "null" unless $flag;
	} else {
	    # no profiles
	    print $fh "null";
	}
	print $fh "\n";
    }
}

#
# return 'true' to package loader
#
1;
__END__
