package GriPhyN::TC;
#
# abstract base class for transformation catalog implementations
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
# Revision : $Revision: 1.2 $
#
# $Id: TC.pm,v 1.2 2005/08/11 01:35:57 griphyn Exp $
#
use 5.006;
use strict;
use vars qw($VERSION $AUTOLOAD);
use Exporter;

our @ISA = qw(Exporter);
our @EXPORT = qw();
sub _add(\@$);			# { }
our @EXPORT_OK = qw($VERSION _add);

$VERSION=$1 if ( '$Revision: 1.2 $' =~ /Revision:\s+([0-9.]+)/o );

#
# --- start -----------------------------------------------------
#
#
# description of basic (deep) data structur in 'pool' and 'xform' hashes
#
# 'pool' =>
#    $site =>
#       'special' => [ eno, rawsite, gk, ncpus ]
#       'xform' =>
#          $tr => [ $app, { $profile }, $inst, $osarch ]
#
# 'xform' =>
#    $tr => [ $site ]
#
use Carp;
use POSIX qw(strftime);
use GriPhyN::Log qw();
our $prefix = '[' . __PACKAGE__ . '] ';

#
# ctor
#
sub new {
    my $proto = shift;
    my $class = ref($proto) || $proto; # || __PACKAGE__;
    my $self = bless { @_ }, $class; 

    # create default forward and reverse mappings
    $self->{'_permitted'}->{style} = 1;
    $self->{pool} = {} unless exists $self->{pool};
    $self->{xform} = {} unless exists $self->{xform};

    # return handle to self
    $self;
}

sub AUTOLOAD {
    # purpose: catch-all accessor (set and get) for all data fields
    #          ever defined in any great-grandchild of this class
    # paramtr: ?
    # returns: ?
    my $self = shift;
    my $type = ref($self) || croak $prefix, "$self is not an object";

    my $name = $AUTOLOAD;
    $name =~ s/.*:://;   # strip fully-qualified portion

    # *ONLY* permit those fields which are registered in _permitted
    unless ( exists $self->{'_permitted'}->{$name} ) {
	croak $prefix, "Can't access >>$name<< field in class $type";
    }

    my $result = $self->{$name};
    $self->{$name} = shift if (@_);
    $result;
}

sub _add(\@$) {
    # purpose: class method to add a site only once
    my $vref = shift;
    my $what = shift;

    push( @$vref, $what ) if ( (grep { /$what/ } @$vref) == 0 );
}

#
# --- concrete methods ----------------------------------------
# Note: overwrite these methods, say, for a database implementation
#

sub all_sites {
    # purpose: lists all available sites in the TC
    # paramtr: $force (opt. IN): if true, do not omit site local.
    # returns: a list of sites, possibly empty
    my $self = shift;
    my @result = keys %{$self->{pool}};
    defined $_[0] ? @result : grep { $_ ne 'local' } @result;
}

sub sites {
    # purpose: obtain list of sites for a given logical transformation
    # paramtr: $xform (IN): transformation name to look up.
    # returns: a list of sites, possibly empty
    my $self = shift;
    my $xform = shift || croak( $prefix, "need a logical transformation name" );
#    $xform =~ tr/:/_/ unless ( $self->{keep} );
    GriPhyN::Log::log( ref($self), 'sites(', $xform, ')' ) 
	if ( ($main::DEBUG & 0x200) );

    # sanity
    croak( $prefix, "no such transformation $xform" )
	unless exists $self->{xform}->{$xform};

    # xform will only contain INSTALLED transformations
    grep { $_ ne 'local' } @{$self->{xform}->{$xform}};
}

sub resolve {
    # purpose: resolves a given site and lTR into the application 
    # paramtr: $site (IN): site handle to resolve for
    # paramtr: $xform (IN): name of transformation to resolve
    # returns: vector: [0] name of application location
    #                  {1} hashref to profiles, may be empty
    #                  [2] INSTALLED, ...
    #                  [3] OS-arch flag
    #          scalar: name of application
    my $self = shift;
    my $site = shift || croak( $prefix, 'need a site to resolve for' );
    my $xform = shift || croak( $prefix, 'need a logical transformation name' );
#    $xform =~ tr/:/_/ unless ( $self->{keep} );
    GriPhyN::Log::log( ref($self), 'resolve(', $xform, ')' ) 
	if ( ($main::DEBUG & 0x200) );

    # sanity
    croak( $prefix, "no such site $site" )
	unless exists $self->{pool}->{$site};
    croak( $prefix, "no such transformations known for site $site" )
	unless exists $self->{pool}->{$site}->{xform};
    croak( $prefix, "no such transformation $xform at site $site" )
	unless exists $self->{pool}->{$site}->{xform}->{$xform};
    my @result = @{$self->{pool}->{$site}->{xform}->{$xform}};

    wantarray ? @result : $result[0];
}

sub dump_site {
    # purpose: Dumps all TR known at a given site
    # paramtr: $site (IN): site handle to resolve
    # returns: vector of logical TR installed at that site
    my $self = shift;
    my $site = shift || croak( $prefix, "need a site to resolve for" );

    # sanity
    croak( $prefix, "no such site $site" )
	unless exists $self->{pool}->{$site};
    
    # done
    exists $self->{pool}->{$site}->{xform} ? 
	keys %{$self->{pool}->{$site}->{xform}} : 
	();
}

sub profile {
    # purpose: obtains list of profiles for a site and transformation
    # paramtr: $site (IN): site handle
    #          $tr (IN): transformation
    #          $ns (opt. IN): optional namespace
    # returns: a hash?
    my $self = shift;
    my $site = shift || croak( $prefix, "need a site parameter" );
    my $xform = shift || croak( $prefix, "need a logical transformation name" );
    my $ns = shift;		# may not be defined
#    $xform =~ tr/:/_/ unless $self->{keep};

    GriPhyN::Log::log( ref($self), "profile($xform", ( defined $ns ? ",$ns)" : ')' ) )
	if ( ($main::DEBUG & 0x200) );

    # sanity
    croak( $prefix, "no such site $site" )
	unless exists $self->{pool}->{$site};
    croak( $prefix, "no such transformations known for site $site" )
	unless exists $self->{pool}->{$site}->{xform};
    croak( $prefix, "no such transformation $xform at site $site" )
	unless exists $self->{pool}->{$site}->{xform}->{$xform};
    croak( $prefix, "no profile settings for $xform at site $site" )
	unless defined $self->{pool}->{$site}->{xform}->{$xform}->[1];
    croak( $prefix, "no profile settings for $xform at site $site" )
	unless exists $self->{pool}->{$site}->{xform}->{$xform}->[1];

    if ( defined $ns ) {
	# all keys from the profiles namespace
	if ( exists $self->{pool}->{$site}->{xform}->{$xform}->[1]->{$ns} ) {
	    return %{ $self->{pool}->{$site}->{xform}->{$xform}->[1]->{$ns} };
	} else {
	    GriPhyN::Log::log( ref($self), "profile $ns for $xform at site $site is empty" );
	    return ();
	}
    } else {
	# all namespaces
	return %{ $self->{pool}->{$site}->{xform}->{$xform}->[1] };
    }
}

#
# --- methods not used inside planner --------------------------------------
#

sub entry {
    # purpose: accessor to the entry item count number
    my $self = shift;
    my $site = shift || croak $prefix, "need a site handle";

    my $result = $self->{pool}->{$site}->{special}->[0];
    $self->{pool}->{$site}->{special}->[0] = shift if (@_);
    $result;
}

sub isodate {
    # purpose: generate an ISO timestamp
    my $self = shift;
    my @x = localtime( shift() || time() );
    POSIX::strftime( '%Y-%m-%dT%H:%M:%S%z', @x );
}

sub add {
    # purpose: add a complete entry to the transformation catalog
    # paramtr: $site (IN): site handle
    #          $xform (IN): logical transformation name
    #          $app, (IN): physical application name
    #          $pref (opt. IN): reference to profiles, or undef
    #          $inst (opt. IN): installation status, e.g. 'INSTALLED'
    #          $osarch (opt. IN): OS Arch stuff, 'null', or undef
    my $self = shift;
    my $site = shift || croak( $prefix, "need a site parameter" );
    my $xform = shift || croak( $prefix, "need a logical transformation name" );
#    $xform =~ tr/:/_/ unless $self->{keep};
    my $app = shift || croak( $prefix, 'need a physical application path' );

    # optional arguments
    my $pref = shift || { };	
    my $inst = shift || 'INSTALLED';
    my $arch = shift || 'null';
    if ( exists $self->{pool}->{$site} &&
	 exists $self->{pool}->{$site}->{xform} &&
	 exists $self->{pool}->{$site}->{xform}->{$xform} ) {
	warn ref($self), "Warning: overwriting existing $site->$xform\n";
	$self->{xform}->{$xform} = [ grep { $_ ne $site } 
				     @{ $self->{xform}->{$xform} } ]; 
	delete $self->{pool}->{$site}->{xform}->{$xform};
    }
    $self->{pool}->{$site}->{xform}->{$xform} = [ $app, $pref, $inst, $arch ];
    _add( @{$self->{xform}->{$xform}}, $site ) 
	if uc($inst) eq 'INSTALLED';
}

sub add_special {
    my $self = shift;
    my $site = shift || croak( $prefix, "need a site parameter" );
    $self->{pool}->{$site}->{special} = [ @_ ];
}

sub delete {
    # purpose: removes an entry from the catalog
    # paramtr: $site (IN): site handle to remove
    # paramtr: $xform (opt. IN): if specified, just remove that TR from $site
    # returns: removed entry reference, or undef not exists
    my $self = shift;
    my $site = shift || croak $prefix, "need a site parameter";
    my $xform = shift;		# optional

    my $result = undef;
    if ( defined $xform ) {
	# just remove entry for $xform from $site
	$result = delete( $self->{pool}->{$site}->{xform}->{$xform} )
	    if ( exists $self->{pool}->{$site}->{xform}->{$xform} );
    } else {
	# remove all entries for $site
	$result = delete( $self->{pool}->{$site} )
	    if ( exists $self->{pool}->{$site} );
    }
    $result;
}

sub show ($[*$];@) {
    # purpose: dumps 1+ or all records on the given filehandle
    # paramtr: *FH (IN): filehandle open for writing
    #          $site (opt. IN): specific site list, 1+
    #                           no site list means all entries
    # returns: ?
    my $self = shift;
    my $fh = undef;
    if ( ref $_[0] ) {
	$fh = shift;
    } else {
	local(*FH) = shift;
	$fh = \*FH;
    }

    if ( scalar @_ ) {
	# just show site records for specified sites
	foreach my $site ( @_ ) {
	    $self->show_site( $fh, $site );
	}
    } else {
	# show full extent
	$self->show_preamble($fh);
	foreach my $site ( sort { $self->entry($a) <=> $self->entry($b) } 
			   grep { $_ ne 'local' }
			   $self->all_sites) {
	    $self->show_site( $fh, $site );
	}
	# always make "local" last
	$self->show_site( $fh, 'local' ) 
	    if exists $self->{pool}->{local};
	$self->show_postamble($fh);
    }
}

sub show_preamble {
    # purpose: method to write a preamble before dumping records
    #          to be overwritten in child class
    # paramtr: $fh (IN): filehandle open for writing
    my $self = shift;
    my $fh = shift || croak $prefix, "need an open filehandle";
    croak $prefix, "need an open filehandle" unless ref $fh;

    print $fh "##\n";
    print $fh '## generated: ', strftime('%Y-%m-%dT%H:%M:%S%z', 
					 localtime() ), "\n";
    print $fh '## gen. user: ', scalar getpwuid($>), "\n";
    print $fh '## generator: ', ref $self, " $VERSION\n";
}

sub show_postamble {
    # purpose: method to write a postamble after dumping records
    #          noop, to be overwritten in child class
    # paramtr: $fh (IN): filehandle open for writing
    1;
}

#
# --- abstract methods ------------------------------------------
#

#
# return 'true' to package loader
#
1;
__END__
