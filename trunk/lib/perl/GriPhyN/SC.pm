package GriPhyN::SC;
#
# abstract base class for site catalog (SC) implementations
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
# $Id$
#
use 5.006;
use strict;
use vars qw($VERSION $AUTOLOAD);
use Exporter;

our @ISA = qw(Exporter);
sub show ($[*$];@);		# { }
our @EXPORT = qw(show);
sub sanitize(\%);		# { }
sub break_uri($);		# { }
sub major_from_contact($);	# { }
our @EXPORT_OK = qw($VERSION sanitize break_uri major_from_contact);

$VERSION=$1 if ( '$Revision$' =~ /Revision:\s+([0-9.]+)/o );

#
# --- start -----------------------------------------------------
#
# description of basic (deep) data structure in 'site' hash reference:
#
# 'site' =>
#    $site =>
#       'lrc' => [ $uri ]
#       'gridshell => $
#       'workdir' => $
#       'ncpus' => $                              # presence unreliable
#       'special' => [ $eno, $raw, $gk, $cpus ]   # all optional
#       'sysinfo' => $                            # optional? 
#       'gridftp' => [ [$host,$path,$maj,$min,$plv] ] # optional: $maj, $min
#       'contact' =>
#	   $universe => [ $jm,$type,$maj,$min,$plv ]  # $type may be undef'd
#       'profile' =>
#          $ns =>
#             $key => value
#
use Carp;
use GriPhyN::Log qw();
use POSIX qw(strftime);
our $prefix = '[' . __PACKAGE__ . '] ';

sub break_uri($) {
    # purpose: breaks an absolute URI into server and path components
    # paramtr: $uri (IN): full gsiftp:// URI
    # returns: vector with server URI in first part, path component in last
    my $x = shift;
    my $pos = index( $x, '/', index($x,'//')+2 );
    if ( $pos == -1 ) {
	# "gsiftp://server.name"
	( $x, '/' );
    } else {
	# "gsiftp://server.name:port/where/to"
	( substr($x,0,$pos), substr($x,$pos) );
    }
}

sub major_from_contact($) {
    # purpose: looks at the jobmanager URI to determine GT2 or GT4
    # paramtr: $jm (IN): jobmanager contact string (or factory EPR)
    # returns: 2 for GT2, 4 for GT4, undef if not parsable
    my $jm = shift;
    my $result;
    if ( $jm =~ m{^([-A-Za-z0-9.:]+)/jobmanager([-a-zA-Z0-9]*)} ) {
	$result = 2;
    } elsif ( $jm =~ m{^https} && $jm =~ m{ManagedJobFactoryService$} ) {
	$result = 4;
    } else {
	undef $result;
    }
    $result;
}

sub sanitize(\%) {
    # purpose: sanitizes site list by checking information semantically
    # paramtr: %result (IO): reference to hash of all sites
    my $result = shift;
    croak "Need a hash argument" unless ref $result eq 'HASH';

    # sanitize results
    my ($site,%bad);
    foreach $site ( keys %{$result} ) {
	# adjust gridftp URI, if not in broken down version
	if ( exists $result->{$site}->{gridftp} ) {
	    my @gftp = ();
	    foreach my $gftp ( @{$result->{$site}->{gridftp}} ) {
		if ( ref $gftp eq 'ARRAY' && @{$gftp} >= 2 ) {
		    push( @gftp, $gftp );
		} else {
		    push( @{$bad{$site}}, "Illegal gridftp reference" );
		    last;
		}
	    }
	    $result->{$site}->{gridftp} = [ @gftp ];
	} else {
	    push( @{$bad{$site}}, 'no gridftp server' );
	}

	push( @{$bad{$site}}, 'no grid shell' )
	    unless ( exists $result->{$site}->{gridshell} ); 
	push( @{$bad{$site}}, 'no known LRC' )
	    unless ( exists $result->{$site}->{lrc} &&
		     @{$result->{$site}->{lrc}} > 0 );
	push( @{$bad{$site}}, 'unknown remote working directory' )
	    unless ( exists $result->{$site}->{workdir} );

	if ( exists $result->{$site}->{contact} &&
	     exists $result->{$site}->{contact}->{vanilla} &&
	     @{$result->{$site}->{contact}->{vanilla}} > 0 ) {
	} else {
	    push( @{$bad{$site}}, 'unknown remote vanilla scheduler' );
	}
    }

    foreach $site ( keys %bad ) {
	delete $result->{$site};
	if ( $main::DEBUG & 0x10 ) {
	    foreach my $reason ( @{$bad{$site}} ) {
		warn( $prefix, "removing site $site, because $reason!\n" );
	    }
	}
    }
}

#
# ctor
#
sub new {
    my $proto = shift;
    my $class = ref($proto) || $proto; # || __PACKAGE__;
    my $self = bless { @_ }, $class;

    # create defaults
    $self->{'_permitted'}->{style} = 1;
    $self->{site} = {} unless exists $self->{site};

    # return handle to self
    $self;
}

sub AUTOLOAD {
    # purpose: catch-all accessor (set and get) for all data fields
    #          ever defined in any great-grandchild of this class
    # warning: The autoload maps the data fields m_XYZ to method XYZ
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

#
# --- concrete methods ----------------------------------------
# Note: overwrite these methods, say, for a database implementation
#

sub sites {
    # purpose: find all sites
    # returns: list of all sites including local
    my $self = shift;
    keys %{ $self->{site} };
}

sub ncpus {
    # purpose: Gets or sets the number of CPUs that a site reports to have
    # paramtr: $site (IN): site handle
    #          optional: new value for number of CPUs
    # returns: old value of number of CPUs
    my $self = shift;
    my $site = shift || croak $prefix, "need a site handle";
    my $oldv;
    if ( exists $self->{site}->{$site} ) {    
	$oldv = $self->{site}->{$site}->{ncpus} || 0;
	$self->{site}->{$site}->{ncpus} = $_[0] if ( @_ );
    } else {
	GriPhyN::Log::log( $prefix, 'ERROR: site ', $site, ' is not in catalog!' );
    }
    $oldv;
}

sub sysinfo {
    # purpose: Gets or sets the system information for a given site
    # paramtr: $site (IN): site handle
    #          optional: new value for system information
    # returns: old value for system information
    my $self = shift;
    my $site = shift || croak $prefix, "need a site handle";
    my $oldv;
    if ( exists $self->{site}->{$site} ) {
	$oldv = $self->{site}->{$site}->{sysinfo} || '';
	$self->{site}->{$site}->{sysinfo} = $_[0] if ( @_ );
    } else {
	GriPhyN::Log::log( $prefix, 'ERROR: site ', $site, ' is not in catalog!' );
    }
    $oldv;
}

sub gridshell {
    # purpose: Accessor to the location of gridshell (kickstart)
    # paramtr: $site (IN): site handle
    #          $newv (IN): optional new value to set
    # returns: $oldv, old value in set case, current value in get case
    my $self = shift;
    my $site = shift || croak $prefix, "need a site parameter";
    my $oldv;
    if ( exists $self->{site}->{$site} ) {    
	$oldv = $self->{site}->{$site}->{gridshell};
	$self->{site}->{$site}->{gridshell} = shift if ( @_ );
    } else {
	GriPhyN::Log::log( $prefix, 'ERROR: site ', $site, ' is not in catalog!' );
    }
    $oldv;
}

sub workdir {
    # purpose: get or set the workdir for a given site
    # paramtr: $site (IN): site for which to modify the workdir
    # returns: the current or old value
    my $self = shift;
    my $site = shift || croak $prefix, "need a site parameter";
    my $oldv;
    if ( exists $self->{site}->{$site} ) {
	$oldv = $self->{site}->{$site}->{workdir};
	$self->{site}->{$site}->{workdir} = shift if ( @_ );
    } else { 
	GriPhyN::Log::log( $prefix, 'ERROR: site ', $site, ' is not in catalog!' );
    }
    $oldv;
}

sub contact {
    # purpose: set or obtain list of jobmanagers for a site and universe
    # paramtr: $site (IN): site handle
    #          $sched (IN): universe to search for
    #          optional: list of jobmanager in the set case (3-item vrefs)
    # returns: vector: possibly empty list of jobmanager contact strings
    #          scalar: first in above list, may be undef
    # warning: result is a vector of 3-item vectors
    my $self = shift;
    my $site = shift || croak $prefix, "need a site parameter";
    my $sched = shift || croak $prefix, "need a universe parameter";
    GriPhyN::Log::log( $prefix, 'contact(', $site, ',', $sched, ')' )
	if ( ($main::DEBUG & 0x200) );

    my @oldv = ();
    if ( exists $self->{site}->{$site} ) {
	@oldv = @{ $self->{site}->{$site}->{contact}->{$sched} };
	$self->{site}->{$site}->{contact}->{$sched} = [ @_ ] if ( @_ );
    } else { 
	GriPhyN::Log::log( $prefix, 'ERROR: site ', $site, ' is not in catalog!' );
    }
    wantarray ? @oldv : ( defined $oldv[0] ? $oldv[0][0] : undef );
}

sub gridftp {
    # purpose: [1] obtains all gridftp servers (no arguments)
    #          [2] gets all gridftp servers for a site (scalar arg)
    #          [3] sets all gridftp servers for a site (list arg)
    # paramtr: $site (opt. IN): if specified, only servers for the site
    #          @list (opt. IN): new list of gridftp servers for $site
    #                           this list is a vector of 2-item vectors
    # returns: vector: case 1: list of all gridftp server entries
    #                  case 2: list of gridftp server for $site
    #                  case 3: list of previous gridftp server for $site
    #          scalar: first entry from above list
    # warning: The result vectors are 4-item vectors!
    my $self = shift;
    my $site = shift;
    GriPhyN::Log::log( $prefix, 'gridftp(', $site, ')' )
	if ( ($main::DEBUG & 0x200) );

    my @oldv = ();
    if ( defined $site ) {
	# regular case $self->gridftp($site)
	@oldv = @{$self->{site}->{$site}->{gridftp}}
	    if exists $self->{site}->{$site}->{gridftp};
	@{$self->{site}->{$site}->{gridftp}} = @_ if ( @_ );
    } else {
	# irregular case: query all gridftp for all sites
	foreach my $site ( keys %{$self->{site}} ) {
	    @oldv = ( @oldv, @{$self->{site}->{$site}->{gridftp}} )
		if exists $self->{site}->{$site}->{gridftp};
	}
    }
    wantarray ? 
	@oldv : 
	(defined $oldv[0] && ref $oldv[0] ? $oldv[0][0] . $oldv[0][1] : undef);
}

sub profile {
    # purpose: obtains list of profiles for a site
    # paramtr: $site (IN): site handle
    #          $ns (opt. IN): optional namespace
    # returns: hash according to question
    my $self = shift;
    my $site = shift || croak $prefix,"need a site parameter";
    my $ns = shift;
    GriPhyN::Log::log( $prefix, 'profile(', $site, (defined $ns ? ",$ns" : ''), ')' )
	if ( ($main::DEBUG & 0x200) );
    
    return () unless exists $self->{site}->{$site} &&
	exists $self->{site}->{$site}->{profile};

    if ( defined $ns ) {
	# all keys from the profiles namespace
	return () unless exists $self->{site}->{$site}->{profile}->{$ns};
	return %{ $self->{site}->{$site}->{profile}->{$ns} };
    } else {
	# all namespaces
	return %{ $self->{site}->{$site}->{profile} };
    }
}

sub resolve {
    # purpose: finds all sites that match a gridftp prefix handle
    # paramtr: $gftp (IN): gridftp URL prefix to do prefix matching
    # returns: vector: list of all sites that match, may be empty. 
    #          scalar: first site that matches, may be undef.
    my $self = shift;
    my $gftp = shift || croak $prefix, "need a gridftp server or its prefix";

    my @result = ();
    if ( (break_uri($gftp))[1] eq '/' ) {
	# just server prefixes, simple matches
	foreach my $site ( $self->sites ) {
	    push( @result, $site ) 
		if ( (grep( /^$gftp/, 
			    map { $_->[0] }
			    @{$self->{site}->{$site}->{gridftp}} )) > 0 );
	}
    } else {
	# full path prefix matches, sigh, this is expensive
	foreach my $site ( $self->sites ) {
	    push( @result, $site ) 
		if ( (grep( /^$gftp/,
			    map { File::Spec->catfile($_->[0],$_->[1]) }
			    @{$self->{site}->{$site}->{gridftp}} )) > 0 );
	}
    }
    wantarray ? @result : $result[0];
}

#
# --- methods not used inside planner --------------------------------------
#

sub isodate {
    # purpose: generate an ISO timestamp
    my $self = shift;
    my @x = localtime( shift() || time() );
    POSIX::strftime( '%Y-%m-%dT%H:%M:%S%z', @x );
}

sub add {
    # purpose: Adds an entry to the catalog in memory.
    # warning: Existing entries are overwritten
    # paramtr: $site (IN): site handle to use
    #       'lrc' => [ $uri ]
    #       'gridshell => $
    #       'workdir' => $
    #       'ncpus' => $                              # presence unreliable
    #       'special' => [ $eno, $raw, $gk, $cpus ]   # all optional
    #       'gridftp' => [ [$host,$path,$maj,$min,$plv] ] # optional: $plv
    #       'contact' =>
    #          $universe => [ $jm,$type,$maj,$min,$plv ]  # $type,$plv optional
    #       'profile' =>                              # optional element
    #          $ns =>
    #             $key => value
    # returns: -
    my $self = shift;
    my $site = shift || croak ref($self), "need a site parameter";

    if ( exists $self->{site}->{$site} ) {
	warn ref($self), "Warning: overwriting existing site $site\n";
	delete $self->{site}->{$site};
    }
    $self->{site}->{$site} = { @_ };
}

sub delete {
    # purpose: removes an entry from the catalog
    # paramtr: $site (IN): site handle to remove
    # returns: removed entry reference, or undef not exists
    my $self = shift;
    my $site = shift || croak $prefix, "need a site parameter";

    my $result = undef;
    $result = delete( $self->{site}->{$site} )
	if ( exists $self->{site}->{$site} );
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
	foreach my $site ( sort { $self->ncpus($b) <=> $self->ncpus($a) } 
			   grep { $_ ne 'local' }
			   $self->sites) {
	    $self->show_site( $fh, $site );
	}
	$self->show_site( $fh, 'local' ) if exists $self->{site}->{local};
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
    print $fh '## generated: ', $self->isodate(), "\n";
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

sub show_site {
    # purpose: dumps one site entry into a stream
    # paramtr: $fh (IN): filehandle open for writing
    #          $site (IN): site handle to show
    # returns: -
    my $self = shift;
    croak $prefix, "forgot to implement show_site method in ", ref($self);
}

#
# return 'true' to package loader
#
1;
__END__
