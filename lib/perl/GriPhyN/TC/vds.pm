package GriPhyN::TC::vds;
#
# DBI access to the TC rDBMS shipped with Pegasus.
# VDS property vds.tc.mode Database
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
use Exporter;
use GriPhyN::TC;
use vars qw/$VERSION/;

our @ISA = qw(Exporter GriPhyN::TC);
our @EXPORT = qw();
our @EXPORT_OK = qw($VERSION _add);

$VERSION=$1 if ( '$Revision$' =~ /Revision:\s+([0-9.]+)/o );

#
# --- start -----------------------------------------------------
#
use Carp;
use DBI;
use File::Spec;
use Work::Properties;
use GriPhyN::Log qw/log/;

our $prefix = '[' . __PACKAGE__ . '] ';

my %translate = ( 'mysql' => 'mysql',
		  'postgresql' => 'Pg',
		  'oracle' => 'Oracle' );

use Data::Dumper;

sub tc_properties {
    # purpose: Extract the TC related properties from regular properties
    # returns: uri => DBI-uri
    #          dbuser => database account username
    #          dbpass => database account password
    #
    # vds.db.(*|tc).driver	(Postgres|MySQL)
    # vds.db.(*|tc).driver.url	jdbc:jdbtype:[//dbhost[:dbport]/]dbname
    # vds.db.(*|tc).driver.user dbuser
    # vds.db.(*|tc).driver.password dbpass
    #
    my $p = Work::Properties->new( shift(), shift() ); 
    my $mode = $p->property('vds.tc') || $p->property('vds.tc.mode');
    croak $prefix, " ERROR: Please adjust vds.tc=Database\n"
	unless ( defined $mode && $mode eq 'Database' );

    # return the perl way
    $p->jdbc2perl('tc');
}

#
# ctor
#
sub new {
    my $proto = shift;
    my $class = ref($proto) || $proto || __PACKAGE__;

    my %x = ( @_ );
    # avoid property parsings, if not necessary
    %x = ( %x, tc_properties( $x{conffile}, $x{rundirprops} ) ) 
	unless ( exists $x{uri} && 
		 exists $x{dbuser} && 
		 exists $x{dbpass} );
    my $self = bless GriPhyN::TC->new( style => 'vds', %x ), $class;

    # remember things
    $self->{'t_c'} = File::Spec->catfile( $ENV{'VDS_HOME'}, 
					  'bin', 'tc-client' );
    croak( $prefix, "Unable to execute ", $self->{'t_c'} )
	unless -x $self->{'t_c'};

    # connect
    for ( my $retries=1; $retries <= 5; $retries++ ) {
	$self->{_handle} = DBI->connect( $self->{uri}, 
					 $self->{dbuser} || '',
					 $self->{dbpass} || '', 
					 { PrintError => 0,
					   RaiseError => 0, 
					   AutoCommit => 0 } );
	last if defined $self->{_handle};
	log( $prefix, "retry $retries: Unable to connect to database ",
	     $self->{uri} );
	die( $prefix, 'out of retries' ) if $retries == 5;
    }

    # check for schema
    my @temp;
    my $q = "select version from vds_schema where catalog='tc'";
    for ( my $retries=1; $retries <= 5; $retries++ ) {
	@temp = $self->{_handle}->selectrow_array($q);
	last if @temp > 0;
    }
    croak( $prefix, "ERROR: Please set-up the TC schema first" )
	if ( @temp == 0 );
    log( $prefix, "Found TC schema catalog version ", $temp[0] )
	if ( ($main::DEBUG & 0x200) );

    # return handle to self
    $self;
}

sub DESTROY {
    my $self = shift;
    if ( exists $self->{_handle} && defined $self->{_handle} ) {
	$self->{_handle}->disconnect() ||
	    log( $prefix, $DBI::errstr );
    }
}

#
# --- concrete methods ----------------------------------------
# Note: overwrite these methods, say, for a database implementation
#

sub all_sites {
    # purpose: lists all available sites in the TC
    # paramtr: -
    # returns: a list of sites, possibly empty
    my $self = shift;

    my $q = 'SELECT distinct resourceid FROM tc_physicaltx';
    my $sth = $self->{_handle}->prepare_cached( $q ) ||
	croak $prefix, "prepare $q: ", $DBI::errstr;
    $sth->execute() || 
	croak $prefix, "execute $q: ", $sth->errstr;

    # cursor
    my (@row,@result);
    while ( (@row = $sth->fetchrow_array()) != 0 ) {
	push( @result, $row[0] );
    }
    croak( $prefix, "fetch $q: ", $sth->errstr ) if $sth->err;
    $sth->finish() || log( $prefix, $DBI::errstr );

    # done
    grep { $_ ne 'local' } @result;
}

sub split_tr($) {
    # purpose: split a transformation name into components
    # paramtr: $x (IN): logical transformation FQDI
    # returns: [0] namespace or empty string
    #          [1] name 
    #          [2] version or empty string
    my $x = shift;
    my @result;

    my $p = index($x,'::');
    if ( $p != -1 ) {
	# has a namespace
	push( @result, substr($x,0,$p) );
	substr( $x, 0, $p+2, '' );
    } else {
	# no namespace
	push( @result, '' );
    }

    $p = rindex($x,':');
    if ( $p != -1 ) {
	# has a version
	push( @result, substr( $x, 0, $p ) );
	push( @result, substr( $x, $p+1 ) );
    } else {
	# no version
	push( @result, $x );
	push( @result, '' );
    }

    log( $prefix, 'DEBUG: ', join( ' + ', @result ) )
	if ( ($main::DEBUG & 0x200) );
    @result;
}

sub sites {
    # purpose: obtain list of sites for a given logical transformation
    # paramtr: $xform (IN): transformation name to look up.
    # returns: a list of sites, possibly empty
    my $self = shift;
    my $xform = shift || croak( $prefix, "need a logical transformation name" );
    log( $prefix, 'sites(', $xform, ')' ) 
	if ( ($main::DEBUG & 0x200) );
    my @xform = split_tr($xform);

    my $q = q{SELECT distinct resourceid 
	FROM tc_physicaltx p, tc_lfnpfnmap m, tc_logicaltx l
	WHERE l.namespace=? AND l.name=? AND l.version=?
	AND l.id=m.lfnid AND m.pfnid=p.id};
    my $sth = $self->{_handle}->prepare_cached($q) || 
	croak $prefix, "prepare $q: ", $DBI::errstr;
    $sth->execute(@xform) || 
	croak $prefix, "execute $q: ", $sth->errstr;

    # cursor
    my (@row,@result);
    while ( (@row = $sth->fetchrow_array()) != 0 ) {
	push( @result, $row[0] );
    }
    croak( $prefix, "fetch $q: ", $sth->errstr ) if $sth->err;
    $sth->finish() || log( $prefix, $DBI::errstr );

    # done
    grep { $_ ne 'local' } @result;
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
    log( $prefix, 'resolve(', $xform, ')' ) 
	if ( ($main::DEBUG & 0x200) );
    my @xform = split_tr($xform);

    my $q = q{SELECT l.id, p.id, p.pfn, p.type, a.architecture, a.os
	FROM tc_physicaltx p, tc_lfnpfnmap m, tc_logicaltx l, tc_sysinfo a
	WHERE l.namespace=? AND l.name=? AND l.version=? AND p.resourceid=? 
	AND l.id=m.lfnid AND m.pfnid=p.id AND p.archid=a.id};
    my $sth = $self->{_handle}->prepare_cached( $q ) ||
	croak $prefix, "prepare $q: ", $DBI::errstr;

    # start query
    $sth->execute( (@xform,$site) ) || 
	croak $prefix, "execute $q: ", $sth->errstr;

    # cursor
    my (@row,@x,$st2,$qq);
    my @result = ();
    if ( (@row = $sth->fetchrow_array()) != 0 ) {
	my %prof = ();
	$qq = 'SELECT * FROM tc_lfnprofile WHERE lfnid=?';
	$st2 = $self->{_handle}->prepare_cached( $qq ) ||
	    croak $prefix, "prepare $qq: ", $DBI::errstr;
	$st2->execute( $row[0] ) || 
	    croak $prefix, "execute $qq: ", $st2->errstr;
	while ( (@x = $st2->fetchrow_array()) != 0 ) {
	    $prof{$x[0]}->{$x[1]} = $x[2];
	}
	croak( $prefix, "fetch $qq: ", $st2->errstr ) if $st2->err;
	$st2->finish() || log( $prefix, $DBI::errstr );

	$qq = 'SELECT * FROM tc_pfnprofile WHERE pfnid=?';
	$st2 = $self->{_handle}->prepare_cached( $qq ) ||
	    croak $prefix, "prepare $qq: ", $DBI::errstr;
	$st2->execute( $row[1] ) || 
	    croak $prefix, "execute $qq: ", $st2->errstr;
	while ( (@x = $st2->fetchrow_array()) != 0 ) {
	    $prof{$x[0]}->{$x[1]} = $x[2];
	}
	croak( $prefix, "fetch $qq: ", $st2->errstr ) if $st2->err;
	$st2->finish() || log( $prefix, $DBI::errstr );

	@result = ( $row[2], { %prof }, $row[3], $row[4] + '::' + $row[5] );
    }
    croak( $prefix, "fetch $q: ", $sth->errstr ) if $sth->err;
    $sth->finish() || log( $prefix, $DBI::errstr );

    wantarray ? @result : $result[0];
}

sub dump_site {
    # purpose: Dumps all TR known at a given site
    # paramtr: $site (IN): site handle to resolve
    # returns: vector of logical TR installed at that site
    my $self = shift;
    my $site = shift || croak( $prefix, "need a site to resolve for" );

    my $q = q{SELECT distinct l.namespace, l.name, l.version
	FROM tc_physicaltx p, tc_lfnpfnmap m, tc_logicaltx l
	WHERE p.resourceid=?
	AND l.id=m.lfnid AND m.pfnid=p.id};
    my $sth = $self->{_handle}->prepare_cached($q) || 
	croak $prefix, "prepare $q: ", $DBI::errstr;
    $sth->execute($site) || 
	croak $prefix, "execute $q: ", $sth->errstr;

    # cursor
    my @row;
    my @result = ();
    while ( (@row = $sth->fetchrow_array()) != 0 ) {
	my $x = ( length($row[0]) ? "$row[0]::$row[1]" : $row[1] );
	$x .= ":$row[2]" if length($row[2]);
	push( @result, $x );
    }
    croak( $prefix, "fetch $q: ", $sth->errstr ) if $sth->err;
    $sth->finish() || log( $prefix, $DBI::errstr );

    @result;
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
    log( $prefix, "profile($xform", ( defined $ns ? ",$ns)" : ')' ) )
	if ( ($main::DEBUG & 0x200) );
    my @xform = split_tr($xform);

    my $q = q{SELECT l.id, p.id
	FROM tc_physicaltx p, tc_lfnpfnmap m, tc_logicaltx l
	WHERE l.namespace=? AND l.name=? AND l.version=? AND p.resourceid=? 
	AND l.id=m.lfnid AND m.pfnid=p.id};
	
    my $sth = $self->{_handle}->prepare_cached( $q ) ||
	croak $prefix, "prepare $q: ", $DBI::errstr;

    # start query
    $sth->execute( (@xform,$site) ) || 
	croak $prefix, "execute $q: ", $sth->errstr;

    # cursor
    my (@row,@x,$st2,$qq);
    my %prof = ();
    if ( (@row = $sth->fetchrow_array()) != 0 ) {
	# logical profiles first
	$qq = 'SELECT * FROM tc_lfnprofile WHERE lfnid=?';
	$st2 = $self->{_handle}->prepare_cached( $qq ) ||
	    croak $prefix, "prepare $qq: ", $DBI::errstr;
	$st2->execute( $row[0] ) || 
	    croak $prefix, "execute $qq: ", $st2->errstr;
	while ( (@x = $st2->fetchrow_array()) != 0 ) {
	    $prof{$x[0]}->{$x[1]} = $x[2];
	}
	croak( $prefix, "fetch $qq: ", $st2->errstr ) if $st2->err;
	$st2->finish() || log( $prefix, $DBI::errstr );

	# physical profiles next
	$qq = 'SELECT * FROM tc_pfnprofile WHERE pfnid=?';
	$st2 = $self->{_handle}->prepare_cached( $qq ) ||
	    croak $prefix, "prepare $qq: ", $DBI::errstr;
	$st2->execute( $row[1] ) || 
	    croak $prefix, "execute $qq: ", $st2->errstr;
	while ( (@x = $st2->fetchrow_array()) != 0 ) {
	    $prof{$x[0]}->{$x[1]} = $x[2];
	}
	croak( $prefix, "fetch $qq: ", $st2->errstr ) if $st2->err;
	$st2->finish() || log( $prefix, $DBI::errstr );
    }
    croak( $prefix, "fetch $q: ", $sth->errstr ) if $sth->err;
    $sth->finish() || log( $prefix, $DBI::errstr );

    if ( defined $ns ) {
	if ( exists $prof{$ns} ) {
	    return %{ $prof{$ns} };
	} else {
	    log( $prefix, "profile $ns for $xform at site $site is empty" );
	    return ();
	}
    } else {
	# all namespaces
	return %prof;
    }
}

#
# --- methods not used inside planner --------------------------------------
#

sub pipe_out_cmd {
    # purpose: bypass /bin/sh when executing an external command
    # paramtr: $stdin_fn (IN): use undef if unused
    #          @_: any other argument, starting with executable
    # returns: lines collected from stdout and stderr.
    # globals: $? will be set to the exit code
    my $self = shift;
    my $input = shift;		# filename to become STDIN
    my @result = ();
    local(*READ);

    log( $prefix, join(' ',@_) ) if ( $main::DEBUG & 0x100 );
    my $pid = open( READ, "-|" );
    return undef unless defined $pid;

    if ( $pid ) {
	# parent
	@result=<READ>;
	close(READ);
    } else {
	# child
	if ( defined $input ) {
	    open( STDIN, "<$input" ) || exit(126);
	}
	open( STDERR, ">&STDOUT" ) || exit(126);
	select(STDERR); $|=1;
	select(STDOUT); $|=1;
	# $^F = fileno(STDERR);
	exec { $_[0] } @_;
	exit(127);
    }
    
    # return the perl way
    @result;
}

sub entry {
    # purpose: accessor to the entry item count number
    my $self = shift;
    my $site = shift || croak $prefix, "need a site handle";
    $self->{count}=0 unless exists $self->{count};
    $self->{site}->{$site} = ++$self->{count} 
        unless exists $self->{site}->{$site};

    $self->{site}->{$site};
}

sub add {
    # purpose: add a complete entry to the transformation catalog
    # paramtr: $site (IN): site handle
    #          $xform (IN): logical transformation name
    #          $app, (IN): physical application name
    #          $pref (opt. IN): reference to profiles, or undef
    #          $inst (opt. IN): installation status, e.g. 'INSTALLED'
    #          $arch (opt. IN): OS Arch stuff, 'null', or undef
    my $self = shift;
    my $site = shift || croak( $prefix, "need a site parameter" );
    my $xform = shift || croak( $prefix, "need a logical transformation name" );
    my $app = shift || croak( $prefix, 'need a physical application path' );

    my $pref = shift || {};
    my $inst = shift || 'INSTALLED';
    my $arch = shift || 'INTEL32::LINUX';

    my @arg = ( $self->{'t_c'}, '-a',
		'-l', $xform, '-p', $app, '-r', $site,	
		'-t', $inst, '-s', $arch );
    foreach my $ns ( keys %{$pref} ) {
	foreach my $key ( keys %{ $pref->{$ns} } ) {
	    push( @arg, '-e', "$ns::$key=" . $pref->{$ns}->{$key} );
	}
    }

    my $result = 0;
    my @temp = $self->pipe_out_cmd( undef, @arg );

    # check result
    if ( $? == 0 ) {
	# all is well
	$result++;
    } elsif ( ($? & 127) > 0 ) {
	# *died* on signal - this is not good
	croak( $prefix, "died on signal @{[$?&127]}: ", 
	       @temp > 5 ? @temp[0..5] : @temp );
    } else {
	# leaves non-zero exit code
	;			# ignore
    }

    $result;
}

sub add_special {
    my $self = shift;
    my $site = shift || croak( $prefix, "need a site parameter" );
    # ignore
    1;
}

sub delete {
    # purpose: removes an entry from the catalog
    # paramtr: $site (IN): site handle to remove
    # paramtr: $xform (opt. IN): if specified, just remove that TR from $site
    # returns: removed entry reference, or undef not exists
    my $self = shift;
    my $site = shift || croak $prefix, "need a site parameter";
    my $xform = shift;

    my @arg = ( $self->{'t_c'}, '-d' );
    if ( defined $xform ) {
	push( @arg, '-L', '-l', $xform, '-r', $site );
    } else {
	push( @arg, '-R', '-r', $site );
    }

    my $result = 0;
    my @temp = $self->pipe_out_cmd( undef, @arg );

    # check result
    if ( $? == 0 ) {
	# all is well
	$result++;
    } elsif ( ($? & 127) > 0 ) {
	# *died* on signal - this is not good
	croak( $prefix, "died on signal @{[$?&127]}: ", 
	       @temp > 5 ? @temp[0..5] : @temp );
    } else {
	# leaves non-zero exit code
	;			# ignore
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

    my @arg = ( $self->{'t_c'}, '-q', '-B' );
    my $result = 0;
    my @temp = $self->pipe_out_cmd( undef, @arg );

    # check result
    if ( $? == 0 ) {
	# all is well
	if ( scalar @_ ) {
	    # restricted subset
	    my %list = map { $_ => 1 } @_;
	    my ($site);
	    foreach ( @temp ) {
		($site) = split /\s+/,$_,2;
		if ( exists $list{$site} ) {
		    print $fh $_ ;
		    $result++;
		}
	    }
	} else {
	    # full subset
	    print $fh @temp;
	    $result = scalar @temp;
	}
    } elsif ( ($? & 127) > 0 ) {
	# *died* on signal - this is not good
	croak( $prefix, "died on signal @{[$?&127]}: ", 
	       @temp > 5 ? @temp[0..5] : @temp );
    } else {
	# leaves non-zero exit code
	;			# ignore
    }

    $result;
}

#
# --- abstract methods ------------------------------------------
#

#
# return 'true' to package loader
#
1;
__END__
