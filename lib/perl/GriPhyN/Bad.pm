package GriPhyN::Bad;
#
# maintains a list of workflow-global bad sites in BSD DB file
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
# Author: Jens-S. Vöckler   voeckler at cs dot uchicago dot edu
# $Id$
#
use 5.006;
use strict;
use subs qw(log);
use warnings;

use Exporter;
our @ISA = qw(Exporter);

# version management
our $VERSION='1.0';
$VERSION=$1 if ( '$Revision$' =~ /Revision:\s+([0-9.]+)/o );

# prototypes
sub create_lock($);		# { }
sub delete_lock($);		# { }

# create the function prototypes for type coercion
our @EXPORT_OK = qw($VERSION create_lock delete_lock);
our @EXPORT = ();
our %EXPORT_TAGS = ();

#
# --- start -----------------------------------------------------
#
use Carp;
use DB_File;
use GriPhyN::Log qw(log clog);
use Fcntl qw(:DEFAULT :flock);

our $prefix = '[' . __PACKAGE__ . '] ';

# auto-cleanup
my %atexit = ();
END { unlink( keys %atexit ) if %atexit }

sub create_lockfile($) {
    # purpose: create a lock file NFS-reliably
    # paramtr: $fn (IN): name of main file to lock
    # returns: 1 on success, undef on failure to lock
    my $tolock = shift || die;
    local(*LOCK);
    my $lock = "$tolock.lock";
    my $uniq = "$tolock.$$";

    if ( sysopen( LOCK, $uniq, O_CREAT|O_EXCL|O_TRUNC|O_WRONLY ) ) {
	#log( $prefix, "created unique $uniq" ) if ( $main::DEBUG & 0x01 );
	$atexit{$uniq} = 1;
	syswrite( LOCK, "$$\n" );
	close LOCK;

	if ( link( $uniq, $lock ) ) {
	    # created link
	    #log( $prefix, "hardlink locked" ) if ( $main::DEBUG & 0x01 );
	    $atexit{$lock} = 1;
	    unlink($uniq) && delete $atexit{$uniq};
	    return 1;
	} else {
	    # unable to create link, check errno
	    #log( $prefix, "while locking: $!\n" ) if ( $main::DEBUG & 0x01 );
	    if ( (stat($uniq))[3] == 2 ) {
		# lock was still successful
		$atexit{$lock} = 1;
		#log( $prefix, 'link-count locked' ) if ( $main::DEBUG & 0x01 );
		return 1;
	    }
	}
    } else {
	log( $prefix, "Locking: open $uniq: $!\n" );
    }

    unlink $uniq;
    undef;
}

sub create_lock($) {
    # purpose: blockingly wait for lock file creation
    # paramtr: $fn (IN): name of file to create lock file for
    # returns: 1: lock was created.
    my $fn = shift || croak "Need a filename to lock";
    while ( ! create_lockfile($fn) ) { 
	log( $prefix, "lock on $fn is busy, waiting..." );
	sleep(1);
    }
    log( $prefix, "obtained lock for $fn" );
    1;
}

sub delete_lock($) {
    # purpose: removes a lock file NFS-reliably
    # paramtr: $fn (IN): name of main file to lock
    # returns: 1 on success, undef on failure to lock
    my $tolock = shift || croak "Need a filename to unlock";
    unlink( "$tolock.lock", "$tolock.$$" );
    log( $prefix, "released lock for $tolock" );
}

sub new {
    my $proto = shift;
    my $class = ref($proto) || $proto || __PACKAGE__;
    my $file = shift || croak "Need the name of the database file";

    # return the perl way
    bless { 'm_file' => $file }, $class;
}

sub file {
    my $self = shift;
    my $oldv = $self->{'m_file'};
    $self->{'m_file'} = shift if @_;
    $oldv;
}

sub read {
    # purpose: read bad site list
    # paramtr: $timeout (opt. IN): timeout for retries
    # returns: hash with bad sites, may be empty
    my $self = shift || croak;
    my $timeout = shift || 600;	# default timeout for retry

    create_lock( $self->file );
    my %result = ();
    my %db;
    my $now = time;
    if ( tie( %db, 'DB_File', $self->file ) ) {
	my ($k,$v);
	while ( ($k,$v) = each %db ) {
	    if ( $v+$timeout > $now ) {
		$result{$k} = $v;
	    } else {
		log( $prefix, "re-admitting site $k" );
	    }
	}
	untie %db;
    }

    delete_lock( $self->file );
    %result;
}

sub add {
    # purpose: add an entry -- expensive operation
    # paramtr: $site (IN): site handle
    #          $when (IN): current timestamp
    # returns: 1 for success, undef for failure
    my $self = shift || croak;
    my $site = shift || croak "Need a resource handle";
    my $when = shift || time();
    
    create_lock( $self->file );
    my (%db, $result);
    if ( tie( %db, 'DB_File', $self->file ) ) {
	$db{$site} = $when;
	untie %db;
	$result=1;
    }
    delete_lock( $self->file );
    $result;
}

sub write {
    # purpose: dump bad site list
    # paramtr: $hashref (IN): hash ref with bad sites
    # returns: 1 for success, undef for failure
    my $self = shift;
    my $hashref = shift;
    croak "Need a hash ref" unless ref($hashref) eq 'HASH';

    create_lock( $self->file );
    my (%db,$result);
    if ( tie( %db, 'DB_File', $self->file ) ) {
	my ($k,$v);
	while ( ($k,$v) = each %{$hashref} ) {
	    $db{$k} = $v if ( $k ne '!!SITE!!' && $v > 0 );
	}
	untie %db;
	$result=1;
    }
    delete_lock( $self->file );
    $result;
}

sub show {
    # purpose: dump bad site list onto stdout -- for debugging
    my $self = shift;
    create_lock( $self->file );
    my (%db,$result);
    if ( tie( %db, 'DB_File', $self->file ) ) {
	$result=1;
	foreach my $site ( sort { $db{$a} <=> $db{$b} } keys %db ) {
	    printf "%-32s %lu\n", $site, $db{$site};
	}
	untie %db;
    }
    delete_lock( $self->file );
    $result;
}
#
# return 'true' to package loader
#
1;
__END__
