package GriPhyN::RC::DBI;
#
# DBI-based replica manager implementation. Should be able to speak to
# any database that DBI can speak to, e.g. SQLite2, Pg, MySQL, ...
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
use vars qw($VERSION);
use Exporter;
use DBI;

our @ISA = qw(Exporter GriPhyN::RC);
our @EXPORT = qw();
our @EXPORT_OK = qw($VERSION);

$VERSION=$1 if ( '$Revision$' =~ /Revision:\s+([0-9.]+)/o );

#
# --- start -----------------------------------------------------
#
use Carp;
use GriPhyN::Log qw(log);

our $prefix = '[' . __PACKAGE__ . '] ';

#
# ctor
#
sub new {
    my $proto = shift;
    my $class = ref($proto) || $proto || __PACKAGE__;
    my $self = bless GriPhyN::RC->new( style => 'DBI', @_ ), $class;

    # connect
    for ( my $retries=1; $retries <= 5; $retries++ ) {
	$self->{_handle} = DBI->connect( $self->{uri}, 
					 $self->{dbuser} || '',
					 $self->{dbpass} || '', 
					 { PrintError => 0,
					   RaiseError => 0, 
					   AutoCommit => 0 } );
	last if defined $self->{_handle};
	log $prefix, "retry $retries: Unable to connect to database";
	die( $prefix, 'out of retries' ) if $retries == 5;
    }

    # check for schema
    my @temp;
    for ( my $retries=1; $retries <= 5; $retries++ ) {
	@temp = $self->{_handle}->tables('%','','RC_MAP');
	last if @temp > 0;
	@temp = $self->{_handle}->tables('%','','rc_map');
	last if @temp > 0;
    }
    croak( $prefix, "ERROR: Please setup table RC_MAP first" )
	if ( @temp == 0 );

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

sub insert {
    # purpose: insert a LFN with all PFNs into the RC
    # paramtr: $lfn (IN): the LFN to insert
    #          @pfn (IN): list of PFNs to insert
    # returns: true if inserted, false if not (e.g. existed or failure)
    my $self = shift;
    my $lfn = shift || croak $prefix, "what is the LFN";
    croak( $prefix, "there are no PFNs" ) unless @_ > 0;

    my $sth = $self->{_handle}->prepare_cached( q{
	INSERT INTO rc_map(lfn,pfn) VALUES(?,?) 
	} ) || croak $prefix, $DBI::errstr;

    my $result = 0;
    foreach my $pfn ( @_ ) {
	if ( $sth->execute( $lfn, $pfn ) ) {
	    $result++;
	} else {
	    log( $prefix, $sth->errstr );
	}
    }
    $sth->finish() || log( $prefix, $DBI::errstr );

    $self->{_handle}->commit();
    $result;
}


sub bulk_insert {
    # purpose: insert a mapping from unique LFNs to all PFNs into the RC
    # paramtr: { $lfn => [ $pfn1 ... ], ... }
    # returns: number of successful insertions
    # warning: override with more efficient implementation.
    my $self = shift;
    my $href = shift || croak $prefix, "need a hash reference";
    croak( $prefix, 'need a hash reference' ) unless ref($href) eq 'HASH';

    my $sth =  $self->{_handle}->prepare_cached( q{
	INSERT INTO rc_map(lfn,pfn) VALUES(?,?) 
	} ) || croak $prefix, $DBI::errstr;

    my $result = 0;
    foreach my $lfn ( keys %{$href} ) {
	foreach my $pfn ( @{ $href->{$lfn} } ) {
	    if ( $sth->execute( $lfn, $pfn ) ) {
		$result++;
	    } else {
		log( $prefix, $sth->errstr );
	    }
	}
    }
    $sth->finish() || log( $prefix, $DBI::errstr );

    $self->{_handle}->commit();
    $result;
}

sub remove {
    # purpose: remove a specifc PFN mapping for a given LFN
    # paramtr: $lfn (IN): LFN to work with
    #          @pfn (IN): PFN(s) to remove
    # returns: number of deleted columns;
    my $self = shift;
    my $result = 0;
    my $lfn = shift || croak $prefix, "what is the LFN";
    croak( $prefix, "what are the PFN mappings" ) unless @_ > 0;

    my $sth =  $self->{_handle}->prepare_cached( q{
	DELETE FROM rc_map WHERE lfn=? AND pfn=?
	} ) || croak $prefix, $DBI::errstr;

    # delete
    foreach my $pfn ( @_ ) {
	my $rc = $sth->execute( $lfn, $pfn ) || log( $prefix, $sth->errstr );
	$result += $rc;
    }
    $sth->finish() || log( $prefix, $DBI::errstr );

    $self->{_handle}->commit();
    $result;
}

sub bulk_remove {
    # purpose: remove all mapping for a given LFN
    # paramtr: one or more LFNs
    # returns: - (number of existing LFNs that were removed)
    my $self = shift;
    croak( $prefix, "what is the LFN" ) unless @_ > 0;

    my $sth =  $self->{_handle}->prepare_cached( q{
	DELETE FROM rc_map WHERE lfn=?
	} ) || croak $prefix, $DBI::errstr;

    my $result = $sth->execute_array( { }, [ @_ ] ) || 
	log( $prefix, $sth->errstr );
    $sth->finish() || log( $prefix, $DBI::errstr );

    $self->{_handle}->commit();
    $result;
}

sub lookup {
    # purpose: look-up the PFNs for a given LFN
    # paramtr: $lfn (IN): the LFN to search for
    # returns: vector: list of PFNs for the given LFN (may be empty)
    #          scalar: first element of above list (may be undef)
    my $self = shift;
    my $lfn = shift || croak $prefix, "what is the LFN";

    my $sth =  $self->{_handle}->prepare_cached( q{
	SELECT pfn FROM rc_map WHERE lfn=?
	} ) || croak $prefix, $DBI::errstr;

    my (@row,@result);

    $sth->execute($lfn) || croak $prefix, $sth->errstr;
    while ( (@row = $sth->fetchrow_array()) != 0 ) {
	push( @result, $row[0] );
    }
    croak( $prefix, $sth->errstr ) if $sth->err;
    $sth->finish() || log( $prefix, $DBI::errstr );

    wantarray ? @result : $result[0];
}

sub bulk_lookup {
    # purpose: look-up the PFNs for a list of LFNs
    # paramtr: @lfn (IN): the unique LFNs to search for
    # returns: a hash with the LFN as key, and a list of PFNs as value
    # warning: marginally more efficient. 
    my $self = shift;
    croak( $prefix, "what are the LFNs" ) unless @_ > 0;

    my $sth = $self->{_handle}->prepare_cached( q{
	SELECT pfn from rc_map WHERE lfn=?
	} ) || croak $prefix, $DBI::errstr;

    my (@row,%result);
    foreach my $lfn ( @_ ) {
	$sth->execute($lfn) || croak $prefix, $sth->errstr;
	while ( (@row = $sth->fetchrow_array()) != 0 ) {
	    push( @{$result{$lfn}}, $row[0] );
	}
	croak( $prefix, $sth->errstr ) if $sth->err;
    }
    $sth->finish() || log( $prefix, $DBI::errstr );

    %result;
}

sub wildcard_lookup {
    # purpose: look-up LFNs that match with asterisk wildcarding
    # paramtr: $pattern (IN): pattern to match
    # returns: a hash with the LFN as key, and a list of PFNs as value
    my $self = shift;
    my $pattern = shift || croak $prefix, "what is the LFN pattern";

    my $sth =  $self->{_handle}->prepare_cached( q{
	SELECT lfn,pfn FROM rc_map WHERE lfn LIKE ?
	} ) || croak $prefix, $DBI::errstr;

    # start query
    $sth->execute($pattern) || croak $prefix, $sth->errstr;

    # cursor
    my (@row,%result);
    while ( (@row = $sth->fetchrow_array()) != 0 ) {
	push( @{$result{$row[0]}}, $row[1] );
    }
    croak( $prefix, $sth->errstr ) if $sth->err;
    $sth->finish() || log( $prefix, $DBI::errstr );

    %result;
}


# Autoload methods go after =cut, and are processed by the autosplit program.

#
# return 'true' to package loader
#
1;
__END__
