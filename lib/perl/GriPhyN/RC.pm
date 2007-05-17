package GriPhyN::RC;
#
# abstract base class for replica manager implementations
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
# Revision : $Revision: 1.1 $
#
# $Id: RC.pm,v 1.1 2005/08/08 22:04:43 griphyn Exp $
#
use 5.006;
use strict;
use vars qw($VERSION);
use Exporter;

our @ISA = qw(Exporter);
our @EXPORT = qw();
our @EXPORT_OK = qw($VERSION);

$VERSION=$1 if ( '$Revision: 1.1 $' =~ /Revision:\s+([0-9.]+)/o );

#
# --- start -----------------------------------------------------
#
use Carp;
our $prefix = '[' . __PACKAGE__ . '] ';

#
# ctor
#
sub new {
    # purpose: Initialize base class
    # paramtr: @_ (IN): any garden variety
    # returns: a blessed object
    my $proto = shift;
    my $class = ref($proto) || $proto;
    my $self = bless { @_ }, $class; # any other business

    # activate the default pool
    $self->{pool} = undef;

    # return handle to self
    $self;
}

# Preloaded methods go here.

sub pool {
    # purpose: get/set the current pool value for insertion
    my $self = shift;
    my $oldv = $self->{pool};
    $self->{pool} = shift() if ( @_ );
    $oldv;
}

sub insert {
    # purpose: insert a LFN with all PFNs into the RC
    # paramtr: $lfn (IN): the LFN to insert
    #          @pfn (IN): list of PFNs to insert
    # returns: true if inserted, false if not (e.g. existed or failure)
    my $self = shift;
    croak $prefix, "Somebody forgot to implement the RC insert method";
}

sub bulk_insert {
    # purpose: insert a mapping from unique LFNs to all PFNs into the RC
    # paramtr: { $lfn => [ $pfn1 ... ], ... }
    # returns: number of successful insertions
    # warning: override with more efficient implementation.
    my $self = shift;
    my $href = shift || croak $prefix, "need a hash reference";

    my $result = 0;
    foreach my $lfn ( keys %{$href} ) {
	$result += $self->insert( $lfn, @{ $href->{$lfn} } );
    }
    $result;
}

sub remove {
    # purpose: remove a specifc PFN mapping for a given LFN
    # paramtr: $lfn (IN): LFN to work with
    #          @pfn (IN): PFN(s) to remove
    # returns: -
    my $self = shift;
    croak( $prefix, "what is the LFN/PFN" ) unless @_ > 1;
    croak $prefix, "Somebody forgot to implement the RC remove method";    
}

sub bulk_remove {
    # purpose: remove all mapping for a given LFN
    # paramtr: one or more LFNs
    # returns: -
    my $self = shift;
    croak( $prefix, "what is the LFN" ) unless @_ > 0;
    croak $prefix, "Somebody forgot to implement the RC bulk_remove method";
}

sub lookup {
    # purpose: look-up the PFNs for a given LFN
    # paramtr: $lfn (IN): the LFN to search for
    # returns: vector: list of PFNs for the given LFN (may be empty)
    #          scalar: first element of above list (may be undef)
    my $self = shift;
    my $lfn = shift || croak $prefix, "what is the LFN";
    croak $prefix, "Somebody forgot to implement the RC lookup method";
}

sub bulk_lookup {
    # purpose: look-up the PFNs for a list of LFNs
    # paramtr: @lfn (IN): the unique LFNs to search for
    # returns: a hash with the LFN as key, and a list of PFNs as value
    # warning: override with more efficient method.
    my $self = shift;
    croak( $prefix, "what are the LFNs" ) unless @_ > 0;

    my %result;
    foreach my $lfn ( @_ ) {
	$result{$lfn} = [ $self->lookup($lfn) ];
    }
    %result;
}

sub wildcard_lookup {
    # purpose: look-up LFNs that match with asterisk wildcarding
    # paramtr: $pattern (IN): pattern to match
    # returns: a hash with the LFN as key, and a list of PFNs as value
    my $self = shift;
    croak( $prefix, "what is the LFN pattern" ) unless @_ > 0;
    croak $prefix, "Somebody forgot to implement the RC wildcard_lookup method";
}


# Autoload methods go after =cut, and are processed by the autosplit program.

#
# return 'true' to package loader
#
1;
__END__
