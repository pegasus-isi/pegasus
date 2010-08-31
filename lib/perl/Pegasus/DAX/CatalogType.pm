##
#  Copyright 2007-2010 University Of Southern California
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#
##
# $Id$
#
package Pegasus::DAX::CatalogType;
use 5.006;
use strict;
use Carp; 

use Pegasus::DAX::Base qw(:xml); 
use Exporter;
our @ISA = qw(Pegasus::DAX::Base Exporter); 

our $VERSION = '3.2'; 
our @EXPORT = (); 
our %EXPORT_TAGS = ();
our @EXPORT_OK = ();

sub new {
    my $proto = shift;
    my $class = ref($proto) || $proto;
    my $self = $class->SUPER::new();

    if ( @_ > 1 ) {
	# called with a=>b,c=>d list
	%{$self} = ( @_ ); 
    } elsif ( @_ == 1 && ref $_[0] eq 'HASH' ) { 
	# called with { a=>b, c=>d } hashref
	%{$self} = %{ shift() }; 
    } else {
	croak "invalid c'tor for ", __PACKAGE__; 
    }

    bless $self, $class; 
}

sub addMeta {
    my $self = shift;

    my $meta; 
    if ( @_ == 3 ) {
	# explicit
	$meta = Pegasus::DAX::MetaData->new( shift(), shift(), shift() ); 
    } elsif ( @_ == 1 && ref $_[0] && $_[0]->isa('Pegasus::DAX::MetaData') ) {
	$meta = shift; 
    } else {
	croak "argument is not a valid MetaData";
    }

    if ( exists $self->{metas} ) {
	push( @{$self->{metas}}, $meta );
    } else {
	$self->{metas} = [ $meta ]; 
    }
}

sub addPFN {
    my $self = shift;

    my $pfn; 
    if ( @_ == 1 && ! ref $_[0] ) {
	# plain string argument as PFN, no pfn-profiles
	$pfn = Pegasus::DAX::PFN->new( shift() ); 
    } elsif ( @_ == 2 && ! ref $_[0] && ! ref $_[1] ) {
	# two plain strings, no pfn-profiles
	$pfn = Pegasus::DAX::PFN->new( shift(), shift() ); 
    } elsif ( @_ == 1 && $pfn->isa('Pegasus::DAX::PFN' ) ) {
	# ok
	$pfn = shift; 
    } else {
	croak "argument is not a valid PFN";
    }

    if ( exists $self->{pfns} ) {
	push( @{$self->{pfns}}, $pfn );
    } else {
	$self->{pfns} = [ $pfn ];
    }
}

sub addProfile {
    my $self = shift;

    my $prof; 
    if ( @_ == 3 ) {
	# explicit
	$prof = Pegasus::DAX::Profile->new( shift(), shift(), shift() ); 
    } elsif ( @_ == 1 && ref $_[0] && $_[0]->isa('Pegasus::DAX::Profile') ) {
	$prof = shift; 
    } else {
	croak "argument is not a valid Profile";
    }

    if ( exists $self->{profiles} ) {
	push( @{$self->{profiles}}, $prof );
    } else {
	$self->{profiles} = [ $prof ]; 
    }
}

sub innerXML {
    # purpose: put self onto stream as XML
    # paramtr: F (IN): perl file handle open for writing
    #          ident (IN): indentation level
    #          xmlns (IN): namespace of element, if necessary
    #
    my $self = shift; 
    my $f = shift; 
    my $indent = shift || '';
    my $xmlns = shift; 

    #
    # <profile>
    #
    if ( exists $self->{profiles} ) {
	foreach my $i ( @{$self->{profiles}} ) { 
	    $i->toXML($f,"  $indent",$xmlns);
	}
    }

    #
    # <metadata>
    #
    if ( exists $self->{metas} ) {
	foreach my $i ( @{$self->{metas}} ) { 
	    $i->toXML($f,"  $indent",$xmlns);
	}
    }

    #
    # <pfn>
    #
    if ( exists $self->{pfns} ) {
	foreach my $i ( @{$self->{pfns}} ) { 
	    $i->toXML($f,"  $indent",$xmlns);
	}
    }
}

1; 
