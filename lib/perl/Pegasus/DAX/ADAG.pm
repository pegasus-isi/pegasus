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
package Pegasus::DAX::ADAG;
use 5.006;
use strict;
use Carp; 

use Pegasus::DAX::Base qw(:xml); 
use Pegasus::DAX::AbstractJob; 
use Exporter;
our @ISA = qw(Pegasus::DAX::AbstractJob Exporter); 

use constant SCHEMA_NAMESPACE => 'http://pegasus.isi.edu/schema/DAX'; 
use constant SCHEMA_LOCATION => 'http://pegasus.isi.edu/schema/dax-3.2.xsd';
use constant SCHEMA_VERSION => 3.2;

our $VERSION = '3.2'; 
our @EXPORT = (); 
our @EXPORT_OK = qw(SCHEMA_NAMESPACE SCHEMA_LOCATION SCHEMA_VERSION); 
our %EXPORT_TAGS = (); 

sub new {
    my $proto = shift;
    my $class = ref($proto) || $proto;
    my $self = $class->SUPER::new();
    $self->{index} = 0;
    $self->{count} = 1; 
    
    if ( @_ > 1 ) {
	# called with a=>b,c=>d list
	%{$self} = ( %{$self}, @_ ); 
    } elsif ( @_ == 1 && ref $_[0] eq 'HASH' ) { 
	# called with { a=>b, c=>d } hashref
	%{$self} = ( %{$self}, %{ shift() } ); 
    }

    bless $self, $class; 
}

# forward declarations
sub name;
sub index;
sub count; 

sub addDependency {
    my $self = shift; 
    my $parent = shift;
    my $child = shift; 
    my $label = shift; 

    # we only need the job identifier string
    if ( ref $parent ) {
	if ( $parent->isa('Pegasus::DAX::AbstractJob') ) {
	    $parent = $parent->id;
	} else {
	    croak "parent is not a job type";
	}
    }

    # we only need the job identifier string
    if ( ref $child ) {
	if ( $child->isa('Pegasus::DAX::AbstractJob') ) {
	    $child = $child->id;
	} else {
	    croak "child is not a job type"; 
	}
    }

    # spring into existence -- store undef, if necessary
    $self->{deps}->{$child}->{$parent} = $label; 
}


sub toXML {
    # purpose: put self onto stream as XML
    # paramtr: F (IN): perl file handle open for writing
    #          ident (IN): indentation level
    #          xmlns (opt. IN): namespace of element, if necessary
    #
    my $self = shift; 
    my $f = shift; 
    my $indent = shift || '';
    my $xmlns = shift; 
    my $tag = defined $xmlns && $xmlns ? "$xmlns:adag" : 'adag';
    $f->print( "$indent<$tag"
	     , attribute('xmlns',SCHEMA_NAMESPACE)
	     , attribute('xmlns:xsi','http://www.w3.org/2001/XMLSchema-instance')
	     , attribute('xsi:schemaLocation',SCHEMA_NAMESPACE . ' ' . SCHEMA_LOCATION)
	     , attribute('version',SCHEMA_VERSION)
	     , attribute('name',$self->{name})
	     , attribute('index',$self->{index})
	     , attribute('count',$self->{count})
	     , ">\n" ); 

    #
    # <file>
    #
    if ( exists $self->{files} ) {
	foreach my $i ( @{$self->{files}} ) {
	    $i->toXML($f,"  $indent",$xmlns); 
	}
    }

    #
    # <executable>
    #
    if ( exists $self->{executables} ) { 
	foreach my $i ( @{$self->{executables}} ) { 
	    $i->toXML($f,"  $indent",$xmlns);
	}
    }

    #
    # <transformation>
    #
    if ( exists $self->{transformations} ) { 
	foreach my $i ( @{$self->{transformations}} ) {
	    $i->toXML($f,"  $indent",$xmlns);
	}
    }

    #
    # <DAG|DAX|Job|ADAG>
    #
    if ( exists $self->{jobs} ) {
	foreach my $i ( @{$self->{jobs}} ) {
	    $i->toXML($f,"  $indent",$xmlns);
	}
    }
    
    #
    # <child>
    # 
    if ( exists $self->{deps} ) {
	my $ctag = defined $xmlns && $xmlns ? "$xmlns:child" : 'child';
	my $ptag = defined $xmlns && $xmlns ? "$xmlns:parent" : 'parent';
	while ( my ($child,$r) = each %{$self->{deps}} ) { 
	    $f->print( "  $indent<$ctag"
		     , attribute('ref',$child)
		     , ">\n" );
	    while ( my ($parent,$label) = each %{$r} ) {
		$f->print( "    $indent<$ptag", attribute('ref',$parent) );
		$f->print( attribute('edge-label',$label) ) 
		    if ( defined $label && $label ne '' ); 
		$f->print( "/>\n" ); 
	    }
	    $f->print( "  $indent</$ctag>\n" ); 
	}
    }

    $f->print( "$indent</$tag>\n" ); 
}

1; 
