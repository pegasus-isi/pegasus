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
package Pegasus::DAX::File;
use 5.006;
use strict;
use Carp; 

use Pegasus::DAX::Base qw(:xml); 
use Pegasus::DAX::CatalogType; 
use Exporter;
our @ISA = qw(Pegasus::DAX::CatalogType Exporter); 

our $VERSION = '3.2'; 
our @EXPORT = (); 
our @EXPORT_OK = (); 
our %EXPORT_TAGS = (); 

sub new {
    my $proto = shift;
    my $class = ref($proto) || $proto;
    my $self = $class->SUPER::new();
    
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
sub link;
sub optional;

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
    my $tag = defined $xmlns && $xmlns ? "$xmlns:file" : 'file';

    $f->print( "$indent<$tag"
	     , attribute('name',$self->name)
	     , attribute('link',$self->link)
	     , attribute('optional',$self->optional)
	     , ">\n" );
    $self->innerXML($f,"  $indent",$xmlns); 
    $f->print( "$indent</$tag>\n" );
}

1; 
