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
package Pegasus::DAX::MetaData;
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

    if ( @_ == 3 ) {
	# called as namespace, key, value
	@{$self}{'key','type','value'} = @_; 
    } elsif ( @_ > 2 && (@_ & 1) == 0 ) {
	# even: called with a=>b,c=>d list
	%{$self} = ( %{$self}, @_ ); 
    } elsif ( @_ == 1 && ref $_[0] eq 'HASH' ) { 
	# called with { a=>b, c=>d } hashref
	%{$self} = ( %{$self}, %{ shift() } ); 
    } else {
	croak "invalid c'tor for ", __PACKAGE__; 
    }

    bless $self, $class; 
}

# forward declarations so can we check using 'can'
sub key;
sub type;
sub value;

sub toXML {
    # purpose: put self onto stream as XML
    # paramtr: F (IN): perl file handle open for writing
    #          ident (IN): indentation level
    #          xmlns (IN): namespace of element, if necessary
    #
    my $self = shift; 
    my $f = shift; 
    my $indent = shift || '';
    my $xmlns = shift; 
    my $tag = defined $xmlns && $xmlns ? "$xmlns:metadata" : 'metadata';

    $f->print( "$indent<$tag", 
	     , attribute('key',$self->key)
	     , attribute('type',$self->type)
	     , ">"
	     , quote($self->value)
	     , "</$tag>\n"
	     ); 
}

1; 
