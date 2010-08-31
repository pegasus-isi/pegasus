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
package Pegasus::DAX::Base;
use 5.006;
use strict;
use vars qw($AUTOLOAD);

use Exporter;
our @ISA = qw(Exporter); 

sub quote($);			# { }
sub attribute($$);		# { }

our $VERSION = '3.2'; 
our @EXPORT = (); 
our @EXPORT_OK = qw(quote attribute $escape %escape); 
our %EXPORT_TAGS = ( xml => [ @EXPORT_OK ] ); 

our $prefix = '[' . __PACKAGE__ . '] ';

sub new {
    my $proto = shift;
    my $class = ref($proto) || $proto;
    my $self = bless { '_permitted' => { } }, $class; 

    $self;
}

sub AUTOLOAD {
    # purpose: catch-all accessor (set and get) for all data fields
    #          ever defined in any great-grandchild of this class
    # warning: The autoload maps the data fields XYZ to method XYZ
    # paramtr: ?
    # returns: ?
    my $self = shift;
    my $type = ref($self) or croak( $prefix, "$self is not an object" );

    my $name = $AUTOLOAD;
    $name =~ s/.*:://;   # strip fully-qualified portion

    unless ( exists $self->{$name} ) {
        croak( $prefix, "Can't access >>$name<< field in class $type" );
    }

    my $result = $self->{$name};
    $self->{$name} = shift if (@_);
    $result;
}

use Carp; 

our %escape = ( '&' => '&amp;'
	      , '<' => '&lt;'
	      , '>' => '&gt;'
	      , "'" => '&apos;'
	      , '"' => '&quot;' 
    );
our $escape = '([' . join( '', keys %escape ) . '])'; 

sub quote($) {
    # static method
    # purpose: quote XML entities inside a value string
    # paramtr: $s (IN): value string
    # returns: quoted version, possibly same string
    #
    my $s = shift; 
    $s =~ s/$escape/$escape{$1}/ge if defined $s; 
    $s; 
}

sub attribute($$) { 
    # purpose: format an element attribute
    # paramtr: $key (IN): name of attribute
    #          $val (IN): value for attribute
    # returns: formatted string
    # warning: may return empty string if key is empty
    #
    my $key = shift; 
    my $val = shift; 
    if ( defined $key && $key && defined $val ) {
	" $key=\"", quote($val), "\""; 
    } else {
	'';
    }
}

sub toXML {
    # purpose: put self onto stream as XML
    # paramtr: F (IN): perl file handle open for writing
    #          ident (IN): indentation level
    #          xmlns (opt. IN): namespace of element, if necessary
    #
    my $self = shift; 
    croak "Called ", __PACKAGE__, "::toXML"; 
}

1; 
