#
# License: (atend)
# $Id$
#
package Pegasus::DAX::CatalogType;
use 5.006;
use strict;
use Carp; 

use Pegasus::DAX::Base qw(:xml); 
use Exporter;
our @ISA = qw(Pegasus::DAX::Base Exporter); 

our $VERSION = '3.3'; 
our @EXPORT = (); 
our %EXPORT_TAGS = ();
our @EXPORT_OK = ();

# one AUTOLOAD to rule them all
BEGIN { *AUTOLOAD = \&Pegasus::DAX::Base::AUTOLOAD }

sub new {
    my $proto = shift;
    my $class = ref($proto) || $proto;
    my $self = $class->SUPER::new();

    if ( @_ == 0 ) { 
	# nothing to do
    } elsif ( @_ > 1 ) {
	# called with a=>b,c=>d list
	%{$self} = ( %{$self}, @_ ); 
    } elsif ( @_ == 1 && ref $_[0] eq 'HASH' ) { 
	# called with { a=>b, c=>d } hashref
	%{$self} = ( %{$self}, %{ shift() } ); 
    } else {
	croak "invalid c'tor for ", __PACKAGE__; 
    }

    bless $self, $class; 
}

# forward declaration
sub name; 

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
    } elsif ( @_ == 1 && $_[0]->isa('Pegasus::DAX::PFN' ) ) {
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
    # returns: number of inner elements produced
    #
    my $self = shift; 
    my $f = shift; 
    my $indent = shift || '';
    my $xmlns = shift; 
    my $result = 0; 

    #
    # <profile>
    #
    if ( exists $self->{profiles} ) {
	foreach my $i ( @{$self->{profiles}} ) { 
	    $result++;
	    $i->toXML($f,$indent,$xmlns);
	}
    }

    #
    # <metadata>
    #
    if ( exists $self->{metas} ) {
	foreach my $i ( @{$self->{metas}} ) { 
	    $result++;
	    $i->toXML($f,$indent,$xmlns);
	}
    }

    #
    # <pfn>
    #
    if ( exists $self->{pfns} ) {
	foreach my $i ( @{$self->{pfns}} ) { 
	    $result++;
	    $i->toXML($f,$indent,$xmlns);
	}
    }

    $result; 
}

1; 
__END__


=head1 NAME

Pegasus::DAX::CatalogType - abstract class for included transformation-
and replica catalogs. 

=head1 SYNOPSIS

This is an abstract class. You do not instantiate abstract classes. 

=head1 DESCRIPTION

This class is the base for the included transformation- and replica
catalog entry. 

=head1 METHODS

=over 4

=item new()

The constructor is used by child classes to establish data structures. 

=item addProfile( $namespace, $key, $value )

=item addProfile( $profile_instance )

This method will add a specified profile, either as three strings or
instance of L<Pegasus::DAX::Profile>, to the collection of profiles
associated with the logical level catalog entry. 

=item addMeta( $key, $type, $value )

=item addMeta( $metadata_instance )

This method adds a piece of meta data to the collection of meta data,
either as trhee strings or instance of L<Pegasus::DAX::MetaData>,
associated with this logical catalog entry.

=item addPFN( $url )

=item addPFN( $url, $site )

=item addPFN( $pfn_instance )

This method adds a physical filename, either as url and site string or
instance of L<Pegasus::DAX::PFN>, to the collection of PFNs associated
with this catalog entry.

=item innerXML( $handle, $indent, $xmlns )

The purpose of the C<innerXML> function is to recursively generate XML from
the internal data structures. Since this class is abstract, it will not
create the element tag nor attributes. However, it needs to create the
inner elements as necessary. 

The first argument is a file handle open for writing. This is where the
XML will be generated.  The second argument is a string with the amount
of white-space that should be used to indent elements for pretty
printing. The third argument may not be defined. If defined, all element
tags will be prefixed with this name space.

=back 

=head1 SEE ALSO

=over 4

=item L<Pegasus::DAX::Base>

Base class. 

=item L<Pegasus::DAX::File>

Replica catalog entry child class.

=item L<Pegasus::DAX::Executable>

Transformation catalog entry child class. 

=back 

=head1 COPYRIGHT AND LICENSE

Copyright 2007-2010 University Of Southern California

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

=cut
