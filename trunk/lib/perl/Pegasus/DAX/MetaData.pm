#
# License: (atend)
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

# one AUTOLOAD to rule them all
BEGIN { *AUTOLOAD = \&Pegasus::DAX::Base::AUTOLOAD }

sub new {
    my $proto = shift;
    my $class = ref($proto) || $proto;
    my $self = $class->SUPER::new();

    if ( @_ == 0 ) { 
	# nothing to do
    } elsif ( @_ == 3 ) {
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
	     , attribute('key',$self->key,$xmlns)
	     , attribute('type',$self->type,$xmlns)
	     , ">"
	     , quote($self->value)
	     , "</$tag>\n"
	     ); 
}

1; 
__END__


=head1 NAME

Pegasus::DAX::Metadata - stores a Pegasus piece of meta data.

=head1 SYNOPSIS

    use Pegasus::DAX::Metadata; 

    my $a = Pegasus::DAX::Profile->new( 'key', 'type', 'fubar' ); 
    my $b = Pegasus::DAX::Profile->new( key => 'foo'
    				      , type => 'integer'
                                      , value => 'bar' );

=head1 DESCRIPTION

This class remembers a Pegasus meta data. The internal transformation-
and replica catalog may use meta data associated with entries. 

=head1 METHODS

=over 4

=item new()

=item new( $key, $type, $value )

=item new( a => b, c => d, ... )

=item new( { a => b, c => d, ... } )

The default constructor will create an empty instance whose scalar
attributes can be adjusted using the getters and setters provided by the
C<AUTOLOAD> inherited method.

When invoked with exactly 3 arguments, the first argument is the meta
data key, the second argument the type identifier, and the third
argument the value to set.

Other means of construction is to use named lists.

=item key

Setter and getter for a key string. The key value may be of restricted
range, dependinng on the namespace, but this is not checked at this
point.

=item type

Setter and getter for a type string. Types are not standardized in any
way.

=item value

Setter and getter for the value to be transported. 

=item toXML( $handle, $indent, $xmlns )

The purpose of the C<toXML> function is to recursively generate XML from
the internal data structures. The first argument is a file handle open
for writing. This is where the XML will be generated.  The second
argument is a string with the amount of white-space that should be used
to indent elements for pretty printing. The third argument may not be
defined. If defined, all element tags will be prefixed with this name
space.

=back 

=head1 SEE ALSO

=over 4

=item L<Pegasus::DAX::Base>

Base class. 

=item L<Pegasus::DAX::CatalogType>

Abstract class using meta data. 

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
