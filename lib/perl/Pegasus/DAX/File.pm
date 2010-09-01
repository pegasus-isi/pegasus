#
# License: (atend)
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
our %EXPORT_TAGS = (); 
our @EXPORT = (); 
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
	croak "invalid c'tor invocation"; 
    }

    bless $self, $class; 
}

# forward declarations
#sub name;			# inherited
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
	     , attribute('name',$self->name,$xmlns)
	     , attribute('link',$self->link,$xmlns)
	     , attribute('optional',boolean($self->optional),$xmlns)
	     , ">\n" );
    $self->innerXML($f,"  $indent",$xmlns); 
    $f->print( "$indent</$tag>\n" );
}

1; 
__END__


=head1 NAME

Pegasus::DAX::File - stores an included replica catalog entry. 

=head1 SYNOPSIS

    use Pegasus::DAX::File; 

    my $a = Pegasus::DAX::File(); 
    $a->name( 'lfn' );
    $a->link( LINK_IN );
    $a->optional( 0 ); 
    $a->addPFN( ... ); 
  
=head1 DESCRIPTION

This class remembers an included Pegasus replica catalog entry. 

=head1 METHODS

=over 4

=item new()

=item new( a => b, c => d, ... )

=item new( { a => b, c => d, ... } )

The default constructor will create an empty instance whose scalar
attributes can be adjusted using the getters and setters provided by the
C<AUTOLOAD> inherited method.

Other means of construction is to use named lists. However, you will
have to be aware of the internals to be able to use these lists
successfully.

=item name

Setter and getter for the logical filename. 

=item link

Setter and getter for the link attribute string. Please use the
constants from L<Pegasus::DAX::Filename> for proper linkage.

=item optional

Setter and getter for the I<optional> boolean attribute. Please use
Perl's notion of Truth. If you give the setter the string I<false>, it
will be printed as I<true>, because it is true by Perl's logic.

=item toXML( $handle, $indent, $xmlns )

The purpose of the C<toXML> function is to recursively generate XML from
the internal data structures. The first argument is a file handle open
for writing. This is where the XML will be generated.  The second
argument is a string with the amount of white-space that should be used
to indent elements for pretty printing. The third argument may not be
defined. If defined, all element tags will be prefixed with this name
space.

=back 

=head1 INHERITED METHODS

Please refer to L<Pegasus::DAX::CatalogType> for inherited methods. 

=over 4

=item addMeta( $key, $type, $value )

=item addMeta( $metadata_instance )

=item addPFN( $url )

=item addPFN( $url, $site )

=item addPFN( $pfn_instance )

=item addProfile( $namespace, $key, $value )

=item addProfile( $profile_instance )

=back

=head1 SEE ALSO

=over 4

=item L<Pegasus::DAX::CatalogType>

Base class.

=item L<Pegasus::DAX::ADAG>

Class using L<Pegasus::DAX::File>. 

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
