#
# License: (atend)
# $Id$
#
package Pegasus::DAX::TUType;
use 5.006;
use strict;
use Carp; 

use Pegasus::DAX::Base qw(:xml); 
use Pegasus::DAX::PlainFilename; 
use Exporter;
our @ISA = qw(Pegasus::DAX::PlainFilename Exporter); 

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
    } elsif ( @_ > 1 && (@_ & 1) == 0 ) {
	# called with a=>b,c=>d list
	%{$self} = ( %{$self}, @_ ); 
    } elsif ( @_ == 1 && ref $_[0] && 
	      ( ref $_[0] eq 'HASH' || ref $_[0] eq __PACKAGE__ ) ) { 
	# called with { a=>b, c=>d } hashref
	# or called as copy-c'tor (deep copy)
	%{$self} = ( %{$self}, %{ shift() } ); 
    } else {
	croak "invalid c'tor invocation"; 
    }

    bless $self, $class; 
}

# forward declarations so can we check using 'can'
#sub name;			# inherited
sub namespace;
sub version;
sub executable; 

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
    my $tag = defined $xmlns && $xmlns ? "$xmlns:uses" : 'uses';

    $f->print( "$indent<$tag", 
	     , attribute('namespace',$self->namespace,$xmlns)
	     , attribute('name',$self->name,$xmlns)
	     , attribute('version',$self->version,$xmlns)
	     , attribute('executable',boolean($self->executable),$xmlns)
	     , " />\n" ); 
}

1; 
__END__

=head1 NAME

Pegasus::DAX::TUType - class for Transformation referenced entities.

=head1 SYNOPSIS

    use Pegasus::DAX::TUType; 

    my $i = Pegasus::DAX::TUType->new( name => 'filename.txt' );
    $i->exectuable = 'false'; 
   
=head1 DESCRIPTION

This class remembers a reference expressed in the C<Transformation>
class. The reference becomes part of the transformation's C<uses>
bundle.

=head1 METHODS

=over 4

=item new()

=item new( a => b, c => d, ... )

=item new( { a => b, c => d, ... } )

The default constructor will create an empty instance whose scalar
attributes can be adjusted using the getters and setters provided by the
C<AUTOLOAD> inherited method. Other means to set attributes is to used
named lists. 

=item name

This setter and getter is inherited. 

=item namespace

Setter and getter for a namespace string.

=item version

Setter and getter for a version string. 

=item executable

Setter and getter for boolean values. Please use Perl truth. 

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

=item Pegasus::DAX::PlainFilename

Base class. 

=item Pegasus::DAX::Transformation

Aggregating class.

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
