#
# License: (atend)
# $Id$
#
package Pegasus::DAX::Filename;
use 5.006;
use strict;
use Carp; 

use Pegasus::DAX::Base qw(:xml); 
use Pegasus::DAX::PlainFilename; 
use Exporter;
our @ISA = qw(Pegasus::DAX::PlainFilename Exporter); 

use constant LINK_NONE  => 'none';
use constant LINK_IN    => 'in'; 
use constant LINK_OUT   => 'out';
use constant LINK_INOUT => 'inout'; 
use constant LINK_IO    => 'inout'; 

use constant TRANSFER_TRUE => 'true';
use constant TRANSFER_FALSE => 'false';
use constant TRANSFER_OPTIONAL => 'optional'; 

our $VERSION = '3.2'; 
our @EXPORT = qw(LINK_NONE LINK_IN LINK_OUT LINK_INOUT LINK_IO
	TRANSFER_TRUE TRANSFER_FALSE TRANSFER_OPTIONAL); 
our @EXPORT_OK = (); 
our %EXPORT_TAGS = (); 

sub new {
    my $proto = shift;
    my $class = ref($proto) || $proto;
    my $self = $class->SUPER::new();

    if ( @_ == 0 ) { 
	# nothing to do 
    } elsif ( @_ > 1 && (@_ & 1) == 0 ) {
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

# forward declarations so can we check using 'can'
sub namespace;
sub version;
sub link;
sub optional;
sub register; 
sub transfer;
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
	     , attribute('namespace',$self->{namespace})
	     , attribute('name',$self->{name})
	     , attribute('version',$self->{version})
	     , attribute('link',$self->{link})
	     , attribute('optional',boolean($self->{optional}))
	     , attribute('register',boolean($self->{register}))
	     , attribute('transfer',$self->{transfer})
	     , attribute('executable',boolean($self->{exectuable}))
	     , " />\n" ); 
}

1; 

=head1 NAME

Pegasus::DAX::Filename - class for complete file names. 

=head1 SYNOPSIS

    use Pegasus::DAX::Filename; 

    my $i = Pegasus::DAX::Filename->new( name => 'filename.txt' );
    $i->link = LINK_IN;
    $i->register = 1;
    $i->optional = 0; 
   
=head1 DESCRIPTION

This class remembers a simple filename. These filenames are aggregated
by the C<AbstractJob> class. A simple filename is either part of a
concrete job's argument list.

=head1 CONSTANTS

=over 4

=item LINK_NONE

Constant denoting that a file has no linkage. To be used with the
I<link> attribute.

=item LINK_IN

Constant denoting that a file is an input file. To be used with the
I<link> attribute.

=item LINK_OUT

Constant denoting that a file is an output file. To be used with the
I<link> attribute.

=item LINK_INOUT

Constant denoting that a file is an input- and output file. To be used
with the I<link> attribute.

=item LINK_IO

Alias for C<LINK_INOUT>. 

=back

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

=item link

Setter and getter for a linkage string. Please use the pre-defined
constants starting with C<LINK_>.

=item optional

=item register

=item executable

Setter and getter for boolean values. Please use Perl truth. 

=item transfer

Setter and getter for tri-state value. Please use strings. 

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

=item Pegasus::DAX::AbstractJob

=item Pegasus::DAX::Transformation

Aggregating classes. 

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
