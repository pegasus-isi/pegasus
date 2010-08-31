#
# License: (atend)
# $Id$
#
package Pegasus::DAX::PlainFilename;
use 5.006;
use strict;
use Carp; 

use Pegasus::DAX::Base qw(:xml); 
use Exporter;
our @ISA = qw(Pegasus::DAX::Base Exporter); 

our $VERSION = '3.2'; 
our @EXPORT = (); 
our @EXPORT_OK = (); 
our %EXPORT_TAGS = (); 

sub new {
    my $proto = shift;
    my $class = ref($proto) || $proto;
    my $self = $class->SUPER::new();

    if ( @_ == 0 ) { 
	# nothing to do
    } elsif ( @_ > 1 && (@_ & 1) == 0) { 
	# even: called with a=>b,c=>d list
	%{$self} = ( %{$self}, @_ ); 
    } elsif ( @_ == 1 && ! ref $_[0] ) { 
	# called with single scalar
	$self->{name} = shift; 
    } elsif ( @_ == 1 && ref $_[0] eq 'HASH' ) {
	# called with { a=>b,c=>d } hashref
	%{$self} = ( %{$self}, %{ shift() } ); 
    } else {
	carp "invalid c'tor for ", __PACKAGE__; 
    }

    bless $self, $class; 
}

sub name;			# { } forward declaration

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
    my $tag = defined $xmlns && $xmlns ? "$xmlns:file" : 'file';

    $f->print( "$indent<$tag"
	     , attribute('name',$self->{name})
	     , " />" ); 
}

1; 

=head1 NAME

Pegasus::DAX::PlainFilename - class for simple file names. 

=head1 SYNOPSIS

    use Pegasus::DAX::PlainFilename; 

    my $i = Pegasus::DAX::PlainFilename->new( 'asdf.txt' );
    print "name is ", $i->name, "\n";
    $i->name = 'newname.txt';
    print "name is ", $i->name, "\n";
   
=head1 DESCRIPTION

This class remembers a simple filename. These filenames are aggregated
by the C<AbstractJob> class. A simple filename is either part of a
concrete job's argument list.

=head1 METHODS

=over 4

=item new()

=item new( $filename )

=item new( name => $filename )

=item new( { name => $filename } )

The constructor may be called with a single scalar argument, which is
the filename string. Alternative ways to invoke the c'tor pass the 
arguments as named list. 

=item name()

This is the getter. 

=item name( $name )

This is the setter. 

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

=item L<Pegasus::DAX::Filename>

Child class. 

=item L<Pegasus::DAX::AbstractJob>

The abstract job class aggregates instances of this class in
C<arguments> and in C<stdio>.

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
