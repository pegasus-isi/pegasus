#
# License: (atend)
# $Id$
#
package Pegasus::DAX::DAX;
use 5.006;
use strict;
use Carp; 

use Pegasus::DAX::Base qw(:xml); 
use Pegasus::DAX::AbstractJob; 
use Exporter;
our @ISA = qw(Pegasus::DAX::AbstractJob Exporter); 

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
    my $tag = defined $xmlns && $xmlns ? "$xmlns:dax" : 'dax';

    $f->print( "$indent<$tag"
	     , attribute('name',$self->name)
	     , attribute('id',$self->id)
	     , attribute('node-label',$self->nodelabel)
	     , ">\n" );
    $self->innerXML($f,"  $indent",$xmlns); 
    $f->print( "$indent</$tag>\n" );
}

1; 


=head1 NAME

Pegasus::DAX::DAX - Job node to store an unplanned workflow. 

=head1 SYNOPSIS

    use Pegasus::DAX::DAX; 

    my $a = Pegasus::DAX::DAX->new( name => 'fubar' ); 
    $a->addArgument( '-flag' ); 

=head1 DESCRIPTION

This class stores the job that describes a workflow (in DAX file format)
that still needs to be planned. The job refers to the external filename
for the workflow.

=head1 METHODS

=over 4

=item new()

=item new( a => b, c => d, ... )

=item new( { a => b, c => d, ... } )

The default constructor will create an empty instance whose scalar
attributes can be adjusted using the getters and setters provided by the
C<AUTOLOAD> inherited method.

Other means of construction is to use named lists.

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

Please refer to L<Pegasus::DAX::AbstractJob> for inherited methods. 

=over 4

=item addArgument( $string )

=item addArgument( $plainfilename_instance )

=item addArgument( $filename_instance )

=item addArgument( $file_instance )

=item addArgument( $exectuable_instance )

=item addProfile( $namespace, $key, $value )

=item addProfile( $profile_instance )

=item stdin

=item stdout

=item stderr

=item name

=item id

=item nodelabel

=item addUses( .. )

=item uses( $filename_instance )

=item uses( $file_instance )

=item uses( $executable_instance )

=item addInvoke( $when, $cmd )

=item notify( $when, $cmd ) 

=item invoke( $when $cmd )

=item innerXML( $handle, $indent, $xmlns )

=back

=head1 SEE ALSO

=over 4

=item L<Pegasus::DAX::AbstractJob>

Base class. 

=item L<Pegasus::DAX::ADAG>

=item L<Pegasus::DAX::DAG>

=item L<Pegasus::DAX::Job>

Sibling classes. 

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
