#
# License: (atend)
# $Id$
#
package Pegasus::DAX::Job;
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

my $count = 0; 

# one AUTOLOAD to rule them all
BEGIN { *AUTOLOAD = \&Pegasus::DAX::Base::AUTOLOAD }

sub new {
    my $proto = shift;
    my $class = ref($proto) || $proto;
    my $self = $class->SUPER::new();

    # default identifier using class variable $count
    $self->{id} = sprintf( "ID%06u", ++$count ); 

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

# forward declaration to auto loaders
sub namespace;
sub version;

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
    my $tag = defined $xmlns && $xmlns ? "$xmlns:job" : 'job';

    $f->print( "$indent<$tag"
	     , attribute('namespace',$self->namespace,$xmlns)
	     , attribute('name',$self->name,$xmlns)
	     , attribute('version',$self->version,$xmlns)
	     , attribute('id',$self->id,$xmlns)
	     , attribute('node-label',$self->nodelabel,$xmlns)
	     , ">\n" );
    $self->innerXML($f,"  $indent",$xmlns); 
    $f->print( "$indent</$tag>\n" );
}

1; 
__END__


=head1 NAME

Pegasus::DAX::Job - Job node to describe a job in the current workflow. 

=head1 SYNOPSIS

    use Pegasus::DAX::Job; 

    my $j = Pegasus::DAX::Job->new( namespace => undef,
                                    name => 'fubar',
                                    version => '3.0' ); 
    $j->addArgument( '-flag' ); 


=head1 DESCRIPTION

This class stores a single job within the current workflow. Most of the
heavy lifting is done in the base class L<Pegasus::DAX::AbstractJob>. 

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

=item L<Pegasus::DAX::DAX>

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
