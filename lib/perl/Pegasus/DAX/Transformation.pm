#
# License: (atend)
# $Id$
#
package Pegasus::DAX::Transformation;
use 5.006;
use strict;
use Carp; 

use Pegasus::DAX::Base qw(:xml); 
use Pegasus::DAX::Filename; 
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
    } elsif ( @_ == 3 ) {
	# assume namespace,name,version
	@{$self}{'namespace','name','version'} = @_; 
    } elsif ( @_ > 1 && (@_ & 1) == 0 ) {
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

# forward declarations -- AUTOLOAD will provide getter/setter
sub namespace;
sub name;
sub version;

sub addUses {
    my $self = shift; 
    $self->uses(@_); 
}

sub uses {
    my $self = shift; 
    my $uses = shift; 
    if ( defined $uses && ref $uses ) {
	if ( $uses->isa('Pegasus::DAX::Filename') ) {
	    $self->{uses}->{ $uses->name } = $uses; 
	} elsif ( $uses->isa('Pegasus::DAX::Executable') ) {
	    $self->{uses}->{ $uses->name } =
		Pegasus::DAX::Filename->new( namespace => $uses->namespace,
					     name => $uses->name,
					     version => $uses->version,
					     executable => 1 );
	} elsif ( $uses->isa('Pegasus::DAX::File') ) {
	    $self->{uses}->{ $uses->name } =
		Pegasus::DAX::Filename->new( name => $uses->name,
					     link => $uses->link,
					     optional => $uses->optional,
					     executable => 0 ); 
	} else {
	    croak "argument is not an instance I understand";
	}
    } else {
	croak "invalid argument";
    }
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
    my $tag = defined $xmlns && $xmlns ? "$xmlns:executable" : 'executable';

    $f->print( "$indent<$tag"
	     , attribute('namespace',$self->namespace)
	     , attribute('name',$self->name)
	     , attribute('version',$self->version)
	     , ">\n" );

    #
    # <uses> -- at least one
    #
    while ( my ($name,$i) = each %{$self->{uses}} ) {
	$i->toXML($f,"  $indent",$xmlns);
    }

    $f->print( "$indent</$tag>\n" );
}

1; 
__END__

=head1 NAME

Pegasus::DAX::Transformation - aggregates multiple executables and data files. 

=head1 SYNOPSIS

    use Pegasus::DAX::Transformation; 
    use Pegasus::DAX::Filename; 

    my $a = Pegasus::DAX::Transformation->new( undef, 'pre', '1.0' );
    my $b = Pegasus::DAX::Profile->new( namespace => 'foo'
    				      , name => 'bar'
                                      , version => '3.1416' );

    $a->uses( $filename_instance ); 
    $b->uses( Pegasus::DAX::Filename->new( ... ) ); 

=head1 DESCRIPTION

This class aggregates multiple logical data files and transformations
under a single handle that acts like a transformation itself. 

=head1 METHODS

=over 4

=item new()

=item new( $namespace, $name, $version )

=item new( a => b, c => d, ... )

=item new( { a => b, c => d, ... } )

The default constructor will create an empty instance whose scalar
attributes can be adjusted using the getters and setters provided by the
C<AUTOLOAD> inherited method.

When invoked with exactly 3 arguments, the first argument is the logical
transformation namespace or I<undef>, the second argument the required
and defined transformation name, and the third argument the optional 
version string. 

Other means of construction is to use named lists.

=item namespace

Setter and getter for the optional transformation namespace identifier. 

=item name

Setter and getter for required transformation name. 

=item version

Setter and getter for the optional transformation version string. 

=item addUses

Alias method for C<uses> method.

=item uses( $filename_instance )

=item uses( $file_instance )

=item uses( $executable_instance )

This method adds a filename, file, or executable to the things that will
end up in the uses section of a job.

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

=item L<Pegasus::DAX::ADAG>

Class that aggregates the L<Pegasus::DAX::Transformation> class. 

=item L<Pegasus::DAX::Filename>

=item L<Pegasus::DAX::File>

=item L<Pegasus::DAX::Executable>

Permissible ways to specify a file that is being used. 

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
