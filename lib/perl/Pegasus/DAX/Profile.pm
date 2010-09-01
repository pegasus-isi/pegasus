#
# License: (atend)
# $Id$
#
package Pegasus::DAX::Profile;
use 5.006;
use strict;
use Carp; 

use Pegasus::DAX::Base qw(:xml); 
use Exporter;
our @ISA = qw(Pegasus::DAX::Base Exporter); 

use constant PROFILE_PEGASUS => 'pegasus';
use constant PROFILE_CONDOR  => 'condor';
use constant PROFILE_DAGMAN  => 'dagman';
use constant PROFILE_ENV     => 'env';
use constant PROFILE_HINTS   => 'hints'; 
use constant PROFILE_GLOBUS  => 'globus';
use constant PROFILE_SELECTOR => 'selector';
use constant PROFILE_STAT    => 'stat'; 

our $VERSION = '3.2'; 
our @EXPORT = (); 
our %EXPORT_TAGS = ( ns => [ qw(PROFILE_PEGASUS PROFILE_CONDOR 
	PROFILE_DAGMAN PROFILE_ENV PROFILE_HINTS PROFILE_GLOBUS 
	PROFILE_SELECTOR PROFILE_STAT ) ] );
$EXPORT_TAGS{all} = [ @{$EXPORT_TAGS{ns}} ]; 
our @EXPORT_OK = ( @{$EXPORT_TAGS{ns}} );

sub new {
    my $proto = shift;
    my $class = ref($proto) || $proto;
    my $self = $class->SUPER::new();

    if ( @_ == 0 ) { 
	# nothing to do
    } elsif ( @_ == 3 ) {
	# called as namespace, key, value
	@{$self}{'namespace','key','value'} = @_; 
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
sub namespace;
sub key;
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
    my $tag = defined $xmlns && $xmlns ? "$xmlns:profile" : 'profile';

    # older DAXes permitted single <file> element inside a profile value
    my $value = ( ref $self->{value} && 
		  $self->{value}->isa('Pegasus::DAX::PlainFilename') ) ? 
	$self->{value}->name : 
	$self->value; 

    $f->print( "$indent<$tag", 
	     , attribute('namespace',$self->namespace)
	     , attribute('key',$self->key)
	     , ">"
	     , quote($value)
	     , "</$tag>\n"
	     ); 
}

1; 
__END__

=head1 NAME

Pegasus::DAX::Profile - stores a Pegasus profile. 

=head1 SYNOPSIS

    use Pegasus::DAX::Profile qw(:ns); 

    my $a = Pegasus::DAX::Profile->new( PROFILE_ENV, 'FOO', 'bar' ); 
    my $b = Pegasus::DAX::Profile->new( namespace => PROFILE_CONDOR,
    				        key => 'getenv',
                                        value => 'True' ); 
  
=head1 DESCRIPTION

This class remembers a Pegasus profile. Pegasus profiles abstracts the
various concrete planning details.

=head1 CONSTANTS

The following constants are imported with the I<ns> tag when using this
module. The constants define the various permissible namespaces. 

=over 4

=item PROFILE_PEGASUS

=item PROFILE_CONDOR

=item PROFILE_DAGMAN

=item PROFILE_ENV

=item PROFILE_HINTS

=item PROFILE_GLOBUS

=item PROFILE_SELECTOR

=item PROFILE_STAT

=back

=head1 METHODS

=over 4

=item new()

=item new( $namespace, $key, $value ); 

=item new( a => b, c => d, ... )

=item new( { a => b, c => d, ... } )

The default constructor will create an empty instance whose scalar
attributes can be adjusted using the getters and setters provided by the
C<AUTOLOAD> inherited method.

When invoked with exactly 3 arguments, the first argument is the profile
namespace, the second argument the key inside the namespace, and the
third argument the value to set. 

Other means of construction is to use named lists.

=item namespace

Setter and getter for a namespace string. Please use the C<PROIFLE_*>
constants defined previously. These constants are not imported by
default, unless you use the I<ns> import tag.

=item key

Setter and getter for a key string. The key value may be of restricted
range, dependinng on the namespace, but this is not checked at this
point.

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

=item L<Pegasus::DAX::AbstractJob>

=item L<Pegasus::DAX::PFN>

=item L<Pegasus::DAX::CatalogType>

Classes using profiles. 

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
