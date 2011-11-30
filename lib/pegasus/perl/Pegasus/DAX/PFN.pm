#
# License: (atend)
# $Id$
#
package Pegasus::DAX::PFN;
use 5.006;
use strict;
use Carp; 

use Pegasus::DAX::Base qw(:xml); 
use Exporter;
our @ISA = qw(Pegasus::DAX::Base Exporter); 

our $VERSION = '3.3'; 
our @EXPORT = (); 
our @EXPORT_OK = (); 
our %EXPORT_TAGS = (); 

# one AUTOLOAD to rule them all
BEGIN { *AUTOLOAD = \&Pegasus::DAX::Base::AUTOLOAD }

sub new {
    my $proto = shift;
    my $class = ref($proto) || $proto;
    my $self = $class->SUPER::new();
    
    if ( @_ == 0 ) { 
	# nothing to do
    } elsif ( @_ == 1 && ! ref $_[0] ) {
	# single string argument
	$self->{url} = shift; 
    } elsif ( @_ == 2 && ! ref $_[0] && ! ref $_[1] ) { 
	# two string arguments
	$self->{url} = shift; 
	$self->{site} = shift;
    } elsif ( @_ > 2 ) {
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


sub addProfile {
    my $self = shift;

    my $prof; 
    if ( @_ == 3 ) {
	# explicit
	$prof = Pegasus::DAX::Profile->new( shift(), shift(), shift() ); 
    } elsif ( @_ == 1 && ref $_[0] && $_[0]->isa('Pegasus::DAX::Profile') ) {
	my $p = shift;
	$prof = $p->clone();  
    } else {
	croak "argument is not a valid Profile";
    }

    if ( exists $self->{profiles} ) {
	push( @{$self->{profiles}}, $prof );
    } else {
	$self->{profiles} = [ $prof ]; 
    }
}

# forward declarations
sub url;
sub site;

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
    my $tag = defined $xmlns && $xmlns ? "$xmlns:pfn" : 'pfn';

    $f->print( "$indent<$tag"
	     , attribute('url',$self->url,$xmlns)
	     , attribute('site',$self->site,$xmlns) 
	     );
    if ( exists $self->{profiles} ) { 
	$f->print(">\n");
	foreach my $i ( @{$self->{profiles}} ) { 
	    $i->toXML($f,"  $indent",$xmlns);
	}
	$f->print( "$indent</$tag>\n" );
    } else {
	$f->print(" />\n"); 
    }
}

1; 
__END__

=head1 NAME

Pegasus::DAX::PFN - stores a Pegasus concrete data- or executable
description.

=head1 SYNOPSIS

    use Pegasus::DAX::PFN; 

    my $a = Pegasus::DAX::PFN->new( 'url1' );
    my $b = Pegasus::DAX::PFN->new( 'url2', 'local' );
    my $c = Pegasus::DAX::PFN->new( url => 'file://foo'
                                  , site => 'local' ); 

   $c->addProfile( PROFILE_ENV, 'FOO', 'bar' );
   $c->addProfile( $profile_instance ); 

=head1 DESCRIPTION

This class remembers a Pegasus concrete remote file location. The file
may refer to an executable (in the transformation catalog), or a data
file (in the replica catalog). 

=head1 METHODS

=over 4

=item new()

=item new( $url )

=item new( $url, $site )

=item new( a => b, c => d, ... )

=item new( { a => b, c => d, ... } )

The default constructor will create an empty instance whose scalar
attributes can be adjusted using the getters and setters provided by the
C<AUTOLOAD> inherited method.

When invoked with exactly 1 or 2 arguments, the first argument is the
location URL, and the second argument is the site handle. 

Other means of construction is to use named lists.

=item url

Setter and getter for the URL string.

=item site

Setter and getter for the site handle string. 

=item addProfile( $namespace, $key, $value )

=item addProfile( $profile_instance )

This method will add a specified profile, either as three string or
instance of L<Pegasus::DAX::Profile>, to the collection of profiles
associated with this PFN.

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

Abstract class using PFNs.

=back 

=head1 COPYRIGHT AND LICENSE

Copyright 2007-2011 University Of Southern California

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
