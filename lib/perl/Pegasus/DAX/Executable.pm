#
# License: (atend)
# $Id$
#
package Pegasus::DAX::Executable;
use 5.006;
use strict;
use Carp; 

use Pegasus::DAX::Base qw(:xml); 
use Pegasus::DAX::CatalogType; 
use Exporter;
our @ISA = qw(Pegasus::DAX::CatalogType Exporter); 

use constant ARCH_IA64    => 'ia64';
use constant ARCH_PPC     => 'ppc';
use constant ARCH_PPC_64  => 'ppc_64';
use constant ARCH_SPARCV7 => 'sparcv7'; 
use constant ARCH_SPARCV9 => 'sparcv9';
use constant ARCH_X86     => 'x86';
use constant ARCH_X86_64  => 'x86_64'; 
use constant ARCH_AMD64   => 'x86_64'; 

use constant OS_AIX       => 'aix';
use constant OS_LINUX     => 'linux';
use constant OS_DARWIN    => 'darwin';
use constant OS_MACOSX    => 'darwin'; 
use constant OS_SUNOS     => 'sunos';
use constant OS_SOLARIS   => 'sunos'; 
use constant OS_WINDOWS   => 'windows'; 

our $VERSION = '3.2'; 
our @EXPORT = (); 
our %EXPORT_TAGS = ( 
    arch =>[qw(ARCH_IA64 ARCH_PPC ARCH_PPC_64 ARCH_SPARCV7 ARCH_SPARCV9 
	ARCH_X86 ARCH_X86_64 ARCH_AMD64)],
    os => [qw(OS_AIX OS_LINUX OS_DARWIN OS_MACOSX OS_WINDOWS OS_SUNOS OS_SOLARIS)]
    ); 
$EXPORT_TAGS{all} = [ map { @{$_} } values %EXPORT_TAGS ]; 
our @EXPORT_OK = ( @{$EXPORT_TAGS{all}} ); 

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
sub namespace;
#sub name;	# inherited from parent
sub version;
sub arch;
sub os;
sub osrelease;
sub osversion;
sub glibc;

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
	     , attribute('arch',$self->arch)
	     , attribute('os',$self->os)
	     , attribute('osrelease',$self->osrelease)
	     , attribute('osversion',$self->osversion)
	     , attribute('glibc',$self->glibc)
	     , ">\n" );
    $self->innerXML($f,"  $indent",$xmlns); 
    $f->print( "$indent</$tag>\n" );
}

1; 
__END__

=head1 NAME

Pegasus::DAX::Executable - stores an included transformation catalog entry. 

=head1 SYNOPSIS

    use Pegasus::DAX::Executable; 

    my $a = Pegasus::DAX::Executable(); 
    $a->namespace( 'somewhere' ); 
    $a->name( 'lfn' );
    $a->version( '1.0' ); 
    $a->os( 'x86_64' ); 
  
=head1 DESCRIPTION

This class remembers an included Pegasus transformation catalog entry. 

=head1 CONSTANTS

These constants describe the architecture for which an executable was
compiled. Note that multi-architectures as available on Mac OS X are
currently not supported.

=over 4

=item ARCH_IA64

=item ARCH_PPC

=item ARCH_PPC_64

=item ARCH_SPARCV7

=item ARCH_SPARCV9

=item ARCH_X86

=item ARCH_X86_64

=item ARCH_AMD64

=back

These constants describe the operating system platform. Some of them are
aliases mapping to the same string.

=over 4

=item OS_AIX

The IBM AIX Unix platform. 

=item OS_LINUX

The Linux platform. 

=item OS_DARWIN

The Mac OS X platform. 

=item OS_MACOSX

An alias for the Mac OS X platform. 

=item OS_SUNOS

The SUN Sparc and SUN Intel platforms. 

=item OS_SOLARIS

An alias for the SUN platforms. 

=item OS_WINDOWS

The Microsoft Windows family of platforms. 

=back 

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

=item namespace

Setter and getter for the optional logical transformation namespace string. 

=item name

Setter and getter for the logical transformation's name string. 

=item version

Setter and getter for the optional logical transformation version number
string.

=item arch

Setter and getter for the optional architecture string. 

=item os

Setter and getter for the optional operating system identifier. 

=item osrelease

Setter and getter for the optional OS release string. 

=item osversion

Setter and getter for the optional OS version string. 

=item glibc

Setter and getter for the optional GNU libc platform identifier string. 

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
