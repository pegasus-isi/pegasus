#
# License: (atend)
# $Id$
#
package Pegasus::DAX::Factory; 
use 5.006;
use strict;
use Carp; 

use Exporter;
our @ISA = qw(Exporter); 

#
# import all instantiatable classes
#
use Pegasus::DAX::Profile qw(:all);
use Pegasus::DAX::PFN;
use Pegasus::DAX::MetaData;
use Pegasus::DAX::PlainFilename;
use Pegasus::DAX::Filename qw(:all);
use Pegasus::DAX::File;
use Pegasus::DAX::Executable qw(:all); 
use Pegasus::DAX::Invoke qw(:all); 
use Pegasus::DAX::Transformation;
use Pegasus::DAX::DAG;
use Pegasus::DAX::DAX;
use Pegasus::DAX::Job;
use Pegasus::DAX::ADAG qw(:all); 

#
# define wrappers around their c'tors
#
sub newProfile	{ Pegasus::DAX::Profile->new(@_) }
sub newPFN	{ Pegasus::DAX::PFN->new(@_) }
sub newMetaData	{ Pegasus::DAX::MetaData->new(@_) }
sub newPlainFilename { Pegasus::DAX::PlainFilename->new(@_) }
sub newFilename	{ Pegasus::DAX::Filename->new(@_) }
sub newFile	{ Pegasus::DAX::File->new(@_) }
sub newExecutable { Pegasus::DAX::Executable->new(@_) }
sub newTransformation { Pegasus::DAX::Transformation->new(@_) }
sub newDAG	{ Pegasus::DAX::DAG->new(@_) }
sub newDAX	{ Pegasus::DAX::DAX->new(@_) }
sub newJob	{ Pegasus::DAX::Job->new(@_) }
sub newADAG	{ Pegasus::DAX::ADAG->new(@_) }
sub newInvoke   { Pegasus::DAX::Invoke->new(@_) }

#
# export bonanza
#
our $VERSION = '3.3'; 
our %EXPORT_TAGS = (
    func => [qw(newADAG newDAG newDAX newExecutable newFile 
	newFilename newJob newMetaData newPFN newPlainFilename 
	newProfile newTransformation newInvoke)],
    link => [ @{$Pegasus::DAX::Filename::EXPORT_TAGS{link}} ],
    transfer => [ @{$Pegasus::DAX::Filename::EXPORT_TAGS{transfer}} ],
    arch => [ @{$Pegasus::DAX::Executable::EXPORT_TAGS{arch}} ],
    os => [ @{$Pegasus::DAX::Executable::EXPORT_TAGS{os}} ],
    ns => [ @{$Pegasus::DAX::Profile::EXPORT_TAGS{ns}} ],
    schema => [ @{$Pegasus::DAX::ADAG::EXPORT_TAGS{schema}} ],
    invoke => [ @{$Pegasus::DAX::Invoke::EXPORT_TAGS{all}} ]
    ); 
$EXPORT_TAGS{all} = [ map { @{$_} } values %EXPORT_TAGS ]; 
our @EXPORT_OK = ( @{$EXPORT_TAGS{all}} );
our @EXPORT = ( @{$EXPORT_TAGS{func}} ); 

sub new {
    my $proto = shift;
    my $class = ref($proto) || $proto;
    my $self = $class->SUPER::new();

    croak "The c'tor on $class is not instantiatable"; 
}



1; 
__END__

=head1 NAME

Pegasus::DAX::Factory - convenience module

=head1 SYNOPSIS

    use Pegasus::DAX::Factory qw(:all); 

    my $a = newProfile( PROFILE_ENV, 'FOO', 'bar' ); 
    my $b = newFilename( name => 'fubar', link => LINK_OUT ); 
  
=head1 DESCRIPTION

This class exports all constructors as convenience functions into the
caller's namespace. In addition, when using the C<:all> tag, all
constants from any class are exported.

=head1 IMPORT TAGS

=over 4

=item C<:func>

This tag imports the convenience wrapper functions around the class
constructors. The wrappers are exported by default.

=item C<:link>

This tag imports the linkage constants C<LINK_*> from L<Pegasus::DAX::Filename>.

=item C<:transfer>

This tag imports the transfer constants C<TRANSFER_*> from L<Pegasus::DAX::Filename>.

=item C<:arch>

This tag imports the architecture constants C<ARCH_*> from L<Pegasus::DAX::Executable>.

=item C<:os>

This tag imports the operating system constants C<OS_*> from L<Pegasus::DAX::Executable>.

=item C<:ns>

This tag imports the profile namespace constants C<PROFILE_*> from L<Pegasus::DAX::Profile>.

=item C<:schema>

This tag imports the XML schema constants C<SCHEMA_*> from L<Pegasus::DAX::ADAG>.

=item C<:invoke>

This tag imports the notification event constants C<INVOKE_*> from L<Pegasus::DAX::Invoke>.

=item C<:all>

All of the above. 

=back

=head1 FUNCTIONS

=over 4

=item newProfile

Factory function for C<L<Pegasus::DAX::Profile>-E<gt>new>.

=item newPFN

Factory function for C<L<Pegasus::DAX::PFN>-E<gt>new>.

=item newMetaData

Factory function for C<L<Pegasus::DAX::MetaData>-E<gt>new>.

=item newPlainFilename

Factory function for C<L<Pegasus::DAX::PlainFilename>-E<gt>new>.

=item newFilename

Factory function for C<L<Pegasus::DAX::Filename>-E<gt>new>.

=item newFile

Factory function for C<L<Pegasus::DAX::File>-E<gt>new>.

=item newExecutable

Factory function for C<L<Pegasus::DAX::Executable>-E<gt>new>.

=item newTransformation

Factory function for C<L<Pegasus::DAX::Transformation>-E<gt>new>.

=item newDAG

Factory function for C<L<Pegasus::DAX::DAG>-E<gt>new>.

=item newDAX

Factory function for C<L<Pegasus::DAX::DAX>-E<gt>new>.

=item newJob

Factory function for C<L<Pegasus::DAX::Job>-E<gt>new>.

=item newADAG

Factory function for C<L<Pegasus::DAX::ADAG>-E<gt>new>.

=item newInvoke

Factory function for C<L<Pegasus::DAX::Invoke>-E<gt>new>.

=back 

=head1 SEE ALSO

=over 4

=item L<Pegasus::DAX::Base>

Base class. 

=item L<Pegasus::DAX::ADAG>

=item L<Pegasus::DAX::Base>

=item L<Pegasus::DAX::DAG>

=item L<Pegasus::DAX::DAX>

=item L<Pegasus::DAX::Executable>

=item L<Pegasus::DAX::Factory>

=item L<Pegasus::DAX::File>

=item L<Pegasus::DAX::Filename>

=item L<Pegasus::DAX::Invoke>

=item L<Pegasus::DAX::Job>

=item L<Pegasus::DAX::MetaData>

=item L<Pegasus::DAX::PFN>

=item L<Pegasus::DAX::PlainFilename>

=item L<Pegasus::DAX::Profile>

=item L<Pegasus::DAX::Transformation>

Classes for which a convenience c'tor is provided. 

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
