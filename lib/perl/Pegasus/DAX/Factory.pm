#
# License: (atend)
# $Id$
#
package Pegasus::DAX::Factory; 
use 5.006;
use strict;
use Carp; 

use Pegasus::DAX::Base qw(:xml); 
use Exporter;
our @ISA = qw(Pegasus::DAX::Base Exporter); 

use Pegasus::DAX::Profile qw(:all);
use Pegasus::DAX::PFN;
use Pegasus::DAX::MetaData;
use Pegasus::DAX::PlainFilename;
use Pegasus::DAX::Filename qw(:all);
use Pegasus::DAX::File;
use Pegasus::DAX::Executable qw(:all); 
use Pegasus::DAX::Transformation;
use Pegasus::DAX::DAG;
use Pegasus::DAX::DAX;
use Pegasus::DAX::Job;
use Pegasus::DAX::ADAG qw(:all); 

sub newProfile {
    Pegasus::DAX::Profile->new(@_);
}

sub newPFN {
    Pegasus::DAX::PFN->new(@_);
}

sub newMetaData {
    Pegasus::DAX::MetaData->new(@_);
}

sub newPlainFilename {
    Pegasus::DAX::PlainFilename->new(@_);
}

sub newFilename {
    Pegasus::DAX::Filename->new(@_);
}

sub newFile { 
    Pegasus::DAX::File->new(@_);
}

sub newExecutable {
    Pegasus::DAX::Executable->new(@_);
}

sub newTransformation {
    Pegasus::DAX::Transformation->new(@_);
}

sub newDAG { 
    Pegasus::DAX::DAG->new(@_);
}

sub newDAX {
    Pegasus::DAX::DAX->new(@_);
}

sub newJob {
    Pegasus::DAX::Job->new(@_);
}

sub newADAG {
    Pegasus::DAX::ADAG->new(@_);
}

our $VERSION = '3.2'; 
our %EXPORT_TAGS = (
    func => [qw(newADAG newDAG newDAX newExecutable newFile newFilename newJob 
	newMetaData newPFN newPlainFilename newProfile newTransformation)],
    link => [ @{$Pegasus::DAX::Filename::EXPORT_TAGS{link}} ],
    arch => [ @{$Pegasus::DAX::Executable::EXPORT_TAGS{arch}} ],
    os => [ @{$Pegasus::DAX::Executable::EXPORT_TAGS{os}} ],
    ns => [ @{$Pegasus::DAX::Profile::EXPORT_TAGS{ns}} ],
    schema => [ @{$Pegasus::DAX::ADAG::EXPORT_TAGS{ns}} ]
    ); 
$EXPORT_TAGS{all} = [ map { @{$_} } values %EXPORT_TAGS ]; 
our @EXPORT_OK = ( @{$EXPORT_TAGS{all}} );
our @EXPORT = ( @{$EXPORT_TAGS{func}} ); 

sub new {
    my $proto = shift;
    my $class = ref($proto) || $proto;
    my $self = $class->SUPER::new();

    croak "You called $class, which is not instantiatable"; 
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
caller's namespace.

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

=item L<Pegasus::DAX::Job>

=item L<Pegasus::DAX::MetaData>

=item L<Pegasus::DAX::PFN>

=item L<Pegasus::DAX::PlainFilename>

=item L<Pegasus::DAX::Profile>

=item L<Pegasus::DAX::Transformation>

Classes for which a convenience c'tor is provided. 

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
