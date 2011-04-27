#
# License: (atend)
# $Id$
#
package Pegasus::DAX::Invoke;
use 5.006;
use strict;
use Carp; 

use Pegasus::DAX::Base qw(:xml); 
use Exporter;
our @ISA = qw(Pegasus::DAX::Base Exporter); 

use constant INVOKE_NEVER => 'never';
use constant INVOKE_START => 'start'; 
use constant INVOKE_ON_SUCCESS => 'on_success';
use constant INVOKE_ON_ERROR => 'on_error';
use constant INVOKE_AT_END => 'at_end'; 
use constant INVOKE_ALL => 'all'; 

our $VERSION = '3.3'; 
our @EXPORT = (); 
our %EXPORT_TAGS = ( all => [qw(INVOKE_NEVER INVOKE_START INVOKE_ON_SUCCESS
	INVOKE_ON_ERROR INVOKE_AT_END INVOKE_ALL) ] ); 
our @EXPORT_OK = ( @{$EXPORT_TAGS{all}} );

# one AUTOLOAD to rule them all
BEGIN { *AUTOLOAD = \&Pegasus::DAX::Base::AUTOLOAD }

my %permitted = qw(never start on_success on_error at_end all);

sub new {
    my $proto = shift;
    my $class = ref($proto) || $proto;
    my $self = $class->SUPER::new();

    my $when = lc shift(); 
    $self->{when} = $when;
    $self->{cmd} = shift; 

    carp( "Invalid value '$when' for 'when' parameter ",
	  "in c'tor for ", __PACKAGE__ )
	unless exists $permitted{$when}; 

    bless $self, $class; 
}

# forward declaration (resolved by AUTOLOAD)
sub when;
sub cmd; 

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
    my $tag = defined $xmlns && $xmlns ? "$xmlns:invoke" : 'invoke';

    $f->print( "$indent<$tag"
	     , attribute('when',$self->{when},$xmlns)
	     , ">"
	     , quote($self->{cmd})
	     , "</$tag>\n"
	     );
}

1; 
__END__

=head1 NAME

Pegasus::DAX::Invoke - class to collect data for callback invocations. 

=head1 SYNOPSIS

    use Pegasus::DAX::Invoke qw(:all);

    my $i = Pegasus::DAX::Invoke->new( INVOKE_AT_END, '....' ); 
    print "when is ", $i->when, "\n";
    $i->cmd = '/bin/mailx -s foo a@b.c'
    print "command is '", $i->cmd, "'\n";
   
=head1 DESCRIPTION

This class remembers a callback invocation. The callback is a command
passed to the shell to be executed on the user's behalf whenever a job
passes a certain event. The event states are available as C<INVOKE_*>
constants. 

=head1 CONSTANTS

=over 4 

=item INVOKE_NEVER

Never run the invoke. This is primarily to temporarily disable an invoke. 

=item INVOKE_START

Run the invoke when the job gets submitted. 

=item INVOKE_ON_SUCCESS

Run the invoke after the job finishes with success (exitcode == 0). 

=item INVOKE_ON_ERROR

Run the invoke after the job finishes with failure (exitcode != 0). 

=item INVOKE_AT_END

Run the invoke after the job finishes, regardless of exit code.

=item INVOKE_ALL

Like C<INVOKE_START> and C<INVOKE_AT_END> combined. 

=back

=head1 METHODS

=over 4

=item new( $when, $cmd )

The construct must be called with two arguments. The first argument
is a string, according to the C<INVOKE_*> constants. The second
argument is the complete command-line to be executed on the user's
behalf. 

=item when()

This is the getter for the event specification. 

=item when( C<INVOKE_*> )

This is the setter for the event specification. 

=item cmd()

This is the getter for the command-line string. 

=item cmd( $cmd )

This is the setter for the complete command-line string. 

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

The abstract job class aggregates instances of this class to be 
called when the proper event is triggered. 

=item L<Pegasus::DAX::ADAG>

The abstract DAX aggregates instances of this class to be called when
the proper event is triggered on a workflow level.

=item L<Pegasus::DAX::Executable>

The executable class aggregates instances of this class to be called
when the proper event is triggered in a job that uses the executable. 

=item L<Pegasus::DAX::Transformation>

The transformation class aggregates instances of this class to be called
when the proper event is triggered in a job that uses the transformation. 

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
