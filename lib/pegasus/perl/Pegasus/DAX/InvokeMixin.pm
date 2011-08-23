#
# Base class for methods to work on Pegasus::DAX::Invoke delegations.
#
# License: (atend)
# $Id$
#
package Pegasus::DAX::InvokeMixin;
use 5.006;
use strict;
use Carp; 

use Exporter;
our @ISA = qw(Exporter); 

our $VERSION = '3.3';
our @EXPORT = ();
our @EXPORT_OK = ();
our %EXPORT_TAGS = (); 

use Pegasus::DAX::Invoke qw(%permitted); 

sub addInvoke {
    my $self = shift;
    $self->invoke(@_);
}

sub notify {
    my $self = shift; 
    $self->invoke(@_);
}

sub invoke {
    my $self = shift; 

    if ( @_ == 0 ) { 
	# assume getter for full list
	return ( exists $self->{invokes} ? 
		 @{ $self->{invokes} } : () ); 
    } elsif ( @_ == 2 ) { 
	# assume setter
	my $when = shift; 
	my $cmd = shift; 

	if ( defined $when && defined $cmd ) { 
	    my $i = Pegasus::DAX::Invoke->new($when,$cmd);
	    if ( exists $self->{invokes} ) {
		push( @{$self->{invokes}}, $i );
	    } else {
		$self->{invokes} = [ $i ]; 
	    }
	} else {
	    croak "use proper arguments to addInvoke(when,cmdstring)";
	}
    } else {
	croak "invalid arguments"; 
    }
}

1; 

__END__

=head1 NAME

Pegasus::DAX::InvokeMixin - base class.

=head1 SYNOPSIS

This is a constructor-less base class. You do not instantiate it.

=head1 DESCRIPTION

This class provides and thus implements dealing with
L<Pegasus::DAX::Invoke> instances inside classes that can contain
instances thereof. 

=head1 METHODS

=over 4

=item addInvoke( $when, $cmd )

Alias for C<invoke> method.

=item notify( $when, $cmd ) 

Alias for C<invoke> method.

=item invoke( ) 

This method is the getter for the full list of L<Pegasus::DAX::Invoke>
objects stored in this instance. 

=item invoke( $when, $cmd )

This method adds a simple executable instruction to run (on the submit
host) when a job reaches the state in C<$when>. Please refer to the 
constants C<INVOKE_*> for details. 

=back

=head1 SEE ALSO

=over 4

=item L<Pegasus::DAX::AbstractJob>

=item L<Pegasus::DAX::Executable>

=item L<Pegasus::DAX::Transformation>

Classes requiring this interface. 

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
