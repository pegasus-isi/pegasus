#
# Base class for methods to work on Pegasus::DAX::MetaData delegations.
#
# License: (atend)
# $Id$
#
package Pegasus::DAX::MetaDataMixin;
use 5.006;
use strict;
use Carp;

use Exporter;
our @ISA = qw(Exporter);

our $VERSION = '3.6';
our @EXPORT = ();
our @EXPORT_OK = ();
our %EXPORT_TAGS = ();


sub addMetaData {
    my $self = shift;
    my $m = shift;

    if ( ref $m ) {
	    if ( $m->isa('Pegasus::DAX::MetaData') ) {
            $self->{metadata}->{$m->{key}} = $m;
	    }  else {
            croak "Invalid type; Expecting Pegasus::DAX::MetaData";
        }
	} else {
        croak "Invalid argument";
    }
}

sub clearMetaData {
    my $self = shift;
    delete $self->{metadata}
}

sub hasMetaData {
    my $self = shift;
    my $m = shift;

    return exists $self->{metadata} && exists $self->{metadata}->{$m->key} && $self->{metadata}->{$m->key}->{value} eq $m->{value} ? 1 : 0;
}

sub removeMetaData {
    my $self = shift;
    my $m = shift;

    my $i;
    my $o;

    if ($self->hasMetaData($m)) {
        delete $self->{metadata}->{$m->{key}};
        return 1;
    }

    return 0;
}

sub metaData {
    my $self = shift;

    if ( @_ == 0 ) {
	    # assume getter for full list
	    if (exists $self->{metadata}) {
	        my $v = values %{$self->{metadata}};
	        return $v;
        }

        return ();
    } elsif ( @_ == 2 ) {
        # assume setter
        my $key = shift;
        my $value = shift;

        if ( defined $key && defined $value ) {
            my $m = Pegasus::DAX::MetaData->new($key,$value);
            $self->{metadata}->{$m->{key}} = $m;
        } else {
            croak "use proper arguments to addMetaData(key,value)";
        }
    } else {
	    croak "invalid arguments";
    }
}

1;

__END__

=head1 NAME

Pegasus::DAX::MetaDataMixin - base class.

=head1 SYNOPSIS

This is a constructor-less base class. You do not instantiate it.

=head1 DESCRIPTION

This class provides and thus implements dealing with
L<Pegasus::DAX::MetaData> instances inside classes that can contain
instances thereof.

=head1 METHODS

=over 4

=item addMetadata( $when, $cmd )

Alias for C<invoke> method.

=item clearMetadata( $when, $cmd )

Alias for C<invoke> method.

=item hasMetadata( )

This method is the getter for the full list of L<Pegasus::DAX::Invoke>
objects stored in this instance.

=item removeMetadata( $when, $cmd )

This method adds a simple executable instruction to run (on the submit
host) when a job reaches the state in C<$when>. Please refer to the
constants C<INVOKE_*> for details.

=back

=head1 SEE ALSO

=over 4

=item L<Pegasus::DAX::ADAG>

=item L<Pegasus::DAX::AbstractJob>

=item L<Pegasus::DAX::Executable>

=item L<Pegasus::DAX::File>

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
