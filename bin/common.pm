#
#  Copyright 2007-2010 University Of Southern California
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#
# $Id$
#
package common; 
use 5.006;
use strict; 

use Exporter;
our @ISA = qw(Exporter); 

our $VERSION='3.0.0'; 
$VERSION = $1 if ( '$Revision$' =~ /Revision:\s+([0-9.]+)/o );
our %EXPORT_TAGS = ( 'all' => [ qw() ] ); 
our @EXPORT_OK = (); 
our @EXPORT = (); 

#
# The following is like setting pre-pending your PERL5LIB search path
# with $PEGASUS_HOME/lib/perl at compile-time. 
#
use File::Spec; 
BEGIN { 
    if ( exists $ENV{'PEGASUS_HOME'} && -d $ENV{'PEGASUS_HOME'} ) {
	my $dir = File::Spec->catdir( $ENV{'PEGASUS_HOME'}, 'lib', 'perl' );
	unshift( @INC, $dir ) if -d $dir;
    }
}

1;

__END__

=head1 NAME

common - Perl module included by all Pegasus Perl scripts

=head1 SYNOPSIS 

=over 4

=item B<Alternative 1>

    use File::Spec;
    use File::Basename;
    BEGIN { 
	require File::Spec->catfile( dirname($0), 'common.pm' );
	common->import(':all');
    }

The explicit path argument to C<require> is used in order to avoid
polluting your search path with the C<bin> directory in which the
C<common> module resides.

=item B<Alternative 2>

    use File::Spec;
    use File::Basename;
    use lib dirname($0);
    use common ':all';

If you don't care about potential security issues that may arise from
including the C<bin> path into your Perl search path, alternative two
shows a more concise and convenient way. 

=back

=head1 DESCRIPTION

This module expands the search path for perl modules when included. At
this point, it does not do anything else, but that may change in the
future. For future behavioral changes, it is important to import I<all>. 

=head1 START-UP

When included, the Perl search path for Pegasus modules is prefixed
to Perl's internal search path in C<@INC>. This is equivalent to 
prefixing C<$PEGASUS_HOME/lib/perl> to your C<PERL5LIB> environment
variable. 

=head1 AUTHOR

Jens-S. VE<ouml>ckler, C<voeckler at isi dot edu>

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
