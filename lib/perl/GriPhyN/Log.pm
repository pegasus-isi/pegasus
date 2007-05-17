package GriPhyN::Log;
#
# contains just some log function - shared and specific.
#
# $Id$
#
use 5.006;
use strict;
use vars qw/$VERSION/;
use subs qw/log/;

use Exporter;
our @ISA = qw(Exporter);

sub utc2iso(;$);		# { }
sub clog($$@);			# { }
sub log;			# { }
sub logging_prefix(;$);		# { }
our @EXPORT = qw(log clog logging_prefix);
our @EXPORT_OK = qw($VERSION utc2iso);

$VERSION=$1 if ( '$Revision$' =~ /Revision:\s+([0-9.]+)/o );
$__PACKAGE__::prefix = undef;
$__PACKAGE__::clogfn = 'euryale.log'; # default name of shared logfile

#
# --- start -----------------------------------------------------
#
use Carp;
use Time::HiRes qw();
use POSIX qw();
use Fcntl qw(:DEFAULT);

sub logging_prefix(;$) {
    # purpose: gets or sets a new logging prefix
    # paramtr: $prefix (opt. IN): is the new logging prefix
    # returns: the old logging prefix
    my $result = $__PACKAGE__::prefix;
    $__PACKAGE__::prefix = shift if ( @_ );
    $result;
}

sub utc2iso(;$) {
    # purpose: converts a UTC timestamp into an ISO timestamp for logging.
    # paramtr: $now (opt. IN): timestamp to convert, or current time
    # returns: ISO 8601 formatted string representing the timestamp.
    my (@now);
    if ( @_ > 0 ) {
	@now = reverse POSIX::modf( shift() );
	$now[1] *= 1E6;
    } else {
	# no argument
	@now = Time::HiRes::gettimeofday();
    }
    
    my @tm = localtime($now[0]);
    sprintf( "%4d%02d%02dT%02d%02d%02d.%03d",
	     $tm[5]+1900, $tm[4]+1, $tm[3],
	     $tm[2], $tm[1], $tm[0], $now[1] / 1000 );
}

sub log {
    # purpose: print whatever onto log stream STDERR with timestamp prefix
    #          This log is a kind of debug log, containing lotsa infos
    # paramtr: any number of more parameters
    # returns: -
    my $prefix = utc2iso() . sprintf( ' %7s ', "[$$]" );
    $prefix .= $__PACKAGE__::prefix if defined $__PACKAGE__::prefix;
    print STDERR $prefix, @_, "\n"; # yes, permit buffering
}

my %tag_translate = ( );	# currently unused

sub clog($$@) {
    # purpose: print whatever onto common shared log stream
    # paramtr: any number of paramters, with the following mandatories:
    #          $tag (IN): some kind of message tag to aide logfile parsing
    #          $job (IN): some kind of job identifier for which log happened
    #          ... (IN): message to log
    # returns: -
    my $tag = shift || croak('A $tag id is required for the common log');
    my $job = shift || croak('A $job id is required for the common log');

    local(*LOG);
    if ( sysopen( LOG, $__PACKAGE__::clogfn, O_CREAT|O_APPEND|O_WRONLY, 0666 ) ) {
	my $xl = $tag_translate{$tag} || $tag || '???';
	my $prefix = $xl . ' ' . utc2iso() . sprintf( ' %7s ', "[$$]" );
	$prefix .= "$job ";
	$prefix .= $__PACKAGE__::prefix if defined $__PACKAGE__::prefix;
	$prefix .= join( '', @_ ) . "\n";
	syswrite( LOG, $prefix ); # no buffering permitted
	close LOG;
    }
}

#
# return 'true' to package loader
#
1;

__END__


=head1 NAME

GriPhyN::Log - provides logging facilities - shared and job-specific ones.

=head1 SYNOPSIS

    use GriPhyN::Log;

    logging_prefix( 'PRE ' || 'POST' );

    log( 'something to say' );
    clog( 'tag', 'jobid', 'something to say' );

=head1 DESCRIPTION

=head1 VARIABLES

By default there are two package variables available.

=over 4

=item $GriPhyN::Log::prefix

This variable is unset, and should be set according to the script that
initializes the Logging module. In a later version, this will be an
argument to the constructor. Please use the accessor function
C<logging_prefix> to set and retrieve values.

=item $GriPhyN::Log::clogfn

This variable contains the filename of the shared, common logfile. It
defaults to F<euryale.log> but can be reset at any time to any other
filename.

=back 

=head1 FUNCTIONS

=over 4

=item utc2iso

This function takes as optional argument a UTC timestamp with
millisecond resolution. If no argument is specified, the current
timestamp will be obtained. The timestamp is ISO 8601 formatted in
extended format with timezone and millisecond extension, e.g.
F<2005-08-01T14:27:17.123-0500>.

This function is not exported by default.

=item logging_prefix

This function permits to get or set the logging prefix maintained in
C<$GriPhyN::Log::prefix>. Called without argument, the function returns
the current value. Called with argument, the new value is set, and the
function returns the old setting of the prefix.

=item log

This function is used to provide debug and info level information that
are specific to a particular job. It is expected that any number of jobs
can run concurrently, and thus each job has its own F<STDERR> file
associated to take debug information.

All information is logged to the stream that F<STDERR> is connected to. 

=item clog

This function is used to provide info and warning level information that
is shared in a workflow. It is expected that all information needs to be
appended atomically to the same logfile, since multiple instances can
run concurrently.

The first argument to the common log function is a 3 letter or digit tag
that is unique to the message generating location. The second argument
is an identifier for the job. Any remaining argument is part of the
message to be logged.

This is an expensive function, so please use sparingly. Each C<clog>
message requires the file to be opened in append mode, the message to be
appended, and the file closed again. 

=back

=head1 SEE ALSO

L<http://www.griphyn.org/>

=head1 AUTHOR

Jens-S. VE<ouml>ckler, C<voeckler at cs dot uchicago dot edu>

=head1 COPYRIGHT AND LICENSE

This file or a portion of this file is licensed under the terms of the
Globus Toolkit Public License, found in file GTPL, or at
http://www.globus.org/toolkit/download/license.html. This notice must
appear in redistributions of this file, with or without modification.

Redistributions of this Software, with or without modification, must
reproduce the GTPL in: (1) the Software, or (2) the Documentation or
some other similar material which is provided with the Software (if
any).

Copyright 1999-2004 University of Chicago and The University of Southern
California. All rights reserved.

=cut

