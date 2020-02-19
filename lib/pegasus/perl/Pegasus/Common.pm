#
# Provides common sub-functions shared by all workflow programs.
#
# This file or a portion of this file is licensed under the terms of
# the Globus Toolkit Public License, found in file GTPL, or at
# http://www.globus.org/toolkit/download/license.html. This notice must
# appear in redistributions of this file, with or without modification.
#
# Redistributions of this Software, with or without modification, must
# reproduce the GTPL in: (1) the Software, or (2) the Documentation or
# some other similar material which is provided with the Software (if
# any).
#
# Copyright 1999-2004 University of Chicago and The University of
# Southern California. All rights reserved.
#
# Author: Jens-S. Vöckler voeckler@cs.uchicago.edu
# Revision : $Revision$
# $Id$
#
package Pegasus::Common;
use 5.006;
use strict;
use warnings;
use File::Basename qw(basename dirname);
require Exporter;
our @ISA = qw(Exporter);

# declarations of methods here. Use the commented body to unconfuse emacs
sub isodate(;$$$);		# { }
sub isomsdate(;$$$);		# { }
sub find_exec($;@);		# { }
sub pipe_out_cmd;		# { }
sub parse_exit(;$);		# { }
sub slurp_braindb($);		# { }
sub version();                  # { }
sub check_rescue($$);           # { }
sub log10($);                   # { }
our $jobbase = 'jobstate.log';	# basename of the job state logfile
our $brainbase = 'braindump.yml'; # basename of brain dump file

# Items to export into callers namespace by default. Note: do not export
# names by default without a very good reason. Use EXPORT_OK instead.
# Do not simply export all your public functions/methods/constants.
our $VERSION = '0.1';
our @EXPORT_OK = qw($VERSION $brainbase $jobbase);
our @EXPORT = qw(isodate isomsdate find_exec pipe_out_cmd parse_exit
		 slurp_braindb version check_rescue log10);
our %EXPORT_TAGS = ( all => [ @EXPORT ] );

# Preloaded methods go here.
use POSIX qw(strftime);
use File::Spec;
use Carp;

BEGIN {
    # non-fatally attempt to load semi-standard Time::HiRes module
    eval { require Time::HiRes; import Time::HiRes qw(time); };
}

sub isodate(;$$$) {
    # purpose: convert seconds since epoch into ISO timestamp
    # paramtr: $seconds (opt. IN): seconds since epoch, now is default
    #          $utc (opt. IN): if true, use a UTC timestamp
    #          $short (opt. IN): if true, use extra short format
    # warning: UTC short format omits the center 'T' separator
    # returns: string of ISO timestamp
    my $now = shift || time();
    my $utc = shift;
    my $short = shift;

    my $result;
    if ( $short ) {
	$result = $utc ?
	    strftime( "%Y%m%d%H%M%SZ", gmtime($now) ) :
	    strftime( "%Y%m%dT%H%M%S%z", localtime($now) );
    } else {
	$result = $utc ?
	    strftime( "%Y-%m-%dT%H:%M:%SZ", gmtime($now) ) :
	    strftime( "%Y-%m-%dT%H:%M:%S%z", localtime($now) );
    }

    $result;
}

sub isomsdate(;$$$) {
    # purpose: see isodate, but with millisecond extension
    # returns: formatted ISO 8601 time stamp
    #
    my $now = shift || time();
    my $utc = shift;
    my $short = shift;
    my $result = isodate($now,$utc,$short);

    my $s = substr( sprintf( "%.3f", $now-int($now) ), 1 );
    substr( $result, ( $utc ? -1 : -5 ), 0, $s );
    $result;
}

sub find_exec($;@) {
    # purpose: determine location of given binary in $PATH
    # paramtr: $program (IN): executable basename to look for
    #          @extra (opt. IN): additional directories to search
    # returns: fully qualified path to binary, undef if not found
    my $program = shift;
    foreach my $dir ( ( File::Spec->path, @_ ) ) {
        my $fs = File::Spec->catfile( $dir, $program );
        return $fs if -x $fs;
    }
    undef;
}

sub pipe_out_cmd {
    # purpose: Runs a cmd w/o invoking a shell, and captures stdout+stderr
    # warning: DO NOT use shell meta characters in the argument string.
    # paramtr: @arg (IN): argument string, executable first
    # returns: failed: undef
    #          scalar: first line of output
    #          vector: all lines of output
    local(*READ);               # must use type glob and local for FDs

    my $pid = open( READ, '-|' );
    return undef unless defined $pid;

    my @result;
    if ( $pid ) {
        # parent
        while ( <READ> ) {
	    s/[\r\n]+$//;
            push( @result, $_ );
        }
        close READ;
    } else {
        # child
        open( STDERR, '>&STDOUT');
        select(STDERR); $|=1;
        select(STDOUT); $|=1;
        exec { $_[0] } @_;      # lotsa magic :-)
        exit 127;               # no such exe :-(
    }

    wantarray ? @result : $result[0];
}

sub slurp_braindb($) {
    # purpose: Read extra configuration from braindump database
    # paramtr: $run (IN): $run directory
    # returns: a hash with the configuration, empty on error.
    my $run = shift;
    my $braindb = File::Spec->catfile( $run, $brainbase );

    my %config = ();
    if ( open( DB, "<$braindb" ) ) {
	while ( <DB> ) {
	    s/[\r\n]*$//;
	    my ($k,$v) = split /: /, $_, 2;
        $v =~ s/^"//;
        $v =~ s/"$//g;
	    if ( $k eq 'run' && $v ne $run && $run ne '.' ) {
		warn "Warning: run directory mismatch, using $run\n";
		$config{$k} = $run;
		next;
	    }
	    $v =~ s/^\s*//;
	    $v =~ s/\s*$//;
	    $config{$k} = $v;
	}
	close DB;
	print STDERR "# slurped $braindb\n" if $main::DEBUG;
    }

    wantarray ? %config : croak "wrong context";
}


sub version() {
    #obtain pegasus version
    my $version = `pegasus-version`;
    chomp($version);
    $version;
}

sub parse_exit(;$) {
    # purpose: parse an exit code any way possible
    # paramtr: $ec (IN): exit code from $?
    # returns: string that shows what went wrong
    my $ec = shift;
    $ec=$? unless defined $ec;

    my $result;
    if ( ($ec & 127) > 0 ) {
	my $signo = ($ec & 127);
	my $core = ( ($ec & 128) == 128 ? ' (core)' : '' );
	$result = "died on signal $signo$core";
    } elsif ( ($ec >> 8) > 0 ) {
	$result = "exit code @{[$ec >> 8]}";
    } else {
	$result = "OK";
    }
    $result;
}


sub check_rescue($$) {
    # purpose: Check for the existence of (multiple levels of) rescue DAGs.
    # paramtr: $dir (IN): directory to check for the presence of rescue DAGs.
    #          $dag (IN): filename of regular DAG file.
    # returns: List of rescue DAGs, may be empty, if none found
    my $dir = shift || croak "Need a directory to check";
    my $dag = shift || croak "Need a dag filename";
    my $base = basename($dag);
    my @result = ();

    local(*DIR);
    if ( opendir( DIR, $dir ) ) {
	while ( defined ($_ = readdir(DIR)) ) {
	    next unless /^$base/o; # only pegasus-planned DAGs
	    next unless /\.rescue$/; # that have a rescue DAG.
	    push( @result, File::Spec->catfile( $dir, $_ ) );
	}
	@result = sort @result;
	closedir DIR;
    }

    wantarray ? @result : $result[$#result];
}

sub log10($) {
    # purpose: Simpler than ceil(log($x) / log(10))
    # paramtr: $x (IN): non-negative number
    # returns: approximate width of number
    use integer;
    my $x = shift;
    my $result = 0;
    while ( $x > 1 ) {
	$result++;
	$x /= 10;
    }
    $result || 1;
}

# must
1;

__END__

=head1 NAME

Pegasus::Common - generally useful collection of methods.

=head1 SYNOPSIS

    use Pegasus::Common;

    $now = isodate();
    $when = isodate( $then );
    $zulu = isodate( time(), 1 );
    $short = isodate( $then, 0, 1 );

    $millis = isomsdate();

    $version = version();

    my $app = find_exec( 'ls' );
    my $gpi = find_exec( 'grid-proxy-info',
        File::Spec->catdir( $ENV{'GLOBUS_LOCATION'}, 'bin' ) );

    my @result = pipe_out_cmd( $app, '-lart' );
    warn "# ", parse_exit($?), "\n";

    my %x = slurp_braindb('rundir');

=head1 DESCRIPTION

This modules collects a few generally useful tools for miscellaneous
Perl work.

=head1 FUNCTIONS

=over 4

=item isodate();

=item isodate($when);

=item isodate($when,$zuluflag);

=item isodate($when,$zuluflag,$shortflag);

The C<isodate> function has various formatting options, permitting
arbitrary time stamps, the choice between local and zulu (UTC) time, and
the choice between a regular and an extra concise output format. It does
not use millisecond extensions (yet).

=item isomsdate();

=item isomsdate($whenms);

=item isomsdate($whenms,$zuluflag);

=item isomsdate($whenms,$zuluflag,$shortflag);

The C<isomsdate> function works like the C<isodate> function. The difference
is the milliseconds extension in the time stamp. In order to properly use
the millisecond extension, and not have C<.000> appear, you need to import
the L<Time::HiRes> module.

=item find_exec( $basename );

=item find_exec( $basename, @extra );

The C<find_exec> function searches the C<PATH> environment variable for
the existence of the given base name. Please only use a base name for
the first argument.

If you need to search additional directories outside your C<PATH>
directories, add as many as you need as additional optional arguments.

=item pipe_out_cmd( @argv );

This is the simple version of the well-known C<pipe_out_cmd> efficient
replacement for the C<popen> function. The first and only mandatory
entry in the argument vector is the fully-qualified path to an
executable. This version does neither provide a I<stdin> override, nor a
time out mechanism. It should preferably used with non-blocking
applications. Please refer to the C<$?> variable after execution.

=item $x = parse_exit( $status );

The C<parse_exit> function parses the C<$?> exit code from a program,
and provides a concise string describing the exit scenario. There are
generally three possibilities.

If the exit code indicated a signal, the signal number and possibly core
file is retunred as string. If the exit code was not 0, the the exit
condition is returned. The remaining case, C<$?> was 0, "OK" is
returned.

=item %db = slurp_braindb($rundir);

The C<slurp_braindb> function reads the contents of the file
C<braindb.txt> in the specified run directory. This is a workflow helper
function of less general applicability.

=item %ver = version();

The C<version> function runs the C<pegasus-version> command and returns
the version of Pegasus being used.

=back

=head1 SEE ALSO

L<http://pegasus.isi.edu/>

=head1 AUTHOR

Jens-S. VE<ouml>ckler, C<voeckler at isi dot edu>
Gaurang Mehta, C<gmehta at isi dot edu>

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
