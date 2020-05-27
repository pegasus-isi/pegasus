#!/usr/bin/env perl
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
# Author  Gaurang Mehta gmehta@isi.edu
# Revision : $Revision$

use 5.006;
use strict;
use Carp;
use Cwd;			# standard module since 5.6.0 or so
use File::Basename; 		# standard module since 5.005
use File::Spec;			# standard module since 5.005 or 5.6.0
use Getopt::Long qw(:config bundling no_ignore_case);

BEGIN { 
    my $pegasus_config = File::Spec->catfile( dirname($0), 'pegasus-config' );
    eval `$pegasus_config --perl-dump`;
    die("Unable to eval pegasus-config output. $@") if $@;
}
use Pegasus::Common;

# debug off
$main::debug = 0;
$_ = '$Revision$';      # don't edit, automatically updated by CVS
$main::revision=$1 if /Revision:\s+([0-9.]+)/o;

sub myversion() {
    my $version = version();
    print "Pegasus $version, @{[basename($0)]} $main::revision\n";
    exit 0;
}

sub usage(;$) {
    my $msg = shift;
    my $flag = defined $msg && lc($msg) ne 'help';
    if ( $flag ) {
	my $tty = -t STDOUT; 
	print "\033[1m" if $tty;
	print "ERROR: $msg\n";
	print "\033[0m" if $tty;
    }

    print << "EOF";

Usage: @{[basename($0)]} -d <dagid> | dagdir
 pegasus_remove helps you remove an entire workflow. 

Optional arguments: 
 -d|--dagid N    The id of the dag to be removed.
 -v|--verbose   Enter verbose mode, default is not.
 -V|--version   Print version number and exit.

Mandatory arguments:
 dagdir         The directory for the dag that you want removed.
                You may use period (.) for the current working directory. 

EOF
    exit( $flag ? 1 : 0 );
}

sub handler {
    # purpose: generic signal handler
    # paramtr: whatever the OS sends a signal handler and Perl makes of it
    # returns: dies
    my $sig = shift;
    # you should not do this in signal handler, but what the heck
    warn "# Signal $sig found\n" if $main::debug;
    die "ERROR: Killed by SIG$sig\n";
}

#
# --- main -------------------------------------------------
#

# FIXME: Why do you need signal handlers at all for this? 
$SIG{HUP} = \&handler;
$SIG{INT} = \&handler;
$SIG{TERM} = \&handler;
$SIG{QUIT} = \&handler;

my ($dagid);
my $condor_rm=find_exec('condor_rm');
GetOptions( "dagid|d=s"    => \$dagid,
	    "version|V" => \&myversion,
	    "verbose|v" => \$main::debug,
	    "help|h|?" => \&usage );

my $run = shift;
$run = getcwd() unless ( defined $run || defined $dagid ); 
# 20110519 (jsv): partial relative path may break the chdir stuff below
$run = Cwd::abs_path($run) if defined $run; 
if ( defined $run ) {
    # extra sanity
    usage( "$run is not a directory." ) unless -d $run;
    usage( "$run is not accessible." ) unless -r _; 

    # where were we...
    my $here = File::Spec->curdir();
    $SIG{'__DIE__'} = sub {
	chdir($here) if defined $here;
    };
    chdir($run) || die "ERROR: Cannot change to directory $run: $!\n";

    my %config = slurp_braindb( $run ) or die "ERROR: Please ensure that either the run directory is provided as an argument or the dagid.\n       Alternatively run the pegasus-remove command from within the run directory without any arguments\n";
    my @rescue = check_rescue($run,$config{dag});
    if ( @rescue > 0 ) {
	my (@stat,%rescue,$maxsize);
	foreach my $fn ( @rescue ) {
	    if ( (@stat = stat($fn)) > 0 ) {
		$rescue{$fn} = [ @stat ];
		$maxsize = $stat[7] if $maxsize < $stat[7];
	    }
	}
	
	print "\n\nDetected the presence of Rescue DAGs:\n";
	my $width = log10($maxsize);
	foreach my $fn ( @rescue ) {
	    printf( " %s %*u %s\n", 
		    isodate($rescue{$fn}[9]), 
		    $width, $rescue{$fn}[7], 
		    basename($fn) );
	}
	
	# overwrite with "latest" (read: longest basename) rescue DAG
	$config{dag} = $rescue[$#rescue];
	print "\nWILL USE ", $config{dag}, "\n\n";
    }

    my $daglogfile = $config{dag} . ".dagman.out";
    open( DID, "<$daglogfile" ) || 
	die "Error: Cannot open file $daglogfile: $!\n";
    while (<DID>) {
        if ( /\.([0-9\.]+) \(CONDOR_DAGMAN\) STARTING UP/ ) {
            $dagid = $1;
        }
    }

    # return to where we were
    chdir($here);
}
if ( defined $dagid ) {
    # construct the command line string
    my @arg = ( $condor_rm, $dagid );
    warn "# run @arg\n" if $main::debug;
    # DO NOT call pipe_out_cmd, if you don't need popen()
    system { $arg[0] } @arg; 	
    print "\nResult: ", parse_exit($?), "\n";
    exit( $? == 0 ? 0 : 42 );
}
 else {
    usage("You must provide either a dagid or dagdirectory to remove a workflow.");
}
