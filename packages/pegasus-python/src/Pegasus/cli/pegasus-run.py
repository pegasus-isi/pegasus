#!/usr/bin/env perl
#
# Wrapper around pegasus-submit-dag to run a workflow
#
# Usage: pegasus-run rundir
##
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
##
#
# Author: Jens-S. Vöckler voeckler at isi dot edu
# Author: Gaurang Mehta gmehta at isi dot edu
# Revision : $Revision$
#
use 5.006;
use strict;
use Carp;
use Cwd;
use File::Spec;
use File::Basename qw(basename dirname);
use Getopt::Long qw(:config bundling no_ignore_case);
use File::Copy;

# path to load our libs..
BEGIN {
    my $pegasus_config = File::Spec->catfile( dirname($0), 'pegasus-config' );
    eval `$pegasus_config --perl-dump`;
    die("Unable to eval pegasus-config output. $@") if $@;
}

# load our own local modules
use Pegasus::Common;
use Pegasus::Properties qw(%initial); # parses -Dprop=val from @ARGV

sub make_path($) {
    my $p = File::Spec->catfile( Cwd::realpath( dirname($0) ), shift() );
    warn "# make_path = $p\n" if $main::DEBUG;
    $p;
}

sub usage(;$);			# { }

# constants
$main::DEBUG = 0;		# for now

my $grid=0;                     # if set, enable grid checks, disabled by default

my $dagman = make_path( 'pegasus-dagman' );
my $conffile;
GetOptions( "help|h" => \&usage
	  , "debug|d=o" => \$main::DEBUG
	  , "verbose|v+" => \$main::DEBUG
          , "grid!" => \$grid
	  , 'conf|c=s' => \$conffile
	  );

my $run = shift;
# NEW: Default to cwd if nothing was specified
unless ( defined $run ) {
    $run = getcwd();
    my $braindb = File::Spec->catfile( $run, $Pegasus::Common::brainbase );
    usage("You need to provide a valid run directory. Cannot find pegasus brain file")
	unless -r $braindb;
}

# extra sanity
usage( "$run is not a directory." ) unless -d $run;
usage( "$run is not accessible." ) unless -r _;
my %config = slurp_braindb( $run ) or die "ERROR: open braindb: $!\n";

# pre-condition: The planner writes all properties per workflow into the DAG dir.
my $props = Pegasus::Properties->new( $conffile, File::Spec->catfile($run,$config{properties}) );


#
# --- functions -------------------------------------------------
#

sub usage(;$) {
    my $msg = shift;
    my $flag = ( defined $msg && lc($msg) ne 'help' );
    if ( $flag ) {
	my $tty = -t STDOUT;
	print "\033[1m" if $tty;
	print "ERROR: $msg\n";
	print "\033[0m" if $tty;
    }

    my $basename = basename($0,'.pl');
    print << "EOF";

Usage: $basename [options] [rundir]

SemiMandatory arguments:
  rundir  is the directory where the workflow resides as well as ancilliary
          files related to the workflow. Defaults to current working directory if not specified.

Optional arguments:
 -Dprop=val         Explicit settings of a property (multi-option) (only use if really required)
 -c|--conf fn	    Use file fn as pegasus properties file. (only use for debugging purposes.
                    pegasus-run will pick the correct planned properties by default.
 -h|--help          Print this help message and exit.
 -d|--debug lvl     Sets the debug level (verbosity), default is $main::DEBUG.
 -v|--verbose       Raises debug level by 1, see --debug.
 --nogrid           Disable checking for grids (default).
 --grid             Enable checking for grids.

EOF
    exit( $flag ? 1 : 0 );
}

sub salvage_logfile($) {
    # purpose: salvage Condor common log file from truncation
    # paramtr: $dagfn (IN): Name of dag filename
    # returns: -
    #
    my $dagfn = shift;
    my $result = undef;
    local(*DAG,*SUB,*LOG);
    if ( open( DAG, "<$dagfn" ) ) {
	# read to to figure out submit files
	my @x;
	my %submit = ();
	while ( <DAG> ) {
	    next unless /^\s*job/i;
	    s/[\r\n]+$//;	# safe chomp
	    @x = split;
	    $submit{$x[1]} = $x[2]; # dagjobid -> subfn
	}
	close DAG;

	if ( $main::DEBUG > 2 ) {
	    print STDERR "# found the following associations:\n";

	    local $Data::Dumper::Indent = 1;
	    local $Data::Dumper::Pad = "# ";
	    print STDERR Data::Dumper->Dump( [\%submit], [qw(config)] );
	}

	# read two submit files to figure out condor common log file
	foreach my $subfn ( values %submit ) {
	    if ( open( SUB, "<$subfn" ) ) {
		my $logfile = undef;
		while ( <SUB> ) {
		    next unless /^log(=|\s)/i;
		    s/[\r\n]+$//; # safe chomp
		    @x = split /\s*=\s*/, $_, 2;
		    $logfile = ( substr( $x[1], 0, 1 ) =~ /[''""]/ ?
				 substr( $x[1], 1, -1 ) : $x[1] );
		    last;
		}
		close SUB;

		print STDERR "# $subfn points to $logfile\n"
		    if ( $main::DEBUG > 1 );

		if ( ! defined $result ) {
		    $result = $logfile;
		} else {
		    last if $result eq $logfile;
		    warn "# Using distinct, different log files, skipping preservation.\n";
		    return undef;
		}
	    } else {
		warn "Unable to read sub file $subfn: $!\n";
	    }
	}

	# try to preserve log file
	if ( defined $result && -s $result ) {
	    my $newfn;
	    print STDERR "# log $result exists, rescuing from DAGMan.\n"
		if $main::DEBUG;
	    for ( my $i=0; $i<1000; ++$i ) {
		$newfn = sprintf "%s.%03d", $result, $i;
		if ( open( LOG, "<$newfn" ) ) {
		    # file exists
		    close LOG;
		} else {
		    # file does not exist, use that
		    my $newresult=$result;
		    #check if the file is a smylink then dereference it.
		    if ( -l $result ) {
			$newresult=readlink($result);
		    }
		    print STDOUT "Rescued $result as $newfn\n"
			if copy( $newresult, $newfn )  or warn "Could not rescue the log file $newresult to $newfn\n $! \nTrying to continue\n";
		    last;
		}
	    }
	} else {
	    print STDERR "# log $result does not yet exist (good)\n"
		if ( $main::DEBUG );
	}
    } else {
	die "ERROR: Unable to read dag file $dagfn: $!\n";
    }

    $result;
}




#
# --- main ------------------------------------------------------
#

# sanity check: lower umask
umask 0002;

# where were we...
my $here = File::Spec->curdir();
$SIG{'__DIE__'} = sub {
    chdir($here) if defined $here;
};
chdir($run) || die "ERROR: chdir $run: $!\n";

# Do GRID check if $grid enabled
if ( $grid ) {
    # sanity check: Is there a GLOBUS_LOCATION?

    die ( "ERROR: Your environment setup misses GLOBUS_LOCATION.\n",
	  "Please check carefully that you have sourced the correct setup files!\n" )
	unless exists $ENV{'GLOBUS_LOCATION'};

    # sanity check: find grid-proxy-init from GLOBUS_LOCATION
    my $g_l = $ENV{'GLOBUS_LOCATION'};
    print STDERR "# GLOBUS_LOCATION=$g_l\n" if $main::DEBUG;

    # sanity check: Is G_L part of L_L_P?
    my @llp = grep { /^$g_l/ } split /:/, $ENV{'LD_LIBRARY_PATH'};
    $ENV{'LD_LIBRARY_PATH'}=File::Spec->catfile($ENV{'GLOBUS_LOCATION'},"/lib") if @llp == 0;

    # Find grid-proxy-init (should we use openssl instead?? )
    my $gpi = File::Spec->catfile( $g_l, 'bin', 'grid-proxy-info' );
    die "ERROR: Unable to find $gpi\n" unless -x $gpi;
    print STDERR "# found $gpi\n" if $main::DEBUG;

    # common user error
    # sanity check: Sufficient time left on grid proxy certificate
    open( GPI, "$gpi -timeleft 2>&1|" ) || die "open $gpi: $!\n";
    my $timeleft = <GPI>;
    chomp($timeleft);
    $timeleft += 0;			# make numeric
    close GPI;
    die( "ERROR: $gpi died on signal ", ($? & 127) ) if ( ($? & 127) > 0 );
    die( "ERROR: Grid proxy not initialized, Please generate a new proxy\n" ) if $timeleft == -1;
    die( "ERROR: Grid proxy expired, please refresh\n" ) if $timeleft == 0;
    die( "ERROR: $gpi exited with status ", $?>>8 ) if ( $? != 0 );
    warn( "ERROR: Too little time left ($timeleft s) on grid proxy. Please refresh your proxy\n" )
	if $timeleft < 7200;
    print STDERR "# grid proxy has $timeleft s left\n" if $main::DEBUG;
} # end if($grid) checks only if grid option is enabled.

if ( $config{dag} ) {
    #PM-797 move away from using pegasus-submit-dag
    my $c_s = find_exec('condor_submit') || die "Unable to locate condor_submit\n";
    die "ERROR: Unable to access condor_submit $c_s\n" unless -x $c_s;
    print STDERR "# found $c_s\n" if $main::DEBUG;

    # PM-870 we have already changed to the directory, don't prepend $run again
    my $dag_submit_file = "$config{dag}.condor.sub";
    print STDOUT "# dagman condor submit file is $dag_submit_file\n" if $main::DEBUG;

    # sanity check: Is the DAG file there?
    die "ERROR: Unable to locate $config{dag}\n" unless -r $config{dag};
    print STDERR "# found $config{dag}\n" if $main::DEBUG;

    # PM-702: clean up .halt files from pegasus-halt
    my $just_released = 0;
    if ( -e $config{dag}.".halt" ) {
        print STDOUT "Found a previously halted workflow. Releasing it now.\n";
        system("find . -name \\*.dag.halt -exec rm {} \\;");
        $just_released = 1;
    }

    # After the switch from condor_submit_dag, we lost the check to see if
    # a workflow is already running. This replaces those checks.
    if ( -e "monitord.pid" ) {
        if ($just_released) {
            # Support the rare instance were a user want to release a halted
            # workflow while it is still running. In that case, we just
            # quietly exit here
            exit 0;
        }
        else {
            print STDERR "ERROR: It looks like the workflow is already running! If you are sure\n" .
                         "       that is not the case, please remove the monitord.pid file and try\n" .
                         "       again.\n";
            exit 1;
        }
    }

    # find the workflow name and timestamp for pegasus-status
    my $workflow=$config{'pegasus_wf_name'};
    my $time=$config{timestamp};

    # start DAGMan with default throttles
    my @extra = ();
    foreach my $k ( keys %initial ) {
	push( @extra, "-D$k=$initial{$k}" );
    }

    if( -r $dag_submit_file ) {
        #PM-797 do condor_submit on dagman.condor.sub file if it exists
        salvage_logfile( $config{dag} );
        print STDOUT "Submitting to condor $dag_submit_file\n";
        my @args = ( $c_s );
        push( @args, $dag_submit_file);
        system(@args) == 0
            or die( "ERROR: Running condor_submit @args failed with ", parse_exit($?));
        print STDERR "# dagman is running\n" if $main::DEBUG;
    }

    my $did=undef;
    my $daglogfile=$config{dag}.".dagman.out";
    if ( open( DID, "<$daglogfile" ) ) {
	while (<DID>) {
	    if ( /condor_scheduniv_exec/ ) {
		# this part was written by a python programmer?
		$did=(split "condor_scheduniv_exec.",  (split)[3],2)[1];
		last;
	    }
	}
	close(DID);
    }
    print << "EOF";

Your workflow has been started and is running in the base directory:

  $run

*** To monitor the workflow you can run ***

  pegasus-status -l $run

*** To remove your workflow run ***

  pegasus-remove $run

EOF

} elsif ( $config{type}=="shell" ) {
    # sanity check: Is the SCRIPT file there?
    die "ERROR: Unable to execute $config{script}\n" unless -x $config{script};
    print STDERR "# found $config{script}\n" if $main::DEBUG;

    my @args=( "/bin/bash", $config{script} );
    system(@args) == 0
	or die( "ERROR: Running $config{script} failed with ",
		parse_exit($?) );
}

chdir($here);
exit 0;
