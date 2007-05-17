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
package MyCondor;
use 5.006;
use strict;
use POSIX qw(mktime);		# standard module
use File::Spec;			# standard module
use File::Basename;		# standard module
use File::Temp qw(tempfile);	# 5.6.1 standard module
use Exporter;			# standard module

use lib dirname($0);
use MyCommon;

# declare prototypes before exporting them
sub check_condor(\@;$);		# { }

our @ISA = qw(Exporter);
our @EXPORT = qw(check_condor);
our @EXPORT_OK = qw();
our $VERSION = '0.1';

sub create_subfile ($$$$$$$$) {
    # purpose: create a standard submit file
    # paramtr: $subfn (IN): the filename to use for the submit file
    #          $logfn (IN): the filename that Condor should produce logs in
    #          $outfn (IN): the filename that stdout goes to, or undef
    #          $errfn (IN): the filename that stderr goes to, or undef
    #          $executable (IN): fqpn of the application to run
    #          $arguments (IN): argument to supply, or undef for no args
    #          $universe (IN): either 'vanilla' or 'globus'
    #          $addin (IN): Additional lines as one string to include in sub
    my $subfn = shift;
    my $logfn = shift;
    my $outfn = shift;
    my $errfn = shift;

    my $executable = shift;
    my $arguments = shift;
    my $universe = lc(shift);
    my $addin = shift;

    # create submit file
    if ( open( SUB, ">$subfn" ) ) {
	print SUB "#\n# Filename: $subfn\n# generated: ", scalar localtime, "\n#\n";
	print SUB "executable\t= $executable\n";
	print SUB "arguments\t= $arguments\n" if ( length($arguments) > 0 );
	print SUB "transfer_executable = false\n"
	    unless ( $universe eq 'scheduler' );
	print SUB "copy_to_spool\t= false\n"
	    unless ( $universe eq 'scheduler' );
	print SUB $addin if ( length($addin) > 0 );
	print SUB "universe\t= $universe\n";
	print SUB "notification\t= NEVER\n";
	print SUB "log\t\t= $logfn\n";
	print SUB "output\t\t= $outfn\n" if ( length($outfn) > 0 );
	print SUB "error\t\t= $errfn\n" if ( length($errfn) > 0 );
	print SUB "queue\n";
	close(SUB);
	$main::unlink{$subfn} = $main::parent;
    } else {
	print "FAILURE!";
	print "Unable to create $subfn: $!\n";
	return undef;
    }

    1;				# success
}

sub slurp_logfile($) {
    my $fn = shift;
    local $/ = "\n...\n";
    local(*LOG);

    my %result = ();
    if ( open( LOG, "<$fn" ) ) {
	while ( <LOG> ) {
	    $result{substr($_,0,3)} = split;
	}
	close LOG;
    }

    %result;
}

sub submit_condor {
    my $universe = shift || 'scheduler';
    my $addin = shift;

    my ($fh, $fn) = tempfile( "test-XXXXXX", DIR => File::Spec->tmpdir(),
			      SUFFIX => '.sub', UNLINK => 1 );
    unless ( defined $fh ) {
	print "FAILURE!\n";
	print "Unable to create a temporary file. Sorry, that\'s it!\n";
	return undef;
    }

    # create sub file
    my $basefn = substr($fn,0,-4) . "-$$";
    my $logfn = "$basefn-log.txt";
    close($fh);
    return undef 
	unless defined create_subfile( $fn, $logfn,
				       "$basefn-out.txt", "$basefn-err.txt",
				       # FIXME: Condor 6.4.x < 2 argv bug
				       # '/bin/date', "-u +%S-%M-%H-%d-%m-%Y"
				       '/bin/date', "-u",
				       $universe, $addin );

    # show file
    system("cat $fn");

    # submit file to condor
    my $cmd = "condor_submit $fn";
    print "\ntrying to submit $fn...\n";
    my @submit;
    if ( open( BATCH, "$cmd|" ) ) {
	while ( <BATCH> ) {
	    s/[\r\n]+$//;
	    push(@submit, $_);
	    print "# $_\n";
	}
	close(BATCH);
    } else {
	my $code = $? >> 8;
	print "FAILURE!\n";
	print "Unable to run $cmd: $code: $!\n";
	print "Please make sure that you can submit simple $universe jobs into your local \n";
	print "Condor system.\n";
	return undef;
    }

    my $jobid;
    my @mine = gmtime(time);	# save submit local time
    my $submit = (grep(/submitted to cluster/,@submit))[0];
    print "# captured: $submit\n";
    if ( $submit =~ /submitted to cluster (\d+)\./ ) {
	$jobid = $1;
	print "OK: submitted as job number $jobid\n";
    } else {
	print "FAILURE!\n";
	print "I cannot detect the job number. \n";
	print "Your Condor submission returned the following info:\n";
	print join("\n",@submit), "\n";
	return undef;
    }

    print "\nWaiting for the job to finish (patience!)\n";
    my ($timeout,$sleep,@line,%tags,$rc);
    do {
	sleep(5);
	if ( ( $timeout & 1) == 0 ) {
	    @line=`condor_q $jobid`;
	    $rc = $?;
	    system('condor_reschedule >> /dev/null 2>&1')
		if $timeout == 0;
	} else { 
	    $rc = 0;
	}
	print '.';
	%tags = slurp_logfile($logfn);
    } while ( ! exists $tags{'005'} && $rc == 0 || ++$timeout > 60 );
    print "\n";

    if ( $timeout > 60 ) {
	print "FAILURE!\n";
	print "Sorry, five minutes, and no job status change. It does not appear, as if\n";
	print "you can submit universe=$universe jobs to yourself.\n";
	system("condor_rm $jobid" );
	return undef;
    } else {
	print "OK: job finished (about time)\n";
    }

    my $flag = 0;
    if ( -z "$basefn-err.txt" ) {
	print "OK: no messages on stderr\n";
    } else {
	print "WARNING: stderr reported a message\n";
	system("cat $basefn-err.txt");
	print "If the message above complained about the number of arguments being\n";
	print "wrong, or something like \"invalid date\", you probably have a version\n";
	print "of Condor that has the zero-args bug. In that case, you should upgrade!\n";
	$flag++;
    }
    unlink("$basefn-err.txt");

    if ( -s "$basefn-out.txt" ) {
	$_ = `cat $basefn-out.txt`;
	s/[\r\n]*$//;
	print "OK: message on stdout: $_\n";

	# check differences
	my $diff;
	my @date = parse_date($_);
	if ( @date == 0 ) {
	    print "FAILURE!\n";
	    print "The remote /bin/date -u command returned a string that I cannot parse.\n";
	    $flag++;
	} else {
	    if ( ($diff = abs(mktime(@mine)-mktime(@date))) < 120 ) {
		print "OK: only $diff s difference, good.\n";
	    } else {
		print "WARNING!\n";
		print "There is a difference between the remote host time and my time of $diff s.\n";
		print "Please note that Globus GSI is very sensitive to mis-timings. You definitely\n";
		print "want to consider installing an NTP service on all machines, the remote\n";
		print "contact and this submit host.\n";
	    }
	}
    } else {
	print "WARNING: stdout reported no message, though it should have.\n";
	$flag++;
    }
    unlink("$basefn-out.txt");

    if ( open( LOG, "<$logfn" ) ) {
	local $/="\n...\n";
	while ( <LOG> ) {
	    my $cc = substr($_,0,3);
	    printf( "%s: condor code %s found\n",
		    ( $cc eq '000' || $cc eq '001' || $cc eq '005' || $cc eq '017' ?
		      'OK' : 'WARNING' ), $cc );
	    if ( /^005/ && /\(return value (\d+)\)/ ) {
		if ( $1 eq 0 ) {
		    print "OK: exit code is 0\n";
		} else {
		    print "WARNING! exit code is $1\n";
		    $flag++;
		}
	    }
	}
	close(LOG);
    } else {
	my $code = $? >> 8;
	print "FAILURE! exit=$code: $!\n";
	print "I am unable to open the log $basefn-log.txt for your job.\n";
	print "This is extremely fishy, and this is it.\n";
	return undef;
    }
    unlink("$basefn-log.txt");

    if ( $flag > 0 ) {
	print "FAILURE!\n";
	print "There were 1+ problems reported with your job. I don\'t\n";
	print "believe that you can run $universe jobs successfully on this host.\n";
	return undef;
    } else {
	print "OK: $universe jobs are runnable\n";
    }
    
    1;
}

sub check_dagman_simple() {
    my ($fh, $fn) = tempfile( "test-XXXXXX", DIR => File::Spec->tmpdir(),
			      SUFFIX => '.dag', UNLINK => 1 );
    $main::unlink{$fn} = $main::parent;

    # check that all is there
    unless ( defined $fh ) {
	print "FAILURE!\n";
	print "Unable to create a temporary DAG file. Sorry, that\'s it!\n";
	return undef;
    }

    # collect data filenames
    my ($d1h, $d1n) = tempfile( "data-1-XXXXXX", DIR => File::Spec->tmpdir(),
				SUFFIX => '.txt', UNLINK => 1 );
    my ($d2h, $d2n) = tempfile( "data-2-XXXXXX", DIR => File::Spec->tmpdir(),
				SUFFIX => '.txt', UNLINK => 1 );
    my ($d3h, $d3n) = tempfile( "data-3-XXXXXX", DIR => File::Spec->tmpdir(),
				SUFFIX => '.txt', UNLINK => 1 );
    my ($d4h, $d4n) = tempfile( "data-4-XXXXXX", DIR => File::Spec->tmpdir(),
				SUFFIX => '.txt', UNLINK => 1 );

    unless ( defined $d1h && defined $d2h && defined $d3h && defined $d4h ) {
	print "FAILURE!\n";
	print "Unable to create a temporary data file. Sorry, that\'s it!\n";
	return undef;
    } else {
	$main::unlink{$d1n} = $main::parent;
	$main::unlink{$d2n} = $main::parent;
	$main::unlink{$d3n} = $main::parent;
	$main::unlink{$d4n} = $main::parent;
    }

    # obtain base name
    my $basefn = substr($fn,0,-4) . "-$$";

    # create sub files
    close($d1h);
    return undef 
	unless defined create_subfile( "$basefn-1.sub", "$basefn.log", 
				       $d1n, "$basefn-1.err",
				       '/bin/date', undef, 
				       'scheduler', undef );
    close($d2h);
    return undef 
	unless defined create_subfile( "$basefn-2.sub", "$basefn.log", 
				       $d2n, "$basefn-2.err",
				       '/bin/cat', "-n",
				       'scheduler', "input = $d1n\n" );

    close($d3h);
    return undef 
	unless defined create_subfile( "$basefn-3.sub", "$basefn.log", 
				       $d3n, "$basefn-3.err",
				       '/bin/cat', "-n",
				       'scheduler', "input = $d1n\n" );

    close($d4h);
    return undef 
	unless defined create_subfile( "$basefn-4.sub", "$basefn.log", 
				       $d4n, "$basefn-4.err",
				       '/bin/cat', "-n $d2n $d3n",
				       'scheduler', undef );
    for ( my $i=1; $i<=4; ++$i ) {
	$main::unlink{"$basefn-$i.out"} = $main::parent;
	$main::unlink{"$basefn-$i.err"} = $main::parent;
    }

    # create DAG file
    print $fh "JOB A $basefn-1.sub\n";
    print $fh "JOB B $basefn-2.sub\n";
    print $fh "JOB C $basefn-3.sub\n";
    print $fh "JOB D $basefn-4.sub\n";
    print $fh "PARENT A CHILD B C\n";
    print $fh "PARENT B C CHILD D\n";
    close($fh);

    # show DAG file
    print "This is the .dag file we are going to use:\n\n";
    system("cat -n $fn");

    # submit file to condor
    my $cmd = "condor_submit_dag -notification NEVER $fn";
    print "\ntrying to submit DAG $fn...\n";
    my @submit;
    if ( open( BATCH, "$cmd|" ) ) {
	while ( <BATCH> ) {
	    s/\r*\n$//;
	    push(@submit, $_);
	    print "# $_\n";
	}
	close(BATCH);
    } else {
	my $code = $? >> 8;
	print "FAILURE!\n";
	print "Unable to run $cmd: $code: $!\n";
	print "Please make sure that you can submit simple DAGs to your local \n";
	print "Condor system.\n";
	return undef;
    }


    my $jobid;
    my @mine = gmtime(time);	# save submit local time
    my $submit = (grep(/submitted to cluster/,@submit))[0];
    print "# captured: $submit\n";
    if ( $submit =~ /submitted to cluster (\d+)\./ ) {
	$jobid = $1;
	print "OK: submitted as job number $jobid\n";
    } else {
	print "FAILURE!\n";
	print "I cannot detect the job number. \n";
	print "Your Condor submission returned the following info:\n";
	print join("\n",@submit), "\n";
	return undef;
    }

    print "\nWaiting for the job to finish (patience!)\n";
    my ($timeout,$sleep,@line,%tags,$rc);
    do {
	sleep(5);
	if ( ($timeout & 1) == 0 ) {
	    @line=`tail -1 $fn.dagman.out`;
	    $rc = $?;
	    system('condor_reschedule >> /dev/null 2>&1')
		if ( ($timeout % 12) == 0 );
	} else {
	    $rc = 0;
	}
	print '.';
    } while ( $line[$#line] !~ /EXITING WITH STATUS/ && $rc == 0 || ++$timeout > 60 );
    print "\n";

    if ( $timeout > 60 ) {
	print "FAILURE!\n";
	print "Sorry, five minutes, and no job status change. It does not appear, as if\n";
	print "you can submit scheduler universe jobs to yourself.\n";
	system("condor_rm $jobid" );
	return undef;
    } else {
	print "OK: job finished (about time)\n";
    }

    my $flag = 0;
    foreach my $me ( qw(1 2 3 4) ) {
	my $errfn = "$basefn-$me.err";
	if ( -z $errfn ) {
	    print "OK: no messages on stderr for job $me\n";
	} else {
	    print "WARNING: job $me reported a message\n";
	    system("cat $errfn");
	    print "If the message above complained about the number of arguments being\n";
	    print "wrong, or something like \"invalid date\", you probably have a version\n";
	    print "of Condor that has the zero-args bug. In that case, you should upgrade!\n";
	    $flag++;
	}
    }

    if ( -s $d4n ) {
	$_ = `cat $d4n`;
	s/[\r\n]*$//;
	print "OK: message on stdout:\n$_\n";
    } else {
	print "WARNING: stdout reported no message, though it should have.\n";
	$flag++;
    }
    unlink( $d4n, $d3n, $d2n, $d1n );

    if ( open( LOG, "<$fn.dagman.log" ) ) {
	local $/="\n...\n";
	while ( <LOG> ) {
	    my $cc = substr($_,0,3);
	    printf( "%s: condor code %s found\n",
		    ( $cc eq '000' || $cc eq '001' || $cc eq '005' || $cc eq '017' ?
		      'OK' : 'WARNING' ), $cc );
	    if ( /^005/ && /\(return value (\d+)\)/ ) {
		if ( $1 eq 0 ) {
		    print "OK: exit code is 0\n";
		} else {
		    print "WARNING! exit code is $1\n";
		    $flag++;
		}
	    }
	}
	close(LOG);
    } else {
	my $code = $? >> 8;
	print "FAILURE! exit=$code: $!\n";
	print "I am unable to open the log $fn.dagman.log for your job.\n";
	print "This is extremely fishy, and this is it.\n";
	return undef;
    }
    unlink( "$fn.condor.sub", "$fn.dagman.out", "$fn.lib.out", 
	    "$fn.dagman.log", "$basefn.log");

    if ( $flag > 0 ) {
	print "FAILURE!\n";
	print "There were 1+ problems reported with your job. I don\'t\n";
	print "believe that you can run DAGs successfully on this host.\n";
	return undef;
    } else {
	print "OK: DAGs are runnable\n";
    }
    
    1;
}

sub check_status (\%\@$$) {
    my $status = shift;		# a hashref
    my $flag = shift;		# an arrayref
    my $key = shift;
    my $condition = shift;

    if ( exists $$status{$key} ) {
	$condition =~ s/XYZ/'$$status{$key}'/g;
	my $result = eval $condition;
	if ( $result ) {
	    print "OK: $key matches condition $condition\n";
	    return 1;
	} else {
	    print "WARNING: $key does not match $condition\n";
	    $$flag[0]++;
	    return 0;
	}
    } else {
	print "WARNING: $key does not exist!\n";
	$$flag[1]++;
	return -1;
    }
}

sub check_condor(\@;$) {
    # purpose: check out local Condor and Condor-G
    # paramtr: $contacts (IN): vector of jobmgr contact strings. 
    # warning: $contacts is a vector ref, which will be modified. 
    # returns: true for ok, undef for error
    my $jmlist = shift || return undef; # must have contacts list
    print ">> ", join("\n>> ",@$jmlist),"\n" if $main::DEBUG;
    my $line;
    my $gotodm = shift || 0;

    #
    # condor binaries
    # 
    foreach my $exe ( qw(condor_submit condor_submit_dag condor_status condor_q condor_rm condor_reschedule condor_version) ) {
	my $prg = find_exec($exe);
	unless ( defined $prg ) {
	    print "FAILURE!\n";
	    print "Unable to find the location of $exe. Please make extra sure that\n";
	    print "your Condor installation is complete, e.g. comes from the VDT, and\n";
	    print "does include Condor-G. Do not run on the central manager, unless you\n";
	    print "have a personal Condor installation.\n\n";
	    return undef;
	} else {
	    print "OK: Found $prg\n";
	}
    }

    #
    # condor version
    #
    my @condor_version = `condor_version`;
    my $condor_version;
    $condor_version = $1 if $condor_version[0] =~ /:\s+(\d+\.\d+\.\d+)/;
    unless ( defined $condor_version ) {
	print "FAILURE!\n";
	print "I am unable to detect the version number of your Condor.\n";
	return undef;
    } else {
	print "OK: You are running Condor version $condor_version\n";
	@condor_version = split /\./,$condor_version;
    }

    if ( $condor_version[0] < 6 ||
	 $condor_version[0] == 6 && $condor_version[1] < 4 ) {
	print "WARNING!\n";
	print "I believe strongly that you should upgrade your current version of\n";
	print "Condor to a supported and well-working branch.\n";
    }

    print "\nThe essential Condor existence checks were successful. Now,\n";
    print "let\'s check the status of Condor on this machine\n";
    wait_enter;

    # 
    # condor_status checks
    #
    my $myself = hostfqdn();
    unless ( defined $myself ) {
	print "FAILURE!\n";
	print "Unable to determine my own hostname. Without it, I cannot do very much\n";
	print "in determining the status. This may be a Perl error, or something else.\n";
	print "You may want to ask your sysadmin to setup up your system correctly.\n";
	return undef;
    }

    my (@status,%status);
    print "Trying to obtain local status information from Condor...\n";
    chomp(@status = `condor_status -l $myself 2>&1`);
    if ( $? == 0 ) {
	my $count = 0;
	foreach ( @status ) {
	    s/\s*\r*\n$//;
	    next unless ( length );
	    my ($k,$v) = split(/\s=\s/,$_,2);
	    $v = substr($v,1,-1) if ( substr($v,0,1) eq "\"" );
	    $status{$k} = $v;
	    $count++;
	}
	print "OK: read $count status variables\n" if $count;
    } else {
	my $code = $? >> 8;
	shift(@status) while ( @status > 0 && length($status[0]) <= 1 );

	print "\nFAILURE!\n";
	print "The exit code is $code. Condor has this to say in its defense:\n";
	print "# ", join("\n# ",@status), "\n";
	print "I cannot use condor_status successfully on this machine. This bodes ill.\n";
	print "Sometimes, the Condor is simply not started on this machine. Please fix\n";
	print "the Condor on this machine, and (re-)start it, if necessary.\n";
	return undef;
    }

    my @flag;
    check_status( %status, @flag, 'Activity',  q{XYZ eq 'Idle'});
    check_status( %status, @flag, 'CpuIsBusy', q{XYZ eq 'FALSE'} );
    check_status( %status, @flag, 'LoadAvg',   q{XYZ <= 0.01} );
    check_status( %status, @flag, 'CondorLoadAvg', q{XYZ <= 0.01} );
    print "\n";

    if ( $flag[0] > 0 ) {
	print "WARNING!\n";
	print "The machine is busy. The check, if Condor jobs run at all, might fail,\n";
	print "or it might succeed, depending on the configuration. Success is unlikely.\n";
    }

    if ( $flag[1] > 0 ) {
	print "FAILURE!\n";
	print "Your Condor system does not know about several essential variables that I\n";
	print "asked about. Your Condor appears to be uncompatible with this test.\n";
	return undef;
    }

    # skip ahead, if requested
    goto DAGMAN if $gotodm;

    #
    # scheduler universe to self
    #
    print "\nLet\'s check, if a simple scheduler universe job can be run.\n";
    print "Type \"skip\" without the quotes, if you want to skip this test.\n";
    # skipping no longer advised
    # print "Skipping this test is advised for a VDT client-side Condor.\n";
    if ( wait_enter("[hit enter or type skip]\n") !~ /skip/i ) {
	return undef unless defined submit_condor( 'scheduler', undef );
    } else {
	print "\nOK: You have chosen to skip the vanilla universe test.\n\n";
    }
	
    # 
    # globus universe to remote vanilla
    #
    print "\nLet\'s check, if a simple globus universe jobs can be run.\n";
    print "Type \"skip\" without the quotes, if you want to skip this test.\n";
    print "Running this test is highly recommended.\n";
    if ( wait_enter("[hit enter or type skip]\n") !~ /skip/i ) {
	my @newlist;
	foreach my $gk ( @$jmlist ) {
	    print "\ntrying contact $gk\n";
	    unless ( defined submit_condor( 'globus', "globusscheduler\t= $gk\n" .
					    "globusrsl\t= (jobType=single)(maxWallTime=2)\n" ) ) {
		print "WARNING!\n";
		print "I will ignore contact $gk for now...\n";
	    } else {
		push(@newlist,$gk);
	    }
	    wait_enter;
	}
	$jmlist = [ @newlist ];
	if ( @$jmlist == 0 ) {
	    print "FAILURE!\n";
	    print "Not a single of your contacts works with CondorG.\n";
	    return undef;
	}
    } else {
	print "\nWARNING!\n";
	print "You chose to forego the Globus tests for CondorG. All bets are off!\n\n";
    }

    #
    # check my own DAGMan
    #
  DAGMAN:
    print "\nLet\'s check, if a simple DAGMan workflow can be run.\n";
    if ( wait_enter("[hit enter or type skip]\n") !~ /skip/i ) {
	unless ( defined check_dagman_simple() ) {
	    print "FAILURE!\n";
	    print "Your DAGMan run workflow did not appear to do the trick.\n";
	    print "You might want to check manually, if you can run simple DAGs.\n";
	    print "Please refer to http://condorproject.org/ for further information.\n";
	    return undef;
	}
    } else {
	print "\nWARNING!\n";
	print "You chose to forego the DAGMan tests. All bets are off!\n\n";
    }

    # return the perl way
    '0 but true';
}

1;
