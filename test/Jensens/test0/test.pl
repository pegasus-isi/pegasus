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
use 5.006;
use strict;
use File::Spec;			# standard module
use File::Basename;		# standard module
use Getopt::Long;		# standard module

use lib dirname($0);
use MyCommon;
use MyGlobus qw($subject $usercertfile $userkeyfile);
use MyGridFTP qw();
use MyCondor qw();
use MyJavaVM qw();
use MyChimera qw();

my $help = '';
my %skip = ( globus => '', gridftp => '', condor => '', javavm => '', chimera => '' );
my @contact = ();
my @servers = ();
my @rls = ();
my $gotorls = 0;
my $gotodm = 0;
$main::DEBUG = 0;

GetOptions( 'help|?' => \$help,
	    'non-stop' => \$MyCommon::nonstop,
	    'contact=s' => \@contact,
	    'gridftp=s' => \@servers,
	    'rls=s' => \@rls,
	    'debug=o' => \$main::DEBUG,
	    'goto-rls' => \$gotorls,
	    'goto-dagman' => \$gotodm,
	    'skip-globus' => \$skip{globus},
	    'skip-gridftp' => \$skip{gridftp},
	    'skip-condor' => \$skip{condor},
	    'skip-java'   => \$skip{javavm},
	    'skip-vds' => \$skip{vds}
	    );

# interrupt handler
BEGIN {
    $SIG{INT} = sub { exit(1) };
}

# unlink-on-exit handler
$main::parent = $$;
%main::unlink = ();
END {
    foreach my $fn ( keys %main::unlink ) {
	unlink $fn if ( $$ == $main::parent );
    }
}

# what's my name?
my $hostname = $ENV{'GLOBUS_HOSTNAME'} || hostfqdn() || 'localhost';

# add defaults
@contact = split(',',join(',',@contact));
push( @contact, "$hostname/jobmanager-fork" ) if @contact == 0;
print ">> ", join("\n>> ",@contact),"\n" if $main::DEBUG;

@servers = split(',',join(',',@servers));
push( @servers, "$hostname" ) if @servers == 0;
print ">> ", join("\n>> ",@servers),"\n" if $main::DEBUG;

@rls = split(',',join(',',@rls));
if ( @rls == 0 ) {
    push( @rls, 'rls://evitable.uchicago.edu' ) 
	if index( $hostname, 'uchicago.edu' ) > 0;
    push( @rls, 'rlsn://smarty.isi.edu' ) 
	if index( $hostname, 'isi.edu' ) > 0;
}
print ">> ", join("\n>> ",@rls),"\n" if $main::DEBUG;


if ( $help ) {
    print << "EOF";

$0 [options]

 --debug l       specify a debug level l for verbose output.
 --contact jm    use once for each job manager contact string jm.
 --gridftp gu    use once for each gridftp server uri gu.
 --rls rls       use once for each RLS server rls.
 --non-stop      run in non-stop mode and assume default answers.
 --skip-globus   skips the GT2 tests.
 --skip-gridftp  skips the GridFTP tests.
 --skip-condor   skips the Condor tests.
 --skip-java     skips the Java tests.
 --skip-vds      skips the VDS tests.

Default: Run all tests (recommended),
         use \"$hostname/jobmanager-fork\" as contact,
         and \"$hostname\" as gridftp service contact.

EOF
    POSIX::_exit(0);		# don't print statistics from END clause
}

#
# say hello, intro screen
#
print "\nHi, this is a small test program in form of a Perl script. Whenever I\n";
print "need to know something from you, you can accept the defaults by hitting\n";
print "enter. Sometimes I just stop, to let you catch up reading on what I did.\n";
print "Let\'s get started.\n\n";
wait_enter;

my %test = ();
unless ( $skip{globus} ) {
    print "Now let\'s look at your GT2 base installation.\n";
    unless ( MyGlobus::check_globus(@contact,@rls,$gotorls) ) {
	$test{globus} = 'FAILED';
	print "\nThe Globus test suite failed, skipping forward to Java.\n\n";
	goto RUN_JAVA;
    } else {
	$test{globus} = 'OK';
	print "\nYou did come far. Your GT2 base actually might do the trick.\n";
	wait_enter;
    }
}

unless ( $skip{gridftp} ) {
    if ( $skip{globus} ) {
	print "\nWARNING!\n";
	print "You chose to skip the Globus test while doing the GridFTP tests.\n";
	print "Proceed at your own risk.\n";
	wait_enter;
    } else {
	print "Now let\'s look at your GT2 GridFTP installation.\n";
    }
    unless ( MyGridFTP::check_gridftp(@servers) ) {
	$test{gridftp} = 'FAILED';
	print "\nThe GridFTP test suite failed, proceeding with next test.\n";
    } else {
	$test{gridftp} = 'OK';
	print "\nWell, some elementary GridFTP capabilities appear to work.\n";
    }
    wait_enter;
}

unless ( $skip{condor} ) {
    print "Now let\'s look at your Condor installation.\n";
    unless ( MyCondor::check_condor(@contact,$gotodm) ) {
	$test{condor} = 'FAILED';
	print "\nThe Condor test suite failed, proceeding with next test.\n\n";
    } else {
	$test{condor} = 'OK';
	print "\nYou did come far. Your Condor actually might do the trick.\n";
    }
    wait_enter;
}

RUN_JAVA:
unless ( $skip{javavm} ) {
    print "Now let\'s look at your Java installation.\n";
    unless ( MyJavaVM::check_javavm() ) {
	$test{javavm} = 'FAILED';
	print "\nThe Java test suite failed, skipping Chimera.\n";
	goto RUN_FINAL;
    } else {
	$test{javavm} = 'OK';
	print "\nYou did come far. Your Java actually might do the trick.\n";
	wait_enter;
    }
}


unless ( $skip{vds} ) {
    if ( $skip{javavm} ) {
	print "\nSorry, but you must run the java test in order to initialize some\n";
	print "variables for the Chimera test. Say skip to skip the Chimera test, too.\n";
	unless ( wait_enter('[enter to run or say skip] ') =~ /skip/i ) {
	    delete $skip{javavm};
	    goto RUN_JAVA;
	} else {
	    $skip{vds} = '1';
	    print "WARNING: Skipping the Chimera test!\n";
	    goto RUN_FINAL;
	}
    } else {
	unless ( MyChimera::check_vds() ) {
	    $test{vds} = 'FAILED';
	    print "\nThe Chimera test suite failed, proceeding with next.\n";
	} else {
	    $test{vds} = 'OK';
	    print "\nYou did come far. Your Chimera actually might do the trick.\n";
	    wait_enter;
	}
    }
}

print "\nThat\'s all, folks!\n";
exit(0);

END {
    print "\nHere are your results:\n\n";
    foreach my $i ( ( qw{globus gridftp condor javavm vds} ) ) {
	my $subject = $i;
	$subject .= '.' while ( length($subject) < 12 );
	print( "\ttest suite $subject: ", 
	       ( exists $test{$i} ? $test{$i} :
		 ( $skip{$i} ? 'user skip request' : 'not tested' ) ), "\n" );
    }
    print "\n";
}
