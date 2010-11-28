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
package MyChimera;
use 5.006;
use strict;
use File::Spec;			# standard module
use File::Basename;		# standard module
use File::Temp qw(tempfile);	# 5.6.1 standard module

use Exporter;
use lib dirname($0);

use MyCommon;
use MyJavaVM qw(check4jar $java $java_home $classpath);

# declare prototypes before exporting them
sub check_vds();		# { }

our @ISA = qw(Exporter);
our @EXPORT = qw(check_chimera);
our @EXPORT_OK = qw();
our $VERSION = '0.1';

sub run_class($$$) {
    # purpose: start a java class
    # paramtr: $class (IN): the name of the class to run
    #          $args (IN): array ref with CLI arguments
    #          $defs (IN): array ref with additional property definitions
    # returns: [0]: the return value as exit code
    #          [1]: first line of output...
    my $class = shift || die "assert: class is invalid\n";
    my $args = shift || [];
    my $defs = shift || [];

    my $cli = "$java \"-Dvds.home=\\\$VDS_HOME\"";
    foreach my $arg ( @$defs ) { $cli .= " \"-D$arg\"" };
    $cli .= " $class";
    foreach my $arg ( @$args ) { $cli .= " $arg" };

    my @output = `$cli 2>&1`;
    if ( $? == 0 ) {
	print "OK: Successfully ran class $class\n";
    } else {
	my $rv = $? >> 8;
	print "FAILURE! exit code $rv\n";
	print "Unable to run class $class. This bodes very ill.\n";
	print "# ", join("\n# ",@output), "\n";
    }
    ( $?, @output );
}

sub mysystem
    # purpose: avoid /bin/sh invocation overhead
    # paramtr: executable and command line arguments, each separatly
    # returns: status of job that ran, 127 for failure to execute.
{
    my $pid = fork();

    return undef unless defined $pid;
    if ( $pid == 0 )
    {
       # child
       $SIG{INT} = 'IGNORE';
       $SIG{QUIT} = 'IGNORE';
       # FIXME: what about blocking SIGCHLD?
       open STDOUT, '>>/dev/null';
       open STDERR, '>&STDOUT'; # dup2()
       exec { $_[0] } @_;
       exit 127;
    }

    # parent
    waitpid($pid,0);   # FIXME: deal with EINTR and EAGAIN
    $?;
}

sub check_vds() {
    # purpose: check out local Java installation
    # paramtr: -
    # returns: undef on error, true otherwise

    #
    # check for $VDS_HOME
    #
    my $vdshome = $ENV{'VDS_HOME'};
    if ( defined $vdshome ) {
	print "OK: VDS_HOME=$vdshome\n";
    } else {
	print "FAILURE!\n";
	print "You need to set the VDS_HOME variable, before you can run Chimera stuff.\n";
	return undef;
    }

    my $bindir = File::Spec->catfile($vdshome,'bin');
    if ( -d $bindir ) {
	print "OK: Found $bindir directory\n";
    } else {
	print "FAILURE!\n";
	print "Though VDS_HOME is set, its $bindir points into Nirwana.\n";
	return undef;
    }

    if ( grep( /$bindir/, File::Spec->path ) > 0 ) {
	print "OK: $bindir is part of your PATH.\n";
    } else {
	print "WARNING: $bindir is not part of your PATH.\n";
	# add to PATH so that next test finds it
	$ENV{PATH} .= ":$bindir";
    }

    #
    # check for all Chimera binaries
    #
    print "\nLooking for Java executables:\n";
    foreach my $exe ( qw(condor-submit-wait d2d dax2dot deletemeta 
			 deletevdc dirmanager exitcode gencdag gendax genmdag 
			 genpoolconfig insertmeta insertvdc kickstart-condor
			 partitiondax reassemble-chunks replanner rls-client
			 rls-query-client rc-client searchmeta searchvdc showmeta 
			 shplanner simconverter tailstatd tc-client 
			 test-version show-properties updatevdc 
			 vdlt2vdlx vdlx2vdlt vdlc
			 vds-bug-report vds-submit-dag vds-version 
			 xsearchvdc) ) {
	my $prg = find_exec($exe);
	unless ( defined $prg ) {
	    print "FAILURE!\n";
	    print "Unable to find the location of $exe. Please make extra sure that\n";
	    print "your Chimera installation is complete, e.g. comes from the VDT.\n";
	    return undef;
	} else {
	    print "OK: Found $prg\n";
	    print "Warning: $exe found outside $bindir first!\n"
		unless index($prg,$bindir) == 0;
	}
    }

    print "\nYour Java executables are all there. Let\'s check for C executables.\n"; 
    wait_enter;

    my $seen;
    foreach my $exe ( qw(kickstart keg transfer k.2 T2) ) {
	my $prg = find_exec($exe);
	unless ( defined $prg ) {
	    print "Warning: Unable to locate $exe\n";
	    unless ( $seen++ ) {
		print "Please make extra sure that your GVDS installation is complete,\n";
		print "e.g. comes from the VDT. This message will only be printed once.\n";
		print "In a development environment, GVDS C binaries are not visible.\n";
	    }
	} else {
	    print "OK: Found $prg\n";
	    print "Warning: $exe found outside $bindir first!\n"
		unless index($prg,$bindir) == 0;
	}
    }

    print "\nYour C executables are all there. Let\'s check for the libraries.\n";
    wait_enter;

    #
    # check for Chimera required 3rd party jars
    #
    foreach my $jar ( qw(cog-jglobus.jar commons-pool.jar cryptix-asn1.jar 
			 cryptix.jar cryptix32.jar exist-optional.jar
			 exist.jar jakarta-oro.jar java-getopt-*.jar
			 jce-jdk*.jar jlinker.jar junit.jar log4j-1*.jar
			 loggerservice-stub.jar 
			 mysql-connector-java-3*.jar postgresql-8.*.jdbc3.jar
			 puretls.jar resolver.jar rls.jar xercesImpl.jar
			 xmlParserAPIs.jar xmldb.jar xmlrpc.jar) ) {
	my $result = check4jar($jar);
	if ( -r $result ) {
	    print "OK: Found $result\n";
	} else {
	    print "WARNING: Missing $jar\n";
	}
    }

    # check for Chimera jar
    my $developer;
    my $chimera = check4jar('gvds.jar');
    if ( defined $chimera ) {
	print "OK: Found $chimera\n";
    } else {
	# not found, are we developers?
	$developer=1;
	$chimera = (grep { -d } grep( /$vdshome/, split(/:/,$MyJavaVM::classpath) ))[0];
	if ( defined $chimera ) {
	    print "OK: Found $chimera in CLASSPATH\n";

	    # extra check: Are the libs compiled?
	    my $class = File::Spec->catfile( $chimera, 'org', 'griphyn', 'vdl', 'Chimera.class' );
	    if ( -r $class ) {
		print "OK: Your development distribution is in a compiled state\n";
	    } else {
		print "WARNING: You are missing essential class files. You appear to be a\n";
		print "developer. I will try to compile the java sources for you...\n";

		# need to compile Chimera: REQUIRES ANT
		my $here = File::Spec->curdir;
		chdir($vdshome);
		my $result = system( "ant jar" );
		chdir( $here );
		print "\n";
		if ( $result != 0 ) {
		    print "Failure! exit code @{[$result >> 8]}\n";
		    print "I am unable to compile GVDS, and I don't have the gvds.jar file.\n";
		    print "I don\'t think that I can run any Chimera binary.\n";
		    return undef;
		}
	    }
	} else {
	    print "\nFailure!\n";
	    print "I found neither the gvds.jar file, nor the VDS_HOME in your\n";
	    print "CLASSPATH. I don\'t think that I can run Chimera successfully.\n";
	    return undef;
	}
    }
    print "\n";

    # create a hello world in VDLt
    my ($fh,$fn) = tempfile( 'testXXXXXX', DIR => ($ENV{TMP} || '/tmp') );
    unless ( defined $fh ) {
	print "FAILURE!\n";
	print "Unable to create a temporary file to store some VDLt.\n";
	print "Maybe the TMP directory does not work?\n";
	return undef;
    }

    print $fh "TR hw( output f )\n{\n";
    print $fh "  argument = \"Hello World!\";\n";
    print $fh '  argument stdout = ${f};', "\n}\n";
    print $fh 'DV d1->hw( f=@{output:"out.txt"} );', "\n";
    close($fh);
    print "OK: Created VDLt file\n";

    # some just-in-case properties
    my @prop = qw( -Dvds.properties=/dev/null 
		   -Dvds.user.properties=/dev/null );

    my ($xml,$db,$dax);
    (undef,$xml) = tempfile( 'testXXXXXX', DIR => ($ENV{TMP} || '/tmp') );
    my $result = mysystem( File::Spec->catfile( $ENV{'VDS_HOME'}, 'bin', 'vdlt2vdlx' ),
			   @prop, $fn, $xml );
    unlink($fn);

    if ( $result != 0 ) {
	unlink($xml);
	print "Failure! ", $result==-1 ? "$!\n" : "exit code @{[$result >> 8]}\n";
	print "I am unable to execute the VDLt to VDLx converter correctly.\n";
	print "You might want to check, if you can run it manually.\n";
	return undef;
    } else {
	print "OK: Converted VDLt to VDLx\n";
    }

    # add file to own personal database
    (undef,$db) = tempfile( 'testXXXXXX', DIR => ($ENV{TMP} || '/tmp') );
    unlink($db);
    $result = mysystem( File::Spec->catfile( $ENV{'VDS_HOME'}, 'bin', 'insertvdc' ),
			@prop, '-d', $db, $xml );
    unlink($xml);

    if ( $result != 0 ) {
	unlink($db);
	print "Failure! ", $result==-1 ? "$!\n" : "exit code @{[$result >> 8]}\n";
	print "I am unable to add the hello world data to a temp. database\n";
	print "You might want to check, if you can run it manually.\n";
	return undef;
    } else {
	print "OK: Inserted VDLx into temporary database\n";
    }

    # run abstract planner
    (undef,$dax) = tempfile( 'testXXXXXX', DIR => ($ENV{TMP} || '/tmp') );
    $result = mysystem( File::Spec->catfile( $ENV{'VDS_HOME'}, 'bin', 'gendax' ),
			@prop, '-d', $db, '-l', 'hw', '-o', $xml, '-f',
			'out.txt' );
    unlink( $db, "$db.0", "$db.nr" );

    if ( $result != 0 ) {
	unlink($dax);
	print "Failure! ", $result==-1 ? "$!\n" : "exit code @{[$result >> 8]}\n";
	print "I am unable to run the abstract planner. This is not good.\n";
	print "I am at loss, why things fail now. Do you use NFS?\n";
	return undef;
    } else {
	print "OK: Ran abstract planner successfully\n";
    }

    print "\nTBD(self): I haven't found the time to add shell planner and\n";
    print "run a simple shell plan.\n";
    unlink($dax);

    # return the perl way
    '0 but true';
}

1;
