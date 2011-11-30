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
package MyJavaVM;
use 5.006;
use strict;
use File::Spec;			# standard module
use File::Basename;		# standard module
use Exporter;			# standard module

use lib dirname($0);
use MyCommon;

# declare prototypes before exporting them
use vars qw($java_home $java $classpath %properties);

sub check_javavm();		# { }
sub check4jar($;$$);		# { }

our @ISA = qw(Exporter);
our @EXPORT = qw(check_javavm);
our @EXPORT_OK = qw($java_home $java $classpath %properties check4jar);
our $VERSION = '0.1';

sub check4jar ($;$$) {
    # purpose: check for the existence of the specified jar basename
    # paramtr: $jar (IN): jar basename to check for, may contain jokers
    #          $jh (opt. IN): optional alternative $JAVA_HOME
    #          $cp (opt. IN): optional alternative $CLASSPATH
    # globals: $javahome (IN)
    #          $classpath (IN)
    # returns: the fqpn of the jar, or undef, if not found.
    my $jar = shift;
    my $jh = shift || $java_home;
    my @cp = split( /:/, shift() || $classpath );
    
    warn "# searching for jar $jar\n" if ( $main::DEBUG );
    # usually the endorsed libs have highest prio (do they?)
    my $endorsed = File::Spec->catfile( $jh, 'jre', 'lib', 'endorsed' );
    if ( -d $endorsed ) {
	my $result = File::Spec->catfile( $endorsed, $jar );
	$result = glob($result) 
	    if ( index($result,'?') != -1 || index($result,'*') != -1 );
	return $result if ( -r $result );
    }

    # now check the CLASSPATH
    foreach my $cp ( grep { ! -d } @cp ) {
	# part is a file, try to match up through the backdoor
	my $result = File::Spec->catfile( dirname($cp), $jar );
	if ( index($result,'?') != -1 || index($result,'*') != -1 ) {
	    my @result = glob($result);
	    return $cp if @result > 0;
	} else {
	    return $cp if ( basename($cp) eq basename($result) );
	}
    }

    # finally, check the extension path
    my $ext = File::Spec->catfile( $jh, 'jre', 'lib', 'ext' );
    if ( -d $ext ) {
	my $result = File::Spec->catfile( $ext, $jar );
	$result = glob($result) 
	    if ( index($result,'?') != -1 || index($result,'*') != -1 );
	return $result if ( -r $result );
    }

    undef;
}

sub check_javavm() {
    # purpose: check out local Java installation
    # paramtr: -
    # returns: true for ok, undef for error

    #
    # test+set for JAVA_HOME
    #
    $java_home = $ENV{'JAVA_HOME'};
    unless ( defined $java_home ) {
	print "WARNING!\n";
	print 'Your $JAVA_HOME environment variable is not set. I will try to locate the', "\n";
	print "java binaries and libraries in some other fashion. Please be advised that\n";
	print "you must set ", '$JAVA_HOME', " in order to run Chimera.\n\n";

	$java = find_exec( 'java' );
	unless ( $java ) {
	    print "FAILURE!\n";
	    print "There is no java binary in your \$PATH. Please adjust your PATH variable,\n";
	    print "set the JAVA_HOME variable, possibly set the CLASSPATH variable, and\n";
	    print "start again. I am expecting to see a binary called \"java\".\n";
	    return undef;
	} else {
	    print "OK: found $java binary\n";
	    
	    $java_home = dirname(dirname($java));
	    if ( -d "$java_home/jre" ) {
		print "OK: You should set JAVA_HOME=$java_home\n"; 
		$ENV{'JAVA_HOME'} = $java_home;
	    } else {
		print "WARNING!\n";
		print "Although I have found a Java binary, I am unable to determine the\n";
		print "installation directory of your Java. There is one last chance.\n";

		print "\nStarting java, initially, this may take a few seconds.\n";
		my @output = grep( /Loaded java.lang.ClassLoader from/,
				   `java -verbose -version 2>&1` );
		while ( length($output[0]) > 0 && my $ch != ']' ) { 
		    $ch=chomp($output[0]);
		}
		if ( $output[0] =~ m/ClassLoader from (.*)/ ) {
		    $java_home = substr($1,0,index($1,'/jre/'));
		    print "OK: Found JAVA_HOME=$java_home\n";
		    $ENV{'JAVA_HOME'} = $java_home;
		} else {
		    print "FAILURE!\n";
		    print "I tried all my tricks, but still cannot determine your JAVA_HOME.\n";
		    print "You really should set that variable. I cannot go on until you do.\n";
		    return undef;
		}
	    }
	}
    }
    print "OK: JAVA_HOME=$java_home\n";

    #
    # test the java version
    #
    print "\nStarting java; initially, this may take a few seconds.\n";
    $java = File::Spec->catfile( $java_home, 'bin', 'java' );
    my @output = grep( /java version/, `$java -version 2>&1` );
    if ( $? == 0 ) {
	print "OK: Ran $java successfully.\n";
    } else {
	my $code = $? >> 8;
	print "WARNING! $java returned exit code $code\n";
    }

    my $java_version;
    if ( $output[0] =~ /(\d+\.\d+\.\d+)/ ) {
	$java_version = $1;
	print "OK: Java is version $java_version\n";
    } else {
	print "FAILURE!\n";
	print "I am unable to determine the version of your Java. Please make sure that you\n";
	print "installed at least a jdk 1.4.* before continuing.\n";
	return undef;
    }

    my ($major,$minor,$patch) = split(/\./, $java_version, 3 );
    if ( $major == 1 ) {
	if ( $minor == 4 ) {
	    print "OK: Java has recommended version.\n";
	} elsif ( $minor > 4 ) {
	    print "WARNING: Your Java is newer than the recommended version.\n";
	} else { # $minor < 4
	    print "FAILURE!\n";
	    print "Your version of Java is too old. Please upgrade.\n";
	    return undef;
	}
    } else {
	print "FAILURE!\n";
	print "Your version of Java is unknown to me. I give up.\n";
	return undef;
    }

    #
    # CLASSPATH
    #
    $classpath = $ENV{CLASSPATH};
    if ( defined $classpath ) {
	print "OK: CLASSPATH is set\n";
    } else {
	print "WARNING! Your CLASSPATH is not set.\n";
    }

    my $vdshome = $ENV{"VDS_HOME"};
    if ( defined $vdshome ){
	print "OK: VDS_HOME is set\n";
    }
    else{
	print "FATAL: Your CLASSPATH is not set.\n";
	print "Please set your VDS_HOME class path before you continue.\n";
	exit 1;
    }

    my $cmd = "$java -cp $vdshome/test/test0:" . File::Spec->curdir() . 
	" ShowProperties";
    print "\nLet\'s try to actually run \"$cmd\"\n";
    @output = `$cmd`;
    if ( $? == 0 ) {
	print "OK: Ran \"$cmd\" successfully.\n";
    } else {
	my $code = $? >> 8;
	print "WARNING! \"$cmd\" returned exit code $code\n";
    }

    foreach ( @output ) {
	my ($k,$v) = split /=/,$_,2;
	$v =~ s/\r*\n$// unless $k eq 'line.separator';
	next unless length($v);
	$properties{$k} = $v;
    }

    # return the perl way
    '0 but true';
}

1;
