#!/usr/bin/env perl
#
# parses the kickstart output WITHOUT putting it into a database.
#
# Usage: exitcode.pl [exitcode options] dot-out-file 
#
# This file or a portion of this file is licensed under the terms of the
# Globus Toolkit Public License, found in file GTPL, or at
# http://www.globus.org/toolkit/download/license.html. This notice must
# appear in redistributions of this file, with or without modification.
#
# Redistributions of this Software, with or without modification, must
# reproduce the GTPL in: (1) the Software, or (2) the Documentation or
# some other similar material which is provided with the Software (if
# any).
#
# Copyright 1999-2008 University of Chicago and The University of
# Southern California. All rights reserved.
#
# Author: Jens-S. Vöckler jens at isi dot edu
# Revision : $Revision: 50 $
#
use 5.006;
use strict;
use vars qw($VERSION);
use subs qw(log);

use File::Spec;
use File::Basename qw(basename);
use XML::Parser;
use Getopt::Long qw(:config no_ignore_case_always bundling);
use File::Temp qw(tempfile);

# version information from CVS -- don't edit
$VERSION = '1.0';
$VERSION = $1 if ( '$Revision: 1.0 $' =~ /Revision:\s+([0-9.]+)/o );



sub usage {
    my $app = basename($0);
    print << "EOF";
Usage: $app [options] fn [fn [..]]

Optional arguments:
 -d|--dbase dbx  Noop: deprecated option.
 -V|--version    print version information and exit.
 -v|--verbose    verbose mode, report what you are doing. 
 -i|--ignore     Unsupported: No ignore mode support, sorry.
 -n|--noadd      Noop: $app implies --noadd always!
 -N|--nofail     Noop: $app implies --noadd, superceding --nofail. 
 -e|--emptyfail  takes empty files to mean failure instead of ok.
 -f|--fail       stops parsing on the first error found instead of going on.
 -l|--label tag  Noop: $app implies no PTC
 -m|--mtime ISO  Noop: $app implies no PTC

Mandatory arguments:
 fn              A kickstart output file, possibly with PBS wrapping.
                 The file may be a multi-document file from seqexec.

The following exit codes are returned (except in -i mode):
  0  remote application ran to conclusion with exit code zero.
  1  remote application concluded with a non-zero exit code.
  2  kickstart failed to start the remote application.
  3  remote application died on a signal, check database.
  4  remote application was suspended, should not happen.
  5  invocation record has an invalid state, unable to parse.
  6  illegal state, inform pegasus-support at isi dot edu
  7  illegal state, stumbled over an exception, try --verbose for details
  8  multiple 0..5 failures during parsing of multiple records

EOF
    exit 1;
}

sub contents($) {
    # purpose: read contents from file
    # paramtr: $fn (IN): filename
    # returns: file contents as single string
    #
    my $fn = shift;
    my $result;
    local(*IN);
    open( IN, "<$fn" ) || die "FATAL: open $fn: $!\n";
    binmode( IN, ':raw' );	# ~@#!
    local $/ = undef;
    $result = <IN>;
    close IN;
    $result;
}

sub log {
    my @now = reverse ((localtime())[0..5]);
    my $msg = sprintf( '# %04u-%02u-%02u %02u:%02u:%02u: ',
                       $now[0]+1900, $now[1]+1, @now[2..5] );
    $msg .= join('',@_) . "\n";
    syswrite( STDERR, $msg );
}

sub ignore {
    warn( 'Info: Option ', $_[0], ' is ignored, because ', basename($0), 
	  " implies --noadd\n" );
}

#
# --- main ------------------------------------------------------
#
my $tmp = $ENV{'MY_TMP'} ||     # Wei likes MY_TMP, so try that first
    $ENV{TMP} ||                # standard
    $ENV{TEMP} ||               # windows standard
    $ENV{TMPDIR} ||             # also somewhat used
    File::Spec->tmpdir() ||     # OK, this gets used if all above fail
    '/tmp';                     # last resort

my @SAVE = ( @ARGV );
$main::debug = 0;
my ($emptyfail,$firstfail,$keep) = (0,0,0);
GetOptions( 'help|h' => \&usage
	  , 'dbase|d=s' => \&ignore
	  , 'version|V' => sub { 
	      warn "Version $VERSION\n";
	      exit 1;
	  }
	  , 'verbose|v+' => \$main::debug
	  , 'ignore|i' => \&ignore
	  , 'noadd|n' => sub { }
	  , 'nofail|N' => \&ignore
	  , 'emptyfail|e' => \$emptyfail
	  , 'fail' => \$firstfail
	  , 'keep' => \$keep
	  , 'label=s' => \&ignore
	  , 'mtime|m=s' => \&ignore
          ) ||
    die( "FATAL: Option processing failed due to an illegal option\n",
         "$0 @SAVE\n" );

# sanity checks
die "ERROR: Missing necessary file argument" unless @ARGV;

# instantiate and setup XML parser
my $xml = new XML::Parser::Expat( ProtocolEncoding => 'UTF-8',
				  Namespaces => 1 ) ||
    die "ERROR: Unable to instantiate an XML parser";

my %jobtype = ( mainjob => 1, prejob => 1, postjob => 1 );
my (@stack,@status,@final);
my $watchit = 0;
$xml->setHandlers( Start => sub {
    my $self = shift;
    my $e = lc(shift);
    my %attr = @_;
    push( @stack, $e );
    if ( exists $jobtype{$e} ) {
	$watchit = $e;
    } elsif ( $e eq 'status' && $watchit ) {
	push( @status, $watchit, $attr{raw} );
    } elsif ( $e eq 'invocation' ) {
	@status = ();
	my $job = $attr{transformation} || '???';
	log( "job $job ran for $attr{duration} s" ) if $main::debug > 1;
    }
    1;
}, End => sub {
    my $self = shift;
    my $e = lc(shift);
    if ( exists $jobtype{$e} ) {
	$watchit = 0;
    } elsif ( $e eq 'invocation' ) {
	my $final = -1;
	while ( @status ) {
	    my ($job,$raw) = splice(@status,0,2);
	    if ( $raw == 0 ) {
		$final = 0;
	    } elsif ( $raw == -1 ) {
		$final = 2;
		last;
	    } elsif ( ($raw >> 8) ) {
		$final = 1;
		last;
	    } elsif ( ($raw & 127) ) {
		$final = 3;
		last;
	    } else {
		$final = 6;
		last; 
	    }
	}
	push( @final, $final );
    }

    die "INVALID: Expecting </$stack[$#stack]>, seen </$e>\n"
        unless $stack[$#stack] eq $e;
    pop( @stack );
    1;
}, Char => sub {
    # quick noop
    1;
} );


my ($tmpfh,$tmpfn) = tempfile( 'ec-XXXXXX', DIR => $tmp, 
			       SUFFIX => '.tmp', UNLINK => ! $keep );
die "ERROR: Unable to create temporary file in $tmp: $!\n" 
    unless defined $tmpfh;
foreach my $fn ( @ARGV ) {
    unless ( -e $fn ) {
	log( "file $fn does not exist, fail with 5" ) if $main::debug > 1;
	warn "file $fn does not exist\n";
	exit 5;
    }

    unless ( -r $fn ) {
	log( "file $fn is not readable, fail with 5" ) if $main::debug > 1;
	warn "file $fn is not readable\n";
	exit 5;
    }

    my $size = -s _;
    if ( $size == 0 ) {
	if ( $emptyfail ) {
	    log( 'zero size file, fail with 5' ) if $main::debug > 1;
	    warn "file $fn has zero length, assuming failure\n";
	    exit 5;
	} else {
	    log( 'zero size file, succeed with 0' ) if $main::debug > 1;
	    warn "file $fn has zero length, assuming success\n";
	    # FIXME !!! HERE !!!
	    next ;
	}
    } else {
	log( "file has size ", $size ) if $main::debug > 1;
    }

    # OK, read file into chunks
    log( "about to extract content of $fn" ) if $main::debug > 1;

    # commit some trickery so we don't have to split the file, nor
    # read all <data> into memory... Involves copying to tmp though
    seek( $tmpfh, 0, 0 ) || die "ERROR: seek $tmpfn: $!\n";
    print $tmpfh "<grmblftz>\n";
    open( IN, "<$fn" ) || die "ERROR: open $fn: $!\n";
    while ( <IN> ) {
	# RAM-friendly way to copy (albeit slower)
	next if /^<\?xml\s/;
	print $tmpfh $_;
    }
    close IN;
    print $tmpfh "</grmblftz>\n";
    log( 'copied contents to ', $tmpfn ) if $main::debug > 1;

    # parse XML
    my $result = undef;
    log( 'about to parse invocation records' ) if $main::debug > 1;
    seek( $tmpfh, 0, 0 ) || die "ERROR: seek $tmpfn: $!\n";
    eval { $result = $xml->parse($tmpfh) };
    $result = -1 if $@;
    log( 'done parsing ', scalar(@final), ' invocation records' ) if $main::debug > 1;

    if ( $result == 1 ) {
	log( 'about to determine exit status' ) if $main::debug > 1; 
	for ( my $i=0; $i < @final; ++$i ) {
	    log( "exit status [$i] is ", $final[$i] ) 
		if $main::debug;
	    exit( $final[$i] ) if $final[$i];
	}
    } elsif ( $result == -1 ) {
	log( 'failure to parse invocation records' );
	warn( "ERROR: XML::Parser choked: $@\n" );
	exit 5; 
    }
}
exit 0;
