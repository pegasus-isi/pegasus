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
package MyGridFTP;
use 5.006;
use strict;
use File::Spec;			# standard module
use File::Basename;		# standard module
use File::Temp qw(tempfile);	# 5.6.1 standard module
use Exporter;			# standard module

use lib dirname($0);
use MyCommon;
use MyGlobus qw($subject $userglobusdir $usercertfile $userkeyfile);

# declare prototypes before exporting them
sub check_gridftp(\@);		# { }
sub junk_file(;$);		# { }

our @ISA = qw(Exporter);
our @EXPORT = qw(check_gridftp);
our @EXPORT_OK = qw(junk_file);
our $VERSION = '0.1';

BEGIN {
    # only load Time::HiRes, if it exists, otherwise ignore
    eval "use Time::HiRes qw(time);";
}

sub md5sum ($) {
    # purpose: determine MD5 of file
    # paramtr: $fn (IN): filename
    # returns: MD5 hex digest, or undef in case of error
    my $fn = shift;
    
    # Why dont I use Digest::MD5? It's not part of standard perl!
    my $md5 = `md5sum -b $fn | cut -c -32 2>>/dev/null`;
    $md5 =~ s/\r*\n$//;

    # return in Perl fashion
    $md5;
}

sub junk_file (;$) {
    # purpose: create a junk file
    # paramtr: size (opt. IN): size of junk file, must be divisible by 4
    # returns: name of created junk file in scalar context, or
    #          [0] file name, [1], file size, [2] file MD5 in array context
    # onerror: returns undef in scalar context, or emptyvec in array context
    # warning: uses system md5sum executable, thus [2] may return undef
    my $size = shift || (1 << 22);
    my ($fh,$fn) = tempfile( 'fileXXXXXX', DIR => File::Spec->tmpdir,
			     SUFFIX => '.junk', UNLINK => 1 );
    return (wantarray ? () : undef) unless defined $fh;

    my $i = 0;
    while ( $i < $size ) {
	my $data = pack( "I2", rand(65536), rand(65536) ^ $$ );
	syswrite( $fh, $data, 4 );
	$i += 4;
    }
    close($fh);
    my $md5 = md5sum($fn);
    return ( wantarray ? ( $fn, $i, ( $?==0 ? $md5 : undef ) ) : $fn );
}

sub transfer_file ($$) {
    # purpose: transfer the file
    # paramtr: $src (IN): source of object
    # paramtr: $dst (IN): destination object
    # returns: [0..2] error codes of attempt, or undef
    #          [3] transfer time in seconds
    my $src = shift;
    my $dst = shift;
    my $guc = File::Spec->catfile( $ENV{'GLOBUS_LOCATION'}, 'bin', 'globus-url-copy' );

    my @rc;
    print "# $src -> $dst\n";
    my $start = time;
    if ( system( $guc, '-tcp-bs', 262144, '-p', 16, $src, $dst ) ) {
	# retry 1
	$rc[0] = $? >> 8;
	print "Warning! error code $rc[0], retrying...\n";
	$start = time;
	if ( system( $guc, '-tcp-bs', 64000, $src, $dst ) ) {
	    # retry 2
	    $rc[1] = $? >> 8;
	    print "Warning! error code $rc[1], retrying...\n";
	    $start = time;
	    if ( system( $guc, $src, $dst ) ) {
		$rc[2] = $? >> 8;
		print "FAILURE! error code $rc[2]\n";
		print "I am unable to transfer from $src\n";
		print "to $dst.\n";
		return ();
	    } else {
		$rc[3] = time - $start;
		print "OK: transferred in 3rd attempt.\n";
	    }
	} else {
	    $rc[3] = time - $start;
	    print "OK: transferrred in 2nd attempt.\n";
	}
    } else {
	$rc[3] = time - $start;
	print "OK: transferred in 1st attempt.\n";
    }

    @rc;
}
 
sub rate ($$) {
    # purpose: calculate data transfer rate and prints message
    # paramtr: $byte (IN): data size in byte
    #          $time (IN): transfer time in seconds
    # returns: always true
    my $size = shift;
    my $time = shift;
    my $rate = ($size * 8.0) / ($time || 0.5);
    my $suffix = '';
    if ( $rate > 1024.0 ) {
	$rate /= 1024.0; 
	$suffix = 'k';
	if ( $rate > 1024.0 ) {
	    $rate /= 1024.0;
	    $suffix = 'M';
	}
    }
    printf "OK: transferred $size Byte in $time s = %.3f %sbps\n", $rate, $suffix;
    '0 but true';
}

sub check_gridftp (\@) {
    # purpose: do a gridftp check-up
    # paramtr: $server (IN): vector of gridftp servers
    # warning: $server is a vector ref, which will be modified. 
    # returns: true for ok, undef for error
    my $server = shift || return undef;	# must have servers list
    print ">> ", join("\n>> ",@$server),"\n" if $main::DEBUG;

    # local 2 local
    print "\nTrying to create a junk file\n";
    my @src = junk_file();
    if ( defined $src[0] ) {
	print "OK: created junk file $src[0]\n";
    } else {
	print "FAILURE!\n";
	print "Cannot create a 1MB junk file in your temporary directory. Without it\n";
	print "it is hard to check gridftp.\n";
	return undef;
    }

    # create a destination file
    my @dst = reverse tempfile( 'fileXXXXXX', DIR => File::Spec->tmpdir,
				SUFFIX => '.junk', UNLINK => 1 );
    my @imf = reverse tempfile( 'fileXXXXXX', DIR => File::Spec->tmpdir,
				SUFFIX => '.junk', UNLINK => 1 );
    close($dst[1]);
    close($imf[1]);

    # local copy
    my @rc = transfer_file( "file://$src[0]", "file://$dst[0]" );
    return undef if @rc == 0;

    # copy to remote
    my @newlist;
    print "\nTesting your upload capabilities\n";
    foreach my $gfs ( @$server ) {
	@rc = transfer_file( "file://$src[0]", "gsiftp://$gfs$dst[0]" );
	if ( @rc == 0 ) {
	    print "FAILURE!\n";
	    print "I will remove contact $gfs\n";
	} else {
	    my $rate = rate( $src[1], $rc[3] );
	    push @newlist, $gfs;
	}
    }
    $server = [ @newlist ];
    undef @newlist;

    # copy from remote
    print "\nTesting your download capabilities\n";
    foreach my $gfs ( @$server ) {
	@rc = transfer_file( "gsiftp://$gfs$dst[0]", "file://$imf[0]", );
	if ( @rc == 0 ) {
	    print "FAILURE!\n";
	    print "I will remove contact $gfs\n";
	} else {
	    my $rate = rate( $src[1], $rc[3] );
	    if ( md5sum($imf[0]) != $src[2] ) {
		print "FAILURE!\n";
		print "Sorry, but a data corruption during transfer appeared to have occurred.\n";
		print "I strongly recommend looking into the cause for this hideous bug.\n";
		return undef;
	    } else {
		print "OK: transferred data matches source\n";
		push @newlist, $gfs;
	    }
	}
    }
    $server = [ @newlist ];
    undef @newlist;

    # 3rd party with source at remote gatekeeper (forces -G bug)
    #print "\nTesting a download from the remote gatekeeper\n";
    print "\nTBD(self): I haven\'t found the time to code 3rd party transfers.\n";

    print "\n";
    if ( @$server == 0 ) {
	print "FAILURE!\n";
	print "Sorry, there are no gridftp servers that I can run jobs on successfully. You\n";
	print "might want to read up on http://www.globus.org/, try a few things, and re-run\n";
	print "me after you successfully transferred a few files using globus-url-copy.\n";
	return undef;
    } else {
	print "OK: got ", @$server+0, " gridftp capable contact(s).\n";
	wait_enter;
    }

    # return the perl way
    '0 but true';
}

1;
