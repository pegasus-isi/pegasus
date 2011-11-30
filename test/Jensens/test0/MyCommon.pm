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
package MyCommon;
use 5.006;
use strict;
use POSIX qw(uname _exit);	# standard module
use File::Spec;			# standard module
use Socket;			# standard module
use Exporter;

# declare prototypes before exporting them
use vars qw($nonstop $namecache %month);

sub parse_date($);		# { }
sub find_exec($;$);		# { }
sub wait_enter(;$);		# { }
sub hostfqdn();			# { }
sub bind_and_connect(*$$);	# { }

our @ISA = qw(Exporter);
our @EXPORT = qw(bind_and_connect find_exec wait_enter hostfqdn parse_date
		 pipe_out_cmd);
our @EXPORT_OK = qw($nonstop $VERSION);
our $VERSION = '0.3';

%month = ( jan => 0, feb => 1, mar => 2, apr => 3, may =>  4, jun =>  5, 
	   jul => 6, aug => 7, sep => 8, oct => 9, nov => 10, dec => 11 );

sub bind_and_connect (*$$) {
    # purpose: optionally bind() followed by connect() on a socket.
    #          The bind will only be attempted in the presence of GTPR
    # paramtr: SOCK (IN): socket filehandle
    #          $host (IN): remote hostname
    #          $port (IN): remote port number
    # returns: 0: OK
    #          1: $host does not resolve
    #          2: unable to find free/legal port to bind() to
    #          3: trouble with connect()
    local (*FTP) = shift;
    my $host = shift;
    my $port = shift;
    # resolve peer
    my $site = inet_aton($host) || return 1;

    # handle non-local peer and G_T_P_R with local bind
    if ( exists $ENV{'GLOBUS_TCP_PORT_RANGE'} && 
         inet_ntoa($site) ne '127.0.0.1' ) {
        my ($lo,$hi) = split /\D/, $ENV{'GLOBUS_TCP_PORT_RANGE'}, 2;
        my ($i,$sin);
        for ( $i = $lo; $i < $hi; ++$i ) {
            $sin = sockaddr_in( $i, INADDR_ANY );
            #bind( FTP, $sin ) ? last : warn "# $!\n";
	    bind( FTP, $sin ) && last;
        }
        return 2 if ( $i >= $hi );
    }

    # connect
    connect( FTP, sockaddr_in( $port, $site ) ) || return 3;

    # autoflush
    my $save = select(FTP);
    $|=1;
    select($save);

    0;
}

our $terminate;

sub pipe_out_cmd {
    # purpose: exec cmd in w/o shell, and read its output
    # paramtr: @arg (IN): command and arguments, each separate
    # globals: $? (OUT): exit code of program invocation
    # returns: scalar: first line of output
    #          vector: full output
    my @result;
    local(*READ);               # protect FH

    # new temporary signal handler for timeouts
    alarm(0);
    local $SIG{ALRM} = sub {
        $terminate=1; 
        kill( 15 => $main::child ) if $main::child;
        close(READ); # SIGPIPE in child
    };

    # hmm
    local $SIG{CHLD} = 'DEFAULT';

    # pipe and fork
    my $pid = open( READ, '-|' );
    return (wantarray ? () : undef) unless defined $pid;

    # successful fork
    $terminate = undef;
    if ( $pid ) {
        # parent
        alarm(30);
        while ( <READ> ) {
            last if $terminate;
            chomp;
            push( @result, $_ );
        }
        close( READ );
        alarm(0);
        undef @result if $terminate;
    } else {
	no warnings;
        # child
        open( STDERR, ">&STDOUT" ); # dup2 STDERR onto STDOUT
        select( STDERR ); $|=1; # autoflush
        select( STDOUT ); $|=1; # autoflush
        exec { $_[0] } @_;
        POSIX::_exit(127);	# don't run atexit code
    }

    wantarray ? @result : $result[0];
}

sub find_exec ($;$) {
    # purpose: determine location of a binary
    # paramtr: $program (IN): basename of application
    #          $extra (IN): also look into the extra path-like string
    # returns: fully qualified path to binary, undef if not found
    my $program = shift;

    foreach my $dir ( File::Spec->path ) {
        my $fs = File::Spec->catfile( $dir, $program );
        return $fs if -x $fs;
    }

    my $extra = shift || '';
    if ( length($extra) ) {
	foreach my $dir ( split /:/, $extra ) {
	    my $fs = File::Spec->catfile( $dir, $program );
	    return $fs if -x $fs;
	}
    }

    undef;
}

sub wait_enter(;$) {
    # purpose: print a message and wait for enter, or just run w/o stoping
    # paramtr: $msg (opt. IN): other than default message
    # globals: $MyCommon::nonstop, if set, don't say and wait.
    # returns: the simulated pressed enter, or the actual user input
    my $msg = shift || "[hit enter]\n";
    print $msg;
    unless ( $MyCommon::nonstop ) {
	$_ = scalar <STDIN>;
	s/\r*\n$//;
	return $_;
    } else {
	# simulate pressed enter -> accept default
	print "\n";
	return "";		
    }
}

sub hostfqdn() {
    # purpose: simulate a Net::Domain::hostfqdn
    # paramtr: -
    # returns: the FQDN of this host, or undef 
    return $namecache if defined $namecache; # and done :)

    my $nodename = `hostname`;	# not everyone has GNU (-f option)
    $nodename =~ s/\r*\n//;	# more reliable than chomp()
    $nodename =(POSIX::uname())[1] if ( $? > 0 || length($nodename) == 0 );

    return undef if length($nodename) == 0; # we need a starting point
    return ($namecache=$nodename) if index($nodename,'.') != -1; # found

    # postcondition: we have at least a name, try DNS, expensive!
    my @hostent = gethostbyname($nodename);
    if ( defined $hostent[0] ) {
	return ($namecache=$hostent[0]);
    } else {
	return undef;
    }
}

sub parse_date ($) {
    # purpose: converts a standard date notation (keep your fingers crossed)
    # paramtr: $x (IN): output from /bin/date -u
    # returns: vector ready for gmtime, or empty vector for error
    if ( (shift) =~ /\w{3}\s+(\w{3})\s+(\d{1,2})\s(\d{1,2}):(\d{2}):(\d{2})\s+\S+\s+(\d{4})/ ) {
	return ($5,$4,$3,$2,$month{lc($1)},$6-1900);
    } else {
	return ();
    }
}

1;
__END__
