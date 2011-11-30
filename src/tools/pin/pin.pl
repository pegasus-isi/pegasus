#!/usr/bin/env perl
#
# internet file pin service
#
require 5.005;
use strict;
use Socket;
use POSIX qw(:sys_wait_h strftime);
use Time::HiRes qw();
use GDBM_File;

my $port = 7876;
my $dbfile = 'pin.db';

my $proto = getprotobyname('tcp') || 
    die "unable to determine protocol number for TCP: $!\n";
socket( SOCKFD, PF_INET, SOCK_STREAM, $proto ) ||
    die "socket( PF_INET, SOCK_STREAM ): $!\n";
bind( SOCKFD, pack_sockaddr_in($port,INADDR_ANY) ) ||
    die "bind($port): $!\n";
setsockopt( SOCKFD, SOL_SOCKET, SO_REUSEADDR, pack("i",1) ) ||
    die "setsockopt( SO_REUSEADDR ): $!\n";
listen( SOCKFD, 128 ) ||
    die "listen: $!\n";

$SIG{INT} = sub { $main::terminate=1 };
$SIG{TERM} = sub { $main::terminate=1 };
$SIG{CHLD} = \&REAPER;

sub logit
{
    printf STDERR "%.3f @_\n", Time::HiRes::time();
}

sub REAPER
{
    my $pid;
    while ( ($pid = waitpid(-1,WNOHANG)) > 0 ) {
	my $status = $?;
	my $rc = $status >> 8;
	logit( "$pid reaped with status $rc" );
    }
    $SIG{CHLD} = \&REAPER;
}

sub serve_pin (*$$)
{
    local(*FD) = shift;
    my $dbase = shift;
    my ($url,$stamp) = split( /\s+/,shift(),2 );
    my $now = time();
    logit( "[$$] in PIN" );

    if ( exists $dbase->{$url} ) {
	if ( $dbase->{$url} > $now ) {
	    # file is pinned, treat as extension
	    print FD "201 Pin adjusted.\r\n";
	} else {
	    # expired pin, treat as new
	    print FD "200 File pinned.\r\n";
	}
    } else {
	print FD "200 File pinned.\r\n";
    }
    
    $dbase->{$url} = $now + $stamp;
}

sub serve_unpin (*$$)
{
    local(*FD) = shift;
    my $dbase = shift;
    my $url = shift;
    logit( "[$$] in UNPIN" );

    if ( exists $dbase->{$url} ) {
	delete $dbase->{$url};
	print FD "200 Pin removed.\r\n";
    } else {
	print FD "401 No such URL.\r\n";
    }
}

sub serve_stat (*$$)
{
    local(*FD) = shift;
    my $dbase = shift;
    my $url = shift;
    logit( "[$$] in STAT" );

    if ( exists $dbase->{$url} ) {
	my $diff = $dbase->{$url} - time();
	print FD "200 $diff remaining.\r\n";
    } else {
	print FD "401 No such URL.\r\n";
    }
}

sub serve_quit (*$$)
{
    local(*FD) = shift;
    logit( "[$$] in QUIT" );
    print FD "200 Good-bye.\r\n";
}

my %callout =
    ( 'PIN'	=> \&serve_pin,
      'UNPIN'	=> \&serve_unpin,
      'STAT' 	=> \&serve_stat,
      'QUIT' 	=> \&serve_quit
    );

sub serve (*)
{
    local(*FD) = shift;

    # connect to pin database
    my %dbase; 
    if ( tie( %dbase, 'GDBM_File', $dbfile, GDBM_WRCREAT, 0644 ) ) {
	logit( "[$$] database opened" );
    } else {
	print FD "501 Internal error.\r\n";
	die "tie $dbfile: $!\n";
    }
	 
    while ( <FD> ) {
	s/[\r\n]+$//;		# safer than chomp
	my ($cmd,$remainder) = split(/\s+/,$_,2);

	if ( exists $callout{$cmd} ) {
	    no strict;
	    $callout{$cmd}->( FD, \%dbase, $remainder );
	} else {
	    logit( "[$$] illegal command: $cmd" );
	    print FD "400 Illegal instruction.\r\n";
	}
	last if $cmd eq 'QUIT';
    }
    logit( "[$$] done" );
    untie( %dbase );
}

while ( ! $main::terminate ) {
    my $paddr = accept( CLIENT, SOCKFD );
    next unless defined $paddr;
    my ($port,$host) = sockaddr_in($paddr);
    logit( "connection from @{[inet_ntoa($host)]}:$port" );

    my $pid = fork();
    if ( $pid == -1 ) {
	# failure
	die "unable to fork: $!\n";
    } elsif ( $pid > 0 ) {
	# parent
	close( CLIENT );
	logit( "forking off $pid" );
    } else {
	# child
	close( SOCKFD );
	select( CLIENT ); $|=1;
	serve( CLIENT );
	close( CLIENT );
	exit(0);
    }
}

exit 0;
