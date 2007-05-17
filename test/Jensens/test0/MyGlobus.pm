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
package MyGlobus;
use 5.006;
use strict;
use Socket;			# standard module
use POSIX qw(mktime);		# standard module
use File::Spec;			# standard module
use File::Basename;		# standard module
use Exporter;			# standard module

use lib dirname($0);
use MyCommon;

# declare prototypes before exporting them
use vars qw($subject $userglobusdir $usercertfile $userkeyfile);
sub check_globus(\@\@;$);	# { }

our @ISA = qw(Exporter);
our @EXPORT = qw(check_globus);
our @EXPORT_OK = qw($subject $userglobusdir $usercertfile $userkeyfile);
our $VERSION = '0.2';

sub find_version($) {
    # purpose: run a program with -version to obtain the version
    # paramtr: $prg (IN): is the name of the program to run.
    # returns: the version identifier, or undef
    my $prg = shift;
    local $_ = `$prg -version`;
    my $version = undef;
    $version = $1 if /(\d+\.\d+(\.\d+)?)/;
    $version;
}

sub check_globus(\@\@;$) {
    # purpose: do the Globus check-up
    # paramtr: $contacts (IN): vector of jobmgr contact strings. 
    #          $rlsvec (IN): vector of RLS servers.
    #          $gotorls (IN): flag, if skipping GT2 jobmanager checks.
    # warning: $contacts is a vector ref, which will be modified. 
    # returns: true for ok, undef for error
    my $jmlist = shift || return undef; # must have contacts list
    print ">> ", join("\n>> ",@$jmlist),"\n" if $main::DEBUG;

    my $rlsvec = shift || return undef;
    print ">> ", join("\n>> ",@$rlsvec),"\n" if $main::DEBUG;

    my $gotorls = shift || 0;

    # 
    # test for g2l
    #
    unless ( exists $ENV{GLOBUS_LOCATION} ) {
	print "FAILURE!\n";
	print "Your \$GLOBUS_LOCATION variable is not set. You might want to consider\n";
	print "finding your Globus-2 installation location. Let us call this place XX.\n";
	print "Then run either run (with a Bourne shell)\n\n";
	print ". XX/etc/globus-user-env.sh\n\n";
	print "or with a C-shell\n\n";
	print "source XX/etc/globus-user-env.csh\n\n";
	print "You might also want to put this into your \$HOME/.profile.\n";
	return undef;
    } else {
	print 'OK: $GLOBUS_LOCATION=', $ENV{GLOBUS_LOCATION}, "\n";	
    }

    # 
    # test for g2 in PATH
    #
    my @path = grep { /$ENV{GLOBUS_LOCATION}/ } File::Spec->path;
    if ( @path == 0 ) {
	print "FAILURE!\n";
	print "Your \$GLOBUS_LOCATION is not part of your \$PATH. This will make running\n";
	print "things rather difficult. Please fix.\n";
	return undef;
    } else {
	print "OK: \$PATH contains $path[0]\n";
    }

    #
    # test G[TU][SP]R
    #
    foreach my $env ( qw(GLOBUS_TCP_PORT_RANGE GLOBUS_TCP_SOURCE_RANGE 
			 GLOBUS_UDP_PORT_RANGE GLOBUS_UDP_SOURCE_RANGE) ) {
	if ( exists $ENV{$env} ) {
	    print 'OK: $', $env, '=', $ENV{$env}, "\n";
	} else {
	    print 'OK: no $', $env, ' found', "\n";
	}
    }

    #
    # check for some g2 binaries
    #
    foreach my $exe ( qw(globusrun globus-job-run grid-proxy-info 
			 grid-proxy-init globus-url-copy grid-cert-info) ) {
	my $prg = find_exec($exe);
	unless ( defined $prg ) {
	    print "FAILURE!\n";
	    print "Unable to find the location of $exe. Please make extra sure that\n";
	    print "your Globus 2 installation is complete, e.g. comes from the VDT.\n";
	    return undef;
	} else {
	    print "OK: Found $prg\n";
	}
    }
    print "\nYour Globus essential binaries appear complete,\n";
    print "now let\'s check your optional binaries.\n";
    wait_enter;

    #
    # non-vital binaries, check for some g2 binaries
    #
    my %nonvital = ( 'globus-rls-cli' => [ '2.0.9', undef ],
		     'globus-rls-admin' => [ '2.0.9', undef ] );
    foreach my $exe ( keys %nonvital ) {
	my $prg = find_exec($exe);
	unless ( defined $prg ) {
	    print "WARNING!\n";
	    print "Unable to find the location of $exe\n";
	} else {
	    print "OK: Found $prg\n";
	    $nonvital{$exe}->[1] = $prg;

	    my $version = find_version($prg);
	    if ( $version ge $nonvital{$exe}->[0] ) {
		print "OK: $version is sufficient for $exe\n";
	    } else {
		print "WARNING for $prg\n";
		print( "Found insufficient version $version. You need at least ",
		       $nonvital{$exe}->[0], "\n" );
	    }
	}
    }
    print "\nYour Globus installation appears complete,\n";
    print "now let\'s check your user certificate.\n";
    wait_enter;

    # skip ahead now.
    goto RLS if $gotorls;

    #
    # prepare for check/run grid-proxy-init
    #
    $userglobusdir = File::Spec->catfile( $ENV{HOME}, '.globus' );
    while ( ! -d $userglobusdir ) {
	print "Sorry, but $userglobusdir is not a directory I can find!\n";
	print "What is the directory where you keep your user certificate?\n";
	my $line = wait_enter( "[$userglobusdir] " );
	$userglobusdir = $line if ( length($line) );
    }
    print "OK: Found $userglobusdir\n";

    # find user cert file
    $usercertfile = File::Spec->catfile( $userglobusdir, 'usercert.pem' );
    while ( ! -r $usercertfile ) {
	print "Sorry, but $usercertfile is not a certifcate I can find!\n";
	print "What is the user certificate file name (usually usercert.pem)?\n";
	my $line = wait_enter( "[$usercertfile] " );
	$usercertfile = $line if ( length($line) );
    }
    print "OK: Found $usercertfile\n";

    # find user key file
    $userkeyfile = File::Spec->catfile( dirname($usercertfile), 'userkey.pem' );
    while ( ! -r $userkeyfile ) {
	print "Sorry, but $userkeyfile is not a key file I can find!\n";
	print "What is the user key file name (usually userkey.pem)?\n";
	my $line = wait_enter( "[$userkeyfile] " );
	$userkeyfile = $line if ( length($line) );
    }
    print "OK: Found $userkeyfile\n";

    #
    # obtain subject line from user cert
    #
    $subject = `grid-cert-info -f $usercertfile -subject 2>> /dev/null`;
    if ( $? != 0 ) {
	my $rc = $? >> 8;

	# Globus 2.2.*, hurray! Let's retry
	$subject = `grid-proxy-info -subject 2>> /dev/null`;
	goto jumpout if ( $? == 0 );

	print "FAILURE!\n";
	print "The grid-proxy-info -subject command returned an exit code of $rc\n";
	print "This means that there is something wrong with your user proxy certificate.\n";
	print "Please check that you can run grid-proxy-info -f $usercertfile -all\n";
	print "on the certificate that you specified.\n";
	return undef;
    }
  jumpout:
    $subject =~ s/[\r\n]*$//;	# safer/better/less efficient than chomp()
    
    if ( ($subject =~ tr{/}{/}) < 3 ) {
	print "FAILURE!\n";
	print "I am unable to determine the subject of your certificate!\n";
	return undef;
    } else {
	print "OK: $subject\n";
    }

    print "\nYour user certificate installation appears complete,\n";
    print "now let\'s check your user proxy certificate.\n\n";

    #
    # check currently active user proxy certificate
    #
  REDO1:
    # DONT: my @proxyinfo = `grid-proxy-info -f $usercertfile -all`;
    my @proxyinfo = `grid-proxy-info -all`;
    if ( $? != 0 || @proxyinfo < 5 ) {
	my $code = $? >> 8;
	if ( $code == 4 ) {
	    # proxy is not initialized
	    print "\nHey, looks as if your proxy cert is not active!\n";
	    print "Let\'s run grid-proxy-init. You will need to type your password.\n\n";

	    my $cmd = "grid-proxy-init -cert $usercertfile -key $userkeyfile";
	    print "$cmd\n";
	    unless ( system($cmd) == 0 ) {
		print "\nFAILURE!\n";
		print "grid-proxy-init returned $?\n";
		return undef;
	    }
	    goto REDO1;
	} else {
	    print "FAILURE!\n";
	    print "There is something unexplicable going on while trying to check your\n";
	    print "proxy certificate. You might want to do some manual debugging using\n";
	    print "the Globus grid-proxy-(info|init|destroy) tools.\n";
	    return undef;
	}
    } else {
	print "OK: grid-proxy-info returned ", @proxyinfo+0, " lines\n";
	# print @proxyinfo;
	chomp(@proxyinfo);
	print '# ', join( "\n# ", @proxyinfo ), "\n"
	    if ( $main::DEBUG );
    }

    #
    # match issuer of proxy cert to cert subject
    #
    my $proxysubject = (grep {/^subject/i} @proxyinfo)[0];
    unless ( $proxysubject ne $subject ) {
	print "WARNING!\n";
	print "There is something fishy going on with your certificate subject. The\n";
	print "proxy certificate issuer does not match your certificate subject. I will\n";
	print "ignore this for now, but be warned!\n";
	wait_enter;
    } else {
	print "OK: The proxy issuer matches the certificate file subject\n";
    }

    #
    # check validity interval
    #
    my $proxytime = (grep {/^time/i} @proxyinfo)[0];
    if ( $proxytime =~ /(\d+):(\d{2}):(\d{2})/ ) {
	my $left = $1*3600 + $2*60 + $3;
	if ( $left < 3600 ) {
	    print "WARNING!\n";
	    print "Your proxy certificate has $left s TTL left. Please (re-)run\n";
	    print "grid-proxy-init in another terminal before you continue!\n\n";
	    wait_enter("[hit enter when ready]\n");
	    goto REDO1;
	} else {
	    print "OK: You got $left s left on your proxy certificate\n";
	    wait_enter;
	}
    } else {
	print "FAILURE!\n";
	print "I am unable to determine the remaining time on your proxy certificate.\n";
	return undef;
    }

    #
    # PING jm contacts
    #
    print "Your Globus proxy certificate for you is active and valid. Now let\'s submit\n";
    print "a few test jobs to Globus, to check for various configuration glitches that\n";
    print "might occur. Please enumerate gatekeepers that you want to contact. You may\n";
    print "use a service contact, if the default jobmanager is not the fork jobmanager.\n\n";
    print "Use commas to separate multiple gatekeeper contacts:\n";
    my $line = wait_enter( '[' . join(',',@$jmlist) . '] ' );
    print "\n";
    $line = join(',',@$jmlist) unless length($line);

    my ($host,$port,@newlist);
    foreach my $gk ( split /,/, $line ) {
	# first test DNS
	($host) = split /\//, $gk, 2; # remove jm portion
	($host) = split /:/, $host, 2 if ( index($host,':') > -1 );

	my @dns;
	if ( $host =~ /(\d{1,3}\.){3}\d{1,3}/ ) {
	    @dns = gethostbyaddr( inet_aton($host), AF_INET );
	} else {
	    @dns = gethostbyname( $host );
	}

	if ( @dns ) {
	    print "OK: $dns[0] is ", inet_ntoa($dns[4]) ,"\n";
	    push( @newlist, $gk );
	} else {
	    print "FAILURE!\n";
	    print "I am unable to look up $host in DNS. This will not work\n";
	    print "with GSI. Maybe you mistyped the hostname portion?\n";
	}
    }

    # copy to new list
    $jmlist = [ @newlist ];
    undef @newlist;

    if ( @$jmlist == 0 ) {
	print "\nFAILURE!\n";
	print "Sorry, there are no gatekeepers that I can contact successfully. You might\n";
	print "want to read up on http://www.globus.org/, try a few things, and re-run me\n";
	print "after you successfully submitted a few jobs.\n";
	return undef;
    } else {
	print "OK: got ", @$jmlist+0, " contact(s) that resolved\n";
	wait_enter;
    }

    # try to connect to the host and port of the gatekeeper
    local(*SOCKFD);
    foreach my $gk ( @$jmlist ) {
	($host) = split /\//, $gk, 2; # remove jm portion
	$port = 2119;		# default port
	($host,$port) = split /:/, $host, 2 if ( index($host,':') > -1 );
	
	my $ok;
	socket( SOCKFD, PF_INET, SOCK_STREAM, getprotobyname('tcp') ) ||
	    die "socket error $!";

	local $SIG{ALRM} = sub { close(SOCKFD) };
	alarm(30);
	my $result = bind_and_connect( *SOCKFD, $host, $port );
	alarm(0);
	if ( $result == 0 ) {
	    # check for shlib problems
	    my $rin = '';
	    vec( $rin, fileno(SOCKFD), 1 ) = 1;
	    if ( select($rin,undef,undef,0) > 0 &&
		 defined ( $_ = <SOCKFD> ) && 
		 /^\w+: error.+libglobus/ ) { 
		print "FAILURE!\n";
		print "The remote gatekeeper forgot to correctly set up the share library\n";
		print "environment (LD_LIBRARY_PATH, /etc/ld.so.*) for the gatekeeper. Please\n";
		print "notify the remote administrator.\n";
	    } else {
		print "OK: connected to $host:$port\n";
		push( @newlist, $gk );
	    }

	    close(SOCKFD);
	} else {
	    print "FAILURE: $result\n";
	    if ( $result == 1 ) {
		print "The hostname $host does not resolve. This may be a transient hick-up in DNS\n";
		print "but most likely a typo.\n";
	    } elsif ( $result == 2 ) {
		print "I am unable to find a free port to bind locally to. However, your environment\n";
		print "does specify a GLOBUS_TCP_PORT_RANGE. Thus, I don't think I am able to connect\n";
		print "to the outside world.\n";
	    } elsif ( $result == 3 ) {
		print "I am unable to connect() to $host:$port. Do you have any\n";
		print "firewalls which drop PDUs in between?\n";
	    } else {
		print "unknown error - should not happen.\n";
	    }
	}
    }

    # copy to new list
    $jmlist = [ @newlist ];
    undef @newlist;

    if ( @$jmlist == 0 ) {
	print "\nFAILURE!\n";
	print "Sorry, there are no gatekeepers that I can contact successfully. You might\n";
	print "want to read up on http://www.globus.org/, try a few things, and re-run me\n";
	print "after you successfully submitted a few jobs.\n";
	return undef;
    } else {
	print "OK: got ", @$jmlist+0, " contact(s) that resolved\n";
	wait_enter;
    }

    # now test GSI authentication
    foreach my $gk ( @$jmlist ) {
	my $cmd = "globusrun -a -r \'$gk\'";
	print "trying to contact $gk\n";
	if ( system("$cmd >> /dev/null 2>&1") == 0 ) {
	    print "OK: $cmd\n";
	    push( @newlist, $gk );
	} else {
	    my $code = $? >> 8;
	    print "FAILURE!\n";
	    print "$cmd returned $code\n";
	    print "I guess, you cannot reach the contact $gk.\n";
	    print "I will remove this contact for now.\n";
	}
    }

    # copy to new list
    $jmlist = [ @newlist ];
    undef @newlist;

    if ( @$jmlist == 0 ) {
	print "\nFAILURE!\n";
	print "Sorry, there are no gatekeepers that I can contact successfully. You might\n";
	print "want to read up on http://www.globus.org/, try a few things, and re-run me\n";
	print "after you successfully submitted a few jobs.\n";
	return undef;
    } else {
	print "OK: got ", @$jmlist+0, " contact(s) that responded\n";
	wait_enter;
    }

    #
    # check to run a simple synchroneous job
    #
    foreach my $gk ( @$jmlist ) {
	# FIXME: Assume GNU date
	my $cmd = "globus-job-run \'$gk\' -l /bin/date -u";
	print "trying to run date on $gk...\n";
	my ($date,@date,@mine,$diff);
	if ( open( DATE, "$cmd|" ) && defined ($date=<DATE>) ) {
	    close(DATE);
	    @mine = gmtime(time);

	    # command ran
	    print "# $date";
	    print "OK: date ran successfully\n";

	    # check differences
	    @date = parse_date($date);
	    if ( @date == 0 ) {
		print "FAILURE!\n";
		print "The remote /bin/date -u command returned a string that I cannot parse.\n";
		print "I will remove this host from the list of acceptable contacts.\n";
	    } else {
		push( @newlist, $gk );
		if ( abs($diff = mktime(@mine)-mktime(@date)) < 120 ) {
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
	    my $code = $? >> 8;
	    print "FAILURE!\n";
	    print "$cmd returned $code\n";
	    print "I guess, you cannot run simple job on the contact $gk.\n";
	    print "I will ignore this contact for now.\n";
	}
    }

    # copy to new list
    $jmlist = [ @newlist ];
    undef @newlist;

    if ( @$jmlist == 0 ) {
	print "FAILURE!\n";
	print "Sorry, there are no gatekeepers that I can run jobs on successfully. You might\n";
	print "want to read up on http://www.globus.org/, try a few things, and re-run me\n";
	print "after you successfully submitted a few jobs.\n";
	return undef;
    } else {
	print "OK: got ", @$jmlist+0, " contact(s) that ran a job\n";
	wait_enter;
    }

    #
    # check to run a asynchroneous job - check for the firewall problem!
    #
    my $sleeptime = 120;
    foreach my $gk ( @$jmlist ) {
	# FIXME: Assume GNU date
	my $rsl = "&(executable=/bin/sleep)(arguments=$sleeptime)";
	my $cmd = "globusrun -b -r \'$gk\' \'$rsl\'";
	print "trying to batch /bin/sleep on $gk\n";

	my $jobstart = time();	# just in case
	my @result;
	if ( open( BATCH, "$cmd|" ) ) {
	    chomp(@result = <BATCH>);
	    close(BATCH);
	    print "# ", join("\n# ",@result), "\n";
	} else {
	    my $code = $? >> 8;
	    print "FAILURE!\n";
	    print "$cmd returned $code\n";
	    print "I guess, you cannot run simple batch jobs on the contact $gk.\n";
	    print "I will ignore this contact for the rest.\n";
	}

	if ( $result[0] =~ /successful/ ) {
	    print "OK: batched sleep successfully\n";
	} else {
	    my $code = $? >> 8;
	    print "FAILURE!\n";
	    print "$cmd returned $code\n";
	    print "# ", join("\n# ", @result ), "\n\n";
	    print "There is something else going wrong, which I am unable to determine.\n";
	    print "I will exit for now, and you will need to re-run me after diagnosing.\n";
	    return undef;
	}

	# extract the URI for contacting the batch job Why didn't I use the 
	# URI module? Because it is not part of a standard installation!
	my $myjobid = (grep {/^https/} @result)[0];
	#my $uri = URI->new( $myjobid );
	#my $sockaddr = sockaddr_in( $uri->port, inet_aton($uri->host) );
	my $sockaddr;
	my $rfc2396 = '^(([^:/?#]+):)(//([^/?#]*))'; # modified from RFC 2396
	if ( $myjobid =~ /$rfc2396/ ) {
	    my %default = ( http => 80, https => 443, ftp => 21, gsiftp => 2811, file => undef );
	    my ($host,$port) = split( /:/, $4 );
	    $port = $default{$2} unless defined $port; # default port for scheme
	    $sockaddr = sockaddr_in( $port, inet_aton($host) );
	} else {
	    print "FAILURE!\n";
	    print "I am unable to extract the hostname and port number from your job\n";
	    print "contact string \"$myjobid\".\n";
	    return undef;
	}

	my ($port,$host) = sockaddr_in($sockaddr);
	$host = inet_ntoa($host);
	print "trying to connect to $host:$port\n";

	# try manual contact first, with 10s timeout
	my $code = eval {
	    local(*S);
	    socket( S, PF_INET, SOCK_STREAM, getprotobyname('tcp') ) or
		return 1;
	    $SIG{ALRM} = sub { close(S); return -1; };
	    alarm(10);
	    my $result = bind_and_connect( *S, $host, $port );
	    alarm(0);
	    close S;
	    $result++ if $result; # leave 0, shift rest up
	    return $result;
	};

	if ( $@ || $code != 0 ) {
	    my ( $rv, $rs ) = ( $!+0, "$!" );
	    if ( $code == 1 ) {
		print "WARNING! $rv: $rs\n";
		print "Unable to construct a socket descriptor with perl. Please make sure that\n";
		print "your /etc/protocols exists and is valid. Otherwise, I am speechless.\n";
	    } elsif ( $code == 2 ) {
		print "WARNING! $rv: $rs\n";
		print "I am unable to resolve $host:$port to something. This may be a transient\n";
		print "hick-up in DNS, but most likely is a mistyped hostname.\n";
	    } elsif ( $code == 3 ) {
		print "WARNING! $rv: $rs\n";
		print "You have defined a GLOBUS_TCP_PORT_RANGE, but all ports are busy.\n";
	    } else {
		print "WARNING!\n";
		print "Time out while trying to contact the remote batch job contact.\n";
		print "Chances are, you ran into the firewall problem with Globus. Please read\n";
		print "http://www.globus.org/ on how to set up the GLOBUS_TCP_PORT_RANGE environment.\n";
	    }
	} else {
	    print "OK: Connection succeeded, trying general status\n";
	}

	my @line;
	my $timeout = 10;	# number of retries, must not be zero
	my $sleep = $sleeptime / $timeout;
	my %pending = ( 'pending' => 1, 'unsubmitted' => 1 );
	do {
	    if ( open( GR, "globusrun -status $myjobid|" ) ) {
		chomp(@line=<GR>);
		print "# ", join("\n# ",@line), "\n";
		close(GR);
	    } else {
		my $rv = $? >> 8;
		print "FAILURE! $rv: $!\n";
		print "I am unable to contact the remote batch job on $gk\n";
		print "This might be an indicator that either the submit host or the remote jobmanager\n";
		print "suffers from an intermediary firewalling problem.\n";
		return undef;
	    }
	    
	    if ( $pending{lc($line[0])} && --$timeout > 0 ) {
		use integer;
		print "OK: Job was not started yet by remote scheduler, sleeping for $sleep s.\n";
		sleep($sleep);
		print "OK: Retrying to retrieve job status.\n";
	    }
	} while ( $pending{lc($line[0])} && $timeout > 0 );

	if ( lc($line[0]) eq 'active' ) {
	    print "OK: status poll succeeded, trying to clean-up\n";
	    push( @newlist, $gk );
	} else {
	    if ( time() - $jobstart >= $sleeptime ) {
		print "WARNING! job reports $line[0]\n";
		print "Uh-oh. It took way too long to get to this point. The diagnosis\n";
		print "cannot be accurate. Let\'s ignore slow contacts for now...\n";
	    } else {
		print "FAILURE! job reports $line[0]\n";
		print "The batch job returns the wrong status word. This is usually a good\n";
		print "indicator for an intermediary firewall.\n";
		return undef;
	    }
	}

	# clean-up now
	$code = system( "globusrun -kill $myjobid" );
	if ( $code != 0 ) {
	    print "WARNING: clean-up returned exit code ", $code >> 8, "\n";
	} else {
	    print "OK: clean-up was attempted, success is doubtful.\n";
	}
    }

    # copy to new list
    $jmlist = [ @newlist ];
    undef @newlist;

    if ( @$jmlist == 0 ) {
	print "FAILURE!\n";
	print "Sorry, there are no gatekeepers that I can run jobs on successfully. You might\n";
	print "want to read up on http://www.globus.org/, try a few things, and re-run me\n";
	print "after you successfully submitted a few jobs.\n";
	return undef;
    } else {
	print "OK: got ", @$jmlist+0, " batch capable contact(s).\n";
	wait_enter;
    }

  RLS:
    #
    # connect() RLS contacts
    #
    print "Let\'s check your Replica Location Service. This may unearth some more subtle\n";
    print "configuration glitches. Please enumerate RLS servers that you want to contact.\n";
    print "You should use the RLS-URI like rls://my.server or rlsn://your.server.\n\n";
    print "Use commas to separate multiple RLS server contacts:\n";
    $line = wait_enter( '[' . join(',',@$rlsvec) . '] ' );
    print "\n";
    $line = join(',',@$rlsvec) unless length($line);

    if ( length($line) > 0 ) {
	local(*S);
	my %rls = ();
	foreach my $rls ( split( /,/, $line) ) {
	    if ( $rls =~ m{(rlsn?://([-a-zA-Z.:0-9]+))/?} ) {
		my $uri = $1;
		my $hp = $2;
		($host,$port) = split /:/, $hp, 2;
		$port = 39281 unless $port;

		socket( S, PF_INET, SOCK_STREAM, getprotobyname('tcp') ) ||
		    die "socket error $!";

		local $SIG{ALRM} = sub { close(S) };
		alarm(30);
		my $result = bind_and_connect( *S, $host, $port );
		alarm(0);
		close S;
		
		if ( $result == 0 ) {
		    print "OK: connected to $host:$port\n";
		    push( @newlist, $uri );
		    $rls{$uri} = [ $host, $port ];
		} else {
		    print "FAILURE: $result\n";
		    if ( $result == 1 ) {
			print "The hostname $host does not resolve. This may be a transient hick-up in DNS\n";
			print "but most likely a typo.\n";
		    } elsif ( $result == 2 ) {
			print "I am unable to find a free port to bind locally to. However, your environment\n";
			print "does specify a GLOBUS_TCP_PORT_RANGE. Thus, I don't think I am able to connect\n";
			print "to the outside world.\n";
		    } elsif ( $result == 3 ) {
			print "I am unable to connect() to $host:$port. Do you have any\n";
			print "firewalls which drop PDUs in between?\n";
		    } else {
			print "unknown error - should not happen.\n";
		    }
		}
		
	    } else {
		print "FAILURE!\n";
		print "The URI $rls is not a valid RLS contact which I can parse. Maybe you\n";
		print "mistyped some portion?\n";
	    }
	}
	
	# copy to new list
	$rlsvec = [ @newlist ];
	undef @newlist;
	
	if ( @$rlsvec == 0 ) {
	    print "\nWARNING!\n";
	    print "Sorry, there are no RLS servers that I can contact successfully. You might\n";
	    print "want to read up on http://www.globus.org/rls/, try a few things, and re-run\n";
	    print "me after you found a RLS service you can contact.\n";
	    return '0 but true';
	} else {
	    print "OK: got ", @$rlsvec+0, " RLS contact(s) that can be contacted\n";
	    wait_enter;
	}

	#
	# PING RLS contacts
	#
	my $gra = $nonvital{'globus-rls-admin'}->[1];
	if ( defined $gra ) {
	    foreach my $rls ( @$rlsvec ) {
		my @result = pipe_out_cmd( $gra, '-p', $rls );
		print "# ", join("\n# ",@result), "\n";
		if ( $? == 0 ) {
		    print "OK: $gra\n";
		    push( @newlist, $rls );
		} else {
		    print "FAILURE: ", $? >> 8, "\n";
		    print "globus-rls-admin -p $rls returned with a failure.\n";
		    print "I guess you cannot use that particular RLS server.\n";
		}
	    }

	    # copy to new list
	    $rlsvec = [ @newlist ];
	    undef @newlist;
	
	    if ( @$rlsvec == 0 ) {
		print "\nWARNING!\n";
		print "Sorry, there are no RLS servers that I can contact successfully. You might\n";
		print "want to read up on http://www.globus.org/rls/, try a few things, and re-run\n";
		print "me after you found a RLS service you can contact.\n";
		return '0 but true';
	    } else {
		print "OK: got ", @$rlsvec+0, " RLS contact(s) that can be contacted\n";
		wait_enter;
	    }

	} else {
	    print "WARNING!\n";
	    print "I am unable to determine the location of your globus-rls-admin command. Thus,\n";
	    print "I am not able to ping your RLS contacts.\n";
	    return '0 but true';
	}

	print "TBD(self): I havn\'t found the time to code more RLS checks. I will check the\n";
	print "rest of RLS another time...\n";
    } else {
	print "OK, no RLS contacts specified, skipping RLS checks\n";
    }

    # return the perl way
    '0 but true';
}

1;
