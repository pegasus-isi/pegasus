package Euryale::Common;
#
# Contains functionality that is shared between prescript and postscript.
# $Id: Common.pm,v 1.3 2005/11/05 06:22:02 voeckler Exp $
#
use 5.006;
use strict;
use vars qw(@ISA @EXPORT @EXPORT_OK $VERSION);
use subs qw(log);		# replace Perl's math log with logging

use Exporter;
@ISA = qw(Exporter);

# create the function prototypes for type coercion
sub load_module($$$);		# { }
sub trim($);			# { }
sub normalize_rsl($);		# { }
sub log_profiles($\%);		# { }
sub read_submit_file($\@\@\@\%\%\%\%); # { }
sub register_files($$\%\@);	# { }
sub extract_properties($$);	# { }
sub setup_gridftp($$\%;$);	# { }
sub canonify_path($);		# { }
sub globusrun($$;$);		# { }
sub globusrun_ws($$$;$$);	# { }
sub setup_cleanup($$$);		# { }
sub on_death($\%;$);		# { }
sub find_exec($);		# { }
sub make_boolean($;$);		# { }
sub tell_queue_manager($$$$$;$); # { }
sub transfer_files($$;$);	# { }
sub obtain_delegation($$$);	# { }

# create the export lists
@EXPORT = qw(trim extract_properties normalize_rsl read_submit_file 
	     register_files setup_gridftp canonify_path mycatfile 
	     linux_trailers log_cwd log_profiles find_exec obtain_delegation
	     globusrun globusrun_ws setup_cleanup load_module
	     on_death make_boolean tell_queue_manager transfer_files);
@EXPORT_OK = qw($VERSION);

$VERSION=$1 if ( '$Revision: 1.3 $' =~ /Revision:\s+([0-9.]+)/o );
$__PACKAGE__::prefix = undef;

#
# --- start -----------------------------------------------------
#
use Carp;
use Socket;
use POSIX qw(setpgid);
use Fcntl qw(:DEFAULT :flock);
use File::Spec;
use File::Spec::Unix;

use Time::HiRes;
use File::Temp qw(tempfile);
use Digest::MD5;

use GriPhyN::WF;
use GriPhyN::SC;
use GriPhyN::RC;
use GriPhyN::Log;
use Euryale::Delegation;

sub extract_properties($$) {
    # purpose: extracts properties of a certain prefix into its own hash.
    # paramtr: $wf (IN): workflow manager handle
    #          $prefix (IN): prefix key
    # returns: a possibly empty hash 
    my $wf = shift || croak 'workflow manager unknown';
    my $prefix = shift || croak "need a property match prefix";

    my $length = ($prefix =~ tr/-._A-Za-z0-9/-._A-Za-z0-9/ );
    my %result = ();
    foreach my $key ( $wf->propertyset($prefix) ) {
	my $minime = substr($key,$length);
	log("found property $minime") if ( $main::DEBUG & 0x10 );
	$result{$minime} = $wf->property($key);
    }

    # done
    %result;
}

sub load_module($$$) {
    # purpose: dynamically load a module at run-time
    # warning: This function must reside in module 'main' ???
    # paramtr: $wf (IN): workflow manager handle
    #          $match (IN): property prefix match string or hashref config
    #          $base (IN): Perl name of the base module (name prefix)
    # returns: an instance towards the module, or undef for failure
    my $wf = shift || die 'workflow manager unknown';
    my %temp = ref($_[0]) eq 'HASH' ? 
	%{ shift() } : 
	extract_properties($wf,shift()); # will die on missing arg
    my $base = shift || die "need the name of the module to load";
    no strict 'refs';

    # create module name
    $base .= '::' unless substr($base,-2) eq '::';
    my $module = $base . $temp{style};
    
    # dynamically load module at run-time into main namespace
    package main;
    eval "require $module;";
    die $@ if $@;
    # import the exported variable (module ISA exporter)
    $module->import() if $module->can('import');
    
    my $handle = undef;
    eval { $handle = $module->new(%temp) };
    if ( defined $handle ) {
	GriPhyN::Log::log( 'loaded ', ref($handle), ' [', 
			   $handle->VERSION, ']' ) 
	    if $main::DEBUG;
    } else {
	$@ =~ s/[\r\n]+//g;
	$@ =~ s/ at .*$// if index($@,__PACKAGE__);
	GriPhyN::Log::log( 'ERROR while instantiating module: ', $@ );
	die( 'ERROR while instantiating module: ', "$@\n" );
    }
    
    $handle;
}

sub log_cwd() {
    # purpose: log location of current working directory
    my $cwd = File::Spec->rel2abs( File::Spec->curdir() );
    log( "cwd=$cwd" );		
}

sub log_profiles($\%) {
    # purpose: log all profiles known at this point
    # paramtr: $prefix (IN): prefix string
    #          %profile (IN): reference to profiles
    # returns: -
    my $prefix = shift;
    my $profile = shift;

    foreach my $ns ( sort keys %{$profile} ) {
	foreach my $key ( sort keys %{$profile->{$ns}} ) {
	    log( $prefix, "profile $ns.$key=", $profile->{$ns}{$key} );
	}
    }
}

sub normalize_rsl($) {
    # purpose: mangles a globus RSL key into a common, comparable format
    # paramtr: $key (IN): wall_time, wallTime, walltime, WaLlTiMe
    # returns: normalized, standardized key
    my $x = shift;
    $x =~ tr/_//d;
    lc($x);
}

sub trim($) {
    # purpose: remove any leading and trailing whitespaces
    # paramtr: $_ (IN): arbitrary string
    # returns: trimmed string. May become empty. 
    local $_ = shift;
    s/^\s*//;
    s/\s*$//;
    $_;
}

sub cleanup($) {
    # purpose: removes outer whitespace and quotes
    # paramtr: $x (IN): unclean value
    # returns: cleaned up version
    local $_ = trim(shift);
    s/^["'](.*)['"]$/$1/;
    $_;
}

#my $rsf_match = '(\d) (\d) "([^"]+)"(?: "([^"]+)")?';
my $rsf_match  = '(\d) (\d) "([^"]+)"(?: "([^"]+)")?(?: "([^"]+)")?'; #"';

sub read_submit_file($\@\@\@\%\%\%\%) {
    # purpose: read the submit file and populate global variables
    # paramtr: $fn (IN): submit filename
    #          @sub (OUT): vector with all lines in the submit file
    #          @input (OUT): list of input LFNs
    #          @output (OUT): list of output LFNs
    #          %rt (IO): hash for register/transfer for each LFN
    #          %stdio (IO): hash for stdio LFNs
    #          %profile (IO): hash to contain profiles from VDL
    #          %job (IO): hash to augment with job information
    # returns: number of lines in the submit file.
    #
    my $sf = shift || croak "need a filename";
    my $file = shift;		# @sub
    my $iref = shift;		# @input
    my $oref = shift;		# @output
    my $rtref = shift;		# %rt
    my $ioref = shift;		# %stdio
    my $pref = shift;		# %profile
    my $jref = shift;		# %job

    my $section;		# section header of late plan information
    local(*SUB);		# just in case protect file handle

    # empty all vector variables
    @{$file} = @{$iref} = @{$oref} = ();

    log("open submit file $sf") if ( $main::DEBUG & 0x01 );
    log_profiles( "sub >> ", %{$pref} );

    open( SUB, "<$sf" ) || croak "open $sf: $!";
    flock( SUB, LOCK_SH ) || warn "flock: $!\n"; # FIXME: infinite wait?
    while ( <SUB> ) {
	s/[\r\n]*$//;		# safe chomp
	push( @{$file}, $_ );	# save the order

	next unless ( /^\#! / ); # next section only for late plan config
	$_ = substr( $_, 3 );	# cut off intro

	if ( /^\[(\w+)\]$/ ) {
	    $section = lc($1);
	    log( "$sf:$.: start section $section" ) 
		if ( $main::DEBUG & 0x01 );
	} elsif ( /^\s*$/ ) {
	    log( "$sf:$.: final section $section" )
		if ( $main::DEBUG & 0x01 );
	    undef $section;
	} else {
	    if ( $section eq 'filenames' ) {
		if ( /$rsf_match/ ) {
		    my ($io,$rt,$lfn,$sfn,$tfn) = ($1,$2,$3,$4,$5);
		    $jref->{io}->{$lfn} = $io;
		    $jref->{sfn}->{$lfn} = $sfn 
			if ( defined $sfn && length($sfn) );
		    $jref->{tfn}->{$lfn} = $tfn
			if ( defined $tfn && length($tfn) );
		    $rtref->{$lfn} = $rt;
		    push( @{$iref}, $lfn ) if ( ($io & 1) == 1);
		    push( @{$oref}, $lfn ) if ( ($io & 2) == 2);
		} else {
		    log( "$sf:$.: mismatch: $_" );
		}
	    } elsif ( $section eq 'stdio' ) {
		my ($what,$value) = split /=/,$_,2;
		$ioref->{$what} = $value;
	    } elsif ( $section eq 'profiles' ) {
		my ($what,$value) = split /=/,$_,2;
		my ($ns,$key) = split /\./, $what, 2;
		$ns = lc $ns; 
		$key = normalize_rsl($key) if $ns eq 'globus';
		$pref->{$ns}->{$key} = substr($value,1,-1);
		#$pref->{$ns}->{$key} = cleanup($value);
	    } elsif ( $section eq 'job' ) {
		my ($what,$value) = split /=/,$_,2;
		$jref->{$what} = $value;
	    } else {
		log( "$sf:$.: Unknown section \"$section\"" );
	    }
	}
    }
    flock( SUB, LOCK_UN );
    close(SUB);

    log_profiles( "sub << ", %{$pref} );
    log( "done with submit file $sf" ) if ( $main::DEBUG & 0x01 );

    # workflow ClassAds into job knowledge
    foreach my $ca ( grep /^\+(vds|wf)_/, @{$file} ) {
	my ($k,$v) = split /=/, $ca, 2;
	$k =~ s{^\+}{};		# remove initial plus sign from key
	$jref->{'ca_' . trim($k)} = cleanup($v);
    }

    # return
    0 + @{$file};
}

sub register_files($$\%\@) {
    # purpose: register the generated files into the RC so other can find'em.
    # paramtr: $rc (IN): the RC handle
    #          $site (IN): the site handle
    #          %job (IN): job description
    #          @files (IN): list of filenames
    # returns: ?
    my $rc = shift || die "need a replica manager";
    my $site = shift || die "need the site handle";
    my $job = shift || die "need a job description";
    my $files = shift || die "need a list of files";
    my $result = 0;
    my $jobdir = $job->{jobdir};

    # set site for some RCs
    $rc->pool($site);

    my ($tfn,%todo);
    foreach my $lfn ( @{$files} ) {
	if ( exists $job->{tfn} ) {
	    # called from prescript - we have the knowledge
	    $tfn = $job->{tfn}{$lfn};
	} else {
	    undef $tfn;
	}

	if ( defined $tfn ) {
	    # extra sanity check
	    if ( index($tfn,$jobdir) >= 0 ) {
		log( "ERROR! Found $jobdir in TFN $tfn");
		log( "ERROR! Will not register $lfn" );
	    } else {
		log( "mapping $lfn to $tfn" ) if ( $main::DEBUG & 0x04 );
		$todo{$lfn} = [ $tfn ];
	    }
	} else {
	    log( "Unable to map $lfn to a TFN, ignoring" );
	}
    }

    # done
    $rc->bulk_insert( \%todo );
}


sub bind_and_connect (*$$) {
    # purpose: use optional bind() follow by connect() on a socket.
    #          The bind will only be attempted in the presence of GTPR
    # warning: Use inside eval { } as this method employs die()!
    # globals: $__PACKAGE__::term will be checked for async timeouts
    # paramtr: SOCK (IN): socket filehandle
    #          $host (IN): remote hostname
    #          $port (IN): remote port number
    # returns: -
    local (*FTP) = shift;
    my $host = shift;
    my $port = shift;

    # resolve peer
    my $site = inet_aton($host) || die "resolve $host: $!\n";

    # handle non-local peer and G_T_P_R with local bind
    if ( exists $ENV{'GLOBUS_TCP_PORT_RANGE'} && 
	 inet_ntoa($site) ne '127.0.0.1' ) {
	my ($lo,$hi) = split /\D/, $ENV{'GLOBUS_TCP_PORT_RANGE'}, 2;
	my ($i,$sin);
	for ( $i = $lo; $i < $hi; ++$i ) {
	    $sin = sockaddr_in( $i, INADDR_ANY );
	    bind( FTP, $sin ) && last;
	}
	die "unable to bind to a local port\n" if ( $i >= $hi );
    }

    # connect
    connect( FTP, sockaddr_in( $port, $site ) ) || die "connect: $!\n";
    die "timeout\n" if $__PACKAGE__::term;

    # protocol our connection
    my @src = unpack_sockaddr_in getsockname(FTP);
    my @dst = unpack_sockaddr_in getpeername(FTP);
    log( 'connection ', inet_ntoa($src[1]), ":$src[0] -> ", 
	 inet_ntoa($dst[1]), ":$dst[0]" );

    # autoflush
    my $save = select(FTP);
    $|=1;
    select($save);
}

sub multiline_response (*;$) {
    # purpose: read 1 or more lines as a response from an ftp-like protocol.
    # paramtr: FTP: is an open socket to the server to read from
    #          $timeout (opt. IN): timeout to use
    # returns: the line which features "^\d{3}\s", usually the last line.
    local (*FTP) = shift;
    my $timeout = shift || 60;

    # I must read all my peer has to blabber: multi-line banners
    my $rin = '';
    vec($rin,fileno(FTP),1) = 1;
    my $buffer = '';
    my ($rout,$nfound,$line);
    my $retries = 0;
  OUTER:
    while ( ($nfound=select($rout=$rin,undef,undef,$timeout/7.0)) >= 0 ) {
	die "timeout" if $__PACKAGE__::term;	# SIGALRM triggered
	if ( $nfound == 0 ) {
	    die "timed out" if ( ++$retries > 6 );
	} else {
	    my $size = length($buffer);
	    my $rsize = sysread( FTP, $buffer, 4096-$size, $size );
	    die "timeout" if $__PACKAGE__::term;	# SIGALRM triggered
	    if ( $rsize < 0 ) {
		die "read: $!";
	    } elsif ( $rsize == 0 ) {
		die "unexpected EOF";
	    } else {
		# process each line right now
		my $pos;
		while ( ($pos=index($buffer,"\n")) >= 0 ) {
		    $line = substr($buffer,0,$pos);
		    $buffer = substr($buffer,$pos+1);
			
		    # process line
		    $line =~ s/[\r\n]+$//;
		    substr( $line, 62 ) = '...' if length($line) > 62;
		    log( 'GFTP << ', $line ) if ( $main::DEBUG & 0x1000 );
		    die "illegal response: $line" unless $line =~ /^\d{3}/;
		    last OUTER if $line =~ /^\d{3}\s/;
		}
	    }
	}
    }

    $line;
}

sub check_gridftp($;$) {
    # purpose: check gridftp server for being up and available
    # paramtr: $host (IN): host or host:port of a gridftp server
    # returns: reverse condition! undef if all is well, error msg
    # in case of failure
    my $ftp = shift;
    my $timeout = shift || 60;	# 60 seconds
    local(*FTP);		# protect from world
    my $line;

    # host may be uri
    my $host = $ftp;
    my $port = 2811;
    my $pos = index( $host, '://' );
    if ( $pos > 0 ) {
	$host = substr( $host, $pos+3 );
	$host = substr( $host, 0, $pos ) if ( ($pos = index($host,'/')) > 0 );
    }

    # host may have had a colon inside...
    ($host,$port) = split( /:/, $host, 2 ) if ( index($host,':') > 0 );
    log( "connecting to $host:$port" ) if $main::DEBUG;

    # setup timeout 
    undef $__PACKAGE__::term;	# avoid interference
    local $SIG{ALRM} = sub { $__PACKAGE__::term=1; close FTP; };
    alarm($timeout);
    eval {
	# get the protocol
	my $proto = getprotobyname('tcp') || die "getprotobyname: $!\n";

	# instantiate a socket
	socket( FTP, PF_INET, SOCK_STREAM, $proto ) || die "socket: $!\n";
    
	# optionally bind, connect, and autoflush
	bind_and_connect( FTP, $host, $port );

	# I must read all my peer has to blabber: multi-line banners
	defined ($line = multiline_response( FTP, $timeout )) 
	    || die "read: $!\n";
	die "unexpected response #1: $line\n"
	    if substr( $line, 0, 3 ) ne '220';

	# say good-bye
	log( 'GFTP >> QUIT' ) if ( $main::DEBUG & 0x1000 );
	defined send( FTP, "QUIT\r\n", 0 ) || die "send: $!\n";
	die "timeout" if $__PACKAGE__::term;

	defined ($line = multiline_response( FTP, $timeout )) 
	    || die "read: $!\n";
	die "timeout" if $__PACKAGE__::term;
	die "unexpected response #2: $line\n" 
	    if substr( $line, 0, 1 ) ne '2';
    };
    alarm(0);
    close FTP;
    $@ ? $@ : undef;
}

sub setup_gridftp($$\%;$) {
    # purpose: obtain the gridftp entry for given site. Any site may
    #          have multiple gridftp servers to handle load balancing. 
    # paramtr: $pc (IN): handle to pool config manager
    #          $site (IN): selected job site
    #          %gridftp (OUT): maps URIs to split host and path portion
    #          $check (opt. IN): if true, do active checks
    # returns: number of available gridftp servers
    #
    my $pc = shift || croak "need pool configuration handler";
    croak "Is $pc supposed to be a pool.config handle?" unless ref $pc;

    my $site = shift || croak "need a run site";
    my $hashref = shift || croak "need a hash variable for gridftp splits";
    my $check = shift;		# may be undef'd

    my @gridftp = $pc->gridftp($site);
    croak "Unable to stage files to/from $site: No gridftp servers are known\n"
	if ( @gridftp == 0 );

    # split server into into gridftp server URL and storage mount point
    my ($pos,@x,%reach,$reach);
    %{$hashref} = ();
    foreach my $ftp ( @gridftp ) {
	@x = @{$ftp};

	if ( $check ) {
	    # check if we can reach it
	    log( "checking reachability of $x[0]" ) if ( $main::DEBUG & 0x08 );
	    $reach = exists $reach{$x[0]} ? 
		$reach{$x[0]} :
		($reach{$x[0]} = check_gridftp($x[0]));
	    if ( defined $reach ) {
		# illegal
		log( "server ", $x[0], " problem: $reach" );
	    } else {
		# ok
		$hashref->{join('',@x[0..1])} = [ @x ];
		log( "server ", $x[0], " ok" ) if ( $main::DEBUG & 0x08 );
	    }
	} else {
	    # don't check reachability -- trust T2/transfer retries
	    $hashref->{join('',@x[0..1])} = [ @x ];
	    log( "server ", $x[0], " unchecked" ) if ( $main::DEBUG & 0x08 );
	}
    }

    # done
    @gridftp = keys %{$hashref};
    0 + @gridftp;
}

sub find_exec($) {
    # purpose: determine location of given binary in $PATH
    # paramtr: $program (IN): basename of the program to execute
    # returns: fully qualified path to binary, undef if not found
    my $program = shift;
    local($_);
    foreach ( File::Spec->path ) {
        my $fs = File::Spec->catfile( $_, $program );
        return $fs if -x $fs;
    }
    undef;
}

sub canonify_path($) {
    # purpose: make the path component of a gridftp-URL canonical
    # paramtr: $url (IN): gridftp-URL
    # returns: a URL with a canonical path component
    my $url = shift;
    my $pos = index( $url, '://' );
    if ( $pos > -1 && ($pos=index($url,'/',$pos+3)) > -1 ) {
	substr( $url, $pos ) = 
	  File::Spec::Unix->canonpath( substr( $url, $pos ) );
    }
    $url =~ s/[\r\n]*$//;	# trim
    $url;
}

sub mycatfile {
    # purpose: connect various pieces with slash between with neither
    #          duplicating nor neglecting any slash between components.
    # paramtr: any number
    # returns: connection of all pieces with slashes in between.
    my $result = shift;
    for my $piece ( @_ ) {
	substr($result,-1,1,'') if ( substr($result,-1,1) eq '/' &&
				     substr($piece,0,1) eq '/' );
	$result .= '/' unless ( substr($result,-1,1) eq '/' ||
				substr($piece,0,1) eq '/' );
	$result .= $piece;
    }
    $result;
}

sub linux_trailers {
    # purpose: log a little about our resource consumption
    if ( open( PROC, "</proc/$$/status" ) ) {
	my @x = map { substr(lc($_),2,-3) } grep { /^Vm/ } <PROC>;
	close(PROC);
	foreach ( @x ) { s/:\s+/=/ }
	log( @x );
    }
}

sub setup_cleanup($$$) {
    # purpose: construct the contact information to run setup/cleanup jobs.
    # paramtr: $pc (IN): handle to pool config manager
    #          $site (IN): where to run the setup or cleanup job
    #          $delegation (IN): delegation EPR to use, undef if unused.
    # returns: [0]: remote fork job contact
    #          [1]: Globus version, 2 or 4
    #          [2]: local gridftp server contact, may be undef for GT2
    my $pc = shift || croak "need pool configuration handler";
    croak "Is $pc supposed to be a pool.config handle?" unless ref $pc;
    my $site = shift || croak "need a run site";
    my $delegation = shift;

    my @contact = $pc->contact($site,'transfer');
    @contact = @{$contact[0]} if @contact > 0;

    my $lgftp;
    if ( defined $contact[2] ) {
	if ( $contact[2] >= 4 ) {
	    my @gftp = $pc->gridftp('local');
	    if ( @gftp > 0 && defined $gftp[0] && 
		 ref $gftp[0] && defined $gftp[0][0] ) {
		$lgftp = $gftp[0]->[0];
	    } else {
		croak( "There is no valid gridftp in pool \'local\' defined, ",
		       "but GT4 requires one." ); 
	    }
	}
    } else {
	$contact[2] = GriPhyN::SC::major_from_contact($contact[0]);
    }

    ( $contact[0], $contact[2], $lgftp, $delegation );
}

sub globusrun_ws($$$;$$) {
    # purpose: run a job through $GLOBUS_LOCATION/bin/globusrun-ws
    # paramtr: $jm (IN): jobmanager contact for web services GT4 (EPR)
    #          $lgs (IN): contact base for local GT4 gridftp server
    #          $sh (IN): shell script to stage and run
    #          $delegation (IN): delegate EPR file, or undef if unused
    #          $to (opt. IN): timeout in seconds before giving up
    # returns: vector: all lines from stdout and stderr
    #          scalar: first line from stdout
    my $jm = shift || croak 'A jobmanager contact string is required';
    my $gftp = shift || croak 'A local gridftp server contact is required';
    my $sh = shift || croak 'A stagable shell script is required';
    my $delegation = shift;	# may be undef
    my $timeout = shift || 600;	# strange conditions at ISI...

    my $gl = $ENV{'GLOBUS_LOCATION'} || croak '$GLOBUS_LOCATION is unset';
    my $gr = File::Spec->catfile( $gl, 'bin', 'globusrun-ws' );
    croak "Missing executable $gr\n" unless -x $gr;

    my ($fh,$fn) = tempfile( 'gr-XXXXXX', SUFFIX => '.rsl', 
			     DIR => File::Spec->tmpdir() );
    fatal( 'unable to create a temporary file for globusrun-ws XMLRSL' )
        unless defined $fh;
    log( "created temporary file $fn" ) if ( $main::DEBUG & 0x02 );

    # create randomized string
    my $ctx = Digest::MD5->new;
    open( SH, "<$sh" ) || die "open $sh: $!\n";
    $ctx->addfile(*SH);
    close(SH);
    my $digest = $ctx->hexdigest;

    # create locally an empty directory to stage as scratchspace
    my $tmp = $ENV{TMP} || $ENV{TEMP} || $ENV{TMPDIR} || File::Spec->tmpdir() ||
	die "ERROR: Unable to determine temporary directory.\n";
    $tmp = File::Spec->rel2abs($tmp) unless substr($tmp,0,1) eq '/';
    my $user = $ENV{USER} || $ENV{LOGNAME} || scalar getpwuid($>) ||
	die "ERROR: Unable to determine symbolic user account name.\n";
    my $scratch = File::Spec->catdir( $tmp, $user );
    if ( ! -d $scratch ) {
	unless ( mkdir($scratch) ) {
	    die "ERROR: mkdir $scratch: $!\n" unless $!{EEXIST};
	}
    }
    my $empty = File::Spec->catdir( $scratch, 'empty' );
    if ( ! -d $empty ) {
	unless ( mkdir($empty) ) {
	    die "ERROR: mkdir $empty: $!\n" unless $!{EEXIST};
	}
    }

    # read EPR
    my %epr = ();
    %epr = parse_epr($delegation) if defined $delegation;
    my $credential = ( exists $epr{Address} && exists $epr{DelegationKey} );
    if ( defined $delegation && $credential ) {
	log( "WILL use delegation" );
    } else {
	log( "WONT use delegation" );
    }

    # write job description language to create remote scratchdir
    # stage in executable, stage out stdout and stderr, and clean
    # up afterwards. 
    my $stream = 0; # '0 but true';
    my $full = File::Spec->rel2abs($sh);
    print $fh "<job>\n";
    print_epr( $fh, 'jobCredentialEndpoint', ' ', %epr )
	if ( defined $delegation && $credential );
    print_epr( $fh, 'stagingCredentialEndpoint', ' ', %epr )
	if ( defined $delegation && $credential );
    print $fh " <executable>/\${GLOBUS_SCRATCH_DIR}/$digest.script.$$</executable>\n";
    print $fh " <directory>/\${GLOBUS_SCRATCH_DIR}/</directory>\n";
    if ( $stream ) {
	print $fh " <stdout>/\${GLOBUS_SCRATCH_DIR}/$digest.out.$$</stdout>\n";
	print $fh " <stderr>/\${GLOBUS_SCRATCH_DIR}/$digest.err.$$</stderr>\n";
    }
    print $fh " <jobType>single</jobType>\n";
    print $fh " <fileStageIn>\n";
    print $fh "  <maxAttempts>5</maxAttempts>\n";
    print_epr( $fh, 'transferCredentialEndpoint', '  ', %epr )
	if ( defined $delegation && $credential );
    print $fh "  <transfer>\n";
    print $fh "   <sourceUrl>$gftp$full</sourceUrl>\n";
    print $fh "   <destinationUrl>file:///\${GLOBUS_SCRATCH_DIR}/$digest.script.$$</destinationUrl>\n";
    print $fh "  </transfer>\n";
    print $fh " </fileStageIn>\n";
    print $fh " <fileCleanUp>\n";
    print $fh "  <maxAttempts>3</maxAttempts>\n";
    print_epr( $fh, 'transferCredentialEndpoint', '  ', %epr )
	if ( defined $delegation && $credential );
    print $fh "  <deletion>\n";
    print $fh "   <file>file:///\${GLOBUS_SCRATCH_DIR}/$digest.script.$$</file>\n";
    print $fh "  </deletion>\n";
    if ( $stream ) {
	print $fh "  <deletion>\n";
	print $fh "   <file>file:///\${GLOBUS_SCRATCH_DIR}/$digest.out.$$</file>\n";
	print $fh "  </deletion>\n";
	print $fh "  <deletion>\n";
	print $fh "   <file>file:///\${GLOBUS_SCRATCH_DIR}/$digest.err.$$</file>\n";
	print $fh "  </deletion>\n";
    }
    print $fh " </fileCleanUp>\n";
    print $fh "</job>\n";
    close $fh;

    # FIXME: Is there something more efficient?
    my @arg = ( $gr, '-submit', '-F', $jm, '-Ft', 'Fork' );
    my $dflag = 0;
    my ($cfh,$cfn);
    if ( defined $delegation ) {
	unless ( $credential ) {
	    # write to temp file
	    ($cfh,$cfn) = tempfile( 'vdc-XXXXXX', 
				    DIR => File::Spec->tmpdir,
				    SUFFIX => '.epr',
				    UNLINK => 0 );
	    if ( defined $cfh ) {
		log( 'WILL use delegation after all');
		chmod 0600, $cfn; # sensitive information
		print_epr( $cfh, 'DelegatedEPR', 0, %epr );
		close $cfh;
		push( @arg, '-Sf', $cfn, '-Tf', $cfn, '-Jf', $cfn );
		$dflag = 1;
	    }
	}
    } else {
	push( @arg, '-S' );
    }
    push( @arg, '-s', '-q' ) if ( $stream > 0 );
    push( @arg, '-f', $fn );
    log( "Starting ", join(' ',@arg) );

    my (@result,$flag);
    local(*READ);
    my $pid = open( READ, "-|" );
    return undef unless defined $pid;

    if ( $pid == 0 ) {
	# child
	setpgid(0,0);
	open STDERR, ">&STDOUT" || exit 126;
	select STDERR; $|=1;
	select STDOUT; $|=1;
	exec { $arg[0] } @arg;
	exit 127;
    } else {
	# parent
	local $SIG{ALRM} = sub { kill -15 => $pid };
	alarm($timeout);

	while ( <READ> ) {
	    s/[\r\n]*$//o;	# safe chomp
	    log( '[g-r] ', $_ );
	    push( @result, $_ );
	}

	alarm(0);
	unless ( close(READ) ) {
	    my $ec = $? >> 8;
	    my $sig = ($? & 127);
	    if ( $sig == 14 ) {
		log( "Globusrun timed out!" );
	    } elsif ( $sig ) {
		log( "Globusrun died on signal $sig" );
	    } elsif ( $ec == 42 ) {
		# 42 is a signal for ok
		@result = ( 'OK' ); # unless @result > 0;
	    } else {
		log( "Failure for globusrun-ws: exit code $ec" );
	    }
	    # Warning: GT4 returns remote exit codes
	    undef @result unless ( $ec < 10 || $ec == 42 );
	}
	log( "globusrun-ws returned ", ($?>>8), '/', ($? & 127) );
    }

    if ( 1 ) {
	# for now, remember what we did
	my $out = substr($full,0,-3) . '.jdd';
	system { '/bin/mv' } '/bin/mv', $fn, $out;
    } else {
	# clean up
	unlink($fn);
    }

    # temporary credential file
    unlink($cfn) if $dflag == 1;

    # return the Perl way
    wantarray ? @result : $result[0];
}


sub globusrun($$;$) {
    # purpose: run a job through $GLOBUS_LOCATION/bin/globusrun
    # paramtr: $jm (IN): job manager contact
    #          $sh (IN): shell script to stage and run
    #          $to (opt. IN): timeout in seconds before giving up
    # returns: vector: all lines from stdout and stderr
    #          scalar: first line from stdout
    my $jm = shift || croak 'A jobmanager contact string is required';
    my $sh = shift || croak 'A stagable shell script is required';
    my $timeout = shift || 600;

    my $gl = $ENV{'GLOBUS_LOCATION'} || croak '$GLOBUS_LOCATION is unset';
    my $gr = File::Spec->catfile( $gl, 'bin', 'globusrun' );
    croak "Missing executable $gr\n" unless -x $gr;

    # FIXME: This is the simplistic solution, resource inefficient. 
    local(*READ);
    my $full = File::Spec->rel2abs($sh);
    my @arg = ( $gr, '-r', $jm, '-s', '-o', 
		"&(executable=\$(GLOBUSRUN_GASS_URL) # \"" . $full . "\")" );

    log( "Starting ", join(' ',@arg) );
    my $pid = open( READ, '-|' );
    return undef unless defined $pid;

    my (@result,$flag);
    if ( $pid == 0 ) {
	# child
	setpgid(0,0);
	open STDERR, ">&STDOUT" || exit 126;
	select STDERR; $|=1;
	select STDOUT; $|=1;
	exec { $arg[0] } @arg;
	exit 127;
    } else {
	# parent
	local $SIG{ALRM} = sub { kill -15 => $pid };
	alarm($timeout);

	while ( <READ> ) {
	    s/[\r\n]*$//o;	# safe chomp
	    log( '[g-r] ', $_ );
	    push( @result, $_ );
	}

	alarm(0);
	unless ( close(READ) ) {
	    my $rc = $?;
	    if ( ($rc & 127) == 14 ) {
		log( "Globusrun timed out!" );
	    } elsif ( ($rc >> 8) > 0 ) {
		log( "Failure for globusrun: exit code ", ($rc>>8) );
	    } else {
		log( "Globusrun died on signal ", ($rc & 127) );
	    }
	    undef @result;
	}
	log( "globusrun returned ", ($?>>8), '/', ($? & 127) );
    }

    # return the Perl way
    wantarray ? @result : $result[0];
}

sub on_death($\%;$) {
    # purpose: run remote cleanup script, if feasible.
    # warning: This function is called from the __DIE__ signal handler
    # paramtr: $submit (IN): basename of the submit file
    #          %sc (IN): knowledge about other scripts that may have run
    #                    {forkjm} remote jobmanager to run cleanup jobs
    #                    {setup} interval the setup script ran (> 0)
    #                    {cleanup} interval the cleanup script ran (==0)
    #          $flag (IN): Expect mv failures, if set
    # returns: -
    my $submit_base = shift || croak 'need the basename of the submit file'; 
    my $sc = shift || croak 'need a hash ref';
    my $flag = shift;		# may be undef'd
    my $cleanup = "$submit_base.cleanup.sh"; # educated guess

    # sanity check
    return unless -r $cleanup;

    if ( exists $sc->{forkjm} && $sc->{setup} > 0 && $sc->{cleanup} == 0 ) {
	log( 'cleaning up remote site setup' );
	my $stamp = time();
	my @result = $sc->{forkjm}->[1] == 4 ? 
	    globusrun_ws( $sc->{forkjm}->[0], $sc->{forkjm}->[2], $cleanup,
			  $sc->{forkjm}->[3] ) :
	    globusrun( $sc->{forkjm}->[0], $cleanup );
	rename( $cleanup, "$cleanup.done" ) if $? == 0;
	if ( @result && 
	     ( (grep { /^OK$/ } @result) > 0 ||
	       ($flag && $result[$#result] =~ /ERROR: (\d+) mv failures/) ) ) {
	    # for myself, log the time spent in X-up script
	    $sc->{cleanup} = time() - $stamp;
	    clog( 'cus', $submit_base, 'ran ', $sc->{forkjm}->[0], 
		  " -s $cleanup in ", sprintf( '%.3f s', $sc->{cleanup} ) );
	} else {
	    log( "cleanup script failed!" );
	}
    } else {
	log( 'NOT running cleanup script - and removing cleanup script.' );
	unlink "$cleanup";
    }
}

sub make_boolean($;$) {
    # purpose: convert an arbitrary property value to a boolean value.
    # warning: only undefined, empty string, 0, any-caps string 'false'
    #          and any-caps string 'off' eval to false, everything else
    #          is true. 
    # paramtr: $x (IN): some value to make boolean (e.g. "True", "on")
    #          $def (IN): The default to return in undef'd case of $x
    # returns: boolean value
    my $x = shift;		# undef is ok, and means false
    return $_[0] unless defined $x;

    return undef unless $x; 	# rules out empty string, undefined, 0. 
    return undef if lc($x) eq 'false';
    return undef if lc($x) eq 'off';

    # true
    1;
}

sub tell_queue_manager($$$$$;$) {
    # purpose: Tell external queue manager about the initial job
    # paramtr: $unid (IN): unique initial ID for this job
    #          $incr (IN): either 1 or -1 to tell for job
    #          $voname (IN): name of the VO; undef maps to 'ivdgl'
    #          $voname (IN): name of the VO; undef maps to 'ivdgl'
    #          $host (IN): ipv4 or hostname to connect to
    #          $port (opt. IN): port number to connect to
    # returns: $@ which is undef for success
    my $unid = shift || die "need a unique job id";
    my $incr = shift || die "need a valid increment";
    my $voname = shift || 'ivdgl';
    my $vogroup = shift || 'ivdgl1';
    my $host = shift || die "need a hostname";
    my $port = shift || 60010;

    local(*SOCK);
    my $query = "newJob $vogroup $voname $incr $unid";

    # setup timeout 
    undef $__PACKAGE__::term;	# avoid interference
    local $SIG{ALRM} = sub { $__PACKAGE__::term=1; close SOCK; };
    alarm(90);			# FIXME: maybe too low? 

    # safe execution
    eval {
	# create a socket
	socket( SOCK, PF_INET, SOCK_STREAM, getprotobyname('tcp') || 6 ) ||
	    die "socket: $!\n";

	# connect from ephemerical port
	bind_and_connect( SOCK, $host, $port );
	die "timeout\n" if $__PACKAGE__::term;

	# read server's prompt
	# FIXME: It does not finish with a newline, grrrr!
	defined recv( SOCK, $_, 4096, 0 ) || die "recv prompt: $!\n"; 
	die "timeout\n" if $__PACKAGE__::term;

	# write query -- NVT *requires* CR and LF both!
	defined send( SOCK, "$query\r\n", 0 ) || die "send query: $!\n";
	die "timeout\n" if $__PACKAGE__::term;

	# read and ignore replies until EOF
	# FIXME: The server should tell me, if it accepts or rejects
	# my request for some reason... 
	while ( <SOCK> ) {
	    die "timeout\n" if $__PACKAGE__::term;
	}

	close SOCK || die "close: $!\n";
    }; 

    # de-activate alarm reliably (outside eval)
    alarm(0);

    # tell what has happened
    $@;
}

sub transfer_files($$;$) {
    # purpose: runs the transfer program to transfer stage files.
    # paramtr: $api (IN): if set, use T2, otherwise use tranfer
    #          $sif (IN): filename of the stage information file
    #          $args (opt. IN): Additional arguments, default -q -R
    # returns: true for success, false for failure
    my $api = shift; 
    my $sif = shift || die "need the stage info filename";
    die "unable to read stage info file $sif" unless -r $sif;
    my $args = shift || '-q -R';
    my $result;

    my $basename = $api ? 'T2' : 'transfer';
    my $prg = File::Spec->catfile( $ENV{'VDS_HOME'}, 'bin', $basename );
    # search $PATH if not found at default location
    $prg = find_exec($basename) unless -x $prg;
    # croak if not found at all
    die "unable to find program $basename anywhere" unless -x $prg;
    log( "using $prg" ) if ( $main::DEBUG & 0x02 );

    # permit extra args for debugging
    my @args = ();
    my $use_sh = index($args,'"')+index($args,"'")+index($args,"\\");
    if ( $use_sh > -3 ) {
	# detected shell quoting/escape characters
	$args = "$prg $args no match $sif";
	log( $args, ' # uses /bin/sh -c' );
    } else {
	@args = $args ? split /\s+/, $args : ();
	push( @args, 'no', 'match', $sif );
	log( "$prg ", join(' ',@args) );
    }

    # use more efficient way pipe_out_cmd
    local(*TRANSFER);
    my $pid = open( TRANSFER, "-|" );
    die "unable to stage $sif: $!" unless defined $pid;

    if ( $pid ) {
	# parent
	my $what = $api ? 'T2' : 'transfer';
	while ( <TRANSFER> ) {
	    s/[ \t\r\n]*$//;	# trim tail
	    log( "[$what] $_" ) if ( ($main::DEBUG & 0x02) && defined $_ );
#	    if ( ! $api && /550 550 (\S+): not a plain file/ ) {
#		my $sfn = $1;
#		die "ERROR: Detected broken file $sfn\n";
#	    }
	}
	close(TRANSFER);
	$result = $? >> 8;
    } else {
	# child
        open( STDERR, ">&STDOUT" );
        select(STDERR); $|=1;
        select(STDOUT); $|=1;
	if ( $use_sh > -3 ) {
	    # sigh, use /bin/sh -c to evaluate
	    exec $args;
	} else {
	    # more efficient way to do business
	    exec { $prg } $prg, @args;
	}
        exit(127);
    }	

    $result;
}


sub obtain_delegation($$$) {
    # purpose: retrieves a delegation for GT4 from delegation cache daemon
    # paramtr: $wf (IN): handle to the workflow properties
    #          $sc (IN): handle to the site catalog
    #          $site (IN): site handle
    # returns: delegation, or undef
    my $wf = shift || croak "need a wfrc reference";
    my $sc = shift || croak "need a site catalog reference";
    my $site = shift || croak "need a site handle";

    # add GT4 delegation to job, if required
    my @contact = $sc->contact( $site, 'vanilla' );
    die "unable to find any contact for $site" unless @contact > 0;
    return undef if ( $contact[0][2] < 4 ); # use first contact, check for GT4
    return undef unless substr( $contact[0][0], 0, 5 ) eq 'https';

    # add delegation for GT4
    my $sockfn = vdd_socket();
    if ( -e $sockfn ) {
	# server appears to be up
	my $sockfh = vdd_connect($sockfn);
	my $delegation = vdd_get_delegation( $sockfh, $contact[0][0] );
	close $sockfh;		# server is 1-shot
	return $delegation;
    } else {
	# server is definitely not up
	return undef;
    }
}

#
# return 'true' to package loader
#
1;
__END__
