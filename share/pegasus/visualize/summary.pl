#!/usr/bin/env perl
#
# summarizes a given DAGMan Condor log file
#
# $Id$
# 
require 5.005;
use strict;
use Fcntl;
use DB_File;
use POSIX qw();
use Socket;
use File::Spec;
use File::Basename;
use Time::Local;
use Getopt::Long;

my $year = (localtime())[5];
my ($tag,$job,$mon,$day,$h,$m,$s,@lines);

my ($help,$kickstart,%submit,%output);
my $result = GetOptions('help' => \$help,
			'kickstart' => \$kickstart );

if ( $help || ( @ARGV == 0 && -t STDIN ) ) {
    print "Usage: $0 [--kickstart] condor.log [..]\n";
    print " --kickstart   enable kickstart record parsing for true runtime.\n";
    exit 1;
}

BEGIN {
    $main::dnsfn = File::Spec->catfile( $ENV{HOME}, '.dns' );
    $main::dbref = tie( %main::dns, 'DB_File', $main::dnsfn )
	or die "dbfile error on $main::dnsfn: $!\n";
}
END {
    untie %main::dns;
    undef $main::dbref;
}

$main::minstamp = 1E300;
$main::maxstamp = -$main::minstamp;
foreach my $log ( @ARGV ) {
    local $/="...\n";
    if ( open( LOG, "<$log" ) ) {
	while ( <LOG> ) {
	    if ( m{^((...)\s+\((\d+)\.\d+\.\d+\)\s+(..)/(..) (..):(..):(..)\s+)} ) {
		($tag,$job,$mon,$day,$h,$m,$s) = ($2,$3,$4,$5,$6,$7,$8);
		substr( $_, 0, length($1), '' );
		@lines = grep { length($_) > 0 } split /\n/, $_;
		process( $ARGV, $tag, $job, 
			 timelocal($s,$m,$h,$day,$mon-1,$year), @lines );
	    } else {
		warn "$ARGV:$.: illegal log line\n";
	    }
	}
	close LOG;
    } else {
	warn "open $log: $!\n";
    }

    my ($job,$sfn);
    my $dag = substr( $log, 0, rindex($log,'.') ) . '.dag';
    $/="\n";
    if ( open( DAG, "<$dag" ) ) {
	while ( <DAG> ) {
	    # map job id to submit file (to find ks)
	    if ( /^\s*job\s+(\S+)\s+(\S+)/i ) {
		($job,$sfn) = ($1,$2);
		$submit{$job} = $sfn;

		if ( open( SUB, "<$sfn" ) ) {
		    while ( <SUB> ) {
			$output{$job} = $1 if ( /^\s*output\s*=\s*(\S+)/ );
		    }
		    close SUB;
		} else {
		    warn "open $sfn: $!\n";
		}
	    }
	}
	close DAG;
    } else {
	warn "open $dag: $!\n";
    }
}
#$/="";

# 
# FIXME: Collapse jobs into DAG nodes (submit files)
# This is for the special occasion that jobs are manually killed (condor_rm),
# and thus create a new Condor job to rerun the same DAG node.
#

sub hsort {
    # symbolically sort by domainname
    my $aa = join( '.', reverse split /\./, $a );
    my $bb = join( '.', reverse split /\./, $b );
    $aa cmp $bb;
}

my (%replan,%finish,%nack);
foreach my $node ( keys %main::node ) {
    my $n = 0 + @{$main::node{$node}};
    # jobs that were replanned
    my ($job,$host);
    for ( my $i=0; $i < $n-1; ++$i ) {
	$job = $main::node{$node}[$i];
	$host = $main::db{$job}{'017'}[1];
	$replan{$host}++;
	$nack{$job}=1;		# remember replanned job id
    }
    # jobs that executed
    $job = $main::node{$node}[$n-1];
    $host = $main::db{$job}{'017'}[1];
    $finish{$host}++;
}

#
# failure rate
#
printf( "%-32s %6s %6s %6s %6s %8s %6s\n", 'JOBMANAGER', 'SUBMIT', 'BEGUN', 
	'SUCCSS', 'FAILED', 'REPLAN', 'FINISH' );
my (%good,@s,$max,$count,@median,$replan,$finish);
foreach my $host ( sort hsort keys %main::host ) {
    next if length($host) < 1;
    my @x = @{$main::host{$host}};
    my $d = $x[2]-$x[1];
    printf( "%-32s %6u %6u %6u %6u %8u %6u\n", $host, $x[2], $x[0], $x[1],
	    $d, $replan{$host}, $finish{$host} );
    $replan += $replan{$host};
    $finish += $finish{$host};
    $s[0] += $x[0];
    $s[1] += $x[2];
    $s[2] += $x[1];
    $good{$host} = $x[1] if $x[1] > 0;
    $max = $x[1] if $x[1] > $max;
    $count++;
    push( @median, $x[1] );
}
printf( "%-32s %6s %6s %6s %6s %8s %6s\n", '', '-' x 6, '-' x 6, '-' x 6, 
	'-' x 6, '-' x 6, '-' x 6 );
printf( "%-32s %6u %6u %6u %6u %8u %6u\n", 'TOTAL JOBS', @s[1,0,2],
	$s[1]-$s[2], $replan, $finish );
print "\n";



#
# Marco's detail
#
sub adjust ($) {
    my $x = shift;
    if ( $x >= 0 ) {
	sprintf( "%6u", $x );
    } else {
	'-';
    }
}

printf( "%-16s %6s %-15s %6s %6s %6s %6s", 'NODENAME', 'JOBID',
	'STARTTIME', 'TOTAL', 'SUBMIT', 'PEND', 'RUN' );
printf( " %8s", 'KS' ) if $kickstart;
printf( " %s\n", 'GATEKEEPER' );

@s = ();
my ($ks,%ks,$ksc);
foreach my $node ( sort { $main::node{$a}->[0] <=> $main::node{$b}->[0] } 
		   keys %main::node ) {
    my $flag = undef;
    foreach my $job ( @{$main::node{$node}} ) {
	next unless $job;
	my $host = $main::db{$job}{'017'}[1];

	my @ts = ( $main::db{$job}{'000'}[0],
		   $main::db{$job}{'017'}[0],
		   $main::db{$job}{'001'}[0],
		   $main::db{$job}{'005'}[0] );

	my @d = ( $ts[3] - $ts[0], $ts[1] - $ts[0],
		  $ts[2] - $ts[1], $ts[3] - $ts[2] );

	printf( "%-16s %6s %15s %6s %6s %6s %6s",
		($flag ? '' : substr($node,-16)), $job,
		mkiso($ts[0]), adjust($d[0]), adjust($d[1]),
		adjust($d[2]), adjust($d[3]) );
	$flag=1;

	unless ( $nack{$job} ) {
	    for ( my $i=0; $i<@d; ++$i ) {
		$s[$i] += $d[$i];
	    }

	    # read kickstart records
	    if ( $kickstart ) {
		my $t1 = dirname($node); # remove node name
		my $t2 = dirname($t1);
#		my $fn = ( defined $t2 ? "$t2/" : '' ) . 
#		    basename($node) . '.out';
		my $fn = ( defined $t2 && $t2 ne '/' ? "$t2/" : '' ) .
		    $output{basename($node)};
		if ( -s $fn && open( KS, "<$fn" ) ) {
		    while ( <KS> ) {
			next if ( index($_,'invocation ') == -1 );
			if ( /duration="([0-9.]+)"/ ) {
			    my $x = $1 + 0.0;
			    $ks{$job} = $x;
			    $ks += $x;
			    printf( " %8.1f", $x );
			    $ksc++;
			}
			last;
		    }
		    close KS;
		} else {
		    warn "open $fn: $!\n";
		}
	    }
	} else {
	    printf( " %8s", '' ) if $kickstart;
	}

	# last column
	print " $host\n";
    }
}
printf( "%-16s %6s %-15s %6s %6s %6s %6s", '', '', '',
	'-----', '-----', '------', '------' );
printf( " %8s", '---------' ) if $kickstart;
print "\n";

printf( "%-16s %6s %-15s %6u %6u %6u %6u", '', '', 'SUCCESSFUL JOBS', @s );
printf( " %8.1f", $ks ) if $kickstart;
print "\n";

my $diff = $main::maxstamp - $main::minstamp;
printf( "%-16s %6u %-15s %6.2f %6.2f %6.2f %6.2f",
	'Time in DAGMans', $diff, 'speedups',
	map { $_ / $diff } @s );
printf( " %8.2f", $ks / $diff ) if $kickstart;
print "\n";
print "\n";

##
## job to node mapping
## 
#printf "%-16s %6s %s\n", 'NODE', 'JOBID', 'SITE';
#foreach my $node ( keys %main::node ) {
#    # only over-defined nodes
#    if ( @{$main::node{$node}} > 1 ) {
#	my $flag = undef;
#	foreach my $job ( @{$main::node{$node}} ) {
#	    printf( "%-16s %6u %s\n", ( $flag ? '' : $node), $job, 
#		    $main::db{$job}{'017'}[1] );
#	    $flag=1;
#	}
#    }
#}
#print "\n";

#
# involvement of successful jobs
#
printf "%-32s %6s %s\n", 'JOBMANAGER', 'OK', 'INVOLVEMENT';
foreach my $host ( sort { $good{$a} <=> $good{$b} } keys %good ) {
    printf( "%-32s %6u %s\n", $host, $good{$host}, 
	    produce( '#', $good{$host}, 39, $max, median(@median) ) );
}
print "\n";

print "# from here, look only  at jobs that were not replanned:\n\n";

#
# site delays
#
@median = ();
my (%a,%b,%c,%d,%e,%r,%r1,%median,$min,$med,$avg);
foreach my $job ( sort keys %main::job ) {
    # only rates successful jobs that were not replanned
    next if $nack{$job};

    # rate
    my $host = myhost($main::db{$job}{'017'}[1]);
    my @ts = ( $main::db{$job}{'000'}[0],
	       $main::db{$job}{'017'}[0],
	       $main::db{$job}{'001'}[0],
	       $main::db{$job}{'005'}[0] );
    push( @{$a{$host}}, $ts[1] - $ts[0] );
    push( @{$b{$host}}, $ts[2] - $ts[1] );
    push( @{$c{$host}}, $ts[3] - $ts[2] );
    push( @{$d{$host}}, $ts[3] - $ts[0] );

    push( @{$e{$host}}, $ks{$job} ) if $kickstart;
}

foreach my $host ( keys %d ) {
    my $a = median( @{$a{$host}} );
    my $b = median( @{$b{$host}} );
    my $c = median( @{$c{$host}} );
    my $d = median( @{$d{$host}} );

    push( @{$median[0]}, $a );
    push( @{$median[1]}, $b );
    push( @{$median[2]}, $c );
    push( @{$median[3]}, $d );
    if ( $kickstart ) {
	my $e = median( @{$e{$host}} );
	$median{$host} = [ $a, $b, $c, $d, $e ];
	push( @{$median[4]}, $e );
	$r1{$host} = [ $e, $d-$c, $d+$e-$c, $e / ($d-$c) ];
    } else {
	# no kickstart
	$median{$host} = [ $a, $b, $c, $d ];
    }
    $r{$host} = [ $b, $c, $b+$c, $c / $b ];
}

#
# other sections
#

($min,$max,$med,$avg) = minmax( @{$median[0]} );
printf "%-32s %6s %s\n", 'JOBMANAGER', '[s]', 'TIME SPENT IN SUBMITTED';
foreach my $host ( sort { $median{$a}->[0] <=> $median{$b}->[0] } 
		   keys %median ) {
    printf( "%-32s %6u %s\n", $host, $median{$host}->[0],
	    produce( '#', $median{$host}->[0], 39, $max, $med ) );
}
print "\n";

($min,$max,$med,$avg) = minmax( @{$median[1]} );
printf "%-32s %6s %s\n", 'JOBMANAGER', '[s]', 'TIME SPENT IN PENDING';
foreach my $host ( sort { $median{$a}->[1] <=> $median{$b}->[1] } 
		   keys %median ) {
    printf( "%-32s %6u %s\n", $host, $median{$host}->[1],
	    produce( '#', $median{$host}->[1], 39, $max, $med ) );
}
print "\n";

($min,$max,$med,$avg) = minmax( @{$median[2]} );
printf "%-32s %6s %s\n", 'JOBMANAGER', '[s]', 'TIME SPENT IN RUNNING';
foreach my $host ( sort { $median{$a}->[2] <=> $median{$b}->[2] } 
		   keys %median ) {
    printf( "%-32s %6u %s\n", $host, $median{$host}->[2],
	    produce( '#', $median{$host}->[2], 39, $max, $med ) );
}
print "\n";

if ( $kickstart ) {
    ($min,$max,$med,$avg) = minmax( @{$median[4]} );
    printf "%-32s %8s %s\n", 'JOBMANAGER', '[s]', 'JOBTIME ACC2 KICKSTART';
    foreach my $host ( sort { $median{$a}->[4] <=> $median{$b}->[4] } 
		       keys %median ) {
	printf( "%-32s %8.2f %s\n", $host, $median{$host}->[4],
		produce( '#', $median{$host}->[4], 39, $max, $med ) );
    }
    print "\n";
}

($min,$max,$med,$avg) = minmax( @{$median[3]} );
printf "%-32s %6s %s\n", 'JOBMANAGER', '[s]', 'TOTAL TIME';
foreach my $host ( sort { $median{$a}->[3] <=> $median{$b}->[3] } 
		   keys %median ) {
    printf( "%-32s %6u %s\n", $host, $median{$host}->[3],
	    produce( '#', $median{$host}->[3], 39, $max, $med ) );
}
print "\n";

printf "%-32s %6s %s\n", 'JOBMANAGER', '[s]', 'SUBMITTED+PENDING+RUNNING';
foreach my $host ( sort { $median{$a}->[3] <=> $median{$b}->[3] }
		   keys %median ) {
    my @x = @{$median{$host}};
    $x[3] = $x[0] + $x[1] + $x[2];
    my @y = ( $x[0]*39.0/$x[3], $x[1]*39.0/$x[3], $x[2]*39.0/$x[3] );
    #printf( "\t%.3f %.3f %.3f %.3f\n", @y, $x[3] );
    printf( "%-32s %6.0f %s%s%s\n", $host, $x[3],
	    's' x $y[0], 'p' x $y[1], 'r' x $y[2] );
}
print "\n";

printf "%-32s %6s %s\n", 'JOBMANAGER', 'RATIO', 'RUNNING / PENDING';
foreach my $host ( sort { $r{$a}->[3] <=> $r{$b}->[3] } keys %r ) {
    my @x = @{$r{$host}};
    printf( "%-32s %6.2f %s%s\n", $host, $x[3],
	    'p' x ( ($x[0] * 39.0 ) / $x[2] ),
	    'r' x ( ($x[1] * 39.0 ) / $x[2] ) );
}
print "\n";

if ( $kickstart ) {
    printf "%-32s %6s %s\n", 'JOBMANAGER', 'RATIO', 'KICKSTART / (TOTAL-RUNNING)';
    foreach my $host ( sort { $r1{$a}->[3] <=> $r1{$b}->[3] } keys %r1 ) {
	my @x = @{$r1{$host}};
	printf( "%-32s %6.2f %s%s\n", $host, $x[3],
		'.' x ( ($x[0] * 39.0 ) / $x[2] ),
		'x' x ( ($x[1] * 39.0 ) / $x[2] ) );
    }
    print "\n";
}

exit 0;

#
# --- helpers
#

sub produce ($$$$$) {
    my $ch = shift;
    my $x = shift;
    my $scale = shift;
    my $max = shift;
    my $med = shift;

    my $result;
    if ( $max > 10 * $med ) {
	# there are extreme values present
	my $cutoff = 2 * $med;
	if ( $x < $cutoff ) {
	    # smaller than cutoff, regular scaling
	    $result = $ch x ( ($x * $scale) / $cutoff );
	} else {
	    # larger than cutoff, extreme value
	    my $f = ($max+1) / $scale;
	    while ( $x > 0 ) {
		$result .= '+';
		$x -= $f;
	    }
	}
    } else {
	# no extreme values present, simple
	$result = $ch x ( ($x * $scale) / $max );
    }

    # done
    $result;
}

sub minmax { 
    my $min = 1E300;
    my $max = -1E300;
    my $med = median(@_);

    my $sum = 0.0;
    foreach my $x ( @_ ) {
	$min = $x if $x < $min;
	$max = $x if $x > $max;
	$sum += $x;
    }

    my $n = 0 + @_;
    ( $min, $max, $med, ($n ? $sum / $n : undef) );
}

sub median {
    my @x = sort { $a <=> $b } @_;
    my $n = 0 + @x;

    if ( ($n & 1) == 1 ) {
	($x[$n/2]+$x[$n/2+1])/2;
    } else {
	$x[$n/2];
    }
}

sub myhost($) {
    # purpose: send hostname back and forth through DNS
    # paramtr: hostname or ipv4
    # returns: canonical primary name of host
    my $host = lc shift;
    my ($stamp,$value) = split /\#/, $main::dns{$host};
    unless ( defined $stamp && $stamp > time() ) {
	$stamp = time() + 604800;
	$value =  (gethostbyaddr( inet_aton($host), AF_INET ))[0];
	$main::dns{$host} = join('#',($stamp,$value));
    }
    $value;
}

sub mkiso(;$) {
    my @ts = localtime(shift() || time());
    POSIX::strftime( "%Y%m%dT%H%M%S", @ts[0..6]);
}

# Job submitted             ULOG_SUBMIT                   = 0,
# Job now running           ULOG_EXECUTE                  = 1,
# Error in executable       ULOG_EXECUTABLE_ERROR         = 2,
# Job was checkpointed      ULOG_CHECKPOINTED             = 3,
# Job evicted from machine  ULOG_JOB_EVICTED              = 4,
# Job terminated            ULOG_JOB_TERMINATED           = 5,
# Image size of job updated ULOG_IMAGE_SIZE               = 6,
# Shadow threw an exception ULOG_SHADOW_EXCEPTION         = 7,
# Generic Log Event         ULOG_GENERIC                  = 8,
# Job Aborted               ULOG_JOB_ABORTED              = 9,
# Job was suspended         ULOG_JOB_SUSPENDED            = 10,
# Job was unsuspended       ULOG_JOB_UNSUSPENDED          = 11,
# Job was held              ULOG_JOB_HELD                 = 12,
# Job was released          ULOG_JOB_RELEASED             = 13,
# Parallel Node executed    ULOG_NODE_EXECUTE             = 14,
# Parallel Node terminated  ULOG_NODE_TERMINATED          = 15,
# POST script terminated    ULOG_POST_SCRIPT_TERMINATED   = 16,
# Job Submitted to Globus   ULOG_GLOBUS_SUBMIT            = 17,
# Globus Submit failed      ULOG_GLOBUS_SUBMIT_FAILED     = 18,
# Globus Resource Up        ULOG_GLOBUS_RESOURCE_UP       = 19,
# Globus Resource Down      ULOG_GLOBUS_RESOURCE_DOWN     = 20

sub process ($$$$@) {
    my $fn = shift;
    my $tag = shift;
    my $job = shift;
    my $stamp = shift;
    # my $msg = join('', @_);

    # adjust global stamps
    $main::minstamp = $stamp if $main::minstamp > $stamp;
    $main::maxstamp = $stamp if $main::maxstamp < $stamp;

    if ( $tag eq '000' ) {
	# SUBMIT
	my ($host,$port,$node);
	($host,$port) = (myhost($1),$2) 
	    if ( $_[0] =~ /from host: <((?:\d{1,3}\.){3}\d{1,3}):(\d+)>/ );
	$node="$fn/$1" if ( $_[1] =~ /DAG Node: (.+)/ );
	$main::db{$job}{$tag} = [ $stamp, $host, $port, $node ];
	push( @{$main::node{$node}}, $job );
    } elsif ( $tag eq '017' ) {
	# GLOBUS SUBMIT
	my ($host,$port,$jm);
	$jm=$1 if ( $_[1] =~ /\/jobmanager-(.+)/ );
	($host,$port) = (myhost($1),$2) if ( $_[2] =~ m{://([^:/]+):(\d+)} );
	$main::db{$job}{$tag} = [ $stamp, $host, $port, $jm || 'fork' ];
	$main::host{$host}[2]++;
    } elsif ( $tag eq '001' ) {
	# EXECUTE
	my $host;
	$host=myhost($1) if ( $_[0] =~ /on host: (.*)/ );
	$main::db{$job}{$tag} = [ $stamp, $host ];
	$main::host{$host}[0]++;
    } elsif ( $tag eq '005' || $tag eq '016' ) {
	# (POST SCRIPT) TERMINATE
	my $ec;
	$ec=$1 if ( $_[1] =~ /return value (\d+)/ );
	$main::db{$job}{$tag} = [ $stamp, $ec ];
	if ( $ec != 0 ) {
	    $main::fail{$job}{$tag}{$stamp}=$ec;
	} else {
	    # remember good jobs
	    my $host = $main::db{$job}{'001'}[1];
	    $main::host{$host}[1]++ if ( $tag eq '016' );
	    $main::job{$job} = 1;
	}
    } elsif ( $tag eq '012' ) {
	# HELD
	my ($ec,$reason);
	($ec,$reason)=($1,$2) if ( $_[1] =~ /Globus error (\d+): (.+)/ );
	$main::db{$job}{$tag} = [ $stamp, $ec, $reason ];
	$main::fail{$job}{$tag}{$stamp}=$ec if ( $ec != 0 );
    } elsif ( $tag eq '013' ) {
	# RELEASED
	$main::db{$job}{$tag} = [ $stamp ];
    } elsif ( $tag eq '009' ) {
	# ABORTED (e.g. by condor_rm)
	push( @{$main::fail{$job}{$tag}}, $stamp );
    } else {
	# AOB
    }
}
