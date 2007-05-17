#!/usr/bin/env perl
#
# common functionality used in the various log processors
#
# $Id: Common.pm,v 1.1 2006/05/08 19:14:48 voeckler Exp $
#
package Common;
use 5.006;
use strict;
use subs qw(log);               # replace Perl's math log with logging

use Exporter;
our @ISA = qw(Exporter);

# create the function prototypes for type coercion
sub log;			# { }
sub unix2iso(;$);		# { }
sub iso2unix($);		# { }
sub find_exec($;@); 		# { }
sub read_submit_file($);	# { }
sub default_title($;$);		# { }

# create the export lists
our $VERSION = '0.1';
our %EXPORT_TAGS = ();
our @EXPORT = qw(log unix2iso iso2unix find_exec read_submit_file
		 default_title);
our @EXPORT_OK = qw($VERSION);

$VERSION=$1 if ( '$Revision: 1.1 $' =~ /Revision:\s+([0-9.]+)/o );
$__PACKAGE__::prefix = undef;

#
# --- start -----------------------------------------------------
#
use Carp;
use POSIX qw(strftime modf);
use Time::Local qw(timegm timelocal);
use Time::HiRes qw(gettimeofday);
use File::Basename;
use File::Spec;

sub utc2iso(;$) {
    # purpose: converts a UTC timestamp into an ISO timestamp for logging.
    # paramtr: $now (opt. IN): timestamp to convert, or current time
    # returns: ISO 8601 dense-formatted string representing the timestamp.
    my (@now);
    if ( @_ > 0 ) {
        @now = reverse POSIX::modf( shift() );
        $now[1] *= 1E6;
    } else {
        # no argument
        @now = Time::HiRes::gettimeofday();
    }
    
    my @tm = localtime($now[0]);
    sprintf( "%4d%02d%02dT%02d%02d%02d.%03d",
             $tm[5]+1900, $tm[4]+1, $tm[3],
             $tm[2], $tm[1], $tm[0], $now[1] / 1000 );
}

sub log {
    # purpose: print whatever onto log stream STDERR with timestamp prefix
    #          This log is a kind of debug log, containing lotsa infos
    # paramtr: any number of more parameters
    # returns: -
    my $prefix = unix2iso() . sprintf( ' %7s ', "[$$]" );
    print STDERR $prefix, @_, "\n"; # yes, permit buffering
}

sub unix2iso (;$) {
    # purpose: Convert a UTC timestamp into ISO 8601 notation
    # paramtr: $stamp (opt. IN): UTC seconds; defaults to current time
    # returns: a ISO 8601 compliant timestamp string
    #
    my $stamp = shift || time();
    my $offset = int($stamp) - timelocal( (gmtime($stamp))[0..5] );
    my @stamp = localtime($stamp);
    my $result = sprintf( "%04d-%02d-%02dT%02d:%02d:%02d", 
			  $stamp[5]+1900, $stamp[4]+1, $stamp[3],
			  $stamp[2], $stamp[1], $stamp[0] );
    $result .= ( ( $offset >= 0 ) ? '+' : '-' );
    $offset = abs($offset);
    $result .= sprintf( "%02d:%02d", $offset / 3600, ($offset % 3600) / 60 );
}

sub iso2unix ($) {
    # purpose: Convert a ISO 8601 timestamp into UTC seconds since epoch
    # paramtr: $stamp (IN): a ISO 8601 compliant time to convert
    # returns: UTC seconds since epoch
    #
    local $_ = shift;
    die unless /(\d{4})-?(\d{2})-?(\d{2})T(\d{2}):?(\d{2}):?(\d{2})/;

    my $stamp = timegm($6,$5,$4,$3,$2-1,$1-1900);
    die unless /\.(\d+)([-+])(\d{2}):?(\d{2})$/;

    my ($fraction,$pm,$offset) = ("0.$1",$2,$3*3600+$4*60);
    $stamp += $fraction;
    $stamp += (( $pm eq '-' ) ? $offset : -$offset);
}

sub find_exec($;@) {
    # purpose: determine location of given binary in $PATH
    # paramtr: $program (IN): basename of application to look for
    #          @additional (opt. IN): additional paths to check (e.g. ".")
    # returns: fully qualified path to binary, undef if not found
    #
    my $program = shift;

    local($_);
    foreach ( (File::Spec->path, @_) ) {
	my $fs = File::Spec->catfile( $_, $program );
	return $fs if -x $fs;
    }
    undef;
}


sub read_submit_file($) {
    # purpose: read the submit file and extracts commands as k-v-pairs
    # paramtr: $subfn (IN): path to submit file
    # globals: $main::debug (IN): If set, log the open read etc.
    # returns: hash representation of Condor submit file, with lc commands
    #
    my $subfn = shift;
    my %result = (); 

    log( "reading sub file $subfn" ) if $main::debug > 1;
    local(*SUB);
    my ($k,$v);
    if ( open( SUB, "<$subfn" ) ) {
	while ( <SUB> ) {
	    next if substr($_,0,1) eq '#';
	    s/[\r\n\t ]+$//;
	    s/^\s*//;
	    next unless length($_);

	    ($k,$v) = split /\s*=\s*/, $_, 2;
	    $v=substr($v,1,-1) 
		if ( substr($v,0,1) eq '"' or substr($v,0,1) eq "'" );
	    $k = lc($k) if ( $k =~ /^[a-z]/i );
	    $result{$k} = $v;
	}
	close SUB;
    } else {
	warn "Warning: open $subfn: $!, ignoring\n"; 
    }

    %result;
}

sub default_title($;$) {
    # purpose: Generate a default title string for a diagram
    # paramtr: $dagfn (IN): some filename to base title upon
    #          $start (opt. IN): a UTC seconds stamp, default DAG file mtime
    # returns: title string
    my $dagfn = shift;
    my @stat = stat( $dagfn );
    my $start = shift;		# may be undef
    if ( @stat == 0 ) {
	# use current time if stat failed
	$start = time() unless defined $start;
    } else {
	# use DAG file mtime 
	$start = $stat[9] unless defined $start;
    }

    my $result = POSIX::strftime( '%Y-%m-%d %H:%M%z', localtime($start) );
    $result .= ' by ' . scalar( getpwuid($stat[4]) );
    my @dir = split /\//, dirname(File::Spec->rel2abs($dagfn));
    if ( @dir >= 2 ) {
	$result .= ' [' . join('/',@dir[$#dir-1,$#dir]) . ']';
    } elsif ( @dir == 1 ) {
	$result .= ' [' . $dir[$#dir] . ']';
    }
    $result;
}

# must
1;

__END__
