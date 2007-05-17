#
# Records the intent to run a job a certain site
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
# Author: Jens-S. Vöckler voeckler@cs.uchicago.edu
# Revision: $Revision: 1.5 $
#
package Site::Intent;
use 5.006;
use strict;
#use warnings;

use File::Basename qw(dirname);
use Fcntl qw(:DEFAULT :flock);
use DB_File;
use Errno qw(EINVAL);

require Exporter;
our @ISA = qw(Exporter);

# Items to export into callers namespace by default. Note: do not export
# names by default without a very good reason. Use EXPORT_OK instead.
# Do not simply export all your public functions/methods/constants.
our $VERSION='1.0';
$VERSION=$1 if ( '$Revision: 1.5 $' =~ /Revision:\s+([0-9.]+)/o );

# prototypes
sub create_lock($);		# { }
sub delete_lock($);		# { }

our %EXPORT_TAGS = ( lock => [ qw!create_lock delete_lock! ] );
our @EXPORT_OK = qw($VERSION create_lock delete_lock);
our @EXPORT = qw();

# auto-cleanup keys
my %atexit = ();
END { unlink( keys %atexit ) if scalar %atexit }

#
# --- methods ---------------------------------------------------
#
use Carp;

sub create_lockfile($) {
    # purpose: create a lock file NFS-reliably
    # warning: use create_lock, not this function
    # paramtr: $fn (IN): name of main file to lock
    # returns: 1 on success, undef on failure to lock
    my $tolock = shift || die;
    local(*LOCK);
    my $lock = "$tolock.lock";
    my $uniq = "$tolock.$$";

    if ( sysopen( LOCK, $uniq, O_CREAT|O_EXCL|O_TRUNC|O_WRONLY ) ) {
	warn( "created unique $uniq\n" ) if ( $main::DEBUG > 1 );
	$atexit{$uniq} = 1;
	syswrite( LOCK, "$$\n" );
	close LOCK;

	if ( link( $uniq, $lock ) ) {
	    # created link
	    warn( "hardlink locked\n" ) if ( $main::DEBUG > 1 );
	    $atexit{$lock} = 1;
	    unlink($uniq) && delete $atexit{$uniq};
	    return 1;
	} else {
	    # unable to create link, check errno
	    warn( "while locking: $!\n" ) if ( $main::DEBUG > 1 );
	    if ( (stat($uniq))[3] == 2 ) {
		# lock was still successful
		$atexit{$lock} = 1;
		warn( "link-count locked\n" ) if ( $main::DEBUG > 1 );
		return 1;
	    }
	}
    } else {
	warn( "Locking: open $uniq: $!\n" );
    }

    unlink $uniq;
    undef;
}

sub break_lock($) {
    # purpose: check for a dead lock file, and remove if dead
    # paramtr: $fn (IN): name of the file to create lock file for
    # returns: undef if the lock is valid, 1..2 if it was forcefully
    #          removed, and 0, if it could not be removed.
    my $fn = shift;
    my $lock = "$fn.lock";

    local(*LOCK);
    if ( open( LOCK, "<$lock" ) ) {
	my $pid = <LOCK>;
	close LOCK;
	chomp($pid);
	if ( kill( 0, $pid ) == 1 ) {
	    # process that owns lock still lives
	    warn( 'lock-owner ', $pid, ' still lives...' ) if $main::DEBUG;
	    undef;
	} else {
	    # process that owns lock is gone?
	    my $uniq = "$fn.$pid";
	    warn( 'lock-owner ', $pid, ' found dead, removing lock!' );
	    unlink($lock,$uniq);
	}
    }
}

sub create_lock($) {
    # purpose: blockingly wait for lock file creation
    # paramtr: $fn (IN): name of file to create lock file for
    # returns: 1: lock was created.
    my $fn = shift;
    my $retries = 0;
    while ( ! create_lockfile($fn) ) { 
	break_lock($fn) if ( ++$retries > 10 );

	my $towait = 5 * rand();
	warn( "lock on $fn is busy, retry $retries, waiting ", 
	      sprintf("%.1f s...",$towait) ) if $main::DEBUG;
	Time::HiRes::sleep($towait);
    }

    warn( "obtained lock for $fn\n" ) if $main::DEBUG;
    1;
}

sub delete_lock($) {
    # purpose: removes a lock file NFS-reliably
    # paramtr: $fn (IN): name of main file to lock
    # returns: 1 or 2 on success, undef on failure to unlock
    my $tolock = shift;
    my $result;

    if ( unlink("$tolock.$$") == 1 ) {
	delete $atexit{"$tolock.$$"};
	$result++;
    }
    if ( unlink("$tolock.lock") == 1 ) {
	delete $atexit{"$tolock.lock"};
	$result++;
    }
    warn( "released lock for $tolock\n" ) if $main::DEBUG;
    $result;
}

#
# ctor
#
sub new {
    # purpose: Initialize an instance variable
    # paramtr: $filename (IN): path to intent database file
    # returns: reference to instance, or undef in case of failure
    my $proto = shift;
    my $class = ref($proto) || $proto || __PACKAGE__;
    my $filename = shift || croak "c'tor requires a filename argument";

    # need to be able to mod/creat file
    if ( -e $filename ) {
	if ( ! -w _ ) {
	    $! = 1;			# EPERM
	    return undef;
	}
    } else {
	if ( ! -w dirname($filename) ) {
	    $! = 1;
	    return undef;
	}
    }

    # should be ok now
    bless { 'm_filename' => $filename }, $class; 
}

sub filename {
    # purpose: returns the name of the communication file
    my $self = shift;
    $self->{'m_filename'};
}

sub dbtie {
    # purpose: Lock a file and tie it to a hash
    # paramtr: $href (IO): reference to hash to be tied
    #          $ro (opt. IN): if true, open in read-only mode
    # returns: undef on error, underlying object otherwise
    no warnings;
    my $self = shift;
    my $href = shift;
    my $readonly = shift;

    # sanity check
    unless ( ref $href eq 'HASH' ) {
	$! = Errno::EINVAL;
	return undef;
    }

    my $fn = $self->filename;
    create_lock($fn) if ( $self->{'m_count'}{$$}++ == 0 );
    my $result = $readonly ? 
	tie( %{$href}, 'DB_File', $fn, O_RDONLY ) :
	tie( %{$href}, 'DB_File', $fn );
    unless ( defined $result ) {
	# remove lock on failure to tie
	my $saverr = $!;
	delete_lock($fn) if ( --$self->{'m_count'}{$$} == 0 );
	$! = $saverr;
    }
    $result;
}

sub locked {
    # purpose: detects already tied databases
    # returns: reference count for lock
    my $self = shift;
    $self->{'m_count'}{$$}+0;
}

sub dbuntie {
    # purpose: untie a hash and release the lock
    # paramtr: $href (IO): reference to hash to be untied
    # returns: -
    no warnings;
    my $self = shift;
    my $href = shift;

    # sanity check
    unless ( ref $href eq 'HASH' ) {
	$! = Errno::EINVAL;
	return undef;
    }

    untie %{$href};
    delete_lock($self->filename) if ( --$self->{'m_count'}{$$} == 0 );
}

sub clone {
    # purpose: obtains all current values into a copy
    # returns: a hash with key => value, may be empty
    my $self = shift;

    my (%db,%result);
    if ( $self->dbtie(\%db) ) {
	%result = ( %db );
	$self->dbuntie(\%db);
    }
    %result;
}

sub inc {
    # purpose: increment the count for a site handle
    # paramtr: $key (IN): key of value to increment
    #          $incr (opt. IN): increment, defaults to 1
    # warning: If the value is not a simple integer, it will be afterwards.
    # returns: new value, undef on error
    my $self = shift;
    my $key = shift || croak "Need a database key";
    my $incr = shift || 1;
    
    my (%db,$result);
    if ( $self->dbtie(\%db) ) {
	$result = ( $db{$key} += $incr );
	$self->dbuntie(\%db);
    }
    $result;
}

sub dec {
    # purpose: decrement the count for a site handle
    # paramtr: $key (IN): key of value to decrement
    #          $decr (opt. IN): decrement, defaults to 1
    # warning: If the value is not a simple integer, it will be afterwards.
    # returns: new value, undef in case of error
    my $self = shift;
    my $key = shift || croak "Need a database key";
    my $decr = shift || 1;
    
    my (%db,$result);
    if ( $self->dbtie(\%db) ) {
	$result = ( $db{$key} -= $decr );
	$self->dbuntie(\%db);
    }
    $result;
}

#
# return 'true' to package loader
#
1;
__END__


=head1 NAME

Site::Intent - provides NFS-safe locking around a BSD DB file.

=head1 SYNOPSIS

    use Site::Intent;

    my $i = Site::Intent->new( 'db.file' ) || die;
    
    my %db;
    if ( $i->dbtie(\%db) ) {
	# work on %db
        $i->dbuntie(\%db);
    } else {
	# complain
    }

    my %copy = $i->clone();

=head1 DESCRIPTION

The Site::Intent coordinates intentions between multiple concurrent
site-selector instances. For this reason, it provides access to a BSD DB
file to record arbitrary scalar intentions into. The BSD file is locked
using NFS-safe (so is hoped) file locks. 

=head1 METHODS

=over 4

=item Site::Intent::create_lock($filename)

This static function attempts to create a file lock around the specified
filename according to Linux conventions. It first creates a unique file
using the process id as unique suffix, then attempts to hardlink said
file to the filename plus suffix C<.lock>. The attempt is randomly
backed off to retry on failure to hardlink. Additionally, the link count
is checked to detect hidden success. 

This is a blocking function, and may block indefinitely on dead-locks,
despite occasional lock acquiry wake-ups.

=item Site::Intent::delete_lock($filename)

This static function deletes all lock files around the given filename. 
It should be a fast function, as no waiting is required. 

=item new( $filename )

The constructor records the filename as the BSD database file to either
create or to connect to. If the file does not exist yet, it will not be
created in the c'tor. However, some simple checks are employed to see,
if the file will be creatable and/or writable, should it not exist.

=item filename

is a simple accessor, returning the filename that was passed to the
constructor.

=item dbtie( $hashref )

This member increases the lock count for the database file, ties the
database file to the hash argument. The method is usually invoked with a
reference to a (true) hash, e.g.:

    dbtie( \%hash )

If the first argument is not a reference to a hash, the call will fail
with an undefined value, and $! is set to EINVAL. Failure to tie the
file will remove the acquired lock.

The return value is the result of the C<tie> call. It may be undefined
in case of failure to tie the hash.

=item locked 

returns the reference count for locks on the file. Refernce counters are
kept on a per-process basis. This is not thread safe.

=item dbuntie( $hashref )

unties the hash reference and relinquishes the lock. This method should
only be called, if the previous C<dbtie> operation was successful,
similar to opening and closing file handles, e.g.:

    if ( $i->dbtie ... ) {
	...
        $i->dbuntie ... 
    }

=item clone

This is a comprehensive function to copy all values from the database
into memory. Please note that you can create nasty dead-locks this way. 

=back

=head1 WARNINGS

Locking files can always lead to very nasty dead-locks, or worse, to WAR
and WAW situations. You should obtain the tie-n-lock once, do B<all>
your operations on the database, and then relinquish the lock. You
should also attempt to hold the lock for the least possible time.

The following is a B<bad example>:

    # BAD EXAMPLE
    my $i = Site::Intent->new('my.db');
    my (%db,$n);
    if ( $i->dbtie(\%db) ) {
	$n = $db{X};
        $i->dbuntie(\%db);
    }
    $n++;
    if ( $i->dbtie(\%db) ) {
	$db{X} = $n;
        $i->dbuntie(\%db);
    }

The above example, in the presence of concurrent instances of the above
program, will result in a write-after-write (WAW) conflict. Two
processes start. The first reads, say, zero from C<$db{X}>, and then is
time-sliced to the next one. That one reads happily C<$db{X}>, and again
obtains a zero. It may even write the 1 back, before the time slice
returns to the first process. Which now writes back a 1. What the
algorithm most likely had intended, however, was getting a 2 at this
point.

I hope you see the subtle flaws. And my reasoning for my
recommendations:

=over 4

=item 1 Try to do everything between one C<dbtie> and C<dbuntie> pair.

=item 2 Keep your I<critical section> short - computation wise. 

=back 

=head1 SEE ALSO

L<http://www.griphyn.org/>

=head1 AUTHOR

Jens-S. VE<ouml>ckler, C<voeckler at cs dot uchicago dot edu>

=head1 COPYRIGHT AND LICENSE

This file or a portion of this file is licensed under the terms of the
Globus Toolkit Public License, found in file GTPL, or at
http://www.globus.org/toolkit/download/license.html. This notice must
appear in redistributions of this file, with or without modification.

Redistributions of this Software, with or without modification, must
reproduce the GTPL in: (1) the Software, or (2) the Documentation or
some other similar material which is provided with the Software (if
any).

Copyright 1999-2004 University of Chicago and The University of Southern
California. All rights reserved.

=cut
