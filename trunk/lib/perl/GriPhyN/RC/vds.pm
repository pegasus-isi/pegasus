package GriPhyN::RC::vds;
#
# Implementation based on new RC API -- uses CLI exclusively for access.
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
# Author: Jens-S. Vöckler voeckler at cs dot uchicago dot edu
# Revision : $Revision$
#
# $Id$
#
use 5.006;
use strict;
use Exporter;
use vars qw/$VERSION/;

our @ISA = qw(Exporter GriPhyN::RC);
our @EXPORT = qw();
our @EXPORT_OK = qw($VERSION);

$VERSION=$1 if ( '$Revision$' =~ /Revision:\s+([0-9.]+)/o );

#
# --- start -----------------------------------------------------
#
use Carp;
use DBI;
use File::Spec;
use File::Temp;
use GriPhyN::Log qw/log/;

our $prefix = '[' . __PACKAGE__ . '] ';
    
sub classpath {
    # purpose: check that we can access any (rls|gvds).jar for Pegasus
    # paramtr: -
    # returns: -
    my $self = shift;
    my $fn;

    # create classpath
    if ( $ENV{CLASSPATH} eq '' ) {
	# empty classpath, uh-oh
	my $vdshome = $ENV{'VDS_HOME'} || 
	    croak( $prefix, "hey, I need the VDS_HOME to be set" );
	my $dir = File::Spec->catfile( $vdshome, 'lib' );

	# find all VDS jars
	my @result = ();
	if ( opendir( DIR, $dir ) ) {
	    foreach my $jar ( grep { /\.jar$/ } readdir(DIR) ) {
		push( @result, $fn ) 
		    if ( -x ($fn = File::Spec->catfile( $dir, $jar )) );
	    }
	    closedir(DIR);
	} else { 
	    carp $prefix, "unable to read dir $dir: $!";
	}

	# create new classpath
	$ENV{CLASSPATH} = join(':',@result);
    }

    # check for gvds.jar
    my $vds = $ENV{'VDS_HOME'};
    if ( index( $ENV{CLASSPATH}, 'gvds.jar' ) == -1 ) {
	# final attempt
	if ( -r ($fn = File::Spec->catfile( $vds, 'lib', 'gvds.jar' )) ) {
	    # user environment
	    $ENV{CLASSPATH} .= ":$fn";
	} elsif ( -d ($fn=File::Spec->catfile( $vds, 'build', 'classes')) &&
		  -r File::Spec->catfile( $fn, 'org', 'griphyn', 'common',
					  'catalog', 'ReplicaCatalog.class' ) ) {
	    # devel environment
	    $ENV{CLASSPATH} = "$fn:" . $ENV{CLASSPATH} 
	        if ( index($ENV{CLASSPATH},$fn) == -1 );
	} else {
	    croak $prefix, "Unable to detect the presence of gvds.jar";
	}
    }

    # check for rls.jar
    if ( index( $ENV{CLASSPATH}, 'rls.jar' ) == -1 ) {
	# final attempts
	if ( -r ($fn = File::Spec->catfile( $vds, 'lib', 'rls.jar' )) ) {
	    $ENV{CLASSPATH} .= ":$fn";
	} elsif ( -r ($fn = File::Spec->catfile( $ENV{'GLOBUS_LOCATION'}, 
						 'lib', 'rls.jar' ) ) ) {
	    $ENV{CLASSPATH} .= ":$fn";
	} else {
	    croak $prefix, "Unable to detect the presence of rls.jar";
	}
    }

    # done
    undef;
}

#
# ctor
#
sub new {
    my $proto = shift;
    my $class = ref($proto) || $proto || __PACKAGE__;
    my $self = bless GriPhyN::RC->new( style => 'vds', @_ ), $class;
    
    # register via rc-client (shrc for now)
    $ENV{'VDS_HOME'} ||=
	$self->{'vds.home'} || 
	croak( $prefix, 'The $VDS_HOME environment variables is unset' );
    # update and check rls-client location unless specified
    $self->{'rc.client'} ||= File::Spec->catfile( $ENV{'VDS_HOME'}, 
						  'bin', 'rc-client' );
    croak( $prefix, "Unable to execute ", $self->{'rc.client'} ) 
	unless -x $self->{'rc.client'};

    # more sanity checks
    $ENV{'JAVA_HOME'} ||=
	$self->{'java.home'} ||
	croak( $prefix, 'The $JAVA_HOME environment variable is unset' );
    my $java = File::Spec->catfile( $ENV{'JAVA_HOME'}, 'bin', 'java' );
    croak( $prefix, "Unable to execute $java" ) unless -x $java;

    # check the CLASSPATH setup
    $ENV{CLASSPATH} ||=	$self->{'java.classpath'};
    $self->classpath();		

    # return handle to self
    $self->{pool} ||= undef;
    $self;
}

sub pipe_out_cmd {
    # purpose: bypass /bin/sh when executing an external command
    # paramtr: @_: any other argument, starting with executable
    # returns: lines collected from stdout and stderr.
    # globals: $? will be set to the exit code
    my $self = shift;
    my @result = ();
    local(*READ);

    log( $prefix, join(' ',@_) ) if ( $main::DEBUG & 0x100 );
    my $pid = open( READ, "-|" );
    return undef unless defined $pid;

    if ( $pid ) {
	# parent
	@result=<READ>;
	close(READ);
    } else {
	# child
	open( STDERR, ">&STDOUT" ) || exit(126);
	select(STDERR); $|=1;
	select(STDOUT); $|=1;
	# $^F = fileno(STDERR);
	exec { $_[0] } @_;
	exit(127);
    }
    
    # return the perl way
    @result;
}

sub insert {
    # purpose: insert a LFN with all PFNs into the RC
    # paramtr: $lfn (IN): the LFN to insert
    #          @pfn (IN): list of PFNs to insert
    # returns: true if inserted, false if not (e.g. existed or failure)
    my $self = shift;
    my $lfn = shift || croak $prefix, "what is the LFN";
    croak( $prefix, "there are no PFNs" ) unless @_ > 0;

    # use bulk mode
    my ($fd,$fn) = File::Temp::tempfile( 'rc-XXXXXX', 
					 UNLINK => 1, 
					 DIR => File::Spec->tmpdir() );

    # create temporary file with triples
    my $result = 0;
    my $addon = ( defined $self->{pool} && length($self->{pool}) ) ? 
	' pool="' . $self->{pool} . '"' : '';
    foreach my $pfn ( @_ ) {
	print $fd "insert $lfn $pfn$addon\n";
	$result++;
    }
    close $fd;

    # create CLI and execute
    my @arg = ( $self->{'rc.client'}, '-f', $fn );
    my @temp = $self->pipe_out_cmd( @arg );
    unlink $fn;

    # check result
    if ( $? == 0 ) {
	# all is well
	$result++;
    } elsif ( ($? & 127) > 0 ) {
	# *died* on signal - this is not good
	croak( $prefix, "died on signal @{[$?&127]}: ", 
	       @temp > 5 ? @temp[0..5] : @temp );
    } else {
	# leaves non-zero exit code
	;			# ignore
    }

    $result;
}

sub bulk_insert {
    # purpose: insert a mapping from unique LFNs to all PFNs into the RC
    # paramtr: { $lfn => [ $pfn1 ... ], ... }
    # returns: number of successful insertions
    # warning: override with more efficient implementation.
    my $self = shift;
    my $href = shift || croak $prefix, "need a hash reference";
    croak( $prefix, 'need a hash reference' ) unless ref($href) eq 'HASH';

    # use bulk mode
    my ($fd,$fn) = File::Temp::tempfile( 'rc-XXXXXX', 
					 UNLINK => 1, 
					 DIR => File::Spec->tmpdir() );

    # create temporary file with triples
    my $result = 0;
    my $addon = ( defined $self->{pool} && length($self->{pool}) ) ? 
	' pool="' . $self->{pool} . '"' : '';
    foreach my $lfn ( keys %{$href} ) {
	foreach my $pfn ( @{ $href->{$lfn} } ) {
	    print $fd "insert $lfn $pfn$addon\n";
	    $result++;
	}
    }
    close($fd);

    # create CLI and execute
    my @arg = ( $self->{'rc.client'}, '-f', $fn );
    my @temp = $self->pipe_out_cmd( @arg );
    unlink($fn);

    # check result
    if ( $? == 0 ) {
	# all is well
	;			# do nothing
    } elsif ( ($? & 127) > 0 ) {
	# *died* on signal - this is not good
	croak( $prefix, "died on signal @{[$?&127]}: ", 
	       @temp > 5 ? @temp[0..5] : @temp );
    } else {
	# leaves non-zero exit code
	undef $result;
    }

    # done
    $result;
}

sub remove {
    # purpose: remove a specifc PFN mapping for a given LFN
    # paramtr: $lfn (IN): LFN to work with
    #          @pfn (IN): PFN(s) to remove
    # returns: number of deleted columns;
    my $self = shift;
    my $lfn = shift || croak $prefix, "what is the LFN";
    croak( $prefix, "what are the PFN mappings" ) unless @_ > 0;

    # use bulk mode via interactive interface
    my ($fd,$fn) = File::Temp::tempfile( 'rc-XXXXXX', UNLINK => 1, 
					 DIR => File::Spec->tmpdir() );

    my $result = 0;
    foreach my $pfn ( @_ ) {
	print $fd "delete $lfn $pfn\n";
	$result++;
    }
    close $fd;

    my @arg = ( $self->{'rc.client'}, '-f', $fn );
    my @temp = $self->pipe_out_cmd( @arg );
    unlink($fn);

    # check result
    if ( $? == 0 ) {
	# all is well
	;			# do nothing
    } elsif ( ($? & 127) > 0 ) {
	# *died* on signal - this is not good
	croak( $prefix, "died on signal @{[$?&127]}: ", 
	       @temp > 5 ? @temp[0..5] : @temp );
    } else {
	# leaves non-zero exit code
	undef $result;
    }

    # done
    $result;
}

sub bulk_remove {
    # purpose: remove all mapping for a given LFN
    # paramtr: one or more LFNs
    # returns: - (number of existing LFNs that were removed)
    my $self = shift;
    croak( $prefix, "what is the LFN" ) unless @_ > 0;

    # use bulk mode via interactive interface
    my ($fd,$fn) = File::Temp::tempfile( 'rc-XXXXXX', UNLINK => 1, 
					 DIR => File::Spec->tmpdir() );

    my $result = 0;
    foreach my $lfn ( @_ ) {
	print $fd "remove $lfn\n";
	$result++;
    }
    close $fd;

    my @arg = ( $self->{'rc.client'}, '-f', $fn );
    my @temp = $self->pipe_out_cmd( @arg );
    unlink($fn);

    # check result
    if ( $? == 0 ) {
	# all is well
	;			# do nothing
    } elsif ( ($? & 127) > 0 ) {
	# *died* on signal - this is not good
	croak( $prefix, "died on signal @{[$?&127]}: ", 
	       @temp > 5 ? @temp[0..5] : @temp );
    } else {
	# leaves non-zero exit code
	undef $result;
    }

    # done
    $result;
}

sub splitup {
    # purpose: splits the result of a lookup/list
    my @x;
    my %result = ();
    foreach ( @_ ) {
	@x = split / /, $_, 3;
	push( @{$result{$x[0]}}, $x[1] );
    }
    %result;
}

sub lookup {
    # purpose: look-up the PFNs for a given LFN
    # paramtr: $lfn (IN): the LFN to search for
    # returns: vector: list of PFNs for the given LFN (may be empty)
    #          scalar: first element of above list (may be undef)
    my $self = shift;
    my $lfn = shift || croak $prefix, "what is the LFN";

    my @arg = ( $self->{'rc.client'}, 'lookup', $lfn );
    my @temp = $self->pipe_out_cmd( @arg );

    # check result
    my @result = ();
    if ( $? == 0 ) {
	# all is well
	my %x = splitup(@temp);
	@result = values %x;
    } elsif ( ($? & 127) > 0 ) {
	# *died* on signal - this is not good
	croak( $prefix, "died on signal @{[$?&127]}: ", 
	       @temp > 5 ? @temp[0..5] : @temp );
    } else {
	# leaves non-zero exit code
	;			# ignore
    }

    wantarray ? @result : $result[0];
}

sub bulk_lookup {
    # purpose: look-up the PFNs for a list of LFNs
    # paramtr: @lfn (IN): the unique LFNs to search for
    # returns: a hash with the LFN as key, and a list of PFNs as value
    # warning: marginally more efficient. 
    my $self = shift;
    croak( $prefix, "what are the LFNs" ) unless @_ > 0;

    # use bulk mode via interactive interface
    my ($fd,$fn) = File::Temp::tempfile( 'rc-XXXXXX', UNLINK => 1, 
					 DIR => File::Spec->tmpdir() );

    my $result = 0;
    foreach my $lfn ( @_ ) {
	print $fd "lookup $lfn\n";
	$result++;
    }
    close $fd;

    my @arg = ( $self->{'rc.client'}, '-f', $fn );
    my @temp = $self->pipe_out_cmd( @arg );
    unlink($fn);

    my %result = ();
    if ( $? == 0 ) {
	# all is well
	%result = splitup(@temp);
    } elsif ( ($? & 127) > 0 ) {
	# *died* on signal - this is not good
	croak( $prefix, "died on signal @{[$?&127]}: ", 
	       @temp > 5 ? @temp[0..5] : @temp );
    } else {
	# leaves non-zero exit code
	;			# ignore
    }

    %result;
}

sub wildcard_lookup {
    # purpose: look-up LFNs that match with asterisk wildcarding
    # paramtr: $pattern (IN): pattern to match
    # returns: a hash with the LFN as key, and a list of PFNs as value
    my $self = shift;
    my $pattern = shift || croak $prefix, "what is the LFN pattern";

    my @arg = ( $self->{'rc.client'}, 'list', $pattern );
    my @temp = $self->pipe_out_cmd( @arg );

    # check result
    my %result = ();
    if ( $? == 0 ) {
	# all is well
	%result = splitup(@temp);
    } elsif ( ($? & 127) > 0 ) {
	# *died* on signal - this is not good
	croak( $prefix, "died on signal @{[$?&127]}: ", 
	       @temp > 5 ? @temp[0..5] : @temp );
    } else {
	# leaves non-zero exit code
	;			# ignore
    }

    %result;
}


# Autoload methods go after =cut, and are processed by the autosplit program.

#
# return 'true' to package loader
#
1;
__END__
