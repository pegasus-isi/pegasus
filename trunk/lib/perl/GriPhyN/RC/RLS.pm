package GriPhyN::RC::RLS;
#
# RLS RLI lookup, LRC update based replica manager implementation
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
# Revision : $Revision$
#
# $Id$
#
use 5.006;
use strict;
use vars qw($VERSION);
use Exporter;

our @ISA = qw(Exporter GriPhyN::RC);
our @EXPORT = qw();
our @EXPORT_OK = qw($VERSION);
our $MAXCMD = 100;		# something in RLS

$VERSION=$1 if ( '$Revision$' =~ /Revision:\s+([0-9.]+)/o );

#
# --- start -----------------------------------------------------
#
use Carp;
use File::Spec;
use File::Temp;
use GriPhyN::Log qw(log);

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
		  -r File::Spec->catfile( $fn, 'org', 'griphyn', 'cPlanner',
					  'toolkit', 'RlsCli.class' ) ) {
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
    my $self = bless GriPhyN::RC->new( style => 'LRC', @_ ), $class;
    
    # update the environment
    $ENV{'GLOBUS_LOCATION'} ||=
	$self->{'globus.location'} ||
	croak( $prefix, 'The $GLOBUS_LOCATION environment variable is unset' );
    my $globus = $ENV{'GLOBUS_LOCATION'};

    # update and check globus-rls-cli location unless specified
    $self->{grc} ||= File::Spec->catfile( $globus, 'bin', 'globus-rls-cli' );
    croak( $prefix, "Unable to execute ", $self->{grc} ) 
	unless -x $self->{grc};
    
    # register via Pegasus' rls-client
    $ENV{'VDS_HOME'} ||=
	$self->{'vds.home'} || 
	croak( $prefix, 'The $VDS_HOME environment variables is unset' );
    # update and check rls-client location unless specified
    $self->{'rls.client'} ||= File::Spec->catfile( $ENV{'VDS_HOME'}, 
						   'bin', 'rls-client' );
    croak( $prefix, "Unable to execute ", $self->{'rls.client'} ) 
	unless -x $self->{'rls.client'};

    # which one is the RLS/LRC we want to use?
    croak( $prefix, 'I require an LRC to be able to do things' )
	unless defined $self->{lrc};
    $self->{rli} ||= $self->{lrc}; # make identical if unspecified
    $self->{pool} ||= undef;

    # more sanity checks
    $ENV{'JAVA_HOME'} ||=
	$self->{'java.home'} ||
	croak( $prefix, 'The $JAVA_HOME environment variable is unset' );
    my $java = File::Spec->catfile( $ENV{'JAVA_HOME'}, 'bin', 'java' );
    croak( $prefix, "Unable to execute $java" ) unless -x $java;

    # check the CLASSPATH setup
    $ENV{CLASSPATH} ||=	$self->{'java.classpath'};
    $self->classpath();		

    # check grid proxy
    my $gpi = File::Spec->catfile( $globus, 'bin', 'grid-proxy-info' );
    croak( $prefix, "Unable to execute $gpi" ) unless -x $gpi;
    
    # check remaining time on user grid proxy
    my @out = $self->pipe_out_cmd( undef, $gpi, '-timeleft' );
    croak( $prefix, "While executing $gpi: ", @out ) if ( $? );
    my $left = 0 + $out[0];
    croak( $prefix, "Only $left s left on your grid proxy, please refresh" )
	if $left < 3600;

    # done
    $self;
}

sub pipe_out_cmd {
    # purpose: bypass /bin/sh when executing an external command
    # paramtr: $stdin_fn (IN): use undef if unused
    #          @_: any other argument, starting with executable
    # returns: lines collected from stdout and stderr.
    # globals: $? will be set to the exit code
    my $self = shift;
    my $input = shift;		# filename to become STDIN
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
	if ( defined $input ) {
	    open( STDIN, "<$input" ) || exit(126);
	}
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

# Preloaded methods go here.

sub insert {
    # purpose: insert an LFN with all PFNs into the RC
    # paramtr: $lfn (IN): the LFN to insert
    #          @pfn (IN): list of PFNs to insert
    # returns: true if inserted, false if not (e.g. existed or failure)
    my $self = shift;
    my $lfn = shift || croak $prefix, "what is the LFN";
    croak( $prefix, "there are no PFNs" ) unless @_ > 0;
    
    my $result = 0;
    foreach my $pfn ( @_ ) {
	my @arg = ( $self->{'rls.client'}, 
		    '--delimiter=@',
		    '--pool=' . $self->{pool}, 
		    '--lrc=' . $self->{lrc},
		    "--mappings=$lfn,$pfn" );
	my @temp = $self->pipe_out_cmd( undef, @arg );

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
    }

    $result;
}

sub bulk_insert {
    # purpose: insert a mapping from unique LFNs to all PFNs into the RC
    # paramtr: { $lfn => [ $pfn1 ... ], ... }
    # returns: number of successful insertions
    my $self = shift;
    my $href = shift || croak $prefix, "need a hash reference";

    # use bulk mode
    my ($fd,$fn) = File::Temp::tempfile( 'rls-XXXXXX', 
					 UNLINK => 1, 
					 DIR => File::Spec->tmpdir() );

    # create temporary file with triples
    my $result = 0;
    foreach my $lfn ( keys %{$href} ) {
	foreach my $pfn ( @{ $href->{$lfn} } ) {
	    print $fd "$lfn,$pfn,", $self->{pool}, "\n";
	    $result++;
	}
    }
    close($fd);

    # create CLI and execute
    my @arg = ( $self->{'rls.client'}, 
		'--lrc=' . $self->{lrc}, 
		'--file=' . $fn );
    my @temp = $self->pipe_out_cmd( undef, @arg );
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
    # returns: -
    my $self = shift;
    my $lfn = shift || croak $prefix, "what is the LFN";
    croak( $prefix, "what are the PFN mappings" ) unless @_ > 0;

    my (@arg,@temp);
    foreach my $pfn ( @_ ) {
	@arg = ( $self->{grc}, 'delete', $lfn, $pfn, $self->{lrc} );
	@temp = $self->pipe_out_cmd( undef, @arg ); # ignore any results
	if ( ($? & 127) > 0 ) {
	    # *died* on signal - this is not good
	    croak( $prefix, "died on signal @{[$?&127]}: ", 
		   @temp > 5 ? @temp[0..5] : @temp );
	}
    }
    undef;
}

sub bulk_remove {
    # purpose: remove all mapping for a given LFN
    # paramtr: one or more LFNs
    # returns: number of LFNs found.
    my $self = shift;
    croak( $prefix, "what is the LFN" ) unless @_ > 0;

    # determine all input PFNs
    my %pfnset = $self->bulk_lookup( @_ );

    # use bulk mode via interactive interface
    my ($fd,$fn) = File::Temp::tempfile( 'rls-XXXXXX', 
					 UNLINK => 1, 
					 DIR => File::Spec->tmpdir() );
    my $result = 0;
    my $line = 'bulk delete';
    my $count = 2;		# "bulk" + "delete"
    foreach my $lfn ( keys %pfnset ) {
	my $flag = 0;
	foreach my $pfn ( @{$pfnset{$lfn}} ) {
	    if ( ( length($line) > 11 && 
		   length($line)+length($lfn)+length($pfn) > 7998 ) ||
		 $count >= $MAXCMD ) {
		print $fd "$line\n";
		$line = 'bulk delete';
		$count = 2;
	    }
	    $line .= " $lfn $pfn";
	    $count += 2;
	    $flag++;
	}
	$result++ if $flag;
    }
    print $fd "$line\n" if ( length($line) > 11 || $count > 2 );
    close($fd);

    if ( $result > 0 ) {
	my @temp = $self->pipe_out_cmd( $fn, $self->{grc}, $self->{lrc} );
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
    } else {
	# nothing to do
	undef $result;
	unlink($fn);
    }

    # done
    $result;
}

sub lookup {
    my $self = shift;
    my $lfn = shift || croak $prefix, "what is the LFN";

    my @arg = ( $self->{grc}, 'query', 'rli', 'lfn', $lfn, $self->{rli} );
    my @temp = $self->pipe_out_cmd( undef, @arg );

    my @lrc = ();
    if ( $? & 127 ) {
	# *died* on signal - this is not good
	croak( $prefix, "died on signal @{[$?&127]}: ", 
	       @temp > 5 ? @temp[0..5] : @temp );
    } else {
	# all is well - or not? what about exitcode != 0?
	foreach ( @temp ) {
	    s/^\s+//;
	    s/[\r\n ]+$//;	# chomp
	    my ($new,$lrc) = split( /:\s+/, $_, 2 );
	    push( @lrc, $lrc ) if $new eq $lfn;
	}
    }

    # now query each LRC
    my @result = ();
    foreach my $lrc ( @lrc ) {
	@arg = ( $self->{grc}, 'query', 'lrc', 'lfn', $lfn, $lrc );
	@temp = $self->pipe_out_cmd( undef, @arg );

	if ( $? & 127 ) {
	    # died on signal - not good
	    croak( $prefix, "died on signal @{[$?&127]}: ", 
		   @temp > 5 ? @temp[0..5] : @temp );
	} elsif ( $? == 0 ) {
	    # all is well
	    foreach ( @temp ) {
		s/^\s+//;
		s/[\r\n ]+$//;	# chomp
		my ($new,$pfn) = split( /:\s+/, $_, 2 );
		push( @result, $pfn ) if $new eq $lfn;
	    }
	} else {
	    # error -- ignore
	    log( $prefix, "ERROR while querying $lrc: ",
		 @temp > 5 ? @temp[0..5] : @temp );
	}
    }

    # done
    wantarray ? @result : $result[0];
}

sub bulk_lookup {
    my $self = shift;
    croak( $prefix, "what are the LFNs" ) unless @_ > 0;

    # use bulk mode via interactive interface
    my ($fd,$fn) = File::Temp::tempfile( 'rls-XXXXXX', 
					 UNLINK => 1, 
					 DIR => File::Spec->tmpdir() );
    my %result = ();
    my $line = 'bulk query rli lfn';
    my $count = 4;
    foreach my $lfn ( @_ ) {
	if ( (length($line) > 18 && length($line)+length($lfn) > 7998) ||
	     $count >= $MAXCMD ) {
	    print $fd "$line\n";
	    $line = 'bulk query rli lfn';
	    $count = 4;
	}
	$line .= " $lfn";
	$count++;
	$result{$lfn} = [];
    }
    print $fd "$line\n" if ( length($line) > 18 || $count > 4 );
    close($fd);

    my @temp = $self->pipe_out_cmd( $fn, $self->{grc}, $self->{rli} );
    unlink($fn);

    if ( $? & 127 ) {
	# *died* on signal - this is not good
	croak( $prefix, "died on signal @{[$?&127]}: ", 
	       @temp > 5 ? @temp[0..5] : @temp );
    } else {
	# all is well - or not? what about exitcode != 0?

	# walk @temp only once
	my %lrc = ();		# collect which LRC to ask what
	foreach ( @temp ) {
	    s/^\s+//;
	    s/[\r\n ]+$//;	# chomp
	    my ($new,$lrc) = split( /:\s+/, $_, 2 );
	    push( @{$lrc{$lrc}}, $new ) 
		if index( $_, 'LFN doesn\'t exist' ) == -1;
	}

	# foreach LRC query em (sigh)
	foreach my $lrc ( sort keys %lrc ) {
	    ($fd,$fn) = File::Temp::tempfile( 'lrc-XXXXXX', UNLINK => 1,
					      DIR => File::Spec->tmpdir() );
	    $line = 'bulk query lrc lfn';
	    $count = 4;
	    foreach my $lfn ( @{$lrc{$lrc}} ) {
		if ( length($line) > 18 && length($line)+length($lfn) > 7998 ||
		     $count >= $MAXCMD ) {
		    print $fd "$line\n";
		    $line = 'bulk query lrc lfn';
		    $count = 4;
		}
		$line .= " $lfn";
		$count ++;
	    }
	    print $fd "$line\n" if ( length($line) > 18 || $count > 4 );
	    close $fd;

	    @temp = $self->pipe_out_cmd( $fn, $self->{grc}, $lrc );
	    unlink $fn;

	    if ( $? & 127 ) {
		# died on signal - not good
		croak( $prefix, "died on signal @{[$?&127]}: ", 
		       @temp > 5 ? @temp[0..5] : @temp );
	    } elsif ( $? == 0 ) {
		# all is well
		foreach ( @temp ) {
		    s/^\s+//;
		    s/[\r\n ]+$//;	# chomp
		    my ($new,$pfn) = split( /:\s+/, $_, 2 );
		    push( @{$result{$new}}, $pfn ) 
			  if index( $_, 'LFN doesn\'t exist' ) == -1;
		}
	    } else {
		# error -- ignore
		log( $prefix, "ERROR while querying $lrc: ",
		     @temp > 5 ? @temp[0..5] : @temp );
	    }
	}
    }

    # return a hash with all results
    %result;
}

sub wildcard_lookup {
    # purpose: look-up LFNs that match with asterisk wildcarding
    # paramtr: $pattern (IN): pattern to match
    # returns: a hash with the LFN as key, and a list of PFNs as value
    my $self = shift;
    my $pattern = shift || croak $prefix, "what is the LFN pattern";
    my %result = ();

    # WARNING: This will _not_ work with bloom filters!
    croak( $prefix, "method not implemented!" );

    # return a hash with all results
    %result;
}


# Autoload methods go after =cut, and are processed by the autosplit program.

#
# return 'true' to package loader
#
1;
__END__
