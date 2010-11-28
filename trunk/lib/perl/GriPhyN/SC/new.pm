package GriPhyN::SC::new;
#
# new-style multi-line formatted Site Catalog file
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

our @ISA = qw(Exporter GriPhyN::SC);
our @EXPORT = qw();
our @EXPORT_OK = qw($VERSION);

$VERSION=1.0;
$VERSION=$1 if ( '$Revision$' =~ /Revision:\s+([0-9.]+)/o );

#
# --- start -----------------------------------------------------
#
use Carp;
use File::Spec;
use GriPhyN::Log qw(log);
use GriPhyN::WF qw/parse_properties/;
use GriPhyN::Scanner qw(%classes @classes);

our $prefix = '[' . __PACKAGE__ . '] ';
my %lookahead = ();

sub parse_pool($) {
    my $scanner = shift;
    my %token = $scanner->next();

    if ( $token{class} ne $classes{IDENTIFIER} ) {
	carp( $prefix, 'line ', $scanner->lineno,
	      ": Expected identifier after keyword \"site\", got ", 
	      $token{class}, "/", $token{value} );
	return undef;
    }
    my $poolid = $token{value};
	    
    %token = $scanner->next();
    if ( ! ( $token{class} == $classes{VERBATIM} && $token{value} eq '{' ) ) {
	carp( $prefix, 'line ', $scanner->lineno,
	      ": Expected open brace to introduce site $poolid" );
	return undef;
    }

    my %pool = ();
    for (;;) {
	%token = $scanner->next();
	if ( $token{class} == $classes{VERBATIM} && $token{value} eq '}' ) {
	    last;
	} elsif ( ! $token{class} == $classes{IDENTIFIER} ) {
	    carp( $prefix, 'line ', $scanner->lineno,
		  ": Expected keyword inside site body of $poolid" );
	    next;
	}

	my $what = lc($token{value});
	if ( $what eq 'workdir' || $what eq 'lrc' || 
	     $what eq 'sysinfo' || $what eq 'gridlaunch' ) {
	    %token = $scanner->next();
	    unless ( $token{class} == $classes{STRING} ) {
		carp( $prefix, 'line ', $scanner->lineno,
		      ": $poolid: $what must be followed by a quoted string" );
		next;
	    }
	    if ( $what eq 'lrc' ) {
		# multi-value
		push( @{$pool{$what}}, $token{value} );
	    } else {
		# once only
		$pool{$what} = $token{value};
	    }
	} elsif ( $what eq 'gridftp' ) {
	    %token = $scanner->next();
	    unless ( $token{class} == $classes{STRING} ) {
		carp( $prefix, 'line ', $scanner->lineno,
		      ": $poolid: $what must be followed by a gridftp URI" );
		next;
	    }
	    my $uri = $token{value};

	    %token = $scanner->next();
	    unless ( $token{class} == $classes{STRING} ) {
		carp( $prefix, 'line ', $scanner->lineno,
		      ": $poolid: 2nd arg to $what is a quoted string" );
		next;
	    }

	    push( @{$pool{$what}}, { uri => $uri, 
				     version => $token{value} } );
	} elsif ( $what eq 'universe' || $what eq 'profile' ) {
	    %token = $scanner->next();
	    unless ( $token{class} == $classes{IDENTIFIER} ) {
		carp( $prefix, 'line ', $scanner->lineno,
		      ": $poolid: $what must be followed by an identifier" );
		next;
	    }
	    my $universe = $token{value};

	    %token = $scanner->next();
	    unless ( $token{class} == $classes{STRING} ) {
		carp( $prefix, 'line ', $scanner->lineno,
		      ": $poolid: 2nd argument to $what is a string" );
		next;
	    }
	    my $jobmanager = $token{value};

	    %token = $scanner->next();
	    unless ( $token{class} == $classes{STRING} ) {
		carp( $prefix, 'line ', $scanner->lineno,
		      ": $poolid: 3rd arg to $what is another string" );
		next;
	    }

	    # note: This also applies to profiles
	    push( @{$pool{$what}}, { universe => $universe, 
				     jobmanager => $jobmanager, 
				     version => $token{value} } );
	} else {
	    carp( $prefix, 'line ', $scanner->lineno,
		  ": Unknown keyword $what in site $poolid" );
	}
    }

    # done
    { poolid => $poolid, site => { %pool } };
}

sub sanitize {
    my $vref = shift || die;
    my @result = ();
    foreach my $what ( @{$vref} ) {
	push @result, $what if ( (grep { /$what/ } @result) == 0 );
    }
    [ @result ];
}
    
sub forward($) {
    # purpose: resynch input stream after error
    # paramtr: $scanner
    # returns: token of EOF or IDENTIFIER/site
    my $scanner = shift || die;
    for (;;) {
	my %token = $scanner->next;
	return %token if ( ( $token{class} == $classes{EOF} ) ||
			   ( $token{class} == $classes{IDENTIFIER} && 
			     lc($token{value}) eq 'site' ) );
    }
}

sub parse_new($) {
    my $fn = shift;
    log( "reading from $fn" ) if ( $main::DEBUG & 0x10 );

    my $scanner = GriPhyN::Scanner->new($fn) || 
	croak $prefix, "open textual SC file $fn: $!\n";

    my %result = ();
    for (;;) {
	my %token = $scanner->next();
      RESYNC:
	last if $token{class} == $classes{EOF};
	if ( $token{class} == $classes{SPECIAL} ) {
	    # cheat token
	    $result{$token{value}}->{special} = [ @{$token{extra}} ];
	    $result{$token{value}}->{ncpus} = $token{extra}->[3];
	    next;
	}

	if ( $token{class} == $classes{IDENTIFIER} &&
	     lc($token{value}) eq 'site' ) {
	    my $temp = parse_pool($scanner);
	    if ( defined $temp && scalar %{$temp->{site}} ) {
		my $id = $temp->{poolid};
		log( $prefix, "Adding entry for $id" ) 
		    if ( $main::DEBUG & 0x10 );
		
		$result{$id}->{gridshell} = $temp->{site}->{gridlaunch};
		$result{$id}->{lrc} = sanitize($temp->{site}->{lrc});
		$result{$id}->{workdir} = $temp->{site}->{workdir};
		my @gftp = ();
		foreach my $x ( @{$temp->{site}->{gridftp}} ) {
		    if ( (grep { /$x->{uri}/ } @gftp) == 0 ) {
			push( @gftp, $x->{uri} );
			push( @{$result{$id}->{gridftp}},
			      [ GriPhyN::SC::break_uri($x->{uri}), 
				(split(/\./,$x->{version}))[0..2] ] );
		    }
		}

		foreach my $x ( @{$temp->{site}->{universe}} ) {
		    push( @{$result{$id}->{contact}->{$x->{universe}}},
			  [ $x->{jobmanager}, undef, 
			    (split(/\./,$x->{version}))[0..2] ] );
		}

		foreach my $x ( @{$temp->{site}->{profile}} ) {
		    # universe=>namespace, jobmanager=>key, version=>value
		    $result{$id}->{profile}->{$x->{universe}}->{$x->{jobmanager}} =
			$x->{version};
		}
	    } else {
		carp( $prefix, 'line ', $scanner->lineno, 
		      ": Illegal entry for site\n" );
		%token=forward($scanner);
		goto RESYNC;
	    }
	} else {
	    carp( $prefix, 'line ', $scanner->lineno, 
		  ": Illegal token $token{class}, expected resword \"site\"\n" );
	    %token = forward($scanner);
	    goto RESYNC;
	}
    }
    undef $scanner;		# done

    GriPhyN::SC::sanitize( %result );
    %result;
}

#
# ctor
#
sub new {
    my $proto = shift;
    my $class = ref($proto) || $proto || __PACKAGE__;
    my $self = bless GriPhyN::SC->new( style => 'new', @_ ), $class;
    $self->{'_permitted'}->{file} = 1;

    if ( exists $self->{file} && ! defined $self->{file} ) {
	# special case file => undef
	$self->{site} = { };
    } else {
	my $file = $self->file || 
	    croak $prefix, "no location specified (missing property ...file)"; 
	$self->{site} = { parse_new($file) };
    }

    # return handle to self
    $self;
}

sub DESTROY {
    # empty
}

sub make_version {
    defined $_[2] ? join('.',@_[0..2] ) : join('.',@_[0..1]);
}

sub show_site { 
    # purpose: dumps one site entry into a stream
    # paramtr: $fh (IN): filehandle open for writing
    #          $site (IN): site handle to show
    # returns: -
    my $self = shift;
    my $fh = shift || croak $prefix, "need an open filehandle";
    croak $prefix, "need an open filehandle" unless ref $fh;
    my $site = shift || croak $prefix, "need a site parameter";
    return unless exists $self->{site}->{$site};
    my $sref = $self->{site}->{$site};

    # preamble
    if ( exists $sref->{special} ) {
	print $fh "#\n";
	printf $fh "# [%u] %s @ %s : %u\n", @{$sref->{special}};
	print $fh "#\n";
    }

    # site entry
    print $fh "site $site {\n";
    foreach my $ns ( sort keys %{ $sref->{profile} } ) {
	foreach my $key ( sort keys %{ $sref->{profile}->{$ns} } ) {
	    print $fh "  profile $ns \"$key\" \"";
	    print $fh $sref->{profile}->{$ns}->{$key}, "\"\n";
	}
    }
    foreach my $lrc ( @{ $sref->{lrc} } ) {
	print $fh "  lrc        \"$lrc\"\n";
    }
    if ( exists $self->{sysinfo} ) {
	print $fh "  sysinfo    \"", $self->{sysinfo}, "\"\n";
    }
    foreach my $ftp ( @{ $sref->{gridftp} } ) {
	print $fh "  gridftp    \"", $ftp->[0], $ftp->[1], '"';
	print $fh ' "', make_version($ftp->[2],$ftp->[3],$ftp->[4]), "\"\n";
    }
    print $fh "  gridlaunch \"", $sref->{gridshell}, "\"\n";
    print $fh "  workdir    \"", $sref->{workdir}, "\"\n";
    foreach my $universe ( reverse sort keys %{ $sref->{contact} } ) {
	foreach my $jm ( @{$sref->{contact}{$universe}} ) {
	    printf $fh "  universe %-9s", $universe;
	    print $fh " \"", $jm->[0], "\""; 
	    print $fh " \"", make_version($jm->[2],$jm->[3],$jm->[4]), "\"\n";
	}
    }
    print $fh "}\n"; 
}

#
# return 'true' to package loader
#
1;
__END__
