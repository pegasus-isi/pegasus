package GriPhyN::WF;
#
# work-flow property manager
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
# Revision : $Revision: 7 $
#
# $Id: WF.pm 7 2007-05-17 20:39:13Z gmehta $
#
use 5.006;
use strict;
use Exporter;
use vars qw/$VERSION %system/;

our @ISA = qw(Exporter);
our @EXPORT = qw();

sub parse_properties($;\%);	# { }
our @EXPORT_OK = qw($VERSION %system parse_properties);

$VERSION=$1 if ( '$Revision: 7 $' =~ /Revision:\s+([0-9.]+)/o );

#
# --- start -----------------------------------------------------
#
use Carp;
use File::Spec;
our $prefix = '[' . __PACKAGE__ . '] ';

sub parse_properties($;\%) {
    # purpose: "static" method to parse properties from a file.
    # paramtr: $fn (IN): is the filename of the property file to read
    #          $hashref (IN): more properties for substitutions
    # globals: %system (IN): more properties for substitutions
    # returns: a map of properties, possibly empty.
    my $fn = shift;
    my $hashref = shift;	# may be undef'd
    my %result = ();

    open( IN, "<$fn" ) || croak "open $fn: $!\n";

    my $save;
    while ( <IN> ) {
	next if /^[!\#]/;	# comments are skipped
	s/[\r\n]*$//;		# safe chomp
	s/\#(.*)$//;		# NEW: chop in-line comments to EOLN
	s/^\s*//;		# replace all starting whitespace
	s/\s*$//;		# replace all trailing whitespace
	next unless length($_);	# skip empty lines

	if ( /\\$/ ) {
	    # continuation line
	    chop ;
	    $save .= $_;
	} else {
	    # regular line
	    $_ = $save . $_ if defined $save;
	    undef $save;
            if ( /(\S+)\s*[:=]?\s*(.*)/ ) {
                my ($k,$v) = ($1,$2);
                # substitutions
                while ( $v =~ /(\$\{([A-Za-z0-9._]+)\})/g ) {
                    my $newval = $hashref->{$2} || $system{$2} || '';
                    substr($v,index($v,$1),length($1),$newval);
                }
                $result{lc($k)} = $v;
            } else {
                carp "Illegal content in $fn:$.\n";
            }
	}
    }
    close(IN);
    %result;
}

BEGIN {
    # assemble %system properties
    use POSIX qw(uname);
    %system = ();               # start empty

    # assemble some default Java properties
    $system{'file.separator'} = File::Spec->catfile('','');
    $system{'java.home'} = $ENV{'JAVA_HOME'} if exists $ENV{'JAVA_HOME'};
    $system{'java.class.path'} = $ENV{CLASSPATH} if exists $ENV{CLASSPATH};
    $system{'java.io.tmpdir'} = $ENV{TMP} || File::Spec->tmpdir();
    # $system{'line.separator'} = "\n"; # Unix
    @system{'os.name','os.version','os.arch'} = (POSIX::uname())[0,2,4];
    $system{'user.dir'} = File::Spec->curdir();
    $system{'user.home'} = $ENV{HOME} || (getpwuid($>))[7];
    $system{'user.language'} = $ENV{LANG} || 'en';
    $system{'user.name'} = $ENV{USER} || $ENV{LOGNAME} || scalar getpwuid($>);
    $system{'user.timezone'} = $ENV{TZ}; # can be undef'd

    # non-standard, but useful and requirable
    $system{'pegasus.home'} = $ENV{'PEGASUS_HOME'};
}

#
# ctor
#
sub new {
    my $proto = shift;
    my $class = ref($proto) || $proto || __PACKAGE__;

    my $config = shift || croak "Need a configuration filename";

    # create instance
    my $self = bless { m_filename => $config,
		       m_config => { parse_properties($config) }
		     }, $class;

    # return handle to self
    $self;
}

sub getProperty {
    # purpose: Accessor to get only methods, enabling defaults
    # paramtr: $key (IN): property name to access
    #          $def (IN): default to use in absence of property definition
    # returns: The current value or the default
    my $self = shift;
    my $key = shift || croak $prefix, "need a property key";
    my $def = shift || undef;

    exists $self->{'m_config'}{$key} ? $self->{'m_config'}{$key} : $def;
}

sub property {
    # purpose: Accessor, simultaneous get (1arg) and set (2arg) method
    # paramtr: $key (IN): property name to access
    #          $val (IN): if specified, the new value to set
    # returns: in get mode, the current value, 
    #          in set mode, the old value. 
    my $self = shift;
    my $key = shift || croak $prefix, "need a property key";
    my $oldv = $self->{'m_config'}{$key};
    $self->{'m_config'}{$key} = shift if ( @_ );
    $oldv;
}

sub propertyset {
    # purpose: returns a set of keys that match a predicate
    my $self = shift;
    my $prefix = shift || croak "need a predicate to match";
    grep { /$prefix/ } keys %{ $self->{'m_config'} };
}

sub dump {
    # purpose: print everything
    # paramtr: $fn (IN): Use hyphen for stdout
    # paramtr: $showsys (opt. IN): if true, show system props also
    my $self = shift;
    my $fn = shift || '-';
    my $showsys = shift;	# may be undef'd

    if ( $fn ne '-' ) {
	open( OUT, "<$fn" ) || croak "open $fn: $!";
    } else {
	open( OUT, ">&STDOUT" ) || croak "dup STDOUT: $!";
    }

    my $len = 0;
    foreach my $key ( ( keys %{$self->{'m_config'}},
			$showsys ? keys %system : () ) ) {
	$len = length($key) if ( length($key) > $len );
    }

    foreach my $key ( sort keys %{$self->{'m_config'}} ) {
	printf OUT "%*s %s\n", -$len, $key, $self->{'m_config'}{$key};
    }

    if ( $showsys ) {
	print OUT "# system properties\n";
	foreach my $key ( sort keys %system ) {
	    printf OUT "%*s %s\n", -$len, $key, $system{$key};
	}
    }

    close OUT;
}

#
# return 'true' to package loader
#
1;
__END__
