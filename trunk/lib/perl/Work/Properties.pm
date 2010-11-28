#
# Provides parsing of Java property files from Perl. 
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
package Work::Properties;
use 5.006;
use strict;
use warnings;
use vars qw(%initial %system);

require Exporter;
our @ISA = qw(Exporter);

# declarations of methods here. Use the commented body to unconfuse emacs
sub parse_properties($;\%); 	# { }
sub PARSE_NONE { 0x0000 }
sub PARSE_ALL { 0x0001 }

# Items to export into callers namespace by default. Note: do not export
# names by default without a very good reason. Use EXPORT_OK instead.
# Do not simply export all your public functions/methods/constants.
our $VERSION = '0.1';
$VERSION=$1 if ( '$Revision$' =~ /Revision:\s+([0-9.]+)/o );

our %EXPORT_TAGS = (parse => [qw(PARSE_NONE PARSE_ALL)]);
our @EXPORT_OK = qw($VERSION parse_properties %initial %system
		    PARSE_NONE PARSE_ALL);
our @EXPORT = ();

# Preloaded methods go here.
use Carp;
use File::Spec;

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
    print STDERR "# parsing properties in $fn...\n"
	if ( $main::DEBUG );

    my $save;
    while ( <IN> ) {
        next if /^[!\#]/;       # comments are skipped
        s/[\r\n]*$//;           # safe chomp
        s/\#(.*)$//;            # NEW: chop in-line comments to EOLN
	s/\\(.)/$1/g;           # replace java properties escaped special characters #!=:
        s/^\s*//;               # replace all starting whitespace
        s/\s*$//;               # replace all trailing whitespace
        next unless length($_); # skip empty lines

        if ( /\\$/ ) {
            # continuation line
            chop ;
            $save .= $_;
        } else {
            # regular line
            $_ = $save . $_ if defined $save;
            undef $save;
	    print "#Property being parsed is # $_\n" if $main::DEBUG;
		if ( /([^:= \t]+)\s*[:=]?\s*(.*)/ ) {   # new fix for auto gen properties
#		if ( /(\S+)\s*[:=]?\s*(.*)/ ) {
		my ($k,$v) = ($1,$2);
			    print "#Property being stored is # $k ==> $v \n" if $main::DEBUG;
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
    %system = ();		# start empty

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

    # not require, but useful
    $system{'pegasus.home'} = $ENV{'PEGASUS_HOME'}; # can be undef'd
}

BEGIN {
    # assemble commandline properties
    %initial = ();		# start empty

    # extracts -Dk=v properties from @ARGV before Getopt sees it
    if ( @ARGV > 0 ) {
	while ( defined $ARGV[0] && substr( $ARGV[0], 0, 2 ) eq '-D' ) {
	    my $arg = shift(@ARGV);
	    my ($k,$v) = split( /=/, 
			 ($arg eq '-D' ? shift(@ARGV) : substr($arg,2)),
			 2 );
	    $initial{lc($k)} = $v if length($k); 
	}
    }

    # CLI properties extend (and overwrite) system properties
    %system = ( %system, %initial );
}

#
# ctor
#
sub new {
    # purpose: Initialize an instance variable
    # paramtr: $flags (opt. IN): limits files to parse
    #          $hashref (opt. IN): key value property list of least priority
    # returns: reference to blessed self
    my $proto = shift;
    my $class = ref($proto) || $proto || __PACKAGE__;
    my $flags = PARSE_ALL;	# default

    # first argument is either scalar number, or hashref
    my $hashref = shift;
    if ( defined $hashref && ref $hashref eq '' ) {
	$flags = $hashref;	# use user-supplied flags
	$hashref = shift;	# advance to next argument
    }
    # next argument is optional, a hash reference to props, or undef
    my %config = ();
    %config = ( %{$hashref} ) if ( ref $hashref eq 'HASH' );

    # magic property
    my $pegasushome;
    my $flag = 0;
    if ( ($flags & PARSE_ALL) ) {
	if ( exists $ENV{'PEGASUS_HOME'} ) {
	    if ( -d $ENV{'PEGASUS_HOME'} ) {
		$pegasushome = $config{'pegasus.home'} = $ENV{'PEGASUS_HOME'};
	    } else {
		warn "Warning: PEGASUS_HOME does not point to a(n accessible) directory!\n";
	    }
	} elsif ( exists $ENV{'VDT_LOCATION'} ) {
	    my $tmp = File::Spec->catdir( $ENV{'VDT_LOCATION'}, 'pegasus' );
	    if ( -d $tmp ) {
		$pegasushome = $config{'pegasus.home'} = $tmp;
	    } else {
		warn "Warning: $tmp does not point to a(n accessible) directory!\n";
	    }
	} else {
	    croak "Warning: Your environment variable PEGASUS_HOME is not set!\n";
	}

	# system properties go first
	if ( exists $system{'pegasus.properties'} ) {
	    # overwrite for system property location from CLI property
	    my $sys = $system{'pegasus.properties'};
	    if ( -r $sys ) {
		%config = ( %config, parse_properties($sys) );
	    } else {
		$flag++;
	    }
	} elsif ( exists $config{'pegasus.properties'} ) {
	    # overwrite for system property location from c'tor property
	    my $sys = $config{'pegasus.properties'};
	    if ( -r $sys ) {
		%config = ( %config, parse_properties($sys) );
	    } else {
		$flag++;
	    }
	} elsif ( defined $pegasushome ) {
	    # default system property location
	    my $sys = File::Spec->catfile( $pegasushome, 'etc', 'properties' );
	    if ( -r $sys ) {
		%config = ( %config, parse_properties($sys) );
	    } else {
		$flag++;
	    }
	} else {
	    $flag++;
	}
#    } else {
#	# asked not to parse
#	$flag++;

## TODO : Delete this section when everything works fine in pegasus-run

    # load wfrc user properties before pegasus user properties
#    if ( ($flags & PARSE_WFRC) ) {
#	if ( exists $system{'wf.properties'} ) {
#	    # overwrite for wfrc property location from CLI property
#	    my $wfrc = $system{'wf.properties'};
#	    if ( -r $wfrc ) {
#		%config = ( %config, parse_properties($wfrc) );
#	    } else {
#		$flag++;
#	    }
#	} elsif ( exists $config{'wf.properties'} ) {
#	    # overwrite for wfrc property location from c'tor property
#	    my $wfrc = $config{'wf.properties'};
#	    if ( -r $wfrc ) {
#		%config = ( %config, parse_properties($wfrc) );
#	    } else {
#		$flag++;
#	    }
#	} elsif  ( exists $ENV{'HOME'} ) {
#	    # default wfrc property location
#	    my $wfrc = File::Spec->catfile( $ENV{HOME}, '.wfrc' );
#	    if ( -r $wfrc ) {
#		%config = ( %config, parse_properties($wfrc) );
#	    } else {
#		$flag++;
#	    }
#	} else { 
#	    $flag++;
#	}
#    } else {
#	# asked not to parse
#	$flag++;
#    }

	# user properties go last
	if ( exists $system{'pegasus.user.properties'} ) {
	    # overwrite for user property location from CLI property
	    my $usr = $system{'pegasus.user.properties'};
	    if ( -r $usr ) {
		%config = ( %config, parse_properties($usr) );
	    } else {
		$flag++;
	    }
	} elsif ( exists $config{'pegasus.user.properties'} ) {
	    # overwrite for user property location from lower property
	    my $usr = $config{'pegasus.user.properties'};
	    if ( -r $usr ) {
		%config = ( %config, parse_properties($usr) );
	    } else {
		$flag++;
	    }
	} elsif ( exists $ENV{'HOME'} ) {
	    # default user property 
	    my $usr2 = File::Spec->catfile( $ENV{HOME}, '.pegasusrc' );
	    if ( -r $usr2 ) {
		# prefer new property definition
		%config = ( %config, parse_properties($usr2) );
	    } else {
		$flag++;
	    }
	} else {
	    $flag++;
	}
#    } else {
#	# asked not to parse
#	$flag++;
    }

    # sanity check -- only in warnings mode
    carp "Warning: Unable to load any properties at all" 
	if ( $^W &&
	     ( $flag == 1 && $flags == PARSE_ALL ) );
	
    # create instance and return handle to self
    # WARNING: Keep ordering of %config before %initial to permit
    # CLI properties to overwrite any other property.
    bless { m_config => { %config, %initial }, 
	    m_flags => $flags }, $class;
}

sub property {
    # purpose: Accessor, simultaneous get (1arg) and set (2arg) method
    # paramtr: $key (IN): property name to access
    #          $val (IN): if specified, the new value to set
    # returns: in get mode, the current value, 
    #          in set mode, the old value. 
    my $self = shift;
    my $key = shift || croak "need a property key";
    my $oldv = $self->{'m_config'}{$key};
    $self->{'m_config'}{$key} = shift if ( @_ );
    $oldv;
}

sub keyset {
    # purpose: finds a subset of keys that matches a RE predicate 
    # paramtr: $predicate (opt. IN): predicate to match against
    # returns: a set of keys that match a predicate, or all w/o predicate
    my $self = shift;
    my $predicate = shift;
    if ( defined $predicate ) {
	grep { /$predicate/ } keys %{ $self->{'m_config'} };
    } else {
	keys %{ $self->{'m_config'} };
    }	
}

sub propertyset {
    # purpose: finds a subset of keys that matches a prefix
    # paramtr: $predicate (IN): predicate to match against
    # paramtr: $remove (IN): if true, remove prefix
    # returns: a hash containing the matching keys and respective values
    my $self = shift;
    my $prefix = shift || croak "need a prefix to match";
    my $length = length($prefix);
    my $remove = shift;

    my %result = ();
    foreach my $key ( grep { substr($_,0,$length) eq $prefix }
		      keys %{ $self->{'m_config'} } ) {
	my $newkey = $remove ? substr($key,$length) : $key;
	$result{$newkey} = $self->{'m_config'}->{$key} 
	    if ( length($newkey) > 0 );
    }
    %result;
}

my %translate = ( 'mysql' => 'mysql',
		  'postgresql' => 'Pg',
		  'sqlite' => 'SQLite2',
		  'oracle' => 'Oracle' );

sub jdbc2perl {
    # purpose: Convert PEGASUS JDBC connect properties into what DBI needs
    # paramtr: $cat (IN): catalog name, e.g. "tc" or "wf". 
    # returns: uri => DBI-uri
    #          dbuser => database account username
    #          dbpass => database account password
    #
    # pegasus.db.(*|$cat).driver	(Postgres|MySQL)
    # pegasus.db.(*|$cat).driver.url	jdbc:jdbtype:[//dbhost[:dbport]/]dbname
    # pegasus.db.(*|$cat).driver.user	dbuser
    # pegasus.db.(*|$cat).driver.password	dbpass
    my $self = shift;
    my $cat = shift || croak "need a catalog name, e.g. 'transformation' , 'replica', 'provenance'or 'work'";

    my %x = ( $self->propertyset( "pegasus.catalog.*.db.", 1 ),
	      $self->propertyset( "pegasus.catalog.$cat.db.", 1 ) );

    # turn JDBC to DBI uri
    my $dbuser = $x{'user'};
    my $dbpass = $x{'password'};
    my $juri = $x{'url'};
    my @x = split /:/, $juri, 3;
    die "ERROR: Is the JDBC URI \"$juri\" valid?" unless $x[0] eq 'jdbc';
    delete @x{'driver.url','driver.user','driver.password'};

    my $uri = 'dbi:' . ( $translate{$x[1]} || ucfirst(lc($x[1])) );
    my $flag = 0;

    my $pos;
    if ( ($pos = index( $x[2], '//' )) >= 0 ) {
	my $fin = index( $x[2], '/', $pos+2 );
	$fin = length($x[2]) if $fin == -1;
	my ($host,$port) = split /:/, substr($x[2],$pos+2,$fin-$pos-2), 2;
	if ( $fin+1 < length($x[2]) ) {
	    $uri .= $flag ? ';' : ':';
	    $uri .= ( $x[1] eq 'mysql' ? 'database' : 'dbname' );
	    $uri .= '=' . substr($x[2],$fin+1);
	    $flag++;
	}
	if ( defined $host && length($host)>0 ) {
	    $uri .= $flag ? ';' : ':';
	    $uri .= "host=$host";
	    $flag++;
	}
	if ( defined $port && length($host)>0 ) {
	    $uri .= $flag ? ';' : ':';
	    $uri .= "port=$port";
	    $flag++;
	}
    } else {
	if ( defined $x[2] && length($x[2])>0 ) {
	    $uri .= $flag ? ';' : ':';
	    $uri .= ( $x[1] eq 'mysql' ? 'database' : 'dbname' ) . '=' . $x[2];
	    $flag++;
	}
    }

    # copy remainder
    foreach my $k ( keys %x ) { 
	next unless length($k) > 7;
	$uri .= $flag ? ';' : ':';
	$uri .= substr($k,7) . '=' . $x{$k};
	$flag++;
    }

    # done
    ( uri => $uri, dbuser => $dbuser, dbpass => $dbpass );
}

sub dump {
    # purpose: prints the key set in property format 
    # paramtr: $fn (opt. IN): Name of file to print into
    # returns: number of things printed, undef for error.
    my $self = shift;
    my $fn = shift || '-';	# defaults to stdout

    my $count = 0;
    if ( open( OUT, ">$fn" ) ) {
	print OUT "# generated ", scalar localtime(), "\n";
	foreach my $key ( sort keys %{ $self->{'m_config'} } ) {
	    print OUT "$key=", $self->{'m_config'}->{$key}, "\n"; 
	}
	close OUT;
    } else {
	carp "open $fn: $!";
	undef $count;
    }
    $count;
}

#
# return 'true' to package loader
#
1;

__END__


=head1 NAME

Work::Properties - parsing of Java property files from Perl. 

=head1 SYNOPSIS

    use Work::Properties qw(:parse);

    $p = Work::Properties->new( );
    $p = Work::Properties->new( $pflags );
    $p = Work::Properties->new( $pflags, { 'weak' => 'property' } );
    $p->dump('-'); # dump all known properties on stdout

    something() if $p->property('pegasus.db');
    $p->property('pegasus.tc.file') = "/some/where";

    foreach my $key ( $p->keyset('^pegasus\.rc') ) { ... }

    %x = $p->propertyset('pegasus.rc.');

=head1 DESCRIPTION

The Work::Properties module reads Java properties for the GriPhyN
Virtual Data System. It permits commandline-based overwrites of
properties using Java's C<-Dprop=val> syntax in Perl by removing initial
definitions from C<@ARGV> during module initialization time. Thus, it is
recommended to use this module before parsing commandline arguments.

Up to three property files from the GriPhyN Virtual Data System are read
from the constructor, please refer to the L<new|/"METHODS"> method.

All property keys are lower cased when read as a safety precaution. 

=head1 VARIABLES

Variables are not exported by default. They must be explicitely imported
when importing this module.

=over 4

=item %initial

This variable is initialed during module initialization. It parses the
commandline vector C<@ARGV> for initial arguments starting with hyphen
capital D like the following:

    perl myprog.pl -Dk1=v1 -Dk2=v2 ...

Such definitions are removed from C<@ARGV>, and the definitions placed
into the initial variable. If your application uses capital-D as a valid
argument switch, you can still use it, alas never as the first argument.

Only property-like definitions that are initial on the commandline will
be removed and put into this variable. Commandline properties have the
highest priority of all properties. You should not write to this
variable.

=item %system

This variable is initialized to mimick some Java system properties.
However, only a smaller subset is provided. These system properties have
the lowest priority. You should not write to this variable. Properties
from C<%initial> are merged with a higher priority into the system
properties, permitting command-line option to overwrite system
properties.

=back

=head1 METHODS

=over 4

=item Work::Properties::parse_properties($fn)

The static method reads a property file, located by $fn, into a
single-level Perl hash. If the optional second argument is specified,
the hash will be used to do variable substitutions from the the second
argument properties or system properties. Not found properties are
replaced by an empty string.

=item new()

The constructor takes an optional hash reference to an arbitrary list of
weak properties (of least priority) for debugging purposes. When
constructing an instance variable, the constructor will parse the
contents of the file pointed to by I<pegasus.properties> (defaults to
F<$PEGASUS_HOME/etc/properties>) and I<pegasus.user.properties> which defaults to
F<$HOME/.pegasusrc>.

The commandline properties are added last, giving them the highest
priority. The constructor arguments were added first, giving them the
least priority. Priorities are important in case of duplicate keys, as
higher priorities overwrite values from lower priorities. 

This prioritizing is as if the constructor was invoked with the property
flag I<PARSE_ALL>.

=item new( $pflags )

If invoked with one of the property flags I<PARSE_NONE> or I<PARSE_ALL>, 
only a subset of the properties above
will be parsed. 

=item new( $pflags, %extra )

TBD: At what priority are the I<%extra> flags added? Likely at lowest.

=item property( $key )

If used as r-value, the property setting of the specified key is
obtained. If the property does not exist, the value of C<undef> is returned. 

If used as l-value, as in an assignment, the property of the specified
value will be set. The old value previously know is the result of the
method.

The emulated system properties will not be considered. 

=item keyset( $predicate )

Given a regular expression predicate, this method returns all keys that
match the predicate. Please note that it is recommended to match
prefixes by anchoring the expression with an initial roof (C<^>)
character, and that you must backslash-escape the period character that
is literal part of most properties.

    foreach my $key ( $p->keyset('^pegasus\.db\.') ) {
	xxx( $p->property($key) );
    }

The above code snippet will only find properties matching the prefix of
C<pegasus.db.>, but not C<pegasus.db> itself.

If invoked without argument, this method will return all keys. 

The emulated system properties will not be considered. 

=item propertyset( $prefix, $truncate )

Given a normal string prefix, this method returns a hash starting with
the prefix string. This is not a regular expression match, just plain
string prefix matching. 
I
f the optional argument $truncate is specified and true, the prefix
string will be removed from the keys in the result set. 

    %x = $p->propertyset('pegasus.db.');
    foreach my $key ( sort keys %x ) {
	xxx( $x{$key} );
    }

=item jdbc2perl( $catalog )

This very special property parses the pegasus.db property database driver
section. The only permitted argument is the (lower case) catalog name.
The method will determine the necessary Perl connection string from 
the JDBC connection information. 

The result is a hash with three values. The B<uri> key stores the Perl
DBI access URI. The B<dbname> store the database user account name as
required for the connect function. The B<dbpass> transports the database
account's password as found in the properties.

=item dump( $filename )

This is mostly a debug function. It dumps all properties except the
artificial system properties into the specified file. For convenience,
you can use the hyphen C<-> for I<stdout>.

=back

=head1 SEE ALSO

L<http://www.griphyn.org/>

=head1 AUTHOR

Jens-S. VE<ouml>ckler, C<voeckler at isi dot edu>
Gaurang Mehta, C<gmehta at isi dot edu>

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
