package Pegasus::Properties;
#
# Provides parsing of Java property files from Perl. 
#
#  Copyright 2007-2010 University Of Southern California
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#
# Author: Jens-S. Vöckler voeckler at isi dot edu
# Revision : $Revision$
# $Id$
#
use 5.006;
use strict;
use warnings;
use vars qw(%initial %system);

require Exporter;
our @ISA = qw(Exporter);

# declarations of methods here. Use the commented body to unconfuse emacs
sub pegasusrc(;$);		# { }
sub parse_properties($;\%); 	# { }

# Items to export into callers namespace by default. Note: do not export
# names by default without a very good reason. Use EXPORT_OK instead.
# Do not simply export all your public functions/methods/constants.
our $VERSION = '1.0';
$VERSION=$1 if ( '$Revision$' =~ /Revision:\s+([0-9.]+)/o );
our $pegasus_env = 'pegasus.env.'; 
our $pegasus_len = length($pegasus_env); 

our @EXPORT_OK = qw($VERSION parse_properties pegasusrc %initial %system
		    $pegasus_env);
our %EXPORT_TAGS = ( all => [ @EXPORT_OK ] );
our @EXPORT = ();

# Preloaded methods go here.
use POSIX qw(uname); 
use Carp;
use File::Spec;

sub pegasusrc(;$) {
    # purpose: "static" method to determine location of pegasusrc
    # paramtr: $home (opt. IN): override home location
    # returns: a string
    #
    my $home = shift() || 
	$ENV{HOME} || 
	(getpwuid($>))[7] || 
	File::Spec->curdir(); 
    File::Spec->catfile( $home, '.pegasusrc' ); 
}

sub parse_properties($;\%) {
    # purpose: "static" method to parse properties from a file.
    # paramtr: $fn (IN): is the filename of the property file to read
    #          $hashref (IN): more properties for substitutions
    # warning: dies, if the $fn cannot be opened properly. 
    # globals: %system (IN): more properties for substitutions
    # returns: a hash of properties, possibly empty.
    #
    my $fn = shift;
    my $hashref = shift || {};	# may be undef'd
    my %result = ();

    open( IN, "<$fn" ) || die "Warning: open $fn: $!\n";
    print STDERR "# parsing properties in $fn...\n" if $main::DEBUG;

    my $save;
    while ( <IN> ) {
	next if /^[!\#]/;	# comments are skipped
	s/[\r\n]*$//;		# safe chomp
	s/\#(.*)$//;		# NEW: chop in-line comments to EOLN
	s/\\(.)/$1/g;	       # replace escaped special characters #!=:
	s/^\s*//;		# replace all starting whitespace
	s/\s*$//;		# replace all trailing whitespace
	next unless length($_); # skip empty lines

	if ( /\\$/ ) {
	    # continuation line
	    chop ;
	    $save .= $_;
	} else {
	    # regular line
	    $_ = $save . $_ if defined $save;
	    undef $save;
	    print STDERR "# Parsing: $_\n" if $main::DEBUG;
	    if ( /([^:= \t]+)\s*[:=]?\s*(.*)/ ) {   
		# new fix for auto gen properties
		my ($k,$v) = ($1,$2);

		# substitutions -- works arbitrarily deep?
		while ( $v =~ /(\$\{([A-Za-z0-9._]+)\})/g ) {
		    my ($a,$b) = ($1,$2); 
		    my $newval = $hashref->{$b} || 
			$system{$b} || 
			$result{$b} || 
			'';
		    substr($v,index($v,$a),length($a),$newval);
		}

		print STDERR "# Storing: $k => $v\n" if $main::DEBUG;
		# 20110519 (jsv): No key lower-casing requested by FS,KV
		$result{$k} = $v;
	    } else {
		carp "Illegal content in $fn:$.\n";
	    }
	}
    }

    close(IN);
    %result;
}

BEGIN {
    #
    # Part 1: Assemble %system properties emulating some Java properties
    #
    %system = ();		# start empty

    # assemble some default Java properties
    $system{'file.separator'} = File::Spec->catfile('','');
    $system{'java.home'} = $ENV{'JAVA_HOME'} if exists $ENV{'JAVA_HOME'};
    $system{'java.class.path'} = $ENV{CLASSPATH} if exists $ENV{CLASSPATH};
    $system{'java.io.tmpdir'} = $ENV{TMP} || File::Spec->tmpdir();
#    $system{'line.separator'} = "\n"; # Unix
    @system{'os.name','os.version','os.arch'} = (POSIX::uname())[0,2,4];
    $system{'user.dir'} = File::Spec->curdir();
    $system{'user.home'} = $ENV{HOME} || (getpwuid($>))[7];
    $system{'user.language'} = $ENV{LANG} || 'en';
    $system{'user.name'} = $ENV{USER} || $ENV{LOGNAME} || scalar getpwuid($>);
    $system{'user.timezone'} = $ENV{TZ}; # can be undef'd

    # not required, but useful
    $system{'pegasus.home'} = $ENV{'PEGASUS_HOME'}; # can be undef'd

    #
    # Part 2: Assemble commandline properties from initial -D argument
    #
    %initial = ();		# start empty

    # Extracts -Dk=v properties from @ARGV before Getopt sees it
    # This will remove *only* the initial -D arguments from the CLI!
    if ( @ARGV > 0 ) {
	while ( defined $ARGV[0] && substr( $ARGV[0], 0, 2 ) eq '-D' ) {
	    my $arg = shift(@ARGV);
	    my ($k,$v) = split( /=/, 
			 ($arg eq '-D' ? shift(@ARGV) : substr($arg,2)),
			 2 );

	    # 20110519 (jsv): No key lower-casing requested by FS,KV
	    #$k = lc $k;
	    if ( $k eq 'pegasus.properties' || $k eq 'pegasus.user.properties' ) { 
		carp "Warning: $k is no longer supported, ignoring, please use --conf\n";
	    } else {
		$initial{$k} = $v if length($k); 
	    }
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
    # paramtr: $conffile (IN): --conf filename (or undef)
    #          $runprops (IN): properties from rundir (or undef)
    # warning: exceptions from parse_properties() may be propagated
    # returns: reference to blessed self
    #
    my $proto = shift;
    my $class = ref($proto) || $proto || __PACKAGE__;
    my $conffile = shift;
    my $rundirpfn = shift; 
    my $pegasusrc = pegasusrc(); 
    
    my %config = (); 
    if ( defined $conffile ) { 
	croak "FATAL: $conffile does not exist" unless -e $conffile;
	croak "FATAL: $conffile is not readable" unless -r _; 
	if ( -s _ ) { 
	    print STDERR "# priority level 1: $conffile\n" if $main::DEBUG; 
	    %config = parse_properties($conffile); 
	} else {
	    carp "Warning: $conffile is empty, trying next"; 
	    goto LEVEL2;
	}
    } elsif ( defined $rundirpfn ) {
      LEVEL2:
	croak "FATAL: $rundirpfn does not exist" unless -e $rundirpfn;
	croak "FATAL: $rundirpfn is not readable" unless -r _;
	if ( -s _ ) { 
	    print STDERR "# priority level 2: $rundirpfn\n" if $main::DEBUG;
	    %config = parse_properties($rundirpfn); 
	} else { 
	    carp "Warning: $rundirpfn is empty, trying next priority"; 
	    goto LEVEL3; 
	}
    } else {
      LEVEL3:
	# $HOME/.pegasusrc may safely not exist, no failures here
	if ( -s $pegasusrc ) {
	    print STDERR "# priority level 3: $pegasusrc\n" if $main::DEBUG; 
	    %config = parse_properties($pegasusrc); 
	} else {
	    warn "Warning: No property files parsed whatsoever\n";
	}
    }
    
    # create instance and return handle to self.
    # last one in chain below has highest priority. 
    my $self = bless { m_config => { %config, %initial } }, $class;
    $self->setenv(); 
    $self; 
}

sub setenv {
    # purpoes: merge properties starting in $pegasus_key into %ENV
    #
    my $self = shift || croak; 
    foreach my $k ( keys %{ $self->{'m_config'} } ) { 
	$ENV{substr($k,$pegasus_len)}=$self->{'m_config'}{$k}
	    if substr($k,0,$pegasus_len) eq $pegasus_env; 
    }
}

sub reinit {
    # purpose: ensure that %initial has highest priority
    # 
    my $self = shift;
    %{ $self->{'m_config'} } = ( %{ $self->{'m_config'} }, %initial );
}

sub merge {
    # purpose: Read and merge a file into the current property set
    # paramtr: $fn (IN): where to read properties from.
    # warning: Properties from the file will overwrite existing ones.
    #          If the instance has keys (a,b), and the file has (b,c)
    #          the updated instance has keys (a,b,c) with b from file.
    # warning: use reinit() to give CLI properties precedence again. 
    # warning: exceptions from parse_properties() will be propagated.
    # returns: hash of all new (merged) properties. 
    #
    my $self = shift; 
    my $where = shift || croak "need a filename";
    
    # the new props from the file will merge with the existing properties,
    # where duplicate keys take precedence from the file. 
    %{ $self->{'m_config'} } = ( %{ $self->{'m_config'} }, 
				 parse_properties($where) );
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

sub has {
    # purpose: Checks for the existence of a given property key
    # paramtr: $key (IN): property name to access
    # returns: true, if a property with this key exists
    #
    my $self = shift;
    my $key = shift || croak "need a property key";
    exists $self->{'m_config'}{$key};
}

sub all {
    # purpose: Return all known properties as simple hash
    # returns: hash
    #
    my $self = shift; 
    %{ $self->{'m_config'} }; 
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

sub _quote($) {
    local $_ = shift;
    s{\015}{\\r}g;
    s{\011}{\\n}g; 
    s{([:= \t\f])}{\\$1}g;
    "$_";
}

sub dump {
    # purpose: prints the key set in property format 
    # paramtr: $fn (opt. IN): Name of file to print into
    # returns: number of things printed, undef for error.
    local(*OUT); 
    my $self = shift;
    my $fn = shift || '-';	# defaults to stdout

    my $count = 0;
    if ( open( OUT, ">$fn" ) ) {
	print OUT "# generated ", scalar localtime(), "\n";
	foreach my $key ( sort keys %{ $self->{'m_config'} } ) {
	    print OUT _quote($key), '=', 
		_quote($self->{'m_config'}->{$key}), "\n"; 
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

Pegasus::Properties - parsing of Java property files from Perl. 

=head1 SYNOPSIS

    use Pegasus::Properties qw(:parse);

    $p = Pegasus::Properties->new( $conffile, undef );
    $p->merge( $fn ); 
    $p->reinit(); 
    $p->dump('-'); # dump all known properties on stdout

    something() if $p->property('pegasus.db');
    $p->property('pegasus.tc.file') = "/some/where";

    foreach my $key ( $p->keyset('^pegasus\.rc') ) { ... }

    %x = $p->propertyset('pegasus.rc.');
    do( $p->property('asdf') ) if $p->has('asdf'); 

=head1 DESCRIPTION

The Pegasus::Properties module reads Java properties for the GriPhyN
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

=head1 STATIC METHODS

=over 4

=item Pegasus::Properties::parse_properties( $fn )

=item Pegasus::Properties::parse_properties( $fn, $hashref )

The static method reads a property file, located by $fn, into a
single-level Perl hash. If the optional second argument is specified,
the hash will be used to do variable substitutions from the the second
argument properties or system properties. Not found properties are
replaced by an empty string.

Please note that the method throws an error, if the file does not exist
or cannot be opened properly. It is up to the caller to catch this
exception.

=item Pegasus::Properties::pegasusrc( )

=item Pegasus::Properties::pegasusrc( $home )

This simple static method constructs a filename where to find the
C<$HOME/.pegasusrc> file. The location of the home directory can 
be passed as optional argument, or auto-detected otherwise. 

=back

=head1 INSTANCE METHODS

=over 4

=item new( $conffile, $rundirpropfn )

The constructor needs to know about the possible I<conf> command-line
option file location, and the property file in the designated run
directory. Either argument may be C<undef> to indicate that it does not
exist. Internally the constructor uses the location of the
C<$HOME/.pegasusrc> file, which is automatically constructed.

The constructor attempts to read from the defined file with the highest
priority first. If the file does not exist or is not readable, it will
throw an exception. If the file is empty (0 byte sized), it will warn
and attempt to read the next lower priority (etc.). 

Values from the C<%initial> hash are merged into the instance with the
highest priority. 

=item merge( $fn )

The C<merge> method permits you to easily add properties from a file
to the current instance. The new properties from the file take a higher
priority than the existing one, in case keys exist in both. 

Typically, you want to follow C<merge> with C<reinit> to give
command-line properties precedence.

=item reinit( )

Will ensure that properties from C<%initial> are merged back into the
instance, overwriting any existing properties with the same key.

=item property( $key )

If used as r-value, the property setting of the specified key is
obtained. If the property does not exist, the value of C<undef> is returned. 

If used as l-value, as in an assignment, the property of the specified
value will be set. The old value previously know is the result of the
method.

The emulated system properties will not be considered. 

=item has( $key )

This method checks for the existence of a given key in the properties.
Unlike the C<property> method, it will not auto-vivify any key.

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

Copyright 2007-2011 University Of Southern California

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

L<http://www.apache.org/licenses/LICENSE-2.0>

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

=cut
