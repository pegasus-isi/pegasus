#
# Basic class to parse and provide site-selector data
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
# Revision: $Revision$
#
package Site::Selector;
use 5.006;
use strict;
use warnings;

require Exporter;
our @ISA = qw(Exporter);

# declare prototypes
sub parse_file_v2($);		# { }

# which keys may occur multiple times?
our %multikeys = ( 'resource.id' => 1, 'input.lfn' => 1, 'resource.bad' => 1 );

# Items to export into callers namespace by default. Note: do not export
# names by default without a very good reason. Use EXPORT_OK instead.
# Do not simply export all your public functions/methods/constants.
our $VERSION='1.0';
$VERSION=$1 if ( '$Revision$' =~ /Revision:\s+([0-9.]+)/o );

our %EXPORT_TAGS = ();
our @EXPORT_OK = qw($VERSION %multikeys parse_file_v2);
our @EXPORT = qw();

#
# --- methods ---------------------------------------------------
#
use Carp;

sub parse_file_v2($) {
    # purpose: static method to parse a site selector file
    # paramtr: $fn (IN): filename of communication file
    # returns: [0]: hash ref to key-value-pairs
    #          [1]: hash ref to site-griftp array
    #          returns undef for both refs in case of error, check $!
    my $fn = shift;

    my (%hash,%site,%bad);
    local(*IN);
    if ( open( IN, "<$fn" ) ) {
	# expect the first line to say "version=2"
	die "Wrong format version: \"$_\"\n" 
	    unless ( ($_ = <IN>) =~ /^version=2/ );
	$hash{version} = 2;
	my ($key,$value);
	while ( <IN> ) {
	    # trim and chomp
	    s/[ \t\r\n]+$//;
	    next if length($_) < 1;

	    # split line at first equal sign
	    ($key,$value) = split /=/, $_, 2; 
	    
	    # check of keys of multiple occurrances
	    if ( exists $multikeys{$key} ) {
		# store vector reference for multikeys
		push( @{$hash{$key}}, $value );
		
		# compile site catalog
		if ( $key eq 'resource.id' ) {
		    ($key,$value) = split /\s+/, $value, 2;
		    push( @{$site{$key}}, $value );
		} elsif ( $key eq 'resource.bad' ) {
		    ($key,$value) = split /\s+/, $value, 2;
		    $bad{$key} = $value;
		}
	    } else {
		# store scalar for singular keys
		$hash{$key} = $value;
	    }
	}

	close IN;
    }

    # done
    (\%hash,\%site,\%bad);
}

#
# ctor
#
sub new {
    # purpose: Initialize an instance variable
    # paramtr: $filename (IN): path to site-selector file
    # returns: reference to instance, or undef in case of failure
    my $proto = shift;
    my $class = ref($proto) || $proto || __PACKAGE__;
    my $filename = shift || croak "c'tor requires a filename argument";

    # slurp file
    my ($hash,$site,$bad) = parse_file_v2($filename);
    return undef unless ( scalar %{$hash} );

    # return the perl way
    bless { 'm_filename' => $filename,
	    'm_hash' => $hash,
	    'm_site' => $site,
	    'm_bad' => $bad }, $class;
}

sub filename {
    # purpose: returns the name of the communication file
    my $self = shift;
    $self->{'m_filename'};
}

sub site {
    # purpose: returns site knowledge
    # paramtr: $site (opt. IN): site handle
    # returns: case [no $site]: list of all site handles
    #          case [$site]: in array context, all gridftp server, 
    #                        in scalar context, first gridftp server.
    my $self = shift;
    my $site = shift;

    if ( defined $site ) {
	wantarray ? 
	    @{$self->{'m_site'}->{$site}} : 
	    $self->{'m_site'}->{$site}->[0];
    } else {
	keys %{$self->{'m_site'}};
    }
}

sub bad {
    # purpose: manages bad site knowledge, which may be empty
    # paramtr: $site (opt. IN): site handle
    # returns: case [no $site]: list of all bad handles, oldest first
    #          case [$site]: UTC timestamp of site being detected bad
    my $self = shift;
    my $site = shift;

    my $x = $self->{'m_bad'};
    return undef unless ( defined $x && scalar %{$x} );

    if ( defined $site ) {
	$x->{$site};
    } else {
	sort { $x->{$b} <=> $x->{$a} } keys %{$x};
    }
}
	

sub hash {
    # purpose: manages hash knowledge
    # paramtr: $key (opt. IN): key to read
    # returns: case [no $key]: return list of all keys
    #          case [$key NOTA %multikeys]: return scalar value
    #          case [$key IN %multikeys]: in scalar context first element,
    #                                     in array context whole array
    my $self = shift;
    my $key = shift;
    
    if ( defined $key ) {
	if ( $multikeys{$key} ) {
	    wantarray ? 
		@{$self->{'m_hash'}->{$key}} : 
		$self->{'m_hash'}->{$key}->[0];
	} else {
	    $self->{'m_hash'}->{$key};
	}
    } else {
	keys %{$self->{'m_hash'}};
    }
}
        
#
# return 'true' to package loader
#
1;
__END__

=head1 NAME

Site::Selector - provides attributes of a site-selection communication file.

=head1 SYNOPSIS

    use Site::Selector;

    my $s = Site::Selector->new( 'file.kss' ) || die;
    
    print $s->filename, "\n";   # prints file.kss
    
    foreach my $site ( $s->site ) {
        work( $s->site($site) );
    }

    foreach my $key ( $s->hash ) {
	print $key, '=', $s->hash($key), "\n";
    }

=head1 DESCRIPTION

The Site::Selector module provides a simple interface work with the
Eurale and Pegasus site-selection mechanism. The mechanism provides
selection information about the sites, files, and the job in a
(temporary) file, which is passed as first and only argument to the 
site selector. In return, the site selectors expect one line on 
I<stdout> of the site selector reading

    SOLUTION:handle

or 

    SOLUTION:handle:more

So don't write anything onto I<stdout> yourself. 

The Site::Selector module parses a given temporary file, which is passed
as mandatory argument to the constructor. The produced instance provides
simplified access to the arguments inside the file.

=head1 VARIABLES

Class variables are not exported by default. They must be explicitely
imported when importing this module.

=over 4

=item %multikeys

is a hash that enumerates all those keys which may occur multiple times
in the temporary file. Values from these keys will always be stored
internally inside a hash.

=back

=head1 METHODS

=over 4

=item Site::Selector::parse_file_v2($filename)

This static function reads a temporary file, and on success returns a
vector with hash references. The first hash reference contains all keys
from the temporary file. The second hash reference maps site handles
from the C<resource.id> key to a vector of gridftp server URIs. The
third hash reference may be unused. It maps bad sites to the UTC
timestamp when they were (last) detected being bad. 

In case of error, undef is returned. The C<die> function is employed, if
the file is of the wrong version format.

The static method is usually invoked by the constructor, and should not
be used directly.

=item new( $filename )

The constructor reads the temporary file passed as F<$filename>, and
compiles the internal data structures. On success, a blessed instance is
returned. On error, undef is returned.

=item filename

is a simple accessor, returning the filename that was passed to the
constructor.

=item site

invoked without arguments, this method returns a list of all site
handles. A site handle is a unique identifier for a remote compute
resource.

=item site( $handle )

invoked with a single argument, the result depends on the caller's
context. In array context, a list of all gridftp-URIs is returned. In
scalar context, only the first (possibly empty) gridftp-URI is returned.

=item bad

invoked without arguments, this method returns a list of all known site
handles that were marked bad. The list is ordered with the oldest first.
A site handle is a unique identifier for a remote compute resource. If
the features is not supported, the C<undef> value is returned.

=item bad( $handle )

invoked with a single argument, the result is the UTC timestamp when the
site was detected being bad. However, if no such site exists, the result
is undefined.

=item hash

invoked without arguments, this method returns a list of all keys
available from the temporary file data structures. 

=item hash( $key )

invoked with single scalar, the result depends on multiple factors:

If the key is not a part of the C<%multikeys> structure, a scalar of the
value for the key is returned.

If the key is in C<%multikeys>, in array context, the full array of
values accumulated is returned. In scalar context, only the first entry
is returned.

=back

=head1 EXAMPLE

The following shows an example for a transient file that is passed
between a concrete planner and the site selector module:

    version=2.0
    transformation=air::alignwarp
    derivation=air3::i1129_3.anon000001
    job.id=ID000001
    wf.manager=dagman
    wf.label=air25-0
    wf.stamp=20040713215939Z
    vo.name=ivdgl
    vo.group=ivdgl1
    resource.id=UM_ATLAS gsiftp://atgrid.grid.umich.edu/
    resource.id=Rice_Grid3 gsiftp://bonner-pcs11.rice.edu/
    resource.id=CalTech_Grid3 gsiftp://citgrid3.cacr.caltech.edu/
    resource.id=ATLAS_SMU gsiftp://mcfarm.physics.smu.edu/
    resource.id=UBuffalo_CCR gsiftp://acdc.ccr.buffalo.edu/
    resource.id=KNU gsiftp://cluster28.knu.ac.kr/
    resource.id=UC_ATLAS_Tier2 gsiftp://tier2-01.uchicago.edu/
    resource.id=UTA_DPCC gsiftp://atlas.dpcc.uta.edu/
    resource.id=BNL_ATLAS gsiftp://spider.usatlas.bnl.gov/
    resource.id=IU_ATLAS_Tier2 gsiftp://atlas.iu.edu/
    resource.id=PDSF gsiftp://pdsfgrid3.nersc.gov/
    resource.id=PDSF gsiftp://pdsfgrid2.nersc.gov/
    resource.id=PDSF gsiftp://pdsfgrid1.nersc.gov/
    resource.id=UNM_HPC gsiftp://lldimu.alliance.unm.edu/
    input.lfn=fmri.3472-3_anonymized.img
    input.lfn=fmri.1129-3_anonymized.hdr
    input.lfn=fmri.1129-3_anonymized.img
    input.lfn=fmri.3472-3_anonymized.hdr

More keys may be added at the leisure of the planners. Site selectors
should ignore keys they don't understand, but not warn.

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
