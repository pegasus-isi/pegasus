package Euryale::Delegation;
#
# Contains GT4 delegation-specific functionality that is shared between
# prescript and postscript.
# $Id$
#
use 5.006;
use strict;
use vars qw(@ISA @EXPORT @EXPORT_OK $VERSION);
use subs qw(log);		# replace Perl's math log with logging

use Exporter;
@ISA = qw(Exporter);

# create the function prototypes for type coercion
sub parse_epr($);		# { }
sub print_epr($$$%);		# { }
sub vdd_socket();		# { }
sub vdd_connect($);		# { }
sub vdd_get_delegation(*$;$); # { }

# create the export lists
$VERSION=$1 if ( '$Revision$' =~ /Revision:\s+([0-9.]+)/o );
@EXPORT = qw(parse_epr print_epr vdd_socket vdd_get_delegation vdd_connect);
@EXPORT_OK = qw($VERSION);

my $prefix = "[Euryale::Delegation] ";

#
# --- start -----------------------------------------------------
#
use Carp;
use Socket;
use XML::Parser::Expat;
use GriPhyN::Log;

sub parse_epr($) {
    # purpose: parse the EPR components from a given EPR file
    # paramtr: $eprfn (IN): EPR filename
    # returns: hash: Address, DelegationKey
    my $eprfn = shift;
    my $parser = new XML::Parser::Expat( 'Namespaces' => 1 ) ||
	croak "ERROR: Unable to instantiate XML parser";
    my (%result,@stack) = ();

    $parser->setHandlers( 'Start' => sub {
	my $self = shift;
	my $element = shift;
	my %attr = @_;
	push( @stack, $element );
    }, 'End' => sub {
	my $self = shift;
	my $element = shift;
	pop(@stack) eq $element;
    }, 'Char' => sub {
	my $self = shift;
	local($_) = shift;
	$result{$stack[$#stack]} = $_ unless ( /^\s*$/ );
	1;
    } );

    if ( index( $eprfn, 'DelegatedEPR' ) >= 0 ) {
	# argument is XML data
	$parser->parse($eprfn);
    } else {
	# argument is a filename
	$parser->parsefile($eprfn);
    }

    %result;
}

sub print_epr($$$%) {
    # purpose: print a parsed EPR as credential into file handle
    # paramtr: $fh (IN): file handle to print to
    #          $tag (IN): credential element name to use
    #          $indent (IN): indentation as string of spaces
    #          %epr (IN): output from parse_epr
    my $fh = shift;
    my $tag = shift;
    my $indent = shift;
    $indent = ' ' x $indent if ( $indent > 0 );
    my %epr = ( @_ );

    print $fh "$indent<$tag xsi:type=\"ns1:EndpointReferenceType\" ";
    print $fh 'xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" ';
    print $fh 'xmlns:ns1="http://schemas.xmlsoap.org/ws/2004/03/addressing">';
    print $fh "\n$indent <ns1:Address xsi:type=\"ns1:AttributedURI\">";
    print $fh $epr{Address}, "</ns1:Address>\n";
    print $fh "$indent <ns1:ReferenceProperties xsi:type=\"ns1:ReferencePropertiesType\">\n";
    print $fh "$indent  <ns1:DelegationKey ";
    print $fh 'xmlns:ns1="http://www.globus.org/08/2004/delegationService">';
    print $fh $epr{DelegationKey}, "</ns1:DelegationKey>\n";
    print $fh "$indent </ns1:ReferenceProperties>\n";
    print $fh "$indent <ns1:ReferenceParameters xsi:type=\"ns1:ReferenceParametersType\"/>\n";
    print $fh "$indent</$tag>\n";
}

sub vdd_socket() {
    # purpose: Obtains the socket filename that should be used
    # returns: The socket filename
    return File::Spec->catfile( File::Spec->tmpdir, "vdd-$>.sock" );
}

sub show_sockaddr($) {
    # purpose: translate a socket address into something symbolic
    # paramtr: $sa (IN): socket address to translate
    # returns: string of translation
    my $sa = shift;
    my $family = sockaddr_family($sa);
    if ( $family == AF_INET ) {
	my ($port,$host) = unpack_sockaddr_in($sa);
	return inet_ntoa($host) . ':' . $port;
    } elsif ( $family == AF_UNIX ) {
	return unpack_sockaddr_un($sa);
    }

    # default: error
    undef;
}

sub vdd_connect($) {
    # purpose: connects to a vds-delegation-daemon server
    # paramtr: $bind (IN): either socket filename or socket address filename
    # returns: file descriptor, or undef if not able to connect
    my $bind = shift;
    local(*SOCK);
    
    # sanity check
    return undef unless ( defined $bind && length($bind) );

    if ( -S $bind ) {
	# unix socket
	unless ( socket( SOCK, PF_UNIX, SOCK_STREAM, 0 ) ) {
	    log( $prefix, "socket PF_UNIX: $!" );
	    return undef;
	}
	unless ( connect( SOCK, pack-sockaddr_un($bind) ) ) {
	    log( $prefix, "connect $bind: $!" );
	    return undef;
	}
    } else {
	# inet socket
	local(*SA);
	unless ( open( SA, "<$bind") ) {
	    log( $prefix, "open $bind: $!" );
	    return undef;
	}
	chomp($_ = <SA>);
	close SA;

	my ($host,$port) = split /:/;
	my $proto = getprotobyname('tcp') || return undef;
	unless ( socket( SOCK, PF_INET, SOCK_STREAM, $proto ) ) {
	    log( $prefix, "socket PF_INET: $!" );
	    return undef;
	}
	my $addr = inet_aton($host);
	unless ( connect( SOCK, pack_sockaddr_in($port,$addr) ) ) {
	    log( $prefix, "connect $host:$port: $!" );
	    return undef;
	}
    }

    # unbuffer
    my $tmp = select SOCK; 
    $|=1;
    select $tmp;

    log( $prefix, "client address ", show_sockaddr(getsockname(SOCK)) );
    return *SOCK;
}

sub vdd_get_delegation(*$;$) {
    # purpose: ask cache for the delegation
    # paramtr: $sock (IN): socket connected to server
    #          $uri (IN): URI to request from cache
    #          $ttl (opt. IN): seconds lifetime
    # returns: delegation as continuous string, or undef in case of failure
    my $sock = shift;
    my $uri = shift || croak "need a URI to request";
    my $ttl = shift;

    # sanity check
    return undef unless defined $sock;

    # send request
    unless ( defined send( $sock, "GET $uri\r\n", 0 ) ) {
	log( $prefix, "GET uri: $!" );
	return undef;
    }

    # read answer
    $_ = <$sock>;		# header
    s/[\r\n]+$//;
    log( $prefix, $_ );

    my $delegate = undef;
    if ( substr( $_, 0, 6 ) eq '200 OK' ) {
	# got delegation
	/length=(\d+)/;
	my $size = $1;
	unless ( defined read( $sock, $_, $size ) ) {
	    log( $prefix, "read: $!" );
	    return undef;
	}
	$delegate = $_;
    }

    $delegate;
}

#
# return 'true' to package loader
#
1;
__END__

=head1 NAME

Euryale::Delegation - client for the GT4 credential delegation cache daemon.

=head1 SYNOPSIS

   use Euryale::Delegation;

   my $sockfn = vdd_socket();
   my $sockfh = vdd_connect( $sockfn ) || die "connect to server: $!";
   my $delegation = vdd_get_delegation( $sockfh, $uri );
   if ( defined $delegation ) {
      my %x = parse_epr( $delegation );
      if ( open( EPR, ">my.epr") ) {
         print_epr( EPR, 'DelegationEPR', '', %x );
         close EPR;
      }
   }
   
=head1 DESCRIPTION

The Euryale::Delegation module deals with the locally running GT4
credential delegation cache daemon. The daemon maintains a cache of
remote user credential delegations.

=head1 METHODS

=over 4

=item parse_epr( $eprfn_or_string )

This method parses the EPR from either a given filename, or given as
data directly. It returns a hash with typically two fields, where
B<Address> points to the remote delegation service, and B<DelegationKey>
contains the UUID of the delegation.

In case of error, a partial or empty hash can be returned. 

=item print_epr( FH, $tag, $indent, %epr )

This method reconstructs the EPR of the delegated credential on the
specified output stream. It uses the C<$tag> content for the root
element of the delegation. C<$indent> determines the amount of 
indentation for pretty-printing. And C<%epr> is the hash, as created
by C<parse_epr>.

=item vdd_socket()

This method determines the socket filename that is used to communicate
the server's address (if INET) or socket filename (if UNIX). A string
will always be returned. 

You can check for the existence of the returned file. If it does not
exist, the server is not up. 

=item vdd_connect( $sockfn )

This method connects to the remote cache server. The server usually puts
a sock file C<vdd-$&gt;.sock> into the temporary directory. The sock file 
is either a Unix (local) socket, or contains the internet address where
the server is listening. 

The method returns an open socket descriptor upon success, or undef in 
case of failure.

=item vdd_get_delegation( $sock, $uri [, $ttl] )

The delegation getter uses an established connection to the server to
obtain a delegation for a given remote site. From the C<$uri>, only its
scheme, host and port are important. Optionally, a time to live in
seconds can be specified. 

This method returns undef in case of error. It returns the full
delegation XML document upon success.

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
