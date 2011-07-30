package Pegasus::Catalog::Site::TG;
#  Copyright 2007 University Of Southern California
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
# Author: Gaurang Mehta gmehta at isi dot edu
# Revision : $Revision$
# Id : $Id$
#
use strict;
use File::Temp qw/ tempfile tempdir/;
use Socket;
use XML::Parser;
use vars qw($VERSION $AUTOLOAD);
use Exporter;

our @ISA = qw(Exporter);
our @EXPORT = qw($VERSION convert_siteinfo get_sites);

sub convert_siteinfo(%);
sub get_sites($$);

#our @EXPORT_OK = qw($VERSION convert_siteinfo_to_vds get_site_information get_sites_in_grid);
our @EXPORT_OK = qw();
$VERSION=$1 if ( '$Revision: 50 $' =~ /Revision:\s+([0-9.]+)/o );
my ( @stack, %sites );
my (
	$siteid,   $resourceid,     $resourcename,
	$kit,      $support,        $service,
	$endpoint, $serviceversion, $servicename
);

###################
# %sites data layout
#kits are of type "SCHEDD", "DATA", "FILESYSTEM"
#service are of type pre-wsgram , ws-gram
#
#   %sites={$site_$support=> { $kit => { $service =>  { Name=<name>, Type=><type>, Endpoint=<ep>, Version=<ver>}
#	                                     }
#	                           }
#             }
###

my %kits = (
	'remote-compute.teragrid.org' => 'SCHEDD',
	'data-movement.teragrid.org'  => 'DATA',
	'wan-gpfs.teragrid.org'       => 'FILESYSTEM'
);



# hashmap that maps  vds-get-sites kerys to keys in VORS namespace
my %sitetacalogkeys = (
    NAME => 'name',
    appdir                 => 'app_loc',
    datadir                => 'data_loc',
    grid3dir               => 'osg_grid', #osg_grid is now populated
    wntmpdir               => 'wntmp_loc',
    tmpdir                 => 'tmp_loc',
    CS_GATEKEEPER => 'cs_gatekeeper_hostname',
    CS_GKPORT => 'cs_gatekeeper_port',
    CS_GRIDFTP => 'cs_gsiftp_hostname',
    CS_GRIDFTP_PORT => 'cs_gsiftp_port',
    SS_GATEKEEPER => 'ss_gatekeeper_hostname',
    SS_GKPORT => 'ss_gatekeeper_port',
    SS_GRIDFTP => 'ss_gsiftp_hostname',
    SS_GRIDFTP_PORT => 'ss_gsiftp_port',
    VO => 'vo',
    VERSION => 'vdt_version'
    
);



sub convert_siteinfo(%){
    #purpose: an adapter function to translate site information from
    #         webmds to sitecatalog format.
    #paramtr: %sites information about sites in the WEBMDS format.
    #
    #returns: a hash of hash containing information about the site in sitecatalog format.
    #  
    my %sites = @_;

    my %sitecatalog; #stores information in sitecatalog format


    for my $id ( keys %sites ) {
   print "Site=$id \n";
       for my $kit ( keys %{ $sites{$id} } ) {
           print "  KIT=$kit \n";
           for my $support ( keys %{ $sites{$id}->{$kit} } ) {
           	   if($support eq "production"){	
           	   
               print "  SUPPORT=$support \n";
                 for my $service ( keys %{ $sites{$id}->{$kit}->{$support} } ) {
                   print "    SERVICE=$service \n";
                                     
                    my %entries = %{$sites{$id}->{$kit}->{$support}->{$service}};
                     print "      $entries{ENDPOINT}\n";
                     print "      $entries{RESOURCE}\n";
                     print "      $entries{NAME}\n";
                     if($service eq "prews-gram"){
                     	my $siteid = $entries{RESOURCE};
                     	$siteid =~ s/ /_/g;
                     	$siteid .= "_$SUPPORT";
                     	$sites{$siteid}->{name} = $siteid;
                        $sites{$siteid}->{cs_gatekeeper_host}=$entries{ENDPOINT};                     	
                     }

                } 
           	}
       }
   }
    
    foreach my $key (keys %keys_vds_to_vors_adapter ){
    $vds_site{$key} = $vors_site{ $keys_vds_to_vors_adapter{$key}};
    }

    #set grid3dir to appdir if undefined in VORS
    if (!defined($vds_site{'grid3dir'}) ){
    warn "# substituting OSG_GRID value to $vds_site{'appdir'} for site $vds_site{'name'} \n";
    $vds_site{'grid3dir'} = $vds_site{'appdir'} 
    }
    #set the type of jobmanagers
    #Note we losing information here, as transfer 
    #is being implicitly set to fork
    my $jm_types = $vors_site{'exec_jm'};
    $jm_types  =~ s/^\S+\/\S+-//;
    $jm_types = 'fork'.','.$jm_types;
    $vds_site{'jobmanagers'} = $jm_types;

    #some other values populated by default till 
    #we figure out how to get them
    $vds_site{'vo'} = 'ivdgl' if !defined($vds_site{'vo'});
    $vds_site{'num_cpus'} = '50';
    $vds_site{'ncpus'} = '50';


    return %vds_site;
}




sub get_sites($$) {

	#purpose: query a WEBMDS server, and information
	#         for all sites.
	#paramtr: $host  the host where the WEBMDS server is running
	#         $port  the port on the host.    
    #returns: Returns a deep hash structure see above.

	my ( $host, $port ) = @_;

	#construct the HTTP get request
	my $http_get_req = "GET /webmds/webmds?info=tgislocal\015\012";

#	print "host = $host:$port, get string = $http_get_req\n";

	socket( SOCK, PF_INET, SOCK_STREAM, getprotobyname('tcp') )
	  || die "ERROR creating socket: $!";
	connect( SOCK, sockaddr_in( $port, inet_aton($host) ) )
	  || die "ERROR Connecting to $host:$port : $!";
	send SOCK, $http_get_req, 0;

	# store the webmds info in a temp file
	my ( $fh, $filename ) = tempfile("webmds.XXXXXX");

	while (<SOCK>) {
		print $fh $_;
	}
	close(SOCK);
	close($fh);
	parse_xml($filename);
	return %sites;
}

sub parse_xml($) {
	#purpose: Parse WEBMDS XML
	#in: $filename XML file to parse
	my $fn = shift;
	open( my $fh, "<$fn" ) || die "Cannot open file $fn !@ \n";

	
	my $parser = new XML::Parser( 'ErrorContext' => 2 );
	$parser->setHandlers(
	    'XMLDecl' => \&ignore_handler,
		'Start'   => \&start_handler,
		'End'     => \&end_handler,
		'Char'    => \&char_handler,
		'Comment' => \&ignore_handler,
		'Default' => \&default_handler
	);
	$parser->parse($fh);
	close($fh);
}

sub ignore_handler{
	#ignore Comments and XML header for now
}

sub start_handler {
	my $self    = shift;
	my $element = shift;    # name of element
	                        #        my %attr = @_;          # attributes

	#	push( @stack, $element );
	if ( @stack == 0 ) {
		push( @stack, $element ) if ( $element eq 'ns0:IndexRP' );
	}
	elsif ( @stack == 1 && $self->current_element eq 'ns0:IndexRP' ) {
		push( @stack, $element ) if ( $element eq 'ns1:V4KitsRP' );
	}
	elsif ( @stack == 2 && $self->current_element eq 'ns1:V4KitsRP' ) {
		push( @stack, $element ) if ( $element eq 'V4KitsRP' );
	}
	elsif ( @stack == 3 && $self->current_element eq 'V4KitsRP' ) {
		push( @stack, $element ) if ( $element eq 'KitRegistration' );
	}
	elsif ( @stack == 4 && $self->current_element eq 'KitRegistration' ) {
		push( @stack, $element )
		  if ( $element eq 'ResourceName'
			|| $element eq 'ResourceID' );
		if ( $element eq 'Kit' ) {
			push( @stack, $element );
		}
	}
	elsif ( @stack == 5 && $self->current_element eq 'Kit' ) {
		push( @stack, $element )
		  if ( $element eq 'SupportLevel'
			|| $element eq 'Name' );
		if ( $element eq 'Service' ) {
			push( @stack, $element );
		}
	}
	elsif ( @stack == 6 && $self->current_element eq 'Service' ) {
		push( @stack, $element )
		  if ( $element eq 'Type'
			|| $element eq 'Version'
			|| $element eq 'Endpoint'
			|| $element eq 'Name' );
	}

	1;
}

sub end_handler {
	my $self = shift;
	my $el   = shift;

	#   if($elements{$el}){ print "END STACK = " . scalar(@stack) . " el=$el\n";
	#   	print "Stack = @stack\n" }

	pop(@stack)
	  if (
		   ( @stack == 1 && $el eq 'ns0:IndexRP' )
		|| ( @stack == 2 && $el eq 'ns1:V4KitsRP' )
		|| ( @stack == 3 && $el eq 'V4KitsRP' )
		|| ( @stack == 6
			&& ( $el eq 'SupportLevel' || $el eq 'Name' ) )
		|| ( @stack == 5
			&& ( $el eq 'ResourceName' || $el eq 'ResourceID' ) )
		|| (
			@stack == 7
			&& (   $el eq 'Name'
				|| $el eq 'Type'
				|| $el eq 'Version'
				|| $el eq 'Endpoint' )
		)
	  );

	if ( @stack == 4 && $el eq 'KitRegistration' ) {
		undef $resourceid;
		undef $resourcename;
		pop(@stack);
	}
	if ( @stack == 5 && $el eq 'Kit' ) {
		undef $kit;
		undef $support;
		pop(@stack);
	}
	if ( @stack == 6 && $el eq 'Service' ) {
		if (
			(
				$kit eq 'remote-compute.teragrid.org'
				&& ( $service eq 'prews-gram' || $service eq 'ws-gram' )
			)
			|| (   $kit eq 'data-movement.teragrid.org'
				&& $service eq 'gridftp' )
			|| ( $kit eq 'wan-gpfs.teragrid.org' && $service eq 'gpfs' )
		  )
		{
			$sites{$resourceid}->{ $kits{$kit} }->{$support}->{$service}
			  ->{'NAME'} = $servicename;
			$sites{$resourceid}->{ $kits{$kit} }->{$support}->{$service}
			  ->{'VERSION'} = $serviceversion;
			$sites{$resourceid}->{ $kits{$kit} }->{$support}->{$service}
			  ->{'ENDPOINT'} = $endpoint;
			$sites{$resourceid}->{ $kits{$kit} }->{$support}->{$service}
			  ->{'RESOURCE'} = $resourcename;
		}
		undef $service;
		undef $servicename;
		undef $serviceversion;
		undef $endpoint;
		pop(@stack);
	}

}

sub char_handler {
	my $self = shift;
	my $text = shift;

	if ( $text =~ /^\s+$/ ) {

		# ignore
	}
	else {
		my $i  = @stack;
		my $el = $stack[ $i - 1 ];
		if ( length($text) ) {
			if ( $i == 5 ) {
				if ( $el eq 'ResourceName' ) {
					$resourcename .= $text;
				}
				elsif ( $el eq 'ResourceID' ) {
					$resourceid .= $text;
				}
			}
			elsif ( $i == 6 ) {
				if ( $el eq 'SupportLevel' ) {
					$support .= $text;
				}
				elsif ( $el eq 'Name' ) {
					$kit .= $text;
				}
			}
			elsif ( $i == 7 ) {
				if ( $el eq 'Type' ) {
					$service .= $text;
				}
				elsif ( $el eq 'Version' ) {
					$serviceversion .= $text;
				}
				elsif ( $el eq 'Endpoint' ) {
					$endpoint .= $text;

				}
				elsif ( $el eq 'Name' ) {
					$servicename .= $text;
				}
			}
		}
	}
	1;
}



sub default_handler {
	my $self = shift;
	my $text = shift;
	if ( $text =~ /^\s*$/ ) {

		# ignore
	} 
	else {
		print "unknown xml \"$text\", ignoring\n";
	}
	1;
}

#
# return 'true' to package loader
#
1;
__END__

#for my $site ( keys %sites ) {
#	print "Site=$site \n";
#	for my $kit ( keys %{ $sites{$site} } ) {
#		print "  KIT=$kit \n";
#		for my $support ( keys %{ $sites{$site}->{$kit} } ) {
#			print "  SUPPORT=$support \n";
#			for my $service ( keys %{ $sites{$site}->{$kit}->{$support} } ) {
#				print "    SERVICE=$service \n";
#				my %entries =
#				print "      $entries{ENDPOINT}\n";
#				print "      $entries{RESOURCE}\n";
#				print "      $entries{NAME}\n";
#
#			}
#		}
#	}
#	print "\n\n";
#}
