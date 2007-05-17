package GriPhyN::VORS;
#
# abstract base class for site catalog (SC) implementations
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
# Author: Karan Vahi vahi aT isi dot edu
# Revision : $Revision: 1.2 $
#
# $Id: VORS.pm,v 1.2 2006/08/30 20:13:28 vahi Exp $
#
use 5.006;
use strict;
use vars qw($VERSION $AUTOLOAD);
use Exporter;
use Socket;

our @ISA = qw(Exporter);
#our @EXPORT = qw();
our @EXPORT = qw($VERSION convert_siteinfo_to_vds get_site_information get_sites_in_grid);

sub convert_siteinfo_to_vds(%);
sub get_site_information($$$$$);
sub get_sites_in_grid($$$$);

#our @EXPORT_OK = qw($VERSION convert_siteinfo_to_vds get_site_information get_sites_in_grid);
our @EXPORT_OK = qw();
$VERSION=$1 if ( '$Revision: 1.2 $' =~ /Revision:\s+([0-9.]+)/o );



# hashmap that maps  vds-get-sites kerys to keys in VORS namespace
my %keys_vds_to_vors_adapter = (
    name        => 'shortname',
    appdir      => 'app_loc',
    datadir     => 'data_loc',
    grid3dir    => 'osg_grid', #osg_grid is now populated
    wntmpdir    => 'wntmp_loc',
    tmpdir      => 'tmp_loc',
    cs_gatekeeper_hostname => 'gatekeeper',
    cs_gatekeeper_port     => 'gk_port',
    cs_gsiftp_hostname     => 'gatekeeper',
    cs_gsiftp_port         => 'gsiftp_port',
    ss_gatekeeper_hostname => 'gatekeeper',
    ss_gatekeeper_port     => 'gk_port',
    ss_gsiftp_hostname     => 'gatekeeper',
    ss_gsiftp_port         => 'gsiftp_port',
    vo                     => 'sponsor_vo'				
);



sub convert_siteinfo_to_vds(%){
    #purpose: an adapter function to translate site information from
    #         vors to vds-get-sites format.
    #paramtr: $vors_site information about site in the VORS format.
    #
    #returns: a hash containing information about the site in VDS format.

    my %vors_site = @_;

    my %vds_site; #stores information in vds-get-sites format
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


sub get_site_information($$$$$) {
    #purpose: query a VORS server, and get detailed site information for
    #         a particular resource
    #paramtr: $host  the host where the VORS server is running
    #         $port  the port on the host.   
    #         $vo    the VO to which the user belongs to. 
    #         $grid  the grid to which the resource belongs to
    #         $res_id  the grid for which sites are required.
    #         
    #returns: a hash containing information about the site in VORS format.
   
    #I should have sent in a hash containing the connection information!

    my ( $host, $port, $vo, $grid, $res_id ) = @_;
    my %site;
    my ( $iaddr, $paddr, $proto, $line );

    #construct the HTTP get request
    my $http_get_req = 'GET /cgi-bin/tindex.cgi' . '?' . "VO=$vo". "&grid=$grid" . "&res=$res_id\n";   
    
    #print "host = $host:$port, get string = $http_get_req\n";
    
    #connect to the remote host   
    $iaddr = inet_aton( $host ) || die "ERROR, no host: $host";
    $paddr = sockaddr_in( $port, $iaddr );
    $proto = getprotobyname( 'tcp' );
    socket( SOCK, PF_INET, SOCK_STREAM, $proto ) || die "ERROR on socket: $!";
    connect( SOCK, $paddr ) || die "ERROR on connect  to $host:$port : $!";
 
    send SOCK, $http_get_req, 0;

    
    #print " ----------------SITE_INFO START ---------------->\n";
    while (defined($line = <SOCK>)) {

       #ignore commented or empty lines
       if($line =~ /^\#/ || $line eq "" || $line =~ /^\s$/ ){
          #print "\nCommented line $line";
	  next;
       }

       #remove trailing line separator
       chomp($line);

       #remove trailing <BR>
       $line =~ s/<BR>$// ;

       #each line is key=value
       my ($key, $value) = split(/=/ , $line);

       #print "$line\n";

       #skip line if incorrect split
       next if (!defined($value) || $value eq "");

       #print "$key=>$value\n";
       $site{$key}=$value;
    }
    #print "\n----------------- SITE_INFO END ---------------->\n";
    close (SOCK);


    #do some sanitizations on wntmp_loc
    # remove :whitespace at the starting of value
    $site{"wntmp_loc"} =~ s/^:\s//;

    return %site;
}




sub get_sites_in_grid ($$$$) {
    #purpose: query a VORS server, and get the first level information 
    #         for all sites.
    #paramtr: $host  the host where the VORS server is running
    #         $port  the port on the host.    #         
    #         $grid  the grid for which sites are required.
    #         $vo    the VO to which the user belongs to.
    #returns: an array of hash references, with each hash containing 
    #         information about one site.

    my ( $host, $port, $grid, $vo ) = @_;
    my %sites;
    my ( $iaddr, $paddr, $proto, $line);
    
    #construct the HTTP get request
    my $http_get_req = 'GET /cgi-bin/tindex.cgi' . '?' . "VO=$vo". "&grid=$grid\n";   

    #print "host = $host:$port, get string = $http_get_req\n";
    
    $iaddr = inet_aton( $host ) || die "ERROR, no host: $host";
    $paddr = sockaddr_in( $port, $iaddr );
    $proto = getprotobyname( 'tcp' );
    socket( SOCK, PF_INET, SOCK_STREAM, $proto ) || die "ERROR on socket: $!";
    connect( SOCK, $paddr ) || die "ERROR on connect to $host:$port : $!";
    send SOCK, $http_get_req, 0;

  
    my @col_headers = ("ID","NAME","GATEKEEPER","TYPE","GRID","STATUS","LAST","TEST","DATE");

    while (defined($line = <SOCK>)) {
	
	#ignore commented or empty lines
	if($line =~ /^\#/ || $line eq "" ){
	    next;
	}
	
	#print $line;
	
	#create a hash for each site 
	#with keys being picked from the column headers
	my $i = 0;
	my $id;
	foreach my $col (split(/,/ , $line) ){
	    if( $i == 0){
		$id = $col;
	    }
	
	    $sites{$id}{$col_headers[$i]} = $col;
	    $i = $i + 1;
	}   
    }# end of while
	
    close (SOCK);
    return %sites;
}


#
# return 'true' to package loader
#
1;
__END__
