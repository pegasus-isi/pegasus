#
# Provides access to the workflow database
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
package Work::Workflow;
use 5.006;
use strict;
use warnings;

require Exporter;
our @ISA = qw(Exporter);

# declarations of methods here. Use the commented body to unconfuse emacs

# Items to export into callers namespace by default. Note: do not export
# names by default without a very good reason. Use EXPORT_OK instead.
# Do not simply export all your public functions/methods/constants.
our $VERSION = '0.1';
$VERSION=$1 if ( '$Revision$' =~ /Revision:\s+([0-9.]+)/o );

# name of tables to check for in the database dictionary of the rDBMS
our @tables = qw(wf_work wf_jobstate wf_siteinfo);

our %EXPORT_TAGS = ();
our @EXPORT_OK = qw($VERSION @tables);
our @EXPORT = qw();

# Preloaded methods go here.
use Carp;
use File::Spec;
use Work::Properties qw(%system);
use Work::Common; 
use DBI 1.38;			# installed_versions was added in 1.38

#
# helpers to translate JDBC to Perl
#
sub determine_driver($;$) {
    # purpose: Determines the database driver to use for DBI
    # paramtr: $props (IN): handle to the properties
    #          $default (opt. IN): if defined, alternative default
    # returns: name of DBD to use, or dies.
    my $props = shift;
    my $default = shift;        # may be undef'd

    # determined the database system we work with - default to SQLite2
    my $driver = $props->property('pegasus.catalog.work.db.driver') || $props->property('pegasus.catalog.*.db.driver') || $default ||
	die "ERROR: Please set property pegasus.catalog.work.db.driver or pegasus.catalog.*.db.driver to a database driver!\n";
    my %installed = %{ scalar DBI->installed_versions };
    if ( exists $installed{$driver} ) {
        # example: DBD::SQLite2, DBD::mysql, DBD::Pg
        substr($driver,0,5,'');
    } elsif ( exists $installed{"DBD::$driver"} ) {
        # example: SQLite2, mysql, Pg
    } else {
        # example: MySQL, pg
        my $a = lc($driver);
        my ($found,$lowkey);
        foreach my $key ( keys %installed ) {
            $lowkey = lc($key);
            if ( $a eq $lowkey || "dbd::$a" eq $lowkey ) {
                $found = 1;
                warn "Warning: Mapping $driver to $key\n" if ( $^W );
                $driver = substr($key,5);
            }               
        }
        unless ( $found ) {
            my $msg = "ERROR: Unable to utilize a DBI driver \"$driver\".\n";
            $msg .= "Available drivers: ";
            $msg .= join( ", ", map { substr($_,5) } sort keys %installed );
            croak( $msg );
        }
    }    
    $driver;
}

sub determine_connection($$) {
    # purpose: Determine the specifics of the database connection
    # paramtr: $props (IN): handle to properties
    #          $driver (IN): chosen DBI driver with DBD:: prefix
    # returns: [0] database uri
    #          [1] database user
    #          [2] database password
    #          [3] database name in case of SQLite
    my $props = shift;
    my $driver = shift;

   
    my %driverprops=$props->jdbc2perl('work');
    my $dburi = $driverprops{'uri'};
    my $dbuser = $driverprops{'dbuser'};
    my $dbpass = $driverprops{'dbpass'};

    my $dbname=undef;
    unless ( defined $dburi ) {
        if ( $driver =~ /SQLite/ ) {
            $dbname = File::Spec->catfile( $props->property('user.home') ||
                                           $system{'user.home'} || '/tmp',
                                           'workstat.db' );
        } else {
            $dbname = $props->property('user.name') || $system{'user.name'};
        }
    }

    # done
    ($dburi,$dbuser,$dbpass,$dbname);
}

sub verify_tables($$;$) {
    my $driver = shift;
    my ($dburi,$dbuser,$dbpass,$dbname) = @{shift()};
    my $autocommit = shift;
    $autocommit = 1 unless defined $autocommit;


    my $dbh = DBI->connect( $dburi, $dbuser, $dbpass,
			    { RaiseError => 1, 
			      AutoCommit => $autocommit } ) ||
	die( "ERROR: Unable to connect $dburi: ", DBI->errstr, "\n" );

    # slurp table names
    my (@row,%tables);
    my $sth = $dbh->table_info('','','%','TABLE');
    while ( @row = $sth->fetchrow_array ) {
	$tables{lc($row[2])} = 1;
    }
    $sth->finish;
    $dbh->commit unless $autocommit; # finish open transactions NOW
    
    # check for presence of mandatory tables
    foreach my $table ( @tables ) {
	unless ( exists $tables{$table} ) {
	    # table is missing
	    die( "ERROR: Missing table $table in database. Please run the database setup\n",
		 "script for the WF catalog from the \$PEGASUS_HOME/sql subdirectory.\n" );
	}
    }

    # return connect URI
    ($dburi,$dbh);
}
    

#
# ctor
#
sub new {
    # purpose: Initialize an instance variable
    # paramtr: $props (IN): If specified, handle to properties to avoid
    #                       resource intensive reparsing of property files. 
    #          $autocommit: value to set auto-commit to, defaults to 1
    # returns: reference to blessed self
    my $proto = shift;
    my $class = ref($proto) || $proto || __PACKAGE__;

    # optional argument saves repeated parsing of property files
    my $props = shift || Work::Properties->new();
    my $autocommit = shift;
    my $driver = determine_driver($props);
    my @connect = determine_connection($props,$driver);

    # verify existence of essential tables
    my ($uri,$dbh) = verify_tables( $driver, \@connect, $autocommit );

    # create instance and return handle to self
    my $self = {
	m_props => $props, 
	m_driver => $driver, 
	m_dbuser => $connect[1],
	m_dbpass => $connect[2],
	m_dbname => $connect[3],
	m_uri    => $uri,
	m_handle => $dbh,
	m_autocommit => $autocommit,
	m_sth => { },
	};

    bless $self, $class;
}

sub _reconnect {
    my $self = shift;
    unless ( defined $self->{'m_handle'} && $self->{'m_handle'}->{Active} ) {
	# reconnect
	my $uri = $self->{'m_uri'};
	warn "# reconnecting to database\n";
	$self->{'m_handle'} = DBI->connect_cached( $uri,
		$self->{'m_dbuser'}, $self->{'m_dbpass'},
		{ RaiseError => 1, AutoCommit => $self->{'m_autocommit'} } ) 
	    || die( "ERROR: Unable to connect $uri: ", DBI->errstr, "\n" );
	1;
    } else {
	'0 but true';
    }
}

sub handle {
    my $self = shift;
    my $oldv = $self->{'m_handle'};
    $self->{'m_handle'} = shift if ( @_ );
    $oldv;
}

sub driver {
    my $self = shift;
    my $oldv = $self->{'m_driver'};
    $self->{'m_driver'} = shift if ( @_ );
    $oldv;
}

sub error {
    my $self = shift;
    $self->handle->errstr;
}

sub commit {
    my $self = shift;
    $self->handle->commit;
}

sub rollback {
    my $self = shift;
    $self->handle->rollback;
}

#
# --- TABLE wf_work ------------------------------------------------
#

sub dump_work($*) {
    # purpose: print a list of all workflows (and just workflows)
    # paramtr: $file (IN): file handle open for writing
    # returns: number of lines printed
    my $self = shift;
    my $file = shift;
    local(*FH) = defined $file ? $file : *STDOUT;

    # sanity check
    $self->_reconnect;

    my $sth = $self->handle->prepare( q{SELECT * FROM wf_work} );
    $sth->execute();
    
    my ($result,@row) = 0;
    while ( (@row = $sth->fetchrow_array) ) {
	@row = map { $_ || '' } @row;
	print FH 'wf|', join('|',@row), "\n";
	$result++;
    }
    $sth->finish;

    # done
    $result;
}

sub new_work {
    # purpose: inserts a newly planned workflow into work database
    # paramtr: 
    # returns: 1: all is well
    #          0 but def'd: maybe already defined?
    #          undef: check error() for failure reason
    my $self = shift;
    my $basedir = shift || croak "Need a base directory";
    my $vogroup = shift || croak "Need a VO group identifier";
    my $workflow = shift || croak "Need a workflow label";
    my $run = shift || croak "Need the run directory name";
    my $creator = shift || croak "Need the creator's name";
    my $ctime = shift || croak "Need a UTC timestamp of the DAX mtime";
    my $state = shift || 0;	# optional
    my $then = shift || time(); # optional

    # sanity check
    $self->_reconnect;

    # delete-before-insert
    $self->handle->do( q{
	DELETE FROM wf_work WHERE basedir=? 
	AND vogroup=? AND workflow=? AND run=?}, undef,
		       $basedir, $vogroup, $workflow, $run );

    $self->handle->do( q{
	INSERT INTO wf_work(basedir,vogroup,workflow,run,creator,ctime,state,mtime)
	VALUES(?,?,?,?,?,?,?,?)}, undef,
		       $basedir, $vogroup, $workflow, $run,
		       $creator, isodate($ctime), 
		       $state, isodate($then) );
}

sub work_id {
    # purpose: Determines the workflow ID from the secondary key
    # paramtr: $basedir (IN): base directory 
    #          $vogroup (IN): VO group
    #          $workflow (IN): label of the workflow
    #          $run (IN): The run directory 
    # returns: The ID of the (first? only!) matching workflow
    #          or undef in case of error. 
    my $self = shift;
    my $basedir = shift || croak "Need a base directory";
    my $vogroup = shift || croak "Need a VO group identifier";
    my $workflow = shift || croak "Need a workflow label";
    my $run = shift || croak "Need the run directory name";

    # sanity check
    $self->_reconnect;

    my $sth = $self->handle->prepare( q{SELECT id FROM wf_work 
	WHERE basedir=? AND vogroup=? AND workflow=? AND run=?} );
    $sth->execute( $basedir, $vogroup, $workflow, $run );
    my @row = $sth->fetchrow_array;
    $sth->finish;

    @row ? $row[0] : undef;
}

sub assert_work {
    # purpose: Determines or creates the workflow ID from the secondary key
    # paramtr: $basedir (IN): base directory 
    #          $vogroup (IN): VO group
    #          $workflow (IN): label of the workflow
    #          $run (IN): The run directory 
    # returns: The ID of the (first? only!) matching workflow
    #          or undef in case of error. 
    my $self = shift;
    my $basedir = shift || croak "Need a base directory";
    my $vogroup = shift || croak "Need a VO group identifier";
    my $workflow = shift || croak "Need a workflow label";
    my $run = shift || croak "Need the run directory name";

    my $id = $self->work_id( $basedir, $vogroup, $workflow, $run );
    unless ( defined $id ) {
	my $user = $ENV{USER} || $ENV{LOGNAME} || getpwuid($>);
	$id = $self->work_id( $basedir, $vogroup, $workflow, $run )
	    if ( $self->new_work( $basedir, $vogroup, $workflow, $run,
				  $user, time(), 1 ) == 1 );
    }

    $id;
}

sub update_work {
    # purpose: updates the state of an already planned workflow
    # paramtr: $wfid (IN): workflow identifier to locate record
    #          $state (opt. IN): new workflow state, defaults to 1
    #          $stamp (opt. IN): UTC timestamp, defaults to now
    # returns: 
    my $self = shift;

    my $wfid = shift;
    croak "Need a workflow ID" unless defined $wfid;

    my $state = shift;
    $state=1 unless defined $state; # 0 is a valid state

    my $stamp = shift || time(); # optional

    # sanity check
    $self->_reconnect;

    my $rv = $self->handle->do( q{UPDATE wf_work SET state=?, mtime=? 
				  WHERE id=?}, undef, 
				$state, isodate($stamp), $wfid );
    print "# updated state in workman\n" if $main::DEBUG;
    $rv;
}

#
# --- TABLE wf_siteinfo --------------------------------------------
#

sub new_siteinfo {
    # purpose: Unconditionally insert a new site into site stats
    # paramtr: $site (IN): site to insert
    #          $then (opt. IN): optional mtime for new site
    # returns: 1 if ok, undef on error to insert
    my $self = shift;
    my $site = shift;
    my $then = shift || time();

    # sanity check
    $self->_reconnect;

    if ( ! exists $self->{'m_sth'}->{newsite} ) {
	$self->{'m_sth'}->{newsite} = $self->handle->prepare_cached(
	    q{INSERT INTO wf_siteinfo(handle,mtime) VALUES(?,?)},
	    { RaiseError => 0 } );
    }
    $self->{'m_sth'}->{newsite}->execute( $site, isodate($then) );
}

sub siteinfo_id {
    # purpose: Determines the id of a given site
    # paramtr: $site (IN): site handle to look up the id for.
    # returns: the ID, or undef in case of failure
    my $self = shift;
    my $site = shift;

    # sanity check
    $self->_reconnect;

    if ( ! exists $self->{'m_sth'}->{siteid} ) {
	$self->{'m_sth'}->{siteid} = $self->handle->prepare_cached(
            q{SELECT id FROM wf_siteinfo WHERE handle=?},
	    { RaiseError => 0 } );
    }

    my $x;
    if ( $self->{'m_sth'}->{siteid}->execute($site) ) {
        $x = $self->{'m_sth'}->{siteid}->fetchall_arrayref;
	undef $x if $self->{'m_sth'}->{siteid}->err;
    }

    defined $x ? $x->[0]->[0] : undef;
}

# translate into column names
my %xlate = ( O => 'other', P => 'pending', R => 'running',
	      S => 'success', F => 'failure' );

sub update_siteinfo {
    # purpose: Update the statistics for a given site
    # paramtr: $site (IN): site handle
    #          $what (IN): state description letter
    #          $diff (IN): +1 or -1 to update the state
    #          $when (opt. IN): timestamp to associate with update
    # returns: 1 for OK, others for errors
    my $self = shift;
    my $site = shift;
    my $what = shift;
    my $diff = shift;
    my $when = shift || time;

    # sanity check
    $self->_reconnect;

    my $rv;
    my $then = isodate($when);
    if ( exists $xlate{$what} ) {
        my $q = 'UPDATE wf_siteinfo SET mtime=?, ';
	$q .= $xlate{$what} . ' = ' . $xlate{$what} . '+?';
	$q .= ', smtime=\'' . $then . '\'' if $what eq 'S';
	$q .= ', fmtime=\'' . $then . '\'' if $what eq 'F';
	$q .= ' WHERE handle=?';

	$rv = $self->handle->do( $q, { RaiseError => 0 }, 
				 $then, $diff, $site );
	carp( "While updating $site: ", $self->errstr, "\n" ) unless $rv;
    }

    $rv;
}

#
# --- TABLE wf_jobstate --------------------------------------------
#

sub dump_jobstate($*;$) {
    # purpose: print a list of all job states of either one or all workflows
    # paramtr: $file (opt. IN): file handle open for writing, default STDOUT
    #          $wfid (opt. IN): discriminating workflow id, default all 
    # returns: number of lines printed
    my $self = shift;
    my $file = shift;
    local(*FH) = defined $file ? $file : *STDOUT;
    my $wfid = shift;

    # sanity check
    $self->_reconnect;

    my $query = 'SELECT * FROM wf_jobstate';
    $query .= " WHERE wfid=$wfid" if defined $wfid;
    my $sth = $self->handle->prepare($query);
    $sth->execute();
    
    my ($result,@row) = 0;
    while ( (@row = $sth->fetchrow_array) ) {
	@row = map { $_ || '' } @row;
	print FH 'job|', join('|',@row), "\n";
	$result++;
    }
    $sth->finish;

    # done
    $result;
}

sub create_ps_jobstate {
    # purpose: Creates the prepared statement handles for jobstates
    # returns: number of created statement handles
    my $self = shift;
    my $flag = 0;

    # what happens to reference copies?
    print "# in Work::WF::create_ps_jobstate\n"
	if ( $main::DEBUG & 0x0100 );

    # sanity check
    $self->_reconnect;

    if ( ! exists $self->{'m_sth'}->{deljob} ) {
	$self->{'m_sth'}->{deljob} = $self->handle->prepare_cached(
	    q{DELETE FROM wf_jobstate WHERE wfid=? AND jobid=?} );
	$flag++ if defined $self->{'m_sth'}->{deljob};
    }
    if ( ! exists $self->{'m_sth'}->{insjob} ) {
	$self->{'m_sth'}->{insjob} = $self->handle->prepare_cached(
            q{INSERT INTO wf_jobstate(wfid,jobid,state,mtime,site)
	      VALUES(?,?,?,?,?)} );
	$flag++ if defined $self->{'m_sth'}->{insjob};
    }
    if ( ! exists $self->{'m_sth'}->{updjob} ) {
	$self->{'m_sth'}->{updjob} = $self->handle->prepare_cached(
	    q{UPDATE wf_jobstate SET state=?, mtime=?, site=?
	      WHERE wfid=? AND jobid=?} );
	$flag++ if defined $self->{'m_sth'}->{updjob};
    }

    $flag;
}

sub add_jobstate {
    # purpose: Assigns a job state when it is unknown, if the job exists.
    # paramtr: $wfid (IN): workflow ID
    #          $jobid (IN): job identifier
    #          $state (IN): job state
    #          $mtime (IN): UTC timestamp of entering state
    #          $site (opt. IN): resource to run at, undef => NULL
    # returns: 1 if all is well,
    #          0 but def'd for inability to update
    #          undef for hard error.
    my $self = shift;
    my $wfid = shift;
    my $jobid = shift || croak "Need a valid job ID";
    my $state = shift || croak "Need a valid job state";
    my $mtime = shift || time;	# optional
    my $site = shift;		# may be undef'd

    print "# ADD: wfid=$wfid, jobid=$jobid, state=$state\n" 
	if ( $main::DEBUG & 0x0100 );
    $self->create_ps_jobstate 
	unless ( exists $self->{'m_sth'}->{deljob} && 
		 exists $self->{'m_sth'}->{insjob} );

    # sanity check
    $self->_reconnect;

    # delete-before-insert assignment emulation
    $self->{'m_sth'}->{deljob}->execute( $wfid, $jobid );
    $self->{'m_sth'}->{insjob}->execute( $wfid, $jobid, 
					 $state, isodate($mtime), $site );
}

sub new_jobstate {
    # purpose: Assigns a new job state. The job most not exist.
    # paramtr: $wfid (IN): workflow ID
    #          $jobid (IN): job identifier
    #          $state (IN): job state
    #          $mtime (IN): UTC timestamp of entering state
    #          $site (opt. IN): resource to run at, undef => NULL
    # returns: 1 if all is well,
    #          0 but def'd for inability to update
    #          undef for hard error.
    my $self = shift;
    my $wfid = shift;
    my $jobid = shift || croak "Need a valid job ID";
    my $state = shift || croak "Need a valid job state";
    my $mtime = shift || time;	# optional
    my $site = shift;		# may be undef'd

    print "# NEW: wfid=$wfid, jobid=$jobid, state=$state\n"
	if ( $main::DEBUG & 0x0100 );
    $self->create_ps_jobstate unless ( exists $self->{'m_sth'}->{insjob} );

    # sanity check
    $self->_reconnect;

    $self->{'m_sth'}->{insjob}->execute( $wfid, $jobid, 
					 $state, isodate($mtime), $site );
}

sub update_jobstate {
    # purpose: Updates an existing job state. The job must exist.
    # paramtr: $wfid (IN): workflow ID
    #          $jobid (IN): job identifier
    #          $state (IN): job state
    #          $mtime (IN): UTC timestamp of entering state
    #          $site (opt. IN): resource to run at, undef => NULL
    # returns: 1 if all is well,
    #          0 but def'd for inability to update
    #          undef for hard error.
    my $self = shift;
    my $wfid = shift;
    my $jobid = shift || croak "Need a valid job ID";
    my $state = shift || croak "Need a valid job state";
    my $mtime = shift || time;	# optional
    my $site = shift;		# may be undef'd

    print "# UPD: wfid=$wfid, jobid=$jobid, state=$state\n"
	if ( $main::DEBUG & 0x0100 );
    $self->create_ps_jobstate unless ( exists $self->{'m_sth'}->{updjob} );

    # sanity check
    $self->_reconnect;

    $self->{'m_sth'}->{updjob}->execute( $state, isodate($mtime), $site,
					 $wfid, $jobid );
}
	
#
# return 'true' to package loader
#
1;
