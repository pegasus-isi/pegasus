--
-- $Id: create-isi.sql,v 1.3 2006/03/13 22:56:49 gmehta Exp $
-- This is a SQLite script
-- sqlite file.db < this-script.sql
--

create table sites ( 
	id			integer primary key, -- *
	owning_id		integer, 
	name			varchar(128), -- *
	services		varchar(128) default 'CS',
	cs_gatekeeper_hostname	varchar(128),	-- *
	cs_gatekeeper_port	integer default 2119, -- *
	cs_gsiftp_hostname	varchar(128), -- *
	cs_gsiftp_port		integer default 2811, -- *
	ss_gatekeeper_hostname	varchar(128), -- * 
	ss_gatekeeper_port	integer default 2119, -- *
	ss_gsiftp_hostname	varchar(128), -- *
	ss_gsiftp_port		integer default 2811, -- *
	ldap_server_hostname	varchar(128),
	ldap_server_port	integer default 2135,
	url			varchar(128),
	jobmanagers		varchar(128), -- * fork, ...
	vo			varchar(128) default '__vo__', -- * (unused) 
	os			varchar(128) default '__os__',
	num_cpus		integer default 0, -- *
	location		varchar(128)
);
	
create table site_info (
	id		integer, -- * references sites.id
	site_name	varchar(128),
	ymdt		varchar(128),
	host_name	varchar(128),
	voname		varchar(255),
	appdir		varchar(255), -- *
	datadir		varchar(255), -- *
	tmpdir		varchar(255), -- *
	wntmpdir	varchar(255), -- * 
	grid3dir	varchar(255), -- *
	jobcon		varchar(255) default 'condor',
	utilcon		varchar(255) default 'fork',
	locpname1	varchar(255),
	locpname2	varchar(255),
	ncpurunning	integer default null,
	ncpus		integer default null -- *
);

--TG-NCSA

insert into sites(id,name,jobmanagers,vo,num_cpus,
  cs_gatekeeper_hostname,cs_gatekeeper_port,cs_gsiftp_hostname,cs_gsiftp_port,
  ss_gatekeeper_hostname,ss_gatekeeper_port,ss_gsiftp_hostname,ss_gsiftp_port) 
values( 1, 'tg_ncsa', 'fork,pbs', 'teragrid', 10,
	'grid-hg.ncsa.teragrid.org', 2119, 'gridftp-hg.ncsa.teragrid.org', 2811, --GT4
	'grid-hg.ncsa.teragrid.org', 2119, 'gridftp-hg.ncsa.teragrid.org', 2811 ); --GT4

insert into site_info(id,site_name,ncpus,appdir,datadir,tmpdir,wntmpdir,grid3dir)
values( 1, 'tg_ncsa', 10, -- vCPUs
	'/usr/projects/Pegasus', '/gpfs_scratch1','/gpfs_scratch1','/tmp','/usr/projects/Pegasus/SOFTWARE/pegasus/default' );

--TG-SDSC

insert into sites(id,name,jobmanagers,vo,num_cpus,
  cs_gatekeeper_hostname,cs_gatekeeper_port,cs_gsiftp_hostname,cs_gsiftp_port,
  ss_gatekeeper_hostname,ss_gatekeeper_port,ss_gsiftp_hostname,ss_gsiftp_port)
values( 2, 'tg_sdsc', 'fork,pbs', 'teragrid', 10,
        'tg-login1.sdsc.teragrid.org', 2119, 'tg-gridftp.sdsc.teragrid.org', 2811, --GT4
        'tg-login1.sdsc.teragrid.org', 2119, 'tg-gridftp.sdsc.teragrid.org', 2811 ); --GT4

insert into site_info(id,site_name,ncpus,appdir,datadir,tmpdir,wntmpdir,grid3dir)
values( 2, 'tg_sdsc', 10, -- vCPUs
        '/gpfs/projects/tg_community/Pegasus', '/gpfs','/gpfs','/tmp','/gpfs/projects/tg_community/Pegasus/SOFTWARE/pegasus/default' );

--TG-UC

insert into sites(id,name,jobmanagers,vo,num_cpus,
  cs_gatekeeper_hostname,cs_gatekeeper_port,cs_gsiftp_hostname,cs_gsiftp_port,
  ss_gatekeeper_hostname,ss_gatekeeper_port,ss_gsiftp_hostname,ss_gsiftp_port)
values( 3, 'tg_uc', 'fork,pbs', 'teragrid', 10,
        'tg-grid.uc.teragrid.org', 2119, 'tg-gridftp.uc.teragrid.org', 2811, --GT4
        'tg-gid.uc.teragrid.org', 2119, 'tg-gridftp.uc.teragrid.org', 2811); --GT4

insert into site_info(id,site_name,ncpus,appdir,datadir,tmpdir,wntmpdir,grid3dir)
values( 3, 'tg_uc', 10, -- vCPUs
        '/soft/community/pegasus/VDS', '/disks/scratchgpfs1','/disks/scratchgpfs1','/tmp','/soft/community/pegasus/SOFTWARE/pegasus/default' );

