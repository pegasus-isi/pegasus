--
-- $Id$
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

--ISI-CONDOR

insert into sites(id,name,jobmanagers,vo,num_cpus,
  cs_gatekeeper_hostname,cs_gatekeeper_port,cs_gsiftp_hostname,cs_gsiftp_port,
  ss_gatekeeper_hostname,ss_gatekeeper_port,ss_gsiftp_hostname,ss_gsiftp_port) 
values( 1, 'isi-condor', 'fork,condor', 'private', 25,
	'columbus.isi.edu', 2119, 'docwriter.isi.edu', 2811, --GT4
	'columbus.isi.edu', 2119, 'smarty.isi.edu', 2811 ); --GT4

insert into site_info(id,site_name,ncpus,appdir,datadir,tmpdir,wntmpdir,grid3dir)
values( 1, 'isi-condor', 25, -- vCPUs
	'/nfs/cgt-scratch/$user', '/nfs/cgt-scratch/$user','/nfs/cgt-scratch/$user','/tmp','/nfs/asd2/pegasus/software/linux/vdt/default' );


--ISI-SKYNET
insert into sites(id,name,jobmanagers,vo,num_cpus,
  cs_gatekeeper_hostname,cs_gatekeeper_port,cs_gsiftp_hostname,cs_gsiftp_port,
  ss_gatekeeper_hostname,ss_gatekeeper_port,ss_gsiftp_hostname,ss_gsiftp_port) 
values( 2, 'isi-skynet', 'fork,pbs', 'private', 25,
	'skynet-login.isi.edu', 2119, 'skynet-1.isi.edu', 2811,
	'skynet-login.isi.edu', 2119, 'skynet-2.isi.edu', 2811 );

insert into site_info(id,site_name,ncpus,appdir,datadir,tmpdir,wntmpdir,grid3dir)
values( 2, 'isi-skynet', 96, -- vCPUs
	'/pvfs2/$user', '/pvfs2/$user','/pvfs2/$user','/scratch','/nfs/software/vds/default' );

--ISI-VIZ

insert into sites(id,name,jobmanagers,vo,num_cpus,
  cs_gatekeeper_hostname,cs_gatekeeper_port,cs_gsiftp_hostname,cs_gsiftp_port,
  ss_gatekeeper_hostname,ss_gatekeeper_port,ss_gsiftp_hostname,ss_gsiftp_port) 
values( 3, 'isi-viz', 'fork,pbs', 'private', 16,
	'viz-login.isi.edu', 2119, 'viz-login.isi.edu', 2811, --GT4
	'viz-login.isi.edu', 2119, 'viz-login.isi.edu', 2811 ); --GT4

insert into site_info(id,site_name,ncpus,appdir,datadir,tmpdir,wntmpdir,grid3dir)
values( 3, 'isi-pbs', 16, -- vCPUs
	'/nfs/home/$user', '/nfs/home/$user','/nfs/home/$user','/tmp','/nfs/home/gmehta/software/linux/vdt/default' );
