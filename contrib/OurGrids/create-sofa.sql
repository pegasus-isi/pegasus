--
-- $Id: create-sofa.sql,v 1.3 2005/10/03 23:14:24 griphyn Exp $
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

insert into sites(id,name,jobmanagers,vo,num_cpus,
  cs_gatekeeper_hostname,cs_gatekeeper_port,cs_gsiftp_hostname,cs_gsiftp_port,
  ss_gatekeeper_hostname,ss_gatekeeper_port,ss_gsiftp_hostname,ss_gsiftp_port) 
values( 1, 'evitable', 'fork,condor', 'private', 2,
	'evitable.uchicago.edu', 2119, 'evitable.uchicago.edu', 2812, -- GT4 
	'evitable.uchicago.edu', 2119, 'evitable.uchicago.edu', 2812 );
insert into sites(id,name,jobmanagers,vo,num_cpus,
  cs_gatekeeper_hostname,cs_gatekeeper_port,cs_gsiftp_hostname,cs_gsiftp_port,
  ss_gatekeeper_hostname,ss_gatekeeper_port,ss_gsiftp_hostname,ss_gsiftp_port) 
values( 2, 'terminable', 'fork,condor', 'private', 2,
	'terminable.uchicago.edu', 2120, 'terminable.uchicago.edu', 2812, -- GT4 
	'terminable.uchicago.edu', 2120, 'terminable.uchicago.edu', 2812 );
insert into sites(id,name,jobmanagers,vo,num_cpus,
  cs_gatekeeper_hostname,cs_gatekeeper_port,cs_gsiftp_hostname,cs_gsiftp_port,
  ss_gatekeeper_hostname,ss_gatekeeper_port,ss_gsiftp_hostname,ss_gsiftp_port) 
values( 3, 'gainly', 'fork,condor', 'private', 2,
	'gainly.uchicago.edu', 2119, 'gainly.uchicago.edu', 2812, -- GT4 
	'gainly.uchicago.edu', 2119, 'gainly.uchicago.edu', 2812 );
insert into sites(id,name,jobmanagers,vo,num_cpus,
  cs_gatekeeper_hostname,cs_gatekeeper_port,cs_gsiftp_hostname,cs_gsiftp_port,
  ss_gatekeeper_hostname,ss_gatekeeper_port,ss_gsiftp_hostname,ss_gsiftp_port) 
values( 4, 'sheveled', 'fork,condor', 'private', 2,
	'sheveled.uchicago.edu', 2119, 'sheveled.uchicago.edu', 2812, -- GT4 
	'sheveled.uchicago.edu', 2119, 'sheveled.uchicago.edu', 2812 );
insert into sites(id,name,jobmanagers,vo,num_cpus,
  cs_gatekeeper_hostname,cs_gatekeeper_port,cs_gsiftp_hostname,cs_gsiftp_port,
  ss_gatekeeper_hostname,ss_gatekeeper_port,ss_gsiftp_hostname,ss_gsiftp_port) 
values( 5, 'ept', 'fork,condor', 'private', 2,
	'ept.uchicago.edu', 2119, 'ept.uchicago.edu', 2812, -- GT4 
	'ept.uchicago.edu', 2119, 'ept.uchicago.edu', 2812 );

insert into site_info(id,site_name,ncpus,
                      appdir,datadir,tmpdir,wntmpdir,grid3dir)
values( 1, 'evitable', 10, -- vCPUs
	'/home/$user', '/home/$user','/tmp','/var/tmp','/opt/vdldemo' );
insert into site_info(id,site_name,ncpus,appdir,datadir,tmpdir,wntmpdir,grid3dir)
values( 2, 'terminable', 10,  -- vCPUs
	'/home/$user', '/home/$user','/tmp','/var/tmp','/opt/vdldemo' );
insert into site_info(id,site_name,ncpus,appdir,datadir,tmpdir,wntmpdir,grid3dir)
values( 3, 'gainly', 2, 
	'/home/$user', '/home/$user','/tmp','/var/tmp','/opt/vdldemo' );
insert into site_info(id,site_name,ncpus,appdir,datadir,tmpdir,wntmpdir,grid3dir)
values( 4, 'sheveled', 10, 
	'/home/$user', '/home/$user','/tmp','/var/tmp','/opt/vdldemo' );
insert into site_info(id,site_name,ncpus,appdir,datadir,tmpdir,wntmpdir,grid3dir)
values( 5, 'ept', 10, 
	'/home/$user', '/home/$user','/tmp','/var/tmp','/opt/vdldemo' );
