-- MySQL dump 9.10
--
-- Host: localhost    Database: txcatalog
-- ------------------------------------------------------
-- Server version	4.0.18-standard


--
-- DROP ALL THE TABLES IF THEY EXIST
--


DROP TABLE IF EXISTS tc_lfnpfnmap;
DROP TABLE IF EXISTS tc_lfnprofile;
DROP TABLE IF EXISTS tc_pfnprofile;
DROP TABLE IF EXISTS tc_physicaltx;
DROP TABLE IF EXISTS tc_logicaltx;
DROP TABLE IF EXISTS tc_sysinfo;

INSERT INTO vds_schema VALUES ('Database','tc','1.3',current_user(),current_timestamp(0));

--
-- Table structure for table `tc_sysinfo`
--

INSERT INTO sequences VALUES('tc_sysinfo_id_seq',0);

CREATE TABLE tc_sysinfo (
	id		BIGINT NOT NULL auto_increment,
	architecture	VARCHAR(10) NOT NULL default '',
	os		VARCHAR(10) NOT NULL default '',
	glibc		VARCHAR(10) DEFAULT NULL,
	osversion	VARCHAR(10) DEFAULT NULL,
	PRIMARY KEY(id),
	UNIQUE KEY unique_arch(architecture,os,glibc,osversion)
) TYPE=InnoDB COMMENT='Stores the different types of architecture';

--
-- Table structure for table `tc_physicaltx`
--


INSERT INTO sequences VALUES('tc_physicaltx_id_seq',0);


CREATE TABLE tc_physicaltx (
	id		BIGINT NOT NULL AUTO_INCREMENT,
	resourceid	VARCHAR(255) NOT NULL DEFAULT '',
	pfn		VARCHAR(255) DEFAULT NULL,
	type		VARCHAR(20) NOT NULL DEFAULT 'INSTALLED',
--	type		enum('INSTALLED','STATIC_BINARY','DYNAMIC_BINARY','SOURCE','PACMAN_PACKAGE','SCRIPT') NOT NULL DEFAULT 'INSTALLED',
	archid		BIGINT DEFAULT NULL,
	PRIMARY KEY(id),
	UNIQUE KEY unique_physicaltx(resourceid,pfn,type),
	INDEX (archid),
	FOREIGN KEY tc_physicaltx(archid) REFERENCES tc_sysinfo(id) ON DELETE CASCADE
) TYPE=InnoDB COMMENT='Stores info about the physical transformation';


--
-- Table structure for table `tc_logicaltx`
--


INSERT INTO sequences VALUES('tc_logicaltx_id_seq',0);

CREATE TABLE tc_logicaltx (
	id		BIGINT NOT NULL AUTO_INCREMENT,
	namespace	VARCHAR(255) NOT NULL DEFAULT '',
	name		VARCHAR(255) NOT NULL DEFAULT '',
	version		VARCHAR(20) NOT NULL DEFAULT '',
	PRIMARY KEY(id),
	UNIQUE KEY unique_logicaltx(namespace,name,version)
) TYPE=InnoDB COMMENT='Stores the infor about the logical transformation';


--
-- Table structure for table `tc_lfnprofile`
--

CREATE TABLE tc_lfnprofile (
--	namespace enum('globus','condor','env','hints','dagman','vds') NOT NULL DEFAULT 'env',
	namespace VARCHAR(20) NOT NULL DEFAULT 'env',
	name	VARCHAR(64) NOT NULL DEFAULT '',
	value	TEXT NOT NULL DEFAULT '',
	lfnid	BIGINT NOT NULL DEFAULT '0',
	PRIMARY KEY(namespace,name,value(255),lfnid),
	INDEX (lfnid),
	FOREIGN KEY tc_lfnprofile(lfnid) REFERENCES tc_logicaltx(id) ON DELETE CASCADE
) TYPE=InnoDB COMMENT='Stores the profile information for lfns';

--
-- Table structure for table `tc_lfnpfnmap`
--

CREATE TABLE tc_lfnpfnmap (
	lfnid	BIGINT NOT NULL DEFAULT '0',
	pfnid	BIGINT NOT NULL DEFAULT '0',
	PRIMARY KEY(lfnid,pfnid),
	INDEX (lfnid),
	INDEX (pfnid),
	FOREIGN KEY tc_lfnpfnmap(lfnid) REFERENCES tc_logicaltx(id) ON DELETE CASCADE,
	FOREIGN KEY tc_lfnpfnmap(pfnid) REFERENCES tc_physicaltx(id) ON DELETE CASCADE
) TYPE=InnoDB COMMENT='Maps Lfns to Pfns';



--
-- Table structure for table `tc_pfnprofile`
--

CREATE TABLE tc_pfnprofile (
--	namespace enum('globus','condor','env','hints','dagman','vds') NOT NULL DEFAULT 'env',
	namespace VARCHAR(20) NOT NULL DEFAULT 'env',
	name		VARCHAR(64) NOT NULL DEFAULT '',
	value		TEXT NOT NULL DEFAULT '',
	pfnid		BIGINT NOT NULL default '0',
	PRIMARY KEY(namespace,name,value(255),pfnid),
	INDEX (pfnid),
	FOREIGN KEY tc_pfnprofile(pfnid) REFERENCES tc_physicaltx(id) ON DELETE CASCADE
) TYPE=InnoDB COMMENT='Stores the profile information for pfns';

