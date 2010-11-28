--
-- schema : org.griphyn.common.catalog.TransformationCatalog
-- driver : PostGreSQL
-- $Revision$
-- Author : Gaurang Mehta gmehta@isi.edu

--
-- Table structure for table `tc_sysinfo`
--
INSERT INTO pegasus_schema(name,catalog,version) VALUES ('Database','tc','1.3');

CREATE TABLE tc_sysinfo (
	id		BIGSERIAL PRIMARY KEY,
	architecture	VARCHAR(10) NOT NULL,
	os		VARCHAR(10) NOT NULL,
	glibc		VARCHAR(10) DEFAULT NULL,
	osversion	VARCHAR(10) DEFAULT NULL,
	UNIQUE(architecture,os,glibc,osversion)
);
--COMMENT='Stores the different types of architecture';

--
-- Table structure for table `tc_physicaltx`
--


CREATE TABLE tc_physicaltx (
	id		BIGSERIAL PRIMARY KEY,
	resourceid	VARCHAR(255) NOT NULL,
	pfn		VARCHAR(255) DEFAULT NULL,
	type		VARCHAR(20) NOT NULL DEFAULT 'INSTALLED',
--enum('INSTALLED','STATIC_BINARY','DYNAMIC_BINARY','SOURCE','PACMAN_PACKAGE','SCRIPT') NOT NULL DEFAULT 'INSTALLED',
	archid		BIGINT DEFAULT NULL,
	UNIQUE(resourceid,pfn,type),
	FOREIGN KEY(archid) REFERENCES tc_sysinfo(id) ON DELETE CASCADE
);
--COMMENT='Stores info about the physical transformation';
CREATE INDEX idx_tc_physicaltx  ON tc_physicaltx(resourceid);
CREATE INDEX idx_tc_physicaltx2 ON tc_physicaltx(type);
CREATE INDEX idx_tc_physicaltx3 ON tc_physicaltx(pfn);
CREATE INDEX idx_tc_physicaltx4 ON tc_physicaltx(archid);

--
-- Table structure for table `tc_logicaltx`
--

CREATE TABLE tc_logicaltx (
	id		BIGSERIAL PRIMARY KEY,
	namespace	VARCHAR(255) NOT NULL,
	name		VARCHAR(255) NOT NULL,
	version		VARCHAR(20) NOT NULL,
	UNIQUE(namespace,name,version)
);
--COMMENT='Stores the infor about the logical transformation';

CREATE INDEX idx_tc_logicaltx ON tc_logicaltx(name);

--
-- Table structure for table `tc_lfnprofile`
--

CREATE TABLE tc_lfnprofile (
	namespace VARCHAR(20) NOT NULL DEFAULT 'env',
--enum('globus','condor','env','hints','dagman','vds') NOT NULL DEFAULT 'env',
	name	VARCHAR(64) NOT NULL,
	value	TEXT NOT NULL,
	lfnid	BIGINT NOT NULL,
	PRIMARY KEY(namespace,name,value,lfnid),
	FOREIGN KEY(lfnid) REFERENCES tc_logicaltx(id) ON DELETE CASCADE
);
--COMMENT='Stores the profile information for lfns';

CREATE INDEX idx_tc_lfnprofile  ON tc_lfnprofile(lfnid);
CREATE INDEX idx_tc_lfnprofile2 ON tc_lfnprofile(namespace);
CREATE INDEX idx_tc_lfnprofile3 ON tc_lfnprofile(name);

--
-- Table structure for table `tc_lfnpfnmap`
--

CREATE TABLE tc_lfnpfnmap (
	lfnid	BIGINT NOT NULL,
	pfnid	BIGINT NOT NULL,
	PRIMARY KEY(lfnid,pfnid),
	FOREIGN KEY(lfnid) REFERENCES tc_logicaltx(id) ON DELETE CASCADE,
	FOREIGN KEY(pfnid) REFERENCES tc_physicaltx(id) ON DELETE CASCADE
); 
--COMMENT='Maps Lfns to Pfns';

CREATE INDEX idx_tc_lfnpfnmap_l ON tc_lfnpfnmap(lfnid);
CREATE INDEX idx_tc_lfnpfnmap_p ON tc_lfnpfnmap(pfnid);

--
-- Table structure for table `tc_pfnprofile`
--

CREATE TABLE tc_pfnprofile (
	namespace VARCHAR(20) NOT NULL DEFAULT 'env',
--enum('globus','condor','env','hints','dagman','vds') NOT NULL DEFAULT 'env',
	name		VARCHAR(64) NOT NULL,
	value		TEXT NOT NULL,
	pfnid		BIGINT NOT NULL,
	PRIMARY KEY(namespace,name,value,pfnid),
	FOREIGN KEY(pfnid) REFERENCES tc_physicaltx(id) ON DELETE CASCADE
);
--COMMENT='Stores the profile information for pfns';

CREATE INDEX idx_tc_pfnprofile  ON tc_pfnprofile(pfnid);
CREATE INDEX idx_tc_pfnprofile2 ON tc_pfnprofile(namespace);
CREATE INDEX idx_tc_pfnprofile3 ON tc_pfnprofile(name);
