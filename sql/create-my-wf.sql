--
-- driver: MySQL 4.*
-- $Revision: 1.3 $
--

-- if the next step fails, you forgot to run "create-init-pg.sql"
INSERT INTO vds_schema 
VALUES('WorkflowSchema','wf','1.1',current_user(),current_timestamp(0));

CREATE TABLE wf_work ( 
	id         BIGINT AUTO_INCREMENT PRIMARY KEY,
	basedir    TEXT,
	vogroup    VARCHAR(255),
	workflow   VARCHAR(255),
	run        VARCHAR(255),
	creator    VARCHAR(32),
	ctime      DATETIME NOT NULL,
	state      INTEGER NOT NULL,
	mtime      DATETIME NOT NULL,

	CONSTRAINT sk_wf_work UNIQUE(basedir(255),vogroup,workflow,run) 
) type=InnoDB;

CREATE TABLE wf_jobstate ( 
	wfid       BIGINT REFERENCES wf_work(id) ON DELETE CASCADE,
	jobid      VARCHAR(64),
	state      VARCHAR(24) NOT NULL,
	mtime      DATETIME NOT NULL,
	site       VARCHAR(64),
	
	CONSTRAINT pk_wf_jobstate PRIMARY KEY (wfid,jobid) 
) type=InnoDB;
CREATE INDEX ix_wf_jobstate ON wf_jobstate(jobid);

CREATE TABLE wf_siteinfo ( 
	id	   BIGINT AUTO_INCREMENT PRIMARY KEY,
	handle     VARCHAR(48) NOT NULL, 
	mtime      DATETIME,
	-- gauges
	other      INTEGER DEFAULT 0,
	pending    INTEGER DEFAULT 0,
	running    INTEGER DEFAULT 0,
	-- counters
	success    INTEGER DEFAULT 0,
	smtime     DATETIME,
	failure    INTEGER DEFAULT 0,
	fmtime     DATETIME,

	CONSTRAINT sk_wf_siteinfo UNIQUE(handle)
) type=InnoDB;
