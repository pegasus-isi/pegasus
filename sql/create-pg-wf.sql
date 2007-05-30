--
-- driver: PostGreSQL 7.*
-- $Revision$
--

-- if the next step fails, you forgot to run "create-init-pg.sql"
INSERT INTO pegasus_schema(name,catalog,version) 
VALUES('WorkflowSchema','wf','1.1');

CREATE TABLE wf_work ( 
	id         BIGSERIAL PRIMARY KEY,
	basedir    TEXT,
	vogroup    VARCHAR(255),
	workflow   VARCHAR(255),
	run        VARCHAR(255),
	creator    VARCHAR(32),
	ctime      TIMESTAMP WITH TIME ZONE NOT NULL,
	state      INTEGER NOT NULL,
	mtime      TIMESTAMP WITH TIME ZONE NOT NULL,

	CONSTRAINT sk_wf_work UNIQUE(basedir,vogroup,workflow,run) 
);

CREATE TABLE wf_jobstate ( 
	wfid       BIGINT REFERENCES wf_work(id) ON DELETE CASCADE,
	jobid      VARCHAR(64),
	state      VARCHAR(24) NOT NULL,
	mtime      TIMESTAMP WITH TIME ZONE NOT NULL,
	site       VARCHAR(64),
	
	CONSTRAINT pk_wf_jobstate PRIMARY KEY (wfid,jobid) 
);
CREATE INDEX ix_wf_jobstate ON wf_jobstate(jobid);

CREATE TABLE wf_siteinfo ( 
	id	   BIGSERIAL PRIMARY KEY,
	handle     VARCHAR(48) NOT NULL, 
	mtime      TIMESTAMP WITH TIME ZONE,
	-- gauges
	other      INTEGER DEFAULT 0,
	pending    INTEGER DEFAULT 0,
	running    INTEGER DEFAULT 0,
	-- counters
	success    INTEGER DEFAULT 0,
	smtime     TIMESTAMP WITH TIME ZONE,
	failure    INTEGER DEFAULT 0,
	fmtime     TIMESTAMP WITH TIME ZONE,

	CONSTRAINT sk_wf_siteinfo UNIQUE(handle)
);
