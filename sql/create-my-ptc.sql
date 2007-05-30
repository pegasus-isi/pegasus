--
-- schema: org.griphyn.vdl.dbschema.InvocationSchema
-- driver: MySQL 4.*
-- $Revision$
--

-- if the next step fails, you forgot to run "create-init-my.sql"
INSERT INTO pegasus_schema VALUES('InvocationSchema','ptc','1.5',current_user(),current_timestamp(0));

INSERT INTO sequences VALUES( 'uname_id_seq', 0 );
INSERT INTO sequences VALUES( 'rusage_id_seq', 0 );
INSERT INTO sequences VALUES( 'stat_id_seq', 0 );
INSERT INTO sequences VALUES( 'invocation_id_seq', 0 );

-- table with architecture information
CREATE TABLE ptc_uname (
        id              BIGINT NOT NULL PRIMARY KEY,

        -- uname data (unique quadruples)
        archmode        VARCHAR(16), -- IA32, IA64, ILP32, LP64, ...
        sysname         VARCHAR(64), -- linux, sunos, ...
        os_release         VARCHAR(64), -- 2.4.12, 5.8, ...
        machine         VARCHAR(64),  -- i686, sun4u, ...

	CONSTRAINT sk_ptc_uname UNIQUE(archmode,sysname,os_release,machine)
) type=InnoDB;

-- table with all usage information for processes
CREATE TABLE ptc_rusage (
	id		BIGINT NOT NULL PRIMARY KEY,

	-- user time, system time, in seconds?
	utime		DOUBLE PRECISION NOT NULL,
	stime		DOUBLE PRECISION NOT NULL,

	-- page faults, swaps, signal, context switches
	minflt		INTEGER,
	majflt		INTEGER,
	nswaps		INTEGER,
	nsignals	INTEGER,
	nvcsw		INTEGER,
	nivcsw		INTEGER
) type=InnoDB;

-- the information from stat calls
CREATE TABLE ptc_stat (
	id		BIGINT NOT NULL PRIMARY KEY,

	-- errno after stat or fstat call
	errno		SMALLINT NOT NULL,

	-- combination of both determines what type of file
	-- fname=def'd, fdesc=null --> regular file
	-- fname=null, fdesc=def'd --> descriptor
	-- fname=def'd, fdesc=def'd --> temporary file
	fname		TEXT,
	fdesc		INTEGER,

	-- struct stat excerpt
	size		BIGINT,
	mode		INTEGER,
	inode		BIGINT,
	atime		DATETIME,
	ctime		DATETIME,
	mtime		DATETIME,
	uid		INTEGER,
	gid		INTEGER
) type=InnoDB;

-- the primary table
CREATE TABLE ptc_invocation (
	id		BIGINT NOT NULL PRIMARY KEY,

	-- who inserts these records 
	creator		VARCHAR(16) NOT NULL, -- DEFAULT USER()
	creationtime	DATETIME NOT NULL, -- DEFAULT NOW()

	-- from which workflow did they come
	wf_label	VARCHAR(32),
	wf_time		DATETIME,

	-- invocation information version (e.g. "1.1")
	version		VARCHAR(4),

	-- total runtime of kickstart
	start		DATETIME NOT NULL,
	duration	DOUBLE PRECISION NOT NULL,

	-- this will change eventually
	tr_namespace	VARCHAR(255),
	tr_name		VARCHAR(255), -- not yet NOT NULL,
	tr_version	VARCHAR(20),
	dv_namespace	VARCHAR(255),
	dv_name		VARCHAR(255),
	dv_version	VARCHAR(20),

	-- where did kickstart run
	resource        VARCHAR(48),
	host		VARCHAR(16),
	pid		INTEGER,
	uid		INTEGER,
	gid		INTEGER,
	cwd		TEXT,

	-- uname data (less redundant)
	arch            BIGINT,

	-- what resource consumption did kickstart incur
	total		BIGINT
) type=InnoDB;

CREATE INDEX ix_ptc_inv_arch ON ptc_invocation(arch);
CREATE INDEX ix_ptc_inv_total ON ptc_invocation(total);

ALTER TABLE ptc_invocation ADD CONSTRAINT fk_ptc_inv_uname FOREIGN KEY(arch) REFERENCES ptc_uname(id) ON DELETE SET NULL;
ALTER TABLE ptc_invocation ADD CONSTRAINT fk_ptc_inv_rusage FOREIGN KEY(total) REFERENCES ptc_rusage(id) ON DELETE SET NULL;


-- the job table
CREATE TABLE ptc_job (
	id		BIGINT NOT NULL,
	type		CHAR BINARY NOT NULL, -- { 'S', 'P', 'M', 'p', 'c' }
	
	-- job start and runtime
	start		DATETIME NOT NULL,
	duration	DOUBLE PRECISION NOT NULL,

	-- pid of jobs
	pid		INTEGER,

	-- resource consumption, application statcall
	rusage		BIGINT NULL,
	stat		BIGINT NULL,

	-- exit code of job (includes -1 for kickstart failure) and msg
	exitcode	INTEGER NOT NULL,
	exit_msg	VARCHAR(255),

	-- command-line arguments
	args		MEDIUMBLOB,

	-- composite primary key
	CONSTRAINT	pk_ptc_job PRIMARY KEY (id,type),
	CONSTRAINT	fk_ptc_job_inv FOREIGN KEY (id) REFERENCES ptc_invocation(id) ON DELETE CASCADE
) type=InnoDB;

CREATE INDEX ix_ptc_job_rusage ON ptc_job(rusage);
CREATE INDEX ix_ptc_job_stat ON ptc_job(stat);

ALTER TABLE ptc_job ADD CONSTRAINT fk_ptc_job_rusage FOREIGN KEY(rusage) REFERENCES ptc_rusage(id) ON DELETE SET NULL;
ALTER TABLE ptc_job ADD CONSTRAINT fk_ptc_job_stat FOREIGN KEY(stat) REFERENCES ptc_stat(id) ON DELETE SET NULL;

-- the LFN table for Seung-Hye
CREATE TABLE ptc_lfn (
	id              BIGINT NOT NULL,
	stat            BIGINT NOT NULL,
	initial         CHAR BINARY DEFAULT NULL,
	lfn             VARCHAR(255)
) type=InnoDB;

CREATE INDEX ix_ptc_lfn_id ON ptc_lfn(id);
CREATE INDEX ix_ptc_lfn_stat ON ptc_lfn(stat);

ALTER TABLE ptc_lfn ADD CONSTRAINT fk_ptc_lfn_id FOREIGN KEY(id) REFERENCES ptc_invocation(id) ON DELETE CASCADE;
ALTER TABLE ptc_lfn ADD CONSTRAINT fk_ptc_lfn_stat FOREIGN KEY(stat) REFERENCES ptc_stat(id) ON DELETE CASCADE;
