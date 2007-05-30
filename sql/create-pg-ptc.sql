--
-- schema: org.griphyn.vdl.dbschema.InvocationSchema
-- driver: PostGreSQL 7.4.*
-- $Revision$
--

-- if the next step fails, you forgot to run "create-init-pg.sql"
INSERT INTO pegasus_schema(name,catalog,version) VALUES('InvocationSchema','ptc','1.5');

CREATE SEQUENCE uname_id_seq MINVALUE 0 MAXVALUE 9223372036854775807 INCREMENT 1;
CREATE SEQUENCE rusage_id_seq MINVALUE 0 MAXVALUE 9223372036854775807 INCREMENT 1;
CREATE SEQUENCE stat_id_seq MINVALUE 0 MAXVALUE 9223372036854775807 INCREMENT 1;
CREATE SEQUENCE invocation_id_seq MINVALUE 0 MAXVALUE 9223372036854775807 INCREMENT 1;

-- table with architecture information
CREATE TABLE ptc_uname (
        id              BIGINT DEFAULT NEXTVAL('uname_id_seq') PRIMARY KEY,

        -- uname data (unique quadruples)
        archmode        VARCHAR(16), -- IA32, IA64, ILP32, LP64, ...
        sysname         VARCHAR(64), -- linux, sunos, ...
        os_release         VARCHAR(64), -- 2.4.12, 5.8, ...
        machine         VARCHAR(64), -- i686, sun4u, ...

        CONSTRAINT      sk_ptc_uname UNIQUE(archmode,sysname,os_release,machine)
);

-- CREATE FUNCTION roi_uname( VARCHAR, VARCHAR, VARCHAR, VARCHAR )
--   RETURNS BIGINT AS '
-- DECLARE result BIGINT DEFAULT -1;
-- BEGIN
--   SELECT INTO result id FROM uname WHERE archmode=$1 AND sysname=$2 AND os_release=$3 AND machine=$4;
--   IF NOT FOUND THEN
--     SELECT INTO result nextval(''uname_id_seq'');
--     INSERT INTO uname(id,archmode,sysname,os_release,machine) VALUES(result,$1,$2,$3,$4);
--   END IF;
--   RETURN result;
-- END;
-- ' LANGUAGE 'plpgsql';

-- table with all usage information for processes
CREATE TABLE ptc_rusage (
	id		BIGINT DEFAULT NEXTVAL('rusage_id_seq') PRIMARY KEY,

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
);

-- the information from stat calls
CREATE TABLE ptc_stat (
	id		BIGINT DEFAULT NEXTVAL('stat_id_seq') PRIMARY KEY,

	-- errno after stat or fstat call
	errno		SMALLINT NOT NULL,

	-- combination of both determines what type of file
	-- fname=def'd, fdesc=null --> regular file
	-- fname=null, fdesc=def'd --> descriptor
	-- fname=def'd, fdesc=def'd --> temporary file
	fname		VARCHAR(1024),
	fdesc		INTEGER,

	-- struct stat excerpt
	size		BIGINT DEFAULT NULL,
	mode		INTEGER DEFAULT NULL,
	inode		BIGINT DEFAULT NULL,
	atime		TIMESTAMP WITH TIME ZONE DEFAULT NULL,
	ctime		TIMESTAMP WITH TIME ZONE DEFAULT NULL,
	mtime		TIMESTAMP WITH TIME ZONE DEFAULT NULL,
	uid		INTEGER DEFAULT NULL,
	gid		INTEGER DEFAULT NULL
);

-- the primary table
CREATE TABLE ptc_invocation (
	id		BIGINT DEFAULT NEXTVAL('invocation_id_seq') PRIMARY KEY,

	-- who inserts these records 
	creator		VARCHAR(16) DEFAULT current_user NOT NULL,
	creationtime	TIMESTAMP WITH TIME ZONE DEFAULT current_timestamp(0) NOT NULL,

	-- from which workflow did they come
	wf_label	VARCHAR(32),
	wf_time		TIMESTAMP WITH TIME ZONE,

	-- invocation information version (e.g. "1.1")
	version		VARCHAR(4),

	-- total runtime of kickstart
	start		TIMESTAMP WITH TIME ZONE NOT NULL,
	duration	DOUBLE PRECISION NOT NULL,

	-- this will change eventually
	tr_namespace	VARCHAR(255),
	tr_name		VARCHAR(255), -- NOT NULL no yet
	tr_version	VARCHAR(20),
	dv_namespace	VARCHAR(255),
	dv_name		VARCHAR(255),
	dv_version	VARCHAR(20),

	-- where did kickstart run
	resource        VARCHAR(48), -- site handle
	host		VARCHAR(16), -- Pg8 is too s@#$!~ for INET
	pid		INTEGER,
	uid		INTEGER,
	gid		INTEGER,
	cwd		VARCHAR(1024),

	-- uname data (less redundant)
	arch            BIGINT,

	-- what resource consumption did kickstart incur
	total		BIGINT,

	CONSTRAINT	fk_ptc_inv_uname FOREIGN KEY(arch) REFERENCES ptc_uname(id) ON DELETE SET NULL,
	CONSTRAINT	fk_ptc_inv_rusage FOREIGN KEY(total) REFERENCES ptc_rusage(id) ON DELETE SET NULL
);

-- the job table
CREATE TABLE ptc_job (
	id		BIGINT NOT NULL,
	type		CHAR NOT NULL, -- { 'S', 'P', 'M', 'p', 'c' }

	-- job start and runtime
	start		TIMESTAMP WITH TIME ZONE NOT NULL,
	duration	DOUBLE PRECISION NOT NULL,

	-- pid of jobs
	pid		INTEGER,

	-- resource consumption
	rusage		BIGINT,
	stat		BIGINT,

	-- exit code of job (includes -1 for kickstart failure) and msg
	exitcode	INTEGER NOT NULL,
	exit_msg	VARCHAR(255),

	-- command-line arguments
	args		TEXT,

	-- composite primary key
	CONSTRAINT	pk_ptc_job PRIMARY KEY(id,type),
	CONSTRAINT	fk_ptc_job_inv FOREIGN KEY (id) REFERENCES ptc_invocation(id) ON DELETE CASCADE,
	CONSTRAINT	fk_ptc_job_rusage FOREIGN KEY(rusage) REFERENCES ptc_rusage(id) ON DELETE SET NULL,
	CONSTRAINT	fk_ptc_job_stat FOREIGN KEY(stat) REFERENCES ptc_stat(id) ON DELETE SET NULL
);

-- the LFN table for Seung-Hye
CREATE TABLE ptc_lfn (
	id              BIGINT NOT NULL,
	stat            BIGINT NOT NULL,
	initial         CHAR DEFAULT NULL,
	lfn             VARCHAR(255),

	CONSTRAINT      fk_ptc_lfn_inv FOREIGN KEY(id) REFERENCES ptc_invocation(id) ON DELETE CASCADE,
	CONSTRAINT      fk_ptc_lfn_stat FOREIGN KEY(stat) REFERENCES ptc_stat(id) ON DELETE CASCADE
);
