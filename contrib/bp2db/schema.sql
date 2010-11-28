-- This is the Stampede schema

CREATE TABLE workflow (
	wf_id INTEGER NOT NULL, 
	wf_uuid VARCHAR(255) NOT NULL, 
	dax_label VARCHAR(255), 
	timestamp NUMERIC(16, 6), 
	submit_hostname VARCHAR(255), 
	submit_dir TEXT,
	planner_arguments TEXT,
	username VARCHAR(255),
	grid_dn VARCHAR(255), 
	planner_version VARCHAR(255), 
	parent_workflow_id INTEGER, 
	PRIMARY KEY (wf_id), 
	FOREIGN KEY(parent_workflow_id) REFERENCES workflow (wf_id)
);

CREATE TABLE workflowstate (
	wf_id INTEGER NOT NULL, 
	state VARCHAR(255) NOT NULL, 
	timestamp NUMERIC(16, 6) NOT NULL, 
	FOREIGN KEY(wf_id) REFERENCES workflow (wf_id)
);

CREATE TABLE host (
	host_id INTEGER NOT NULL, 
	site_name VARCHAR(255) NOT NULL, 
	hostname VARCHAR(255) NOT NULL, 
	ip_address VARCHAR(255) NOT NULL, 
	uname VARCHAR(255), 
	total_ram NUMERIC(16, 6), 
	PRIMARY KEY (host_id)
);

CREATE TABLE job (
	job_id INTEGER NOT NULL, 
	wf_id INTEGER NOT NULL, 
	job_submit_seq INTEGER NOT NULL, 
	name VARCHAR(255) NOT NULL, 
	host_id INTEGER, 
	condor_id VARCHAR(255), 
	jobtype VARCHAR(255) NOT NULL, 
	clustered INTEGER, 
	site_name VARCHAR(255), 
	remote_user VARCHAR(255), 
	remote_working_dir TEXT, 
	cluster_start_time NUMERIC(16, 6), 
	cluster_duration NUMERIC(16, 6), 
	PRIMARY KEY (job_id), 
	FOREIGN KEY(wf_id) REFERENCES workflow (wf_id), 
	CHECK (clustered IN (0, 1)), 
	FOREIGN KEY(host_id) REFERENCES host (host_id)
);

CREATE TABLE jobstate (
	job_id INTEGER NOT NULL, 
	state VARCHAR(255) NOT NULL, 
	timestamp NUMERIC(16, 6) NOT NULL, 
	jobstate_submit_seq INTEGER NOT NULL, 
	PRIMARY KEY (job_id, state, timestamp, jobstate_submit_seq), 
	FOREIGN KEY(job_id) REFERENCES job (job_id)
);

CREATE TABLE task (
	task_id INTEGER NOT NULL, 
	job_id INTEGER NOT NULL, 
	task_submit_seq INTEGER NOT NULL, 
	start_time NUMERIC(16, 6) NOT NULL, 
	duration NUMERIC(16, 6) NOT NULL, 
	exitcode INTEGER NOT NULL, 
	transformation TEXT NOT NULL, 
	executable TEXT, 
	arguments TEXT, 
	PRIMARY KEY (task_id), 
	FOREIGN KEY(job_id) REFERENCES job (job_id)
);

CREATE TABLE edge (
	parent_id INTEGER NOT NULL, 
	child_id INTEGER NOT NULL, 
	PRIMARY KEY (parent_id, child_id), 
	FOREIGN KEY(parent_id) REFERENCES job (job_id), 
	FOREIGN KEY(child_id) REFERENCES job (job_id)
);

CREATE TABLE edge_static (
	wf_uuid VARCHAR(255) NOT NULL, 
	parent VARCHAR(255) NOT NULL, 
	child VARCHAR(255) NOT NULL,
	-- The order of this is wf_uuid, child, parent to make edge lookups faster
	PRIMARY KEY (wf_uuid, child, parent)
);

-- These indexes enforce uniqueness constraints
CREATE UNIQUE INDEX UNIQUE_WORKFLOW ON workflow (wf_uuid);
CREATE UNIQUE INDEX UNIQUE_HOST ON host (site_name, hostname, ip_address);
CREATE UNIQUE INDEX UNIQUE_JOB ON job (wf_id, job_submit_seq);
CREATE UNIQUE INDEX UNIQUE_JOBSTATE ON jobstate (job_id, jobstate_submit_seq);
CREATE UNIQUE INDEX UNIQUE_TASK ON task (job_id, task_submit_seq);

-- To make lookups of parent-child relationships faster
CREATE INDEX IDX_JOB_LOOKUP ON job (wf_id, name);