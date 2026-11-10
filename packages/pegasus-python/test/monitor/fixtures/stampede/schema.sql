CREATE TABLE dbversion (
    id INTEGER PRIMARY KEY,
    version_number INTEGER NOT NULL,
    version TEXT NOT NULL,
    version_timestamp INTEGER NOT NULL
);
INSERT INTO dbversion VALUES (1, 14, '14', 0);

CREATE TABLE workflow (
    wf_id INTEGER PRIMARY KEY,
    wf_uuid TEXT NOT NULL UNIQUE,
    root_wf_id INTEGER,
    dag_file_name TEXT,
    submit_dir TEXT
);

CREATE TABLE workflowstate (
    wf_id INTEGER NOT NULL,
    state TEXT NOT NULL,
    timestamp NUMERIC NOT NULL,
    restart_count INTEGER NOT NULL,
    status INTEGER,
    reason TEXT,
    PRIMARY KEY (wf_id, state, timestamp)
);

CREATE TABLE job (
    job_id INTEGER PRIMARY KEY,
    wf_id INTEGER NOT NULL,
    exec_job_id TEXT NOT NULL,
    type_desc TEXT NOT NULL,
    task_count INTEGER NOT NULL,
    UNIQUE (wf_id, exec_job_id)
);

CREATE TABLE job_instance (
    job_instance_id INTEGER PRIMARY KEY,
    job_id INTEGER NOT NULL,
    job_submit_seq INTEGER NOT NULL,
    sched_id TEXT,
    site TEXT,
    stdout_file TEXT,
    stderr_file TEXT,
    exitcode INTEGER,
    UNIQUE (job_id, job_submit_seq)
);

CREATE TABLE jobstate (
    job_instance_id INTEGER NOT NULL,
    state TEXT NOT NULL,
    timestamp NUMERIC NOT NULL,
    jobstate_submit_seq INTEGER NOT NULL,
    reason TEXT,
    PRIMARY KEY (job_instance_id, state, timestamp, jobstate_submit_seq)
);

CREATE INDEX jobstate_sequence ON jobstate(jobstate_submit_seq);

CREATE TABLE task (
    task_id INTEGER PRIMARY KEY,
    wf_id INTEGER NOT NULL,
    job_id INTEGER,
    transformation TEXT NOT NULL
);

CREATE TABLE invocation (
    invocation_id INTEGER PRIMARY KEY,
    wf_id INTEGER NOT NULL,
    job_instance_id INTEGER NOT NULL,
    task_submit_seq INTEGER NOT NULL,
    maxrss INTEGER
);
