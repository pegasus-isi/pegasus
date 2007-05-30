--
-- driver: PostGreSQL 7.*
-- $Revision$
--

DROP TABLE wf_siteinfo CASCADE;
DROP INDEX ix_wf_jobstate;
DROP TABLE wf_jobstate CASCADE;
DROP TABLE wf_work CASCADE;

DELETE FROM pegasus_schema 
WHERE name='WorkflowSchema' AND catalog='wf' AND version='1.0';
