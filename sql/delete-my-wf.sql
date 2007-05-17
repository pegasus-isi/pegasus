--
-- driver: MySQL 4.*
-- $Revision$
--

DROP TABLE wf_siteinfo CASCADE;
DROP INDEX ix_wf_jobstate ON wf_jobstate;
DROP TABLE wf_jobstate CASCADE;
DROP TABLE wf_work CASCADE;

DELETE FROM vds_schema 
WHERE name='WorkflowSchema' AND catalog='wf' AND version='1.0';
