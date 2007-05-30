--
-- schema: org.griphyn.vdl.dbschema.InvocationSchema
-- driver: PostGreSQL 7.4.*
-- $Revision$
--

DROP TABLE ptc_lfn;
DROP TABLE ptc_job;
DROP TABLE ptc_invocation;
DROP TABLE ptc_rusage;
DROP TABLE ptc_stat;
DROP TABLE ptc_uname;

DROP SEQUENCE invocation_id_seq;
DROP SEQUENCE rusage_id_seq; 
DROP SEQUENCE stat_id_seq;
DROP SEQUENCE uname_id_seq;

DELETE FROM pegasus_schema WHERE name='InvocationSchema' AND catalog='ptc';
