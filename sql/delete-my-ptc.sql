--
-- schema: org.griphyn.vdl.dbschema.InvocationSchema
-- driver: MySQL 4.*
-- $Revision$
--

DROP TABLE ptc_lfn CASCADE;
DROP TABLE ptc_job CASCADE;
DROP TABLE ptc_invocation CASCADE;
DROP TABLE ptc_stat CASCADE;
DROP TABLE ptc_rusage CASCADE;
DROP TABLE ptc_uname CASCADE;

DELETE FROM sequences WHERE name='uname_id_seq';
DELETE FROM sequences WHERE name='rusage_id_seq';
DELETE FROM sequences WHERE name='stat_id_seq';
DELETE FROM sequences WHERE name='invocation_id_seq';

DELETE FROM pegasus_schema WHERE name='InvocationSchema' AND catalog='ptc';
