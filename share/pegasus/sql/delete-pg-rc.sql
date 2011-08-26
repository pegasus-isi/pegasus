--
-- schema: org.griphyn.common.catalog.ReplicaCatalog
-- driver: PostGreSQL 7.4.*
-- $Revision$
--

DROP INDEX ix_rc_attr;
DROP TABLE rc_attr;
DROP INDEX ix_rc_lfn;
DROP TABLE rc_lfn;
DROP SEQUENCE rc_lfn_id;

DELETE FROM pegasus_schema WHERE name='JDBCRC';
