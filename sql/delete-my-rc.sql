--
-- schema: org.griphyn.common.catalog.ReplicaCatalog
-- driver: MySQL 4.*
-- $Revision: 1.5 $
--
DROP INDEX ix_rc_attr ON rc_attr;
DROP INDEX ix_rc_lfn ON rc_lfn;

DROP TABLE rc_attr;
DROP TABLE rc_lfn;

DELETE FROM vds_schema WHERE name='JDBCRC';
