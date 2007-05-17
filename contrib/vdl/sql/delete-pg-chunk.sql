--
-- schema: org.griphyn.vdl.dbschema.ChunkSchema
-- driver: PostGreSQL 7.4.*
-- $Revision$
--

DROP INDEX ix_vdc_blfn;
DROP INDEX ix_vdc_olfn;
DROP INDEX ix_vdc_ilfn;
DROP INDEX ix_vdc_nlfn;

DROP TABLE vdc_blfn;
DROP TABLE vdc_olfn;
DROP TABLE vdc_ilfn;
DROP TABLE vdc_nlfn;
DROP TABLE vdc_definition;
DROP SEQUENCE def_id_seq;

DELETE FROM vds_schema WHERE name='ChunkSchema' AND catalog='vdc';
