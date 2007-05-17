--
-- schema: org.griphyn.vdl.dbschema.ChunkSchema
-- driver: MySQL 4.*
-- $Revision: 1.5 $
--

DROP INDEX ix_vdc_blfn ON vdc_blfn;
DROP INDEX ix_vdc_olfn ON vdc_olfn;
DROP INDEX ix_vdc_ilfn ON vdc_ilfn;
DROP INDEX ix_vdc_nlfn ON vdc_nlfn;

DROP TABLE vdc_blfn CASCADE;
DROP TABLE vdc_olfn CASCADE;
DROP TABLE vdc_ilfn CASCADE;
DROP TABLE vdc_nlfn CASCADE;
DROP TABLE vdc_definition CASCADE;

DELETE FROM sequences WHERE name='def_id_seq';
DELETE FROM vds_schema WHERE name='ChunkSchema' AND catalog='vdc';
