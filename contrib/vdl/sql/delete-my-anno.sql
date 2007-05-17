--
-- schema: org.griphyn.vdl.dbschema.AnnotationSchema
-- driver: MySQL 4.*
-- $Revision$
--

DROP TABLE anno_bool CASCADE;
DROP TABLE anno_int CASCADE;
DROP TABLE anno_float CASCADE;
DROP TABLE anno_date CASCADE;
DROP TABLE anno_text CASCADE;

DROP INDEX ix_anno_call ON anno_call;
DROP INDEX ix_anno_targ ON anno_targ;
DROP INDEX ix_anno_lfn ON anno_lfn;
DROP INDEX ix_anno_dv ON anno_dv;
DROP INDEX ix_anno_tr ON anno_tr;
DROP INDEX ix_anno_call2 ON anno_call;
DROP INDEX ix_anno_targ2 ON anno_targ;
DROP INDEX ix_anno_lfn2 ON anno_lfn;
DROP INDEX ix_anno_dv2 ON anno_dv;
DROP INDEX ix_anno_tr2 ON anno_tr;

DROP TABLE anno_call;
DROP TABLE anno_targ;
DROP TABLE anno_lfn;
DROP TABLE anno_dv;
DROP TABLE anno_tr;

DELETE FROM sequences WHERE name='anno_id_seq';

DROP INDEX ix_anno_lfn_b ON anno_lfn_b;
DROP INDEX ix_anno_lfn_o ON anno_lfn_o;
DROP INDEX ix_anno_lfn_i ON anno_lfn_i;

DROP TABLE anno_lfn_b;
DROP TABLE anno_lfn_o;
DROP TABLE anno_lfn_i;

DROP TABLE anno_definition;

DELETE FROM sequences WHERE name='def_id_seq';
DELETE FROM vds_schema WHERE name='AnnotationSchema';
