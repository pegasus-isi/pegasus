--
-- schema: org.griphyn.vdl.dbschema.ChunkSchema
-- driver: PostGreSQL 7.4.*
-- $Revision$
--

DROP TABLE anno_bool;
DROP TABLE anno_int;
DROP TABLE anno_float;
DROP TABLE anno_date;
DROP TABLE anno_text;

DROP INDEX ix_anno_call;
DROP INDEX ix_anno_targ;
DROP INDEX ix_anno_lfn;
DROP INDEX ix_anno_dv;
DROP INDEX ix_anno_tr;
DROP INDEX ix_anno_call2;
DROP INDEX ix_anno_targ2;
DROP INDEX ix_anno_lfn2;
DROP INDEX ix_anno_dv2;
DROP INDEX ix_anno_tr2;

DROP TABLE anno_call;
DROP TABLE anno_targ;
DROP TABLE anno_lfn;
DROP TABLE anno_dv;
DROP TABLE anno_tr;
DROP SEQUENCE anno_id_seq;

DROP INDEX ix_anno_lfn_b;
DROP INDEX ix_anno_lfn_o;
DROP INDEX ix_anno_lfn_i;
DROP TABLE anno_lfn_b;
DROP TABLE anno_lfn_o;
DROP TABLE anno_lfn_i;
DROP TABLE anno_definition;
DROP SEQUENCE def_id_seq;

DELETE FROM vds_schema WHERE name='AnnotationSchema';
