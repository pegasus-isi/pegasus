--
-- schema: org.griphyn.common.catalog.ReplicaCatalog
-- driver: PostGreSQL 7.4.*
-- $Revision$
--

INSERT INTO vds_schema(name,catalog,version) VALUES ('JDBCRC','rc','1.2');

CREATE SEQUENCE rc_lfn_id;

CREATE TABLE rc_lfn (
   id      BIGINT DEFAULT nextval('rc_lfn_id'::text),
   lfn     VARCHAR(255) NOT NULL,
   pfn     VARCHAR(255) NOT NULL,

   CONSTRAINT pk_rc_lfn PRIMARY KEY(id),
   CONSTRAINT sk_rc_lfn UNIQUE(lfn,pfn)
);

CREATE INDEX ix_rc_lfn ON rc_lfn(lfn);

CREATE TABLE rc_attr (
   id      BIGINT, 
   name    VARCHAR(64) NOT NULL,
   value   VARCHAR(255) NOT NULL,

   CONSTRAINT pk_rc_attr PRIMARY KEY(id,name),
   CONSTRAINT fk_rc_attr FOREIGN KEY(id) REFERENCES rc_lfn(id)
	ON DELETE CASCADE
);

CREATE INDEX ix_rc_attr ON rc_attr(name);
