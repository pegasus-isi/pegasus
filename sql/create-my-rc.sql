--
-- schema: org.griphyn.common.catalog.ReplicaCatalog
-- driver: MySQL 4.*
-- $Revision: 1.5 $
--
INSERT INTO vds_schema VALUES ('JDBCRC','rc','1.2',current_user(),current_timestamp(0));

CREATE TABLE rc_lfn (
   id      BIGINT DEFAULT NULL auto_increment,
   lfn     VARCHAR(245) NOT NULL,
   pfn     VARCHAR(245) NOT NULL,

   CONSTRAINT pk_rc_lfn PRIMARY KEY(id),
   CONSTRAINT sk_rc_lfn UNIQUE(lfn,pfn)
) type=InnoDB;

CREATE INDEX ix_rc_lfn ON rc_lfn(lfn);

CREATE TABLE rc_attr (
   id      BIGINT, 
   name    VARCHAR(64) NOT NULL,
   value   VARCHAR(255) NOT NULL,

   CONSTRAINT pk_rc_attr PRIMARY KEY(id,name),
   CONSTRAINT fk_rc_attr FOREIGN KEY(id) REFERENCES rc_lfn(id) ON DELETE CASCADE
) type=InnoDB;

CREATE INDEX ix_rc_attr ON rc_attr(name);
