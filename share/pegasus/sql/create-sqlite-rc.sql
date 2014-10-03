--
-- schema: edu.isi.pegasus.planner.catalog.ReplicaCatalog
-- driver: sqlite sqlite-jdbc-3.7.2.jar
-- $Revision$
--
INSERT INTO pegasus_schema VALUES ('JDBCRC','rc','1.3',"vahi",NULL);

CREATE TABLE rc_lfn (
   id      INTEGER PRIMARY KEY AUTOINCREMENT,
   lfn     VARCHAR(245) NOT NULL,
   pfn     VARCHAR(245) NOT NULL,
   site    VARCHAR(245),

   CONSTRAINT sk_rc_lfn UNIQUE(lfn,pfn,site)
);

CREATE INDEX ix_rc_lfn ON rc_lfn(lfn);

CREATE TABLE rc_attr (
   id      BIGINT, 
   name    VARCHAR(64) NOT NULL,
   value   VARCHAR(255) NOT NULL,

   CONSTRAINT pk_rc_attr PRIMARY KEY(id,name),
   CONSTRAINT fk_rc_attr FOREIGN KEY(id) REFERENCES rc_lfn(id) ON DELETE CASCADE
);

CREATE INDEX ix_rc_attr ON rc_attr(name);
