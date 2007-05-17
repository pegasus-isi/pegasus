--
-- schema: org.griphyn.vdl.dbschema.AnnotationSchema
-- driver: MySQL 4.*
-- $Revision: 1.4 $
--

-- if the next step fails, you forgot to run "create-init-pg.sql"
-- or the tables already exist. 
INSERT INTO vds_schema VALUES ('AnnotationSchema','vdc','1.3',current_user(),current_timestamp(0));

INSERT INTO sequences VALUES( 'def_id_seq', 0 );

-- Definition storage
CREATE TABLE anno_definition (
        id              BIGINT NOT NULL,
        type            INTEGER NOT NULL,
        name            VARCHAR(255) NOT NULL,
        namespace       VARCHAR(255) NOT NULL,
        version         VARCHAR(20) NOT NULL,
        xml             MEDIUMBLOB DEFAULT NULL,

        -- secondary key definition
        CONSTRAINT sk_anno_definition UNIQUE(type,name,namespace,version)
) type=InnoDB;

-- 'input' filenames
CREATE TABLE anno_lfn_i (
        did        BIGINT NOT NULL,
        name       VARCHAR(255) NOT NULL,

        CONSTRAINT pk_anno_lfn_i PRIMARY KEY(did,name)
--      CONSTRAINT fk_anno_lfn_i FOREIGN KEY(did) REFERENCES anno_definition(id) ON DELETE CASCADE
) type=InnoDB;

-- 'output' filenames
CREATE TABLE anno_lfn_o (
        did        BIGINT NOT NULL,
        name       VARCHAR(255) NOT NULL,

        CONSTRAINT pk_anno_lfn_o PRIMARY KEY(did,name)
--      CONSTRAINT fk_anno_lfn_o FOREIGN KEY(did) REFERENCES anno_definition(id) ON DELETE CASCADE
) type=InnoDB;

-- 'inout' filenames
CREATE TABLE anno_lfn_b (
        did        BIGINT NOT NULL,
        name       VARCHAR(255) NOT NULL,

        CONSTRAINT pk_anno_lfn_b PRIMARY KEY(did,name)
--      CONSTRAINT fk_anno_lfn_b FOREIGN KEY(did) REFERENCES anno_definition(id) ON DELETE CASCADE
) type=InnoDB;

CREATE INDEX ix_anno_lfn_i ON anno_lfn_i(name);
CREATE INDEX ix_anno_lfn_o ON anno_lfn_o(name);
CREATE INDEX ix_anno_lfn_b ON anno_lfn_b(name);

--
-- annotations
--
INSERT INTO sequences VALUES( 'anno_id_seq', 0 );

CREATE TABLE anno_tr (
        id         BIGINT PRIMARY KEY,
        did        BIGINT NOT NULL,
        mkey       VARCHAR(64) NOT NULL,

        CONSTRAINT sk_anno_tr UNIQUE(did,mkey)
--      CONSTRAINT fk_anno_tr FOREIGN KEY(did) REFERENCES anno_definition(id) ON DELETE CASCADE
) type=InnoDB;

CREATE TABLE anno_dv (
        id         BIGINT PRIMARY KEY,
        did        BIGINT NOT NULL,
        mkey       VARCHAR(64) NOT NULL,

        CONSTRAINT sk_anno_dv UNIQUE(did,mkey)
--      CONSTRAINT fk_anno_dv FOREIGN KEY(did) REFERENCES anno_definition(id) ON DELETE CASCADE
) type=InnoDB;

CREATE TABLE anno_lfn (
        id         BIGINT PRIMARY KEY,
        name       VARCHAR(255) NOT NULL,
        mkey       VARCHAR(64) NOT NULL,

        CONSTRAINT sk_anno_lfn UNIQUE(name,mkey)
--	CONSTRAINT fk_anno_lfn FOREIGN KEY(did) REFERENCES anno_definition(id) ON DELETE CASCADE
) type=InnoDB;

CREATE TABLE anno_targ (
        id         BIGINT PRIMARY KEY,
        did        BIGINT NOT NULL,
        name       VARCHAR(64) NOT NULL,
        mkey       VARCHAR(64) NOT NULL,

        CONSTRAINT sk_anno_targ UNIQUE(did,name,mkey)
--      CONSTRAINT fk_anno_targ FOREIGN KEY(did) REFERENCES anno_definition(id) ON DELETE CASCADE
) type=InnoDB;

CREATE TABLE anno_call (
        id         BIGINT PRIMARY KEY,
        did        BIGINT NOT NULL,
        pos        INTEGER NOT NULL,
        mkey       VARCHAR(64) NOT NULL,

        CONSTRAINT sk_anno_call UNIQUE(did,pos,mkey)
--      CONSTRAINT fk_anno_call FOREIGN KEY(did) REFERENCES anno_definition(id) ON DELETE CASCADE
) type=InnoDB;

CREATE INDEX ix_anno_tr    ON anno_tr(mkey);
CREATE INDEX ix_anno_dv    ON anno_dv(mkey);
CREATE INDEX ix_anno_lfn   ON anno_lfn(mkey);
CREATE INDEX ix_anno_targ  ON anno_targ(mkey);
CREATE INDEX ix_anno_call  ON anno_call(mkey);

CREATE INDEX ix_anno_tr2   ON anno_tr(did);
CREATE INDEX ix_anno_dv2   ON anno_dv(did);
CREATE INDEX ix_anno_lfn2  ON anno_lfn(name);
CREATE INDEX ix_anno_targ2 ON anno_targ(did);
CREATE INDEX ix_anno_call2 ON anno_call(did);


-- boolean value annotations
CREATE TABLE anno_bool (
        id              BIGINT NOT NULL PRIMARY KEY,
        value           TINYINT NOT NULL DEFAULT 0
) type=InnoDB;

-- boolean value annotations
CREATE TABLE anno_int (
        id              BIGINT NOT NULL PRIMARY KEY,
        value           BIGINT NOT NULL
) type=InnoDB;

-- boolean value annotations
CREATE TABLE anno_float (
        id              BIGINT NOT NULL PRIMARY KEY,
        value           DOUBLE PRECISION NOT NULL
) type=InnoDB;

-- boolean value annotations
CREATE TABLE anno_date (
        id              BIGINT NOT NULL PRIMARY KEY,
        value           DATETIME NOT NULL
) type=InnoDB;

-- boolean value annotations
CREATE TABLE anno_text (
        id              BIGINT NOT NULL PRIMARY KEY,
        value           MEDIUMBLOB NOT NULL
) type=InnoDB;
