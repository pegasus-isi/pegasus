--
-- schema: org.griphyn.vdl.dbschema.AnnotationSchema
-- driver: PostGreSQL 7.4.*
-- $Revision$
--

-- if the next step fails, you forgot to run "create-init-pg.sql"
-- or the tables already exist. 
INSERT INTO vds_schema(name,catalog,version) VALUES ('AnnotationSchema','vdc','1.3');

CREATE SEQUENCE def_id_seq MINVALUE 0 MAXVALUE 9223372036854775807 INCREMENT 1;

-- Definition storage
CREATE TABLE anno_definition (
        id              BIGINT DEFAULT NEXTVAL('def_id_seq') PRIMARY KEY,
        type            INTEGER NOT NULL,
        name            VARCHAR(255) NOT NULL,
        namespace       VARCHAR(255) NOT NULL,
        version         VARCHAR(20) NOT NULL,
        xml             TEXT DEFAULT NULL,

        -- secondary key definition
        CONSTRAINT      sk_anno_definition UNIQUE(type,name,namespace,version)
);

-- 'input' filenames
CREATE TABLE anno_lfn_i (
        did        BIGINT NOT NULL,
        name       VARCHAR NOT NULL,

        CONSTRAINT pk_anno_lfn_i PRIMARY KEY(did,name)
--      CONSTRAINT fk_anno_lfn_i FOREIGN KEY(did) REFERENCES anno_definition(id) ON DELETE CASCADE
);

-- 'output' filenames
CREATE TABLE anno_lfn_o (
        did        BIGINT NOT NULL,
        name       VARCHAR NOT NULL,

        CONSTRAINT pk_anno_lfn_o PRIMARY KEY(did,name)
--      CONSTRAINT fk_anno_lfn_o FOREIGN KEY(did) REFERENCES anno_definition(id) ON DELETE CASCADE
);

-- 'inout' filenames
CREATE TABLE anno_lfn_b (
        did        BIGINT NOT NULL,
        name       VARCHAR NOT NULL,

        CONSTRAINT pk_anno_lfn_b PRIMARY KEY(did,name)
--      CONSTRAINT fk_anno_lfn_b FOREIGN KEY(did) REFERENCES anno_definition(id) ON DELETE CASCADE
);

CREATE INDEX ix_anno_lfn_i ON anno_lfn_i(name);
CREATE INDEX ix_anno_lfn_o ON anno_lfn_o(name);
CREATE INDEX ix_anno_lfn_b ON anno_lfn_b(name);

--
-- annotations
--
CREATE SEQUENCE anno_id_seq MINVALUE 0 MAXVALUE 9223372036854775807 INCREMENT 1;

CREATE TABLE anno_tr (
        id         BIGINT default nextval('anno_id_seq') PRIMARY KEY,
        did        BIGINT NOT NULL,
        mkey       VARCHAR(64) NOT NULL,

        CONSTRAINT sk_anno_tr UNIQUE(did,mkey)
--      CONSTRAINT fk_anno_tr FOREIGN KEY(did) REFERENCES anno_definition(id) ON DELETE CASCADE
);

CREATE TABLE anno_dv (
        id         BIGINT default nextval('anno_id_seq') PRIMARY KEY,
        did        BIGINT NOT NULL,
        mkey       VARCHAR(64) NOT NULL,

        CONSTRAINT sk_anno_dv UNIQUE(did,mkey)
--      CONSTRAINT fk_anno_dv FOREIGN KEY(did) REFERENCES anno_definition(id) ON DELETE CASCADE
);

CREATE TABLE anno_lfn (
        id         BIGINT DEFAULT NEXTVAL('anno_id_seq') PRIMARY KEY,
        name       VARCHAR NOT NULL,
        mkey       VARCHAR(64) NOT NULL,

        CONSTRAINT sk_anno_lfn UNIQUE(name,mkey)
);

CREATE TABLE anno_targ (
        id         BIGINT DEFAULT NEXTVAL('anno_id_seq') PRIMARY KEY,
        did        BIGINT NOT NULL,
        name       VARCHAR(64) NOT NULL,
        mkey       VARCHAR(64) NOT NULL,

        CONSTRAINT sk_anno_targ UNIQUE(did,name,mkey)
--      CONSTRAINT fk_anno_targ FOREIGN KEY(did) REFERENCES anno_definition(id) ON DELETE CASCADE
);

CREATE TABLE anno_call (
        id         BIGINT DEFAULT NEXTVAL('anno_id_seq') PRIMARY KEY,
        did        BIGINT NOT NULL,
        pos        INTEGER NOT NULL,
        mkey       VARCHAR(64) NOT NULL,

        CONSTRAINT sk_anno_call UNIQUE(did,pos,mkey)
--      CONSTRAINT fk_anno_call FOREIGN KEY(did) REFERENCES anno_definition(id) ON DELETE CASCADE
);

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
        value           BOOLEAN NOT NULL
);

-- boolean value annotations
CREATE TABLE anno_int (
        id              BIGINT NOT NULL PRIMARY KEY,
        value           BIGINT NOT NULL
);

-- boolean value annotations
CREATE TABLE anno_float (
        id              BIGINT NOT NULL PRIMARY KEY,
        value           DOUBLE PRECISION NOT NULL
);

-- boolean value annotations
CREATE TABLE anno_date (
        id              BIGINT NOT NULL PRIMARY KEY,
        value           TIMESTAMP WITH TIME ZONE NOT NULL
);

-- boolean value annotations
CREATE TABLE anno_text (
        id              BIGINT NOT NULL PRIMARY KEY,
        value           TEXT NOT NULL
);
