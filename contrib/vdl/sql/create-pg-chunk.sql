--
-- schema: org.griphyn.vdl.dbschema.ChunkSchema
-- driver: PostGreSQL 7.4.*
-- $Revision$
--

-- if the next step fails, you forgot to run "create-init-pg.sql"
INSERT INTO vds_schema(name,catalog,version) 
VALUES ('ChunkSchema','vdc','1.3');

CREATE SEQUENCE def_id_seq MINVALUE 0 MAXVALUE 9223372036854775807 INCREMENT 1;

-- Definition storage
CREATE TABLE vdc_definition (
	id		BIGINT DEFAULT NEXTVAL('def_id_seq') PRIMARY KEY,
        type            INTEGER NOT NULL,
	name		VARCHAR(255) NOT NULL,
	namespace	VARCHAR(255) NOT NULL,
	version		VARCHAR(20) NOT NULL,
	xml 		TEXT DEFAULT NULL,

	-- secondary key definition
	CONSTRAINT      sk_vdc_definition UNIQUE(type,name,namespace,version)
);

-- 'none' parameters
CREATE TABLE vdc_nlfn (
	id		BIGINT NOT NULL,
	name		VARCHAR NOT NULL,

	CONSTRAINT	pk_vdc_nlfn PRIMARY KEY(id,name),
	CONSTRAINT	fk_vdc_nlfn FOREIGN KEY(id) REFERENCES vdc_definition(id) ON DELETE CASCADE
);

-- 'input' filenames
CREATE TABLE vdc_ilfn (
	id		BIGINT NOT NULL,
	name		VARCHAR NOT NULL,

	CONSTRAINT	pk_vdc_ilfn PRIMARY KEY(id,name),
	CONSTRAINT	fk_vdc_ilfn FOREIGN KEY(id) REFERENCES vdc_definition(id) ON DELETE CASCADE
);

-- 'output' filenames
CREATE TABLE vdc_olfn (
	id		BIGINT NOT NULL,
	name		VARCHAR NOT NULL,

	CONSTRAINT	pk_vdc_olfn PRIMARY KEY(id,name),
	CONSTRAINT	fk_vdc_olfn FOREIGN KEY(id) REFERENCES vdc_definition(id) ON DELETE CASCADE
);

-- 'inout' filenames
CREATE TABLE vdc_blfn (
	id		BIGINT NOT NULL,
	name		VARCHAR NOT NULL,

	CONSTRAINT	pk_vdc_blfn PRIMARY KEY(id,name),
	CONSTRAINT	fk_vdc_blfn FOREIGN KEY(id) REFERENCES vdc_definition(id) ON DELETE CASCADE
);

CREATE INDEX ix_vdc_nlfn ON vdc_nlfn(name);
CREATE INDEX ix_vdc_ilfn ON vdc_ilfn(name);
CREATE INDEX ix_vdc_olfn ON vdc_olfn(name);
CREATE INDEX ix_vdc_blfn ON vdc_blfn(name);
