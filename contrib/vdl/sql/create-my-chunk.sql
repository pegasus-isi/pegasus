--
-- schema: org.griphyn.vdl.dbschema.ChunkSchema
-- driver: MySQL 4.*
-- $Revision$
--

-- if the next step fails, you forgot to run "create-init-my.sql"
INSERT INTO vds_schema VALUES ('ChunkSchema','vdc','1.3',current_user(),current_timestamp(0));

-- create sequence, fails if it already exists
INSERT INTO sequences VALUES( 'def_id_seq', 0 );

-- Definition storage
CREATE TABLE vdc_definition (
	id		BIGINT NOT NULL,
        type            INTEGER NOT NULL,
	name		VARCHAR(255) NOT NULL,
	namespace	VARCHAR(255) NOT NULL,
	version		VARCHAR(20) NOT NULL,
	xml 		MEDIUMBLOB DEFAULT NULL,

	CONSTRAINT      pk_vdc_definition PRIMARY KEY(id),
	CONSTRAINT      sk_vdc_definition UNIQUE(type,name,namespace,version)
) type=InnoDB;

-- 'none' parameters
CREATE TABLE vdc_nlfn (
	id		BIGINT NOT NULL,
	name		VARCHAR(255) NOT NULL,

	CONSTRAINT	pk_vdc_nlfn PRIMARY KEY(id,name),
	CONSTRAINT	fk_vdc_nlfn FOREIGN KEY(id) REFERENCES vdc_definition(id) ON DELETE CASCADE
) type=InnoDB;


-- 'input' filenames
CREATE TABLE vdc_ilfn (
	id		BIGINT NOT NULL,
	name		VARCHAR(255) NOT NULL,

	CONSTRAINT	pk_vdc_ilfn PRIMARY KEY(id,name),
	CONSTRAINT	fk_vdc_ilfn FOREIGN KEY(id) REFERENCES vdc_definition(id) ON DELETE CASCADE
) type=InnoDB;

-- 'output' filenames
CREATE TABLE vdc_olfn (
	id		BIGINT NOT NULL,
	name		VARCHAR(255) NOT NULL,

	CONSTRAINT	pk_vdc_olfn PRIMARY KEY(id,name),
	CONSTRAINT	fk_vdc_olfn FOREIGN KEY(id) REFERENCES vdc_definition(id) ON DELETE CASCADE
) type=InnoDB;

-- 'inout' filenames
CREATE TABLE vdc_blfn (
	id		BIGINT NOT NULL,
	name		VARCHAR(255) NOT NULL,

	CONSTRAINT	pk_vdc_blfn PRIMARY KEY(id,name),
	CONSTRAINT	fk_vdc_blfn FOREIGN KEY(id) REFERENCES vdc_definition(id) ON DELETE CASCADE
) type=InnoDB;


CREATE INDEX ix_vdc_nlfn ON vdc_nlfn(name);
CREATE INDEX ix_vdc_ilfn ON vdc_ilfn(name);
CREATE INDEX ix_vdc_olfn ON vdc_olfn(name);
CREATE INDEX ix_vdc_blfn ON vdc_blfn(name);
