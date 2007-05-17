---
--- schema: all
--- driver: MySQL 4.0.*
--- $Revision: 1.3 $
---

CREATE TABLE sequences (
	name		VARCHAR(32) NOT NULL,
	currval		BIGINT DEFAULT 0,

	CONSTRAINT      pk_sequences PRIMARY KEY(name)
) type=InnoDB;

CREATE TABLE vds_schema (
	name		VARCHAR(64) NOT NULL,
	catalog		VARCHAR(16),
	version		FLOAT,
	creator		VARCHAR(32),
	creation	DATETIME,

	CONSTRAINT	pk_vds_schema PRIMARY KEY(name)
) type=InnoDB;
