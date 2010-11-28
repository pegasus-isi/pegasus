---
--- schema: all
--- driver: PostGreSQL 7.4.*
--- $Revision$
---

CREATE TABLE pegasus_schema (
	name		VARCHAR(64) NOT NULL,
	catalog		VARCHAR(16),
	version		FLOAT,
	creator		VARCHAR(8) DEFAULT current_user,
	creation	TIMESTAMP WITH TIME ZONE DEFAULT current_timestamp(0),

	CONSTRAINT	pk_pegasus_schema PRIMARY KEY(name)
);
