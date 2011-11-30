NetLogger Best Practices Monitoring events
==========================================

As pegasus-monitord parses the various files in a workflow directory
(braindump, workflow-map, dagman.out file), it will generate NetLogger
events that can be used to populate a database using the Stampede
schema. All events have the "stampede." prefix.

As of 4/22/11 the "schema" for these is encoded using Yang. Yang
schema files end in the suffix ".yang". The output
of YUMA's "yangdump" utility is used to document the schema.  In
addition, the newly-added NetLogger nl_loader "xml" output can be used
to validate sample events against the Yang schema.

For more details, see the Pegasus STAMPEDE wiki page:
* https://confluence.pegasus.isi.edu/display/stampede/NetLogger+Monitoring+Events

For validation, see Makefile.

Versions of the schema, at least major changes, are distinguished by the date in
the name of the .yang file. Older versions are peridiocally migrated to the
old/ subdir.

--
Last modified: 5/4/2011
