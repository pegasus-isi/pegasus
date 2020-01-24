/*
 * This file or a portion of this file is licensed under the terms of
 * the Globus Toolkit Public License, found in file ../GTPL, or at
 * http://www.globus.org/toolkit/download/license.html. This notice must
 * appear in redistributions of this file, with or without modification.
 *
 * Redistributions of this Software, with or without modification, must
 * reproduce the GTPL in: (1) the Software, or (2) the Documentation or
 * some other similar material which is provided with the Software (if
 * any).
 *
 * Copyright 1999-2004 University of Chicago and The University of
 * Southern California. All rights reserved.
 */
package org.griphyn.vdl.dbschema;

import edu.isi.pegasus.common.util.DynamicLoader;
import java.io.*;
import java.lang.reflect.*;
import java.sql.*;
import java.sql.SQLException;
import org.griphyn.vdl.classes.*;
import org.griphyn.vdl.dbdriver.*;
import org.griphyn.vdl.parser.*;
import org.griphyn.vdl.util.ChimeraProperties;
import org.griphyn.vdl.util.FileHelper;
import org.griphyn.vdl.util.Logging;

/**
 * This is a class that falls back not on a real database backend, but rather on an existing
 * Definitions data structure that are read from file during construction (or rather, during open),
 * and pushed back into file at destruction (or rather, during close).
 *
 * <p>While schemas in general should fall back onto drivers to perform actions, it is rather
 * difficult to create a JDBC interface to file operations. Thus, the file operations are sneaked
 * into this class.
 *
 * <p>This class is thought more for experimental use than production.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 * @see org.griphyn.vdl.dbdriver
 * @see org.griphyn.vdl.classes.Definitions
 */
public class SingleFileSchema extends InMemorySchema {
    /** Save the name of the database file, so we can dump our memory. */
    private String m_filename;

    /** Save the file locking helper, once dynaloaded. */
    private FileHelper m_filehelper;

    /** An instance of the VDLx XML parser. */
    private org.griphyn.vdl.parser.VDLxParser m_parser;

    /**
     * Fakes a connect to the database. This class load the memory database during construction time
     * from the specified file.
     *
     * @param hyphen_d is the CLI argument being passed, and ignored for now.
     */
    public SingleFileSchema(String hyphen_d)
            throws ClassNotFoundException, NoSuchMethodException, InstantiationException,
                    IllegalAccessException, InvocationTargetException, SQLException, IOException {
        super(); // call minimalistic c'tor, no driver loading!
        ChimeraProperties props = ChimeraProperties.instance();
        // Start a new set of definitions
        this.m_memory = new Definitions();
        this.m_dbschemaprops = props.getDatabaseSchemaProperties(PROPERTY_PREFIX);

        // obtain the schema location URL from the schema properties:
        // url is a list of strings representing schema locations. The
        // content exists in pairs, one of the namespace URI, one of the
        // location URL.
        String url = this.m_dbschemaprops.getProperty("xml.url", props.getVDLSchemaLocation());
        Logging.instance().log("dbschema", 3, "schema=" + url);
        this.m_parser = new org.griphyn.vdl.parser.VDLxParser(url);
        Logging.instance().log("dbschema", 3, "created reader");

        // obtain the file location from the schema properties
        File db = new File(props.getSysConfDir(), "vds.db");
        this.m_filename = this.m_dbschemaprops.getProperty("file.store", db.getAbsolutePath());
        Logging.instance().log("dbschema", 3, "filename=" + m_filename);

        // Determine helper to provide locking functions
        String locker = m_dbschemaprops.getProperty("file.lock", "LockFileLock");
        if (locker.indexOf('.') == -1) locker = "org.griphyn.vdl.util." + locker;

        // dynamically load the file locking helper implementation
        Logging.instance().log("dbschema", 3, "trying to load " + locker);
        DynamicLoader dl = new DynamicLoader(locker);
        String arg[] = new String[1];
        arg[0] = m_filename;
        m_filehelper = (FileHelper) dl.instantiate(arg);

        // FileHelper m_filehelper = new FileHelper2(this.m_filename);
        File file = m_filehelper.openReader();

        if (file == null) {
            Logging.instance().log("dbschema", 3, "openReader returned null");
            throw new SQLException("Can't lock file " + this.m_memory);
        }

        // Does database exist?
        try {
            if (file.exists()) {
                // file exists, read it unless empty
                if (file.length() > 0) {
                    // parse the complete file (database)
                    this.m_parser.parse(
                            new org.xml.sax.InputSource(new BufferedReader(new FileReader(file))),
                            new NoHassleHandler(this.m_memory));
                    Logging.instance()
                            .log(
                                    "app",
                                    1,
                                    this.m_memory.getDefinitionCount()
                                            + " definitions loaded into main memory");
                }
            }
        } catch (Exception e) {
            throw new SQLException(e.getMessage());
        } finally {
            // always release locks
            try {
                m_filehelper.closeReader(file);
            } catch (Exception e) {
                throw new SQLException(e.getMessage());
            }
        }
    }

    /**
     * Disassociate from the database driver before finishing. In this case, dump the memory
     * database back to the file that was saved. Mind that performing this action may throw
     * NullPointerException in later stages!
     */
    public void close() throws SQLException {
        super.close();

        // FileHelper m_filehelper = new FileHelper2(this.m_filename);
        File file = m_filehelper.openWriter();

        if (file == null) {
            throw new SQLException("Unable to create file writer!");
        }

        try {
            BufferedWriter bw = new BufferedWriter(new FileWriter(file));
            this.m_memory.toXML(bw, "");
            bw.flush();
            bw.close();
        } catch (IOException e) {
            throw new SQLException(e.getMessage());
        } finally {
            // always release locks
            try {
                m_filehelper.closeWriter(file);
            } catch (Exception e) {
                throw new SQLException(e.getMessage());
            }
        }
    }
}
