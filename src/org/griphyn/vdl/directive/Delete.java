/*
 * This file or a portion of this file is licensed under the terms of
 * the Globus Toolkit Public License, found in file GTPL, or at
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

package org.griphyn.vdl.directive;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.MissingResourceException;
import org.griphyn.vdl.classes.*;
import org.griphyn.vdl.dbschema.*;

/**
 * This class deletes definitions that either match certain namespace, name, version combination
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 * @see org.griphyn.vdl.parser.VDLxParser
 * @see org.griphyn.vdl.dbschema.VDC
 */
public class Delete extends Directive {
    private DatabaseSchema m_dbschema = null;

    /** Constructor */
    public Delete() throws IOException, MissingResourceException {
        super();
    }

    /**
     * Constructor, set database schema instance
     *
     * @param dbs the database schema instance
     */
    public Delete(DatabaseSchema dbs) throws IOException, MissingResourceException {
        m_dbschema = dbs;
    }

    /**
     * set database schema
     *
     * @param dbs the database schema instance
     */
    public void setDatabaseSchema(DatabaseSchema dbs) {
        m_dbschema = dbs;
    }

    /**
     * Delete one or more definitions from the backend database. The key triple parameters may be
     * wildcards. Wildcards are expressed as <code>null</code> value.
     *
     * @param namespace namespace
     * @param name name
     * @param version version
     * @param clsType definition type (TR or DV)
     * @return a list of definitions that were deleted.
     * @see org.griphyn.vdl.classes.Definition#TRANSFORMATION
     * @see org.griphyn.vdl.classes.Definition#DERIVATION
     */
    public java.util.List deleteDefinition(
            String namespace, String name, String version, int clsType)
            throws java.sql.SQLException {
        return ((VDC) m_dbschema).deleteDefinition(namespace, name, version, clsType);
    }

    /**
     * Delete one or more definitions from the backend database. The key triple parameters may be
     * wildcards. Wildcards are expressed as <code>null</code> value. Output the deleted ones.
     *
     * @param namespace namespace
     * @param name name
     * @param version version
     * @param clsType definition type (TR or DV)
     * @param writer writer to output deleted definitions
     * @return a list of definitions that were deleted.
     * @see org.griphyn.vdl.classes.Definition#TRANSFORMATION
     * @see org.griphyn.vdl.classes.Definition#DERIVATION
     */
    public java.util.List deleteDefinition(
            String namespace, String name, String version, int clsType, Writer writer)
            throws java.sql.SQLException, IOException {
        java.util.List defList =
                ((VDC) m_dbschema).deleteDefinition(namespace, name, version, clsType);
        if (writer != null) {
            for (Iterator i = defList.iterator(); i.hasNext(); ) {
                Definition d = (Definition) i.next();
                d.toXML(writer, "  ");
                m_logger.log("delete", 0, "deleted " + d.identify());
            }
        }
        return defList;
    }

    /**
     * Delete one or more definitions from the backend database. The key triple parameters may be
     * wildcards in force mode. Wildcards are expressed as <code>null</code> value. Output the
     * deleted ones.
     *
     * @param namespace namespace
     * @param name name
     * @param version version
     * @param clsType definition type (TR or DV)
     * @param writer writer to output deleted definitions
     * @param force force wildcard matching
     * @return a list of definitions that were deleted.
     * @see org.griphyn.vdl.classes.Definition#TRANSFORMATION
     * @see org.griphyn.vdl.classes.Definition#DERIVATION
     */
    public java.util.List deleteDefinition(
            String namespace,
            String name,
            String version,
            int clsType,
            Writer writer,
            boolean force)
            throws java.sql.SQLException, IOException {

        java.util.List defList = null;
        if (force) {
            defList = ((VDC) m_dbschema).deleteDefinition(namespace, name, version, clsType);
        } else {
            ArrayList list = new ArrayList();
            if (clsType == -1 || clsType == Definition.TRANSFORMATION) {
                Definition def =
                        ((VDC) m_dbschema)
                                .loadDefinition(
                                        namespace, name, version, Definition.TRANSFORMATION);
                if (def != null) {
                    list.add(def);
                    ((VDC) m_dbschema).deleteDefinition(def);
                }
            }
            if (clsType == -1 || clsType == Definition.DERIVATION) {
                Definition def =
                        ((VDC) m_dbschema)
                                .loadDefinition(namespace, name, version, Definition.DERIVATION);
                if (def != null) {
                    list.add(def);
                    ((VDC) m_dbschema).deleteDefinition(def);
                }
            }
            defList = list;
        }

        if (writer != null) {
            for (Iterator i = defList.iterator(); i.hasNext(); ) {
                Definition d = (Definition) i.next();
                d.toXML(writer, "  ");
                m_logger.log("delete", 0, "deleted " + d.identify());
            }
        }
        return defList;
    }
}
