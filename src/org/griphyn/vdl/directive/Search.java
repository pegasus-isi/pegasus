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
import java.sql.SQLException;
import java.util.Iterator;
import java.util.MissingResourceException;
import org.griphyn.vdl.annotation.*;
import org.griphyn.vdl.classes.*;
import org.griphyn.vdl.dbschema.*;

/**
 * This class searches for definitions that either match certain namespace, name, version
 * combination, or contain a certain LFN.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 * @see org.griphyn.vdl.parser.VDLxParser
 * @see org.griphyn.vdl.dbschema.VDC
 */
public class Search extends Directive {
    /** Defines the output format constants */
    public static final int FORMAT_FQDN = 0;

    public static final int FORMAT_VDLT = 1;
    public static final int FORMAT_VDLX = 2;

    private DatabaseSchema m_dbschema = null;

    /** Constructor */
    public Search() throws IOException, MissingResourceException {
        super();
    }

    /**
     * Constructor
     *
     * @param dbs is the database schema instance
     */
    public Search(DatabaseSchema dbs) throws IOException, MissingResourceException {
        m_dbschema = dbs;
    }

    /**
     * set database schema
     *
     * @param dbs is the database schema instance
     */
    public void setDatabaseSchema(DatabaseSchema dbs) {
        m_dbschema = dbs;
    }

    /**
     * Search for definitions that contain LFN of specific name and link type. This method does not
     * allow jokers.
     *
     * @param filename the LFN name
     * @param link the linkage type of the LFN
     * @return a list of Definition items that match the criterion.
     * @see org.griphyn.vdl.classes.LFN#NONE
     * @see org.griphyn.vdl.classes.LFN#INPUT
     * @see org.griphyn.vdl.classes.LFN#OUTPUT
     * @see org.griphyn.vdl.classes.LFN#INOUT
     */
    public java.util.List searchDefinition(String filename, int link) throws java.sql.SQLException {
        return ((VDC) m_dbschema).searchFilename(filename, link);
    }

    /**
     * Search the database for definitions by ns::name:version triple and by type (either
     * Transformation or Derivation). This version of the search allows for jokers expressed as null
     * value
     *
     * @param namespace namespace, null to match any namespace
     * @param name name, null to match any name
     * @param version version, null to match any version
     * @param clsType type of definition, see below, or -1 as wildcard
     * @return a list of Definition items, which may be empty
     * @see org.griphyn.vdl.classes.Definition#TRANSFORMATION
     * @see org.griphyn.vdl.classes.Definition#DERIVATION
     */
    public java.util.List searchDefinition(
            String namespace, String name, String version, int clsType)
            throws java.sql.SQLException {
        return ((VDC) m_dbschema).searchDefinition(namespace, name, version, clsType);
    }

    /**
     * Checks a string for the presence of joker characters.
     *
     * @param s is the input string
     * @return true, if a joker character was detected, false otherwise and null strings.
     */
    private boolean hasJoker(String s) {
        return (s == null ? false : (s.indexOf('*') + s.indexOf('?') != -2));
    }

    /**
     * Translates the regular shell-style jokers into SQL jokers. Simultaneously protects (with
     * backslash for now) any SQL jokers to make them literal.
     *
     * @param hasJoker is flagged, if the string contains shell jokers
     * @param s input string to translate
     * @return translated string -- may be the original reference
     */
    private String mask(boolean hasJoker, String s) {
        String result = s;
        if (s == null) return result;

        if (result.indexOf('%') + result.indexOf('_') != -2) {
            // has SQL jokers, protect them (backslash for now)
            result = result.replaceAll("([%_])", "\\\\$1");
        }

        if (hasJoker) {
            // turn jokers into SQL jokers
            result = result.replace('*', '%').replace('?', '_');
        }

        return result;
    }

    /**
     * Search the database for definitions by ns::name:version triple and by type (either
     * Transformation or Derivation). This version of the search allows for jokers expressed as null
     * value, or special characters '%' and '_'.
     *
     * @param namespace namespace, null to match any namespace
     * @param name name, null to match any name
     * @param version version, null to match any version
     * @param clsType type of definition, see below, or -1 as wildcard
     * @return a list of Definition items, which may be empty
     * @see org.griphyn.vdl.classes.Definition#TRANSFORMATION
     * @see org.griphyn.vdl.classes.Definition#DERIVATION
     */
    public java.util.List searchDefinitionEx(
            String namespace, String name, String version, int clsType)
            throws java.sql.SQLException {
        boolean b1 = hasJoker(namespace);
        boolean b2 = hasJoker(name);
        boolean b3 = hasJoker(version);
        if (b1 || b2 || b3) {
            // protect and translate jokers
            return ((Advanced) m_dbschema)
                    .searchDefinitionEx(
                            mask(b1, namespace), mask(b2, name), mask(b3, version), clsType);
        } else {
            // no jokers, use potentially more efficient query
            return ((VDC) m_dbschema).searchDefinition(namespace, name, version, clsType);
        }
    }

    /**
     * Search for LFNs or Definitions that has certain annotations
     *
     * @param kind defines the kind/class of object annotated.
     * @param arg is used only for TR ARG and TR CALL. For the former it is the name of the argument
     *     (String), for the latter the position of the call (Integer).
     * @param tree stores the query tree to query the annotation
     * @return a list of LFNs if search for filenames, otherwise a list of definitions.
     * @exception SQLException if something goes wrong with the database.
     * @see org.griphyn.vdl.annotation.QueryTree
     */
    public java.util.List searchAnnotation(int kind, Object arg, QueryTree tree)
            throws java.sql.SQLException {
        return ((Annotation) m_dbschema).searchAnnotation(kind, arg, tree);
    }

    /**
     * Print a list of definitions in different format: fqdn, vdlt, and vdlx
     *
     * @param writer the target to output the list
     * @param defList a list of definitions
     * @param format the output format
     * @see #FORMAT_FQDN
     * @see #FORMAT_VDLT
     * @see #FORMAT_VDLX NOTE: might be better to move into another module?
     */
    public void printDefinitionList(Writer writer, java.util.List defList, int format)
            throws IOException {
        if (defList == null || defList.isEmpty()) return;
        Definitions defs = new Definitions();
        if (format != FORMAT_FQDN) {
            defs.setDefinition(defList);
            if (format == FORMAT_VDLX) defs.toXML(writer, "");
            else if (format == FORMAT_VDLT) defs.toString(writer);
        } else {
            for (Iterator i = defList.iterator(); i.hasNext(); ) {
                Definition def = (Definition) i.next();

                writer.write(def.identify());
                writer.write("\n");
            }
            writer.flush();
        }
    }
}
