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

import java.sql.*;

/**
 * This common schema interface defines advanced search interfaces for VDC. The advanced methods
 * required permit wildcard searches, partial matches, and candidate list compilations that are not
 * part of the simpler @{link VDC} interface.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 * @see org.griphyn.vdl.dbschema.DatabaseSchema
 * @see org.griphyn.vdl.dbdriver
 */
public interface Advanced extends VDC {
    //
    // higher level methods, allowing for partial matching
    //

    /**
     * Delete one or more definitions from the backend database. The key triple parameters may be
     * wildcards. Wildcards are expressed as <code>null</code> value, or have special characters '%'
     * and '_'.
     *
     * @param namespace namespace
     * @param name name
     * @param version version
     * @param type definition type (TR or DV)
     * @return a list of definitions that were deleted.
     * @see org.griphyn.vdl.classes.Definition#TRANSFORMATION
     * @see org.griphyn.vdl.classes.Definition#DERIVATION
     */
    public abstract java.util.List deleteDefinitionEx(
            String namespace, String name, String version, int type) throws SQLException;

    /**
     * Searches the database for definitions by ns::name:version triple and by type (either
     * Transformation or Derivation). This version of the search allows for jokers expressed as null
     * value
     *
     * @param namespace namespace, null to match any namespace
     * @param name name, null to match any name
     * @param version version, null to match any version
     * @param type type of definition, see below, or -1 as wildcard
     * @return a list of Definition items, which may be empty
     * @see org.griphyn.vdl.classes.Definition#TRANSFORMATION
     * @see org.griphyn.vdl.classes.Definition#DERIVATION
     * @see #loadDefinition( String, String, String, int )
     */
    public abstract java.util.List searchDefinitionEx(
            String namespace, String name, String version, int type) throws SQLException;

    /**
     * Searches the database for all LFNs that match a certain pattern. The linkage is an additional
     * constraint. This method allows joker characters such as '%' and '_'.
     *
     * @param lfn the LFN name
     * @param link the linkage type of the LFN
     * @return a list of filenames that match the criterion.
     * @see org.griphyn.vdl.classes.LFN#NONE
     * @see org.griphyn.vdl.classes.LFN#INPUT
     * @see org.griphyn.vdl.classes.LFN#OUTPUT
     * @see org.griphyn.vdl.classes.LFN#INOUT
     */
    public abstract java.util.List searchLFN(String lfn, int link) throws SQLException;

    /**
     * Searches the database for a list of namespaces of the definitions Sorted in ascending order.
     *
     * @param type type of definition, see below, or -1 for both
     * @return a list of namespaces
     * @see org.griphyn.vdl.classes.Definition#TRANSFORMATION
     * @see org.griphyn.vdl.classes.Definition#DERIVATION
     */
    public abstract java.util.List getNamespaceList(int type) throws SQLException;

    /**
     * Searches the database for a list of fully-qualified names of the definitions sorted in
     * ascending order.
     *
     * @param type type of definition, see below, or -1 for both.
     * @return a list of FQDNs
     * @see org.griphyn.vdl.classes.Definition#TRANSFORMATION
     * @see org.griphyn.vdl.classes.Definition#DERIVATION
     */
    public abstract java.util.List getFQDNList(int type) throws SQLException;
}
