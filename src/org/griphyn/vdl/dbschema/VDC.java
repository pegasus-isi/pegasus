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
import org.griphyn.vdl.classes.Definition;

/**
 * This common schema interface defines the schemas in which the abstraction layers access the VDC.
 * This layer is independent of the implementing database, and does so by going via the database
 * driver class API.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 * @see org.griphyn.vdl.dbschema.DatabaseSchema
 * @see org.griphyn.vdl.dbdriver
 */
public interface VDC extends Catalog {
    /** Names the property key prefix employed for schemas dealing with the VDC. */
    public static final String PROPERTY_PREFIX = "vds.db.vdc.schema";

    //
    // taken from (old) DBManager
    //

    /**
     * Loads a single Definition from the backend database into an Java object. This method does not
     * allow wildcarding!
     *
     * @param namespace namespace, null will be converted into empty string
     * @param name name, null will be converted into empty string
     * @param version version, null will be converted into empty string
     * @param type type of the definition (TR or DV), must not be -1.
     * @return the Definition as specified, or null if not found.
     * @see org.griphyn.vdl.classes.Definition#TRANSFORMATION
     * @see org.griphyn.vdl.classes.Definition#DERIVATION
     * @see #saveDefinition( Definition, boolean )
     * @see #containsDefinition( Definition )
     * @see #searchDefinition( String, String, String, int )
     */
    public abstract Definition loadDefinition(
            String namespace, String name, String version, int type) throws SQLException;

    /**
     * Saves a Definition, that is either a Transformation or Derivation, into the backend database.
     *
     * @param definition is the new Definition to store.
     * @param overwrite true, if existing defitions will be overwritten by new ones with the same
     *     primary (or secondary) key (-set), or false, if a new definition will be rejected on key
     *     matches.
     * @return true, if the backend database was changed, or false, if the definition was not
     *     accepted into the backend.
     * @see org.griphyn.vdl.classes.Definition
     * @see org.griphyn.vdl.classes.Transformation
     * @see org.griphyn.vdl.classes.Derivation
     * @see #loadDefinition( String, String, String, int )
     * @see #deleteDefinition( String, String, String, int )
     */
    public abstract boolean saveDefinition(Definition definition, boolean overwrite)
            throws SQLException;

    //
    // higher level methods, allowing for wildcarding as stated.
    //

    /**
     * Check with the backend database, if the given definition exists.
     *
     * @param definition is a Definition object to search for
     * @return true, if the Definition exists, false if not found
     */
    public abstract boolean containsDefinition(Definition definition) throws SQLException;

    /**
     * Delete a specific Definition objects from the database. No wildcard matching will be done.
     * "Fake" definitions are permissable, meaning it just has the secondary key triple.
     *
     * @param definition is the definition specification to delete
     * @return true is something was deleted, false if non existent.
     * @see org.griphyn.vdl.classes.Definition#TRANSFORMATION
     * @see org.griphyn.vdl.classes.Definition#DERIVATION
     */
    public abstract boolean deleteDefinition(Definition definition) throws SQLException;

    /**
     * Delete one or more definitions from the backend database. The key triple parameters may be
     * wildcards. Wildcards are expressed as <code>null</code> value.
     *
     * @param namespace namespace
     * @param name name
     * @param version version
     * @param type definition type (TR or DV)
     * @return a list of definitions that were deleted.
     * @see org.griphyn.vdl.classes.Definition#TRANSFORMATION
     * @see org.griphyn.vdl.classes.Definition#DERIVATION
     */
    public abstract java.util.List deleteDefinition(
            String namespace, String name, String version, int type) throws SQLException;

    /**
     * Search the database for definitions by ns::name:version triple and by type (either
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
    public abstract java.util.List searchDefinition(
            String namespace, String name, String version, int type) throws SQLException;

    /**
     * Searches the database for all derivations that contain a certain LFN. The linkage is an
     * additional constraint. This method does not allow jokers.
     *
     * @param lfn the LFN name
     * @param link the linkage type of the LFN
     * @return a list of Definition items that match the criterion.
     * @see org.griphyn.vdl.classes.LFN#NONE
     * @see org.griphyn.vdl.classes.LFN#INPUT
     * @see org.griphyn.vdl.classes.LFN#OUTPUT
     * @see org.griphyn.vdl.classes.LFN#INOUT
     */
    public abstract java.util.List searchFilename(String lfn, int link) throws SQLException;
}
