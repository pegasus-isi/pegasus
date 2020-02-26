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

import java.io.IOException;
import java.lang.reflect.*;
import java.sql.*;
import java.sql.SQLException;
import java.util.*;
import org.griphyn.vdl.classes.*;
import org.griphyn.vdl.dbdriver.*;
import org.griphyn.vdl.util.ChimeraProperties;
import org.griphyn.vdl.util.Logging;

/**
 * This is a class that falls back not on a real database backend, but rather on an existing
 * Definitions data structure in main memory. This schema is for internal use only.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 * @see org.griphyn.vdl.dbdriver
 * @see org.griphyn.vdl.classes.Definitions
 */
public class InMemorySchema extends DatabaseSchema implements VDC {
    /**
     * Stores a reference to the in-memory data structure that hold all definitions that we can
     * access from within this instance.
     */
    protected Definitions m_memory;

    /** Default ctor does nothing. */
    protected InMemorySchema() {
        super();
        this.m_memory = null;
    }

    /**
     * Dirty hack: Returns a reference to the in-memory database for preliminary routing into DAXes.
     * This is to avoid the duplication of DVs in memory, as memory becomes quickly a scarce
     * resource.
     *
     * @return a reference to the in-memory database.
     */
    public Definitions backdoor() {
        return this.m_memory;
    }

    /**
     * Fakes a connect to the database. This class never uses any database, but instead applies all
     * data to the provided reference to the in-memory structure. Subclasses may refine this view to
     * work with files or URLs.
     *
     * @param memory is a reference to an existing in-memory Java object holding all our necessary
     *     definitions.
     */
    public InMemorySchema(Definitions memory)
            throws ClassNotFoundException, NoSuchMethodException, InstantiationException,
                    IllegalAccessException, InvocationTargetException, SQLException, IOException {
        super(); // call minimalistic c'tor
        this.m_memory = memory;
        this.m_dbschemaprops =
                ChimeraProperties.instance().getDatabaseSchemaProperties(PROPERTY_PREFIX);
    }

    /**
     * Pass-thru to driver. Always returns false, as the backend is main memory.
     *
     * @return true, if it is feasible to cache results from the driver false, if requerying the
     *     driver is sufficiently fast (e.g. driver is in main memory, or driver does caching
     *     itself).
     */
    public boolean cachingMakesSense() {
        return false;
    }

    //
    // lower level methods, working directly on specific definitions
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
     * @see #searchDefinition( String, String, String, int )
     */
    public Definition loadDefinition(String namespace, String name, String version, int type)
            throws SQLException {
        // walk main memory
        Definition result = null;
        for (Iterator i = this.m_memory.iterateDefinition(); i.hasNext(); ) {
            Definition d = (Definition) i.next();
            if (d.match(type, namespace, name, version)) {
                result = d;
                break;
            }
        }
        return result;
    }

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
    public boolean saveDefinition(Definition definition, boolean overwrite) throws SQLException {
        int position = this.m_memory.positionOfDefinition(definition);
        if (position != -1) {
            // definition already exists
            if (overwrite) {
                Logging.instance().log("app", 1, "Modifying " + definition.shortID());
                this.m_memory.setDefinition(position, definition);
                return true;
            } else {
                Logging.instance().log("app", 1, "Rejecting " + definition.shortID());
                return false;
            }
        } else {
            // definition does not exist
            Logging.instance().log("app", 1, "Adding " + definition.shortID());
            this.m_memory.addDefinition(definition);
            return true;
        }
    }

    //
    // higher level methods, allowing for wildcarding as stated.
    //

    /**
     * Check with the backend database, if the given definition exists.
     *
     * @param definition is a Definition object to search for
     * @return true, if the Definition exists, false if not found
     */
    public boolean containsDefinition(Definition definition) throws SQLException {
        return (this.m_memory.positionOfDefinition(definition) != -1);
    }

    /**
     * Delete a specific Definition objects from the database. No wildcard matching will be done.
     * "Fake" definitions are permissable, meaning it just has the secondary key triple.
     *
     * <p>This method is not implemented!
     *
     * @param definition is the definition specification to delete
     * @return true is something was deleted, false if non existent.
     * @see org.griphyn.vdl.classes.Definition#TRANSFORMATION
     * @see org.griphyn.vdl.classes.Definition#DERIVATION
     */
    public boolean deleteDefinition(Definition definition) throws SQLException {
        return this.m_memory.removeDefinition(definition);
    }

    /**
     * Delete one or more definitions from the backend database. Depending on the matchAll flag the
     * key triple parameters may be wildcards. Wildcards are expressed as <code>null</code> value.
     *
     * <p>This method is not implemented!
     *
     * @param namespace namespace
     * @param name name
     * @param version version
     * @param type definition type (TR or DV)
     * @return a list of definitions that were deleted.
     * @see org.griphyn.vdl.classes.Definition#TRANSFORMATION
     * @see org.griphyn.vdl.classes.Definition#DERIVATION
     */
    public java.util.List deleteDefinition(String namespace, String name, String version, int type)
            throws SQLException {
        java.util.List result = new ArrayList();

        // walk the database
        for (ListIterator i = this.m_memory.listIterateDefinition(); i.hasNext(); ) {
            Definition d = (Definition) i.next();
            if (type == -1 || d.getType() == type) {
                // yes, type matches, let's continue
                String ns = d.getNamespace();
                String id = d.getName();
                String vs = d.getVersion();

                if ((namespace == null
                                || // match all for null argument
                                ns != null && ns.equals(namespace))
                        && (name == null
                                || // match all for null argument
                                id != null && id.equals(name))
                        && (version == null
                                || // match all for null argument
                                vs != null && vs.equals(version))) {
                    // there was a match including nulls and jokers etc.
                    result.add(d);
                    i.remove();
                }
            }
        }

        return result;
    }

    /**
     * Search the database for definitions by ns::name:version triple and by type (either
     * Transformation or Derivation). This version of the search allows for jokers expressed as null
     * value.
     *
     * <p>This method is not implemented!
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
    public java.util.List searchDefinition(String namespace, String name, String version, int type)
            throws SQLException {
        java.util.List result = new ArrayList();

        // walk the database
        for (ListIterator i = this.m_memory.listIterateDefinition(); i.hasNext(); ) {
            Definition d = (Definition) i.next();
            if (type == -1 || d.getType() == type) {
                // yes, type matches, let's continue
                String ns = d.getNamespace();
                String id = d.getName();
                String vs = d.getVersion();

                if ((namespace == null
                                || // match all for null argument
                                ns != null && ns.equals(namespace))
                        && (name == null
                                || // match all for null argument
                                id != null && id.equals(name))
                        && (version == null
                                || // match all for null argument
                                vs != null && vs.equals(version))) {
                    result.add(d);
                }
            }
        }

        return result;
    }

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
    public java.util.List searchFilename(String lfn, int link) throws SQLException {
        java.util.List result = new ArrayList();

        // check all Derivations (this may be time consuming!)
        for (Iterator i = this.m_memory.iterateDefinition(); i.hasNext(); ) {
            Definition d = (Definition) i.next();
            if (d instanceof Derivation) {
                Derivation dv = (Derivation) d;
                for (Iterator j = dv.iteratePass(); j.hasNext(); ) {
                    boolean found = false;
                    Value actual = ((Pass) j.next()).getValue();
                    switch (actual.getContainerType()) {
                        case Value.SCALAR:
                            // this is a regular SCALAR
                            if (scalarContainsLfn((Scalar) actual, lfn, link)) {
                                // Logging.instance().log("search", 2, "found " + dv.shortID());
                                result.add(dv);
                                found = true;
                            }
                            break;
                        case Value.LIST:
                            // a LIST is a list of SCALARs
                            org.griphyn.vdl.classes.List list =
                                    (org.griphyn.vdl.classes.List) actual;
                            for (Iterator f = list.iterateScalar(); f.hasNext(); ) {
                                if (scalarContainsLfn((Scalar) f.next(), lfn, link)) {
                                    // Logging.instance().log("search", 2, "found " + dv.shortID());
                                    result.add(dv);
                                    found = true;

                                    // skip all other scalars
                                    break;
                                }
                            }
                            break;
                        default:
                            // this should not happen
                            Logging.instance()
                                    .log(
                                            "default",
                                            0,
                                            "WARNING: An actual argument \""
                                                    + actual.toString()
                                                    + "\" is neither SCALAR nor LIST");
                            break;
                    }

                    // if found in one Pass, skip all the others
                    if (found) break;
                }
            }
        }

        return result;
    }

    /**
     * This helper function checks, if a given Scalar instance contains the specified logical
     * filename as LFN instance anywhere in its sub-structures.
     *
     * @param scalar is a Scalar instance to check
     * @param lfn is a logical filename string to check for
     * @param link is the linkage type of the lfn. if -1, do not check the linkage type.
     * @return true, if the file was found
     */
    protected boolean scalarContainsLfn(Scalar scalar, String lfn, int link) {
        for (Iterator e = scalar.iterateLeaf(); e.hasNext(); ) {
            org.griphyn.vdl.classes.Leaf leaf = (org.griphyn.vdl.classes.Leaf) e.next();
            if (leaf instanceof LFN) {
                LFN local = (LFN) leaf;
                if ((link == -1 || local.getLink() == link)
                        && lfn.compareTo(local.getFilename()) == 0) return true;
            }
        }
        return false;
    }
}
