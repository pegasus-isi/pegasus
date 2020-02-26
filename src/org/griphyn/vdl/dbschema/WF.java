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
import org.griphyn.vdl.workflow.*;

/**
 * This common schema interface defines the schemas in which the abstraction layers access the WF
 * database. This layer is independent of the implementing database, and does so by going via the
 * database driver class API.
 *
 * @author Jens-S. Vöckler
 * @author Mike Wilde
 * @version $Revision$
 * @see org.griphyn.vdl.dbschema.DatabaseSchema
 * @see org.griphyn.vdl.dbdriver
 */
public interface WF extends Catalog {
    /** Names the property key prefix employed for schemas dealing with the VDC. */
    public static final String PROPERTY_PREFIX = "vds.db.wf.schema";

    /**
     * Determines the primary key of a workflow from the provided tuple of secondary keys, and
     * instantiates the corresponding workflow.
     *
     * @param basedir is the base directory
     * @param vogroup is the VO group identifier
     * @param label is the workflow label
     * @param run is the workflow run directory
     * @return the workflow identifier, or -1 if not found.
     */
    public abstract WorkEntry getWorkflow(String basedir, String vogroup, String label, String run)
            throws SQLException;

    /**
     * Returns a set of all workflows that are younger than a cut-off time. The result may be empty,
     * if no matching workflows exist yet.
     *
     * @param mtime is the oldest permissable last modification time
     * @return a map of workflows, indexed by their primary key
     */
    public abstract java.util.Map getWorkflows(java.util.Date mtime) throws SQLException;
}
