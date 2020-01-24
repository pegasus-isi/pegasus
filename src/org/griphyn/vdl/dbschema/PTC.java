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

import edu.isi.pegasus.planner.invocation.InvocationRecord;
import java.net.InetAddress;
import java.sql.SQLException;
import java.util.Date;

/**
 * This interface defines a common base for all database schemas that supports the handling of the
 * provenance tracking catalog. It exists primarily for grouping purposes.
 *
 * <p>For the moment, we are happy to be able to store things inside. The rest, in form of more
 * required methods, comes later.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 */
public interface PTC extends Catalog {
    /** Names the property key prefix employed for schemas dealing with the PTC. */
    public static final String PROPERTY_PREFIX = "pegasus.catalog.provenance.db.schema";

    /**
     * Checks the existence of an invocation record in the database. The information is based on the
     * (start,host,pid) tuple, although with private networks, cases may arise that have this tuple
     * identical, yet are different.
     *
     * @param start is the start time of the grid launcher
     * @param host is the address of the host it ran upon
     * @param pid is the process id of the grid launcher itself.
     * @return the id of the existing record, or -1
     */
    public long getInvocationID(Date start, InetAddress host, int pid) throws SQLException;

    /**
     * Inserts an invocation record into the database.
     *
     * @param ivr is the invocation record to store.
     * @return true, if insertion was successful, false otherwise.
     */
    public boolean saveInvocation(InvocationRecord ivr) throws SQLException;
}
