/**
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
package org.griphyn.cPlanner.selector;

import org.griphyn.cPlanner.classes.ReplicaLocation;

import org.griphyn.common.catalog.ReplicaCatalogEntry;

import java.util.Vector;


/**
 * A prototypical interface for a replica selector. It would be changed when
 * Pegasus interfaces with the new RC API.
 *
 * @author Karan Vahi
 * @author Gaurang Mehta
 * @version $Revision: 1.2 $
 */
public interface ReplicaSelector {

    /**
     * The version of this API.
     */
    public static final String VERSION ="1.3";

    /**
     * Selects a replica amongst all the replicas returned by the implementing
     * Replica Mechanism. It should select all the locations for which the site
     * attribute matches to the preffered site passed. If no match on the
     * preffered site is found, it is upto the implementation to select a replica
     * and return it.
     *
     * This function is  called to determine if a file does exist  on the output
     * pool or not beforehand. We need all the locations to ensure that we are
     * able to make a match if it so exists.
     *
     * @param rl         the <code>ReplicaLocation</code> object containing all
     *                   the pfn's associated with that LFN.
     * @param prefferedSite the preffered site for picking up the replicas.
     *
     * @return <code>ReplicaLocation</code> corresponding to the replicas selected.
     *
     * @see org.griphyn.cPlanner.classes.ReplicaLocation
     */
    public abstract ReplicaLocation selectReplicas( ReplicaLocation rl,
                                                    String prefferedSite );

    /**
     * Selects a single replica amongst all the replicas returned by the implementing
     * Replica Mechanism. If more than one replica is found to be matching the
     * preffered site, a random replica is picked up from the matching replicas.
     * Else, in case of no match any replica maybe returned.
     *
     * @param rl         the <code>ReplicaLocation</code> object containing all
     *                   the pfn's associated with that LFN.
     * @param prefferedSite the preffered site for picking up the replicas.
     *
     * @return <code>ReplicaCatalogEntry</code> corresponding to the location selected.
     *
     * @see org.griphyn.cPlanner.classes.ReplicaLocation
     */
    public abstract ReplicaCatalogEntry selectReplica( ReplicaLocation rl,
                                                       String prefferedSite );

    /**
     * Returns a short description of the replica selector, that is being
     * implemented by the implementing class.
     *
     * @return string corresponding to the description.
     */
    public abstract String description();

}
