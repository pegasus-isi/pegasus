/**
 *  Copyright 2007-2008 University Of Southern California
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package edu.isi.pegasus.planner.selector;

import org.griphyn.cPlanner.classes.ReplicaLocation;

import edu.isi.pegasus.planner.catalog.replica.ReplicaCatalogEntry;



/**
 * A prototypical interface for a replica selector. It would be changed when
 * Pegasus interfaces with the new RC API.
 *
 * @author Karan Vahi
 * @author Gaurang Mehta
 * @version $Revision$
 */
public interface ReplicaSelector {

    /**
     * The version of this API.
     */
    public static final String VERSION ="1.5";

    /**
     * The local site handle.
     */
    public static final String LOCAL_SITE_HANDLE = "local";

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
     * @param allowLocalFileURLs indicates whether Replica Selector can select a replica
     *                      on the local site / submit host.
     *
     * @return <code>ReplicaLocation</code> corresponding to the replicas selected.
     *
     * @see org.griphyn.cPlanner.classes.ReplicaLocation
     */
    public abstract ReplicaLocation selectReplicas( ReplicaLocation rl,
                                                    String prefferedSite,
                                                    boolean allowLocalFileURLs );

    /**
     * Selects a single replica amongst all the replicas returned by the implementing
     * Replica Mechanism. If more than one replica is found to be matching the
     * preffered site, a random replica is picked up from the matching replicas.
     * Else, in case of no match any replica maybe returned.
     *
     * @param rl         the <code>ReplicaLocation</code> object containing all
     *                   the pfn's associated with that LFN.
     * @param prefferedSite the preffered site for picking up the replicas.
     * @param allowLocalFileURLs indicates whether Replica Selector can select a replica
     *                      on the local site / submit host.
     *
     * @return <code>ReplicaCatalogEntry</code> corresponding to the location selected.
     *
     * @see org.griphyn.cPlanner.classes.ReplicaLocation
     */
    public abstract ReplicaCatalogEntry selectReplica( ReplicaLocation rl,
                                                       String prefferedSite,
                                                       boolean allowLocalFileURLs );

    /**
     * Returns a short description of the replica selector, that is being
     * implemented by the implementing class.
     *
     * @return string corresponding to the description.
     */
    public abstract String description();

}
