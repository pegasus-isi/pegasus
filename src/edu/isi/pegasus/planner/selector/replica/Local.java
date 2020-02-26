/**
 * Copyright 2007-2008 University Of Southern California
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.isi.pegasus.planner.selector.replica;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.common.logging.LogManagerFactory;
import edu.isi.pegasus.common.util.PegasusURL;
import edu.isi.pegasus.planner.catalog.replica.ReplicaCatalogEntry;
import edu.isi.pegasus.planner.classes.ReplicaLocation;
import edu.isi.pegasus.planner.common.PegRandom;
import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.planner.selector.ReplicaSelector;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * This replica selector only prefers replicas from the local host and that start with a file: URL
 * scheme. It is useful, when you want to stagin files to a remote site from your submit host using
 * the Condor file transfer mechanism.
 *
 * <p>In order to use the replica selector implemented by this class,
 *
 * <pre>
 *        - the property pegasus.selector.replica must be set to value Local
 * </pre>
 *
 * @see org.griphyn.cPlanner.transfer.implementation.Condor
 * @author Karan Vahi
 * @version $Revision$
 */
public class Local implements ReplicaSelector {

    /** A short description of the replica selector. */
    private static final String mDescription = "Local from submit host";

    /** Sanity Check Error Message. */
    public static final String SANITY_CHECK_ERROR_MESSAGE_PREFIX =
            "Local Replica Selector selects only local file URL's. Set transfers to run on submit host.";

    /** The handle to the logging object that is used to log the various debug messages. */
    protected LogManager mLogger;

    /** The properties object containing the properties passed to the planner. */
    protected PegasusProperties mProps;

    /**
     * The overloaded constructor, that is called by load method.
     *
     * @param properties the <code>PegasusProperties</code> object containing all the properties
     *     required by Pegasus.
     */
    public Local(PegasusProperties properties) {
        mProps = properties;
        mLogger = LogManagerFactory.loadSingletonInstance(properties);
    }

    /**
     * Selects a random replica from all the replica's that have their site handle set to local and
     * the pfn's start with a file url scheme.
     *
     * @param rl the <code>ReplicaLocation</code> object containing all the pfn's associated with
     *     that LFN.
     * @param preferredSite the preffered site for picking up the replicas.
     * @param allowLocalFileURLs indicates whether Replica Selector can select a replica on the
     *     local site / submit host.
     * @return <code>ReplicaCatalogEntry</code> corresponding to the location selected.
     * @see org.griphyn.cPlanner.classes.ReplicaLocation
     */
    public ReplicaCatalogEntry selectReplica(
            ReplicaLocation rl, String preferredSite, boolean allowLocalFileURLs) {

        // sanity check
        if (!allowLocalFileURLs && !preferredSite.equals(ReplicaSelector.LOCAL_SITE_HANDLE)) {
            StringBuffer message = new StringBuffer();
            message.append(SANITY_CHECK_ERROR_MESSAGE_PREFIX)
                    .append("For LFN ")
                    .append(rl.getLFN())
                    .append(" (preferred site , allow local urls) is set to ")
                    .append("(")
                    .append(preferredSite)
                    .append(",")
                    .append(allowLocalFileURLs)
                    .append(")");
            throw new RuntimeException(message.toString());
        }

        ReplicaCatalogEntry rce;
        ArrayList prefPFNs = new ArrayList();
        int locSelected;
        String site = null;

        //        mLogger.log("Selecting a pfn for lfn " + lfn + "\n amongst" + locations ,
        //                    LogManager.DEBUG_MESSAGE_LEVEL);

        for (Iterator it = rl.pfnIterator(); it.hasNext(); ) {
            rce = (ReplicaCatalogEntry) it.next();
            site = rce.getResourceHandle();

            if (site == null) {
                // skip to next replica
                continue;
            }

            // check if has pool attribute as local, and at same time
            // start with a file url scheme
            if (site.equals("local") && rce.getPFN().startsWith(PegasusURL.FILE_URL_SCHEME)) {
                prefPFNs.add(rce);
            }
        }

        if (prefPFNs.isEmpty()) {
            // select a random location from
            // all the matching locations
            // in all likelihood all the urls were file urls and none
            // were associated with the preference pool.
            throw new RuntimeException(
                    "Unable to select any location on local site from "
                            + "the list passed for lfn "
                            + rl.getLFN());

        } else {
            // select a random location
            // amongst the locations
            // on the preference pool
            int length = prefPFNs.size();
            // System.out.println("No of locations found at pool " + prefPool + " are " + length);
            locSelected = PegRandom.getInteger(length - 1);
            rce = (ReplicaCatalogEntry) prefPFNs.get(locSelected);
        }

        return rce;
    }

    /**
     * This chooses a location amongst all the locations returned by the Replica Mechanism. If a
     * location is found with re/pool attribute same as the preference pool, it is taken. This
     * returns all the locations which match to the preference pool. This function is called to
     * determine if a file does exist on the output pool or not beforehand. We need all the location
     * to ensure that we are able to make a match if it so exists. Else a random location is
     * selected and returned
     *
     * @param rl the <code>ReplicaLocation</code> object containing all the pfn's associated with
     *     that LFN.
     * @param preferredSite the preffered site for picking up the replicas.
     * @param allowLocalFileURLs indicates whether Replica Selector can select a replica on the
     *     local site / submit host.
     * @return <code>ReplicaLocation</code> corresponding to the replicas selected.
     * @see org.griphyn.cPlanner.classes.ReplicaLocation
     */
    public ReplicaLocation selectAndOrderReplicas(
            ReplicaLocation rl, String preferredSite, boolean allowLocalFileURLs) {

        // sanity check
        if (!allowLocalFileURLs && !preferredSite.equals(ReplicaSelector.LOCAL_SITE_HANDLE)) {
            StringBuffer message = new StringBuffer();
            message.append(SANITY_CHECK_ERROR_MESSAGE_PREFIX)
                    .append("For LFN ")
                    .append(rl.getLFN())
                    .append(" (preferred site , allow local urls) is set to ")
                    .append("(")
                    .append(preferredSite)
                    .append(",")
                    .append(allowLocalFileURLs)
                    .append(")");
            throw new RuntimeException(message.toString());
        }

        String lfn = rl.getLFN();
        ReplicaLocation result = new ReplicaLocation();
        result.setLFN(rl.getLFN());

        ReplicaCatalogEntry rce;
        String site;
        int noOfLocs = 0;

        for (Iterator it = rl.pfnIterator(); it.hasNext(); ) {
            noOfLocs++;
            rce = (ReplicaCatalogEntry) it.next();
            site = rce.getResourceHandle();

            if (site != null && site.equals(preferredSite)) {
                result.addPFN(rce);
            } else if (site == null) {
                mLogger.log(
                        " pool attribute not specified for the location objects"
                                + " in the Replica Catalog",
                        LogManager.WARNING_MESSAGE_LEVEL);
            }
        }

        if (result.getPFNCount() == 0) {
            // means we have to choose a random location between 0 and (noOfLocs -1)
            int locSelected = PegRandom.getInteger(noOfLocs - 1);
            rce = (ReplicaCatalogEntry) rl.getPFN(locSelected);
            result.addPFN(rce);
        }
        return result;
    }

    /**
     * Returns a short description of the replica selector.
     *
     * @return string corresponding to the description.
     */
    public String description() {
        return mDescription;
    }
}
