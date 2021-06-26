/**
 * Copyright 2007-2021 University Of Southern California
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
package edu.isi.pegasus.planner.transfer;

import edu.isi.pegasus.common.util.PegasusURL;
import edu.isi.pegasus.planner.catalog.site.classes.SiteCatalogEntry;

/**
 * A class that determines whether a transfer job for a File Transfer executes locally or not
 *
 * @author Karan Vahi
 */
public class JobPlacer {

    /** The Transfer Refiner being used. */
    private final Refiner mTXRefiner;

    /**
     * The default constructor
     *
     * @param refiner the Transfer Refiner
     */
    public JobPlacer(Refiner refiner) {
        if (refiner == null) {
            throw new NullPointerException("Transfer Refiner passed to the JobPlacer is null");
        }
        mTXRefiner = refiner;
    }

    /**
     * Returns whether to run a transfer job on local destinationSite or not.
     *
     * @param destinationSite the destinationSite entry associated with the destination URL.
     * @param destinationURL the destination URL
     * @param type the type of transfer job for which the URL is being constructed.
     * @return true indicating if the associated transfer job should run on local destinationSite or
     *     not.
     */
    public boolean runTransferOnLocalSite(
            SiteCatalogEntry destinationSite, String destinationURL, int type) {
        // check if user has specified any preference in config
        boolean result = true;
        String siteHandle = destinationSite.getSiteHandle();

        // short cut for local destinationSite
        if (siteHandle.equals("local")) {
            // transfer to run on local destinationSite
            return result;
        }

        // PM-1024 check if the filesystem on destinationSite visible to the local destinationSite
        if (destinationSite.isVisibleToLocalSite()) {
            return true;
        }

        if (mTXRefiner.refinerPreferenceForTransferJobLocation()) {
            // refiner is advertising a preference for where transfer job
            // should be run. Use that.
            return mTXRefiner.refinerPreferenceForLocalTransferJobs(type);
        }

        if (mTXRefiner.runTransferRemotely(siteHandle, type)) {
            // always use user preference
            return !result;
        }
        // check to see if destination URL is a file url
        else if (destinationURL != null && destinationURL.startsWith(PegasusURL.FILE_URL_SCHEME)) {
            result = false;
        }

        return result;
    }
}
