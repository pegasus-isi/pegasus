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
import edu.isi.pegasus.planner.catalog.replica.ReplicaCatalogEntry;
import edu.isi.pegasus.planner.catalog.site.classes.SiteCatalogEntry;
import edu.isi.pegasus.planner.classes.FileTransfer;
import edu.isi.pegasus.planner.classes.NameValue;

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
     * Returns whether to run a transfer job on local site or on the staging site.
     *
     * @param stagingSite the stagingSite entry associated with the destination URL.
     * @param stagingSiteURL the destination URL
     * @param type the type of transfer job for which the URL is being constructed.
     * @return true indicating if the associated transfer job should run on local stagingSite or
     *     not.
     */
    public boolean runTransferOnLocalSite(
            SiteCatalogEntry stagingSite, String stagingSiteURL, int type) {
        // check if user has specified any preference in config
        boolean result = true;
        String siteHandle = stagingSite.getSiteHandle();

        // short cut for local stagingSite
        if (siteHandle.equals("local")) {
            // transfer to run on local stagingSite
            return result;
        }

        // PM-1024 check if the filesystem on stagingSite visible to the local stagingSite
        if (stagingSite.isVisibleToLocalSite()) {
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
        // check to see if staging site URL is a file url
        else if (stagingSiteURL != null && stagingSiteURL.startsWith(PegasusURL.FILE_URL_SCHEME)) {
            result = false;
        }

        return result;
    }

    /**
     * Returns a boolean indicating whether a particular file transfer should be placed to run
     * remotely.
     *
     * @param ft the file transfer
     * @param stagingSite the staging site entry associated
     * @param ftForContainerToSubmitHost boolean indicating whether this transfer is transferring
     *     container to submit host
     * @param forSymlink boolean indicating if this tx is flagged for symlinking
     * @param isLocalTransfer boolean indicating initial determination if tx can be run locally
     * @return
     */
    public boolean runTransferRemotely(
            FileTransfer ft,
            SiteCatalogEntry stagingSite,
            boolean ftForContainerToSubmitHost,
            boolean forSymlink,
            boolean isLocalTransfer) {

        return this.runTransferRemotely(
                ftForContainerToSubmitHost,
                forSymlink,
                isLocalTransfer,
                this.runTransferRemotely(stagingSite, ft));
    }

    /**
     * Returns a boolean indicating whether a particular file transfer should be placed to run
     * remotely.
     *
     * @param ftForContainerToSubmitHost boolean indicating whether this transfer is transferring
     *     container to submit host
     * @param forSymlink boolean indicating if this tx is flagged for symlinking
     * @param isLocalTransfer boolean indicating initial determination if tx can be run locally
     * @param isRemoteTransfer boolen indicating whether the tx can be run remotely
     * @return
     */
    public boolean runTransferRemotely(
            boolean ftForContainerToSubmitHost,
            boolean forSymlink,
            boolean isLocalTransfer,
            boolean isRemoteTransfer) {

        if (ftForContainerToSubmitHost) {
            // PM-1950  if this particluar file tx is for transferring
            // the container for the job to the submit host directory, then always run
            // locally
            // runTransferRemotely can return true ft for transferring container to submit
            // host; if user has turned symlinking
            return false;
        }

        return (forSymlink // symlinks can run only on staging site
                || !isLocalTransfer // already determined when checking for local transfer, that it
                // should run remotely
                || isRemoteTransfer); // check on the basis of constructed source URL whether to
        // run remotely
    }

    /**
     * Determines a particular created transfer pair has to be binned for remote transfer or local.
     *
     * @param ft the file transfer created
     * @param stagingSite the staging site for the job
     * @return
     */
    public boolean runTransferRemotely(SiteCatalogEntry stagingSite, FileTransfer ft) {
        boolean remote = false;

        NameValue<String, String> destTX = ft.getDestURL();
        for (String sourceSite : ft.getSourceSites()) {
            // traverse through all the URL's on that site
            for (ReplicaCatalogEntry rce : ft.getSourceURLs(sourceSite)) {
                String sourceURL = rce.getPFN();
                // if the source URL is a FILE URL and
                // source site matches the destination site
                // then has to run remotely
                if (sourceURL != null && sourceURL.startsWith(PegasusURL.FILE_URL_SCHEME)) {
                    // sanity check to make sure source site
                    // matches destination site
                    if (sourceSite.equalsIgnoreCase(destTX.getKey())) {

                        if (sourceSite.equalsIgnoreCase(stagingSite.getSiteHandle())
                                && stagingSite.isVisibleToLocalSite()) {
                            // PM-1024 if the source also matches the job staging site
                            // then we do an extra check if the staging site is the same
                            // as the sourceSite, then we consider the auxillary.local attribute
                            // for the staging site
                            remote = false;
                        } else {
                            remote = true;
                            break;
                        }
                    } else if (sourceSite.equals("local")) {
                        remote = false;
                    }
                }
            }
        }
        return remote;
    }
}
