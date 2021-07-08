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
package edu.isi.pegasus.planner.transfer.generator;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.catalog.site.classes.FileServer;
import edu.isi.pegasus.planner.catalog.site.classes.SiteCatalogEntry;
import edu.isi.pegasus.planner.catalog.site.classes.SiteStore;
import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.PlannerOptions;
import edu.isi.pegasus.planner.common.PegasusConfiguration;
import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.planner.mapper.StagingMapper;
import edu.isi.pegasus.planner.transfer.JobPlacer;
import edu.isi.pegasus.planner.transfer.Refiner;
import java.io.File;

/**
 * An Abstract Abstract class Class
 *
 * @author vahi
 */
public abstract class Abstract {

    /** The object holding all the properties pertaining to Pegasus. */
    protected PegasusProperties mProps;

    /** The handle to the parsed Site Catalog */
    protected SiteStore mSiteStore;

    /** Contains the message which is to be logged by Pegasus. */
    protected String mLogMsg = "";

    /**
     * Defines the read mode for transformation catalog. Whether we want to read all at once or as
     * desired.
     *
     * @see org.griphyn.common.catalog.transformation.TCMode
     */
    protected String mTCMode;

    /** The logging object which is used to log all the messages. */
    protected LogManager mLogger;

    /** Contains the various options to the Planner as passed by the user at runtime. */
    protected PlannerOptions mPOptions;

    /** The bag of initialization objects */
    protected PegasusBag mBag;

    /** The dial for integrity checking */
    protected PegasusProperties.INTEGRITY_DIAL mIntegrityDial;

    /** Whether to do any integrity checking or not. */
    protected boolean mDoIntegrityChecking;

    /** A boolean to track whether condor file io is used for the workflow or not. */
    // private final boolean mSetupForCondorIO;
    protected PegasusConfiguration mPegasusConfiguration;

    /**
     * Handle to an Staging Mapper that tells where to place the files on the shared scratch space
     * on the staging site.
     */
    protected StagingMapper mStagingMapper;

    /** The handle to the transfer refiner that adds the transfer nodes into the workflow. */
    protected Refiner mTXRefiner;

    /**
     * Handle to the placer that determines whether a file transfer needs to be handled locally or
     * remotely on the staging site.
     */
    protected JobPlacer mTransferJobPlacer;

    public Abstract() {}

    /**
     * Initializes the File Generator for transfers.
     *
     * @param dag the workflow so far.
     * @param bag bag of initialization objects
     * @param transferRefiner the transfer refiner being used
     */
    protected void initalize(ADag dag, PegasusBag bag, Refiner transferRefiner) {
        mBag = bag;
        mLogger = bag.getLogger();
        mProps = bag.getPegasusProperties();
        mPOptions = bag.getPlannerOptions();
        mSiteStore = bag.getHandleToSiteStore();
        mPegasusConfiguration = new PegasusConfiguration(bag.getLogger());
        mStagingMapper = bag.getStagingMapper();

        // PM-1375 we check if we need to do any integriy checking or not
        mIntegrityDial = mProps.getIntegrityDial();
        mDoIntegrityChecking = mProps.doIntegrityChecking();
        mTXRefiner = transferRefiner;
        mTransferJobPlacer = new JobPlacer(transferRefiner);
    }

    /**
     * Returns a URL on the shared scratch of the staging site
     *
     * @param entry the SiteCatalogEntry for the associated staging site
     * @param job the job
     * @param operation the FileServer operation for which we need the URL
     * @param lfn the LFN can be null to get the path to the directory
     * @return the URL
     */
    protected String getURLOnSharedScratch(
            SiteCatalogEntry entry,
            Job job,
            FileServer.OPERATION operation,
            File addOn,
            String lfn) {
        return mStagingMapper.map(job, addOn, entry, operation, lfn);
    }

    /**
     * This generates a error message for site not found in the site catalog file.
     *
     * @param siteName the name of site that is not found.
     * @param universe the condor universe
     * @return the message.
     */
    protected String siteNotFoundMsg(String siteName, String universe) {
        String st =
                "Error: No matching entry to site = "
                        + siteName
                        + " ,universe = "
                        + universe
                        + "\n found in the Site Catalog ";
        return st;
    }

    /**
     * Complains for head node url prefix not specified
     *
     * @param refiner the name of the refiner
     * @param site the site handle
     * @param operation
     * @throws RuntimeException when URL Prefix cannot be determined for various reason.
     */
    protected void complainForHeadNodeURLPrefix(
            String refiner, String site, FileServer.OPERATION operation) {
        this.complainForHeadNodeURLPrefix(refiner, site, operation, null);
    }

    /**
     * Complains for head node url prefix not specified
     *
     * @param refiner the name of the refiner
     * @param operation the operation for which error is throw
     * @param job the related job if any
     * @param site the site handle
     * @throws RuntimeException when URL Prefix cannot be determined for various reason.
     */
    protected void complainForHeadNodeURLPrefix(
            String refiner, String site, FileServer.OPERATION operation, Job job) {
        StringBuffer error = new StringBuffer();
        error.append("[").append(refiner).append("] ");
        if (job != null) {
            error.append("For job (").append(job.getID()).append(").");
        }
        error.append("Unable to determine URL Prefix for the FileServer ")
                .append(" for operation ")
                .append(operation)
                .append(" for shared scratch file system on site: ")
                .append(site);
        throw new RuntimeException(error.toString());
    }
}
