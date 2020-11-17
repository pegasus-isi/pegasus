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
package edu.isi.pegasus.planner.refiner.cleanup;

import edu.isi.pegasus.planner.catalog.TransformationCatalog;
import edu.isi.pegasus.planner.catalog.site.classes.SiteStore;
import edu.isi.pegasus.planner.catalog.transformation.TransformationCatalogEntry;
import edu.isi.pegasus.planner.catalog.transformation.classes.TCType;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.PegasusFile;
import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.planner.namespace.Condor;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

/**
 * Use's RM to do removal of the files on the remote sites.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class RM implements CleanupImplementation {

    /** The default logical name to rm executable. */
    public static final String DEFAULT_RM_LOGICAL_NAME = "rm";

    /** The default path to rm executable. */
    public static final String DEFAULT_RM_LOCATION = "/bin/rm";

    /** The default priority key associated with the cleanup jobs. */
    public static final String DEFAULT_PRIORITY_KEY = "1000";

    /** The handle to the transformation catalog. */
    protected TransformationCatalog mTCHandle;

    /** Handle to the site catalog. */
    //    protected PoolInfoProvider mSiteHandle;
    protected SiteStore mSiteStore;

    /** The handle to the properties passed to Pegasus. */
    private PegasusProperties mProps;

    /** The default constructor. */
    public RM() {}

    /**
     * Intializes the class.
     *
     * @param bag bag of initialization objects
     */
    public void initialize(PegasusBag bag) {
        mSiteStore = bag.getHandleToSiteStore();
        mTCHandle = bag.getHandleToTransformationCatalog();
        mProps = bag.getPegasusProperties();
    }

    /**
     * Creates a cleanup job that removes the files from remote working directory. This will
     * eventually make way to it's own interface.
     *
     * @param id the identifier to be assigned to the job.
     * @param files the list of <code>PegasusFile</code> that need to be cleaned up.
     * @param job the primary compute job with which this cleanup job is associated.
     * @return the cleanup job.
     */
    public Job createCleanupJob(String id, List files, Job job) {

        // we want to run the clnjob in the same directory
        // as the compute job. We cannot clone as then the
        // the cleanup jobs for clustered jobs appears as
        // a clustered job. PM-368
        Job cJob = new Job(job);

        // we dont want notifications to be inherited
        cJob.resetNotifications();

        cJob.setJobType(Job.CLEANUP_JOB);
        cJob.setName(id);
        cJob.setSiteHandle(job.getStagingSiteHandle());

        // bug fix for JIRA PM-311
        // we dont want cleanup job to inherit any stdout or stderr
        // specified in the DAX for compute job
        cJob.setStdOut("");
        cJob.setStdErr("");

        // inconsistency between job name and logical name for now
        cJob.setTXVersion(null);
        cJob.setTXName("rm");
        cJob.setTXNamespace(null);
        cJob.setLogicalID(id);

        // the compute job of the VDS supernode is this job itself
        cJob.setVDSSuperNode(job.getID());

        // set the list of files as input files
        // to change function signature to reflect a set only.
        cJob.setInputFiles(new HashSet(files));

        // set the path to the rm executable
        TransformationCatalogEntry entry = this.getTCEntry(job.getSiteHandle());
        cJob.setRemoteExecutable(entry.getPhysicalTransformation());

        // set the arguments for the cleanup job
        StringBuffer arguments = new StringBuffer();
        for (Iterator it = files.iterator(); it.hasNext(); ) {
            PegasusFile file = (PegasusFile) it.next();
            arguments.append(" ").append(file.getLFN());
        }
        cJob.setArguments(arguments.toString());

        // the cleanup job is a clone of compute
        // need to reset the profiles first
        cJob.resetProfiles();

        // the profile information from the pool catalog needs to be
        // assimilated into the job.
        cJob.updateProfiles(mSiteStore.lookup(job.getSiteHandle()).getProfiles());

        // add any notifications specified in the transformation
        // catalog for the job. JIRA PM-391
        cJob.addNotifications(entry);

        // the profile information from the transformation
        // catalog needs to be assimilated into the job
        // overriding the one from pool catalog.
        cJob.updateProfiles(entry);

        // the profile information from the properties file
        // is assimilated overidding the one from transformation
        // catalog.
        cJob.updateProfiles(mProps);

        // let us put some priority for the cleaunup jobs
        cJob.condorVariables.construct(Condor.PRIORITY_KEY, DEFAULT_PRIORITY_KEY);

        return cJob;
    }

    /**
     * Returns the TCEntry object for the rm executable on a grid site.
     *
     * @param site the site corresponding to which the entry is required.
     * @return the TransformationCatalogEntry corresponding to the site.
     */
    protected TransformationCatalogEntry getTCEntry(String site) {
        List tcentries = null;
        TransformationCatalogEntry entry = null;
        try {
            tcentries =
                    mTCHandle.lookup(null, DEFAULT_RM_LOGICAL_NAME, null, site, TCType.INSTALLED);
        } catch (Exception e) {
            /* empty catch */
        }

        // see if any record is returned or not
        entry =
                (tcentries == null)
                        ? defaultTCEntry()
                        : (TransformationCatalogEntry) tcentries.get(0);

        return entry;
    }

    /**
     * Returns a default TransformationCatalogEntry object for the rm executable.
     *
     * @return default <code>TransformationCatalogEntry</code>
     */
    private static TransformationCatalogEntry defaultTCEntry() {
        TransformationCatalogEntry entry =
                new TransformationCatalogEntry(null, DEFAULT_RM_LOGICAL_NAME, null);
        entry.setPhysicalTransformation(DEFAULT_RM_LOCATION);

        return entry;
    }
}
