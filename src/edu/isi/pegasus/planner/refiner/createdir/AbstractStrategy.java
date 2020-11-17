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
package edu.isi.pegasus.planner.refiner.createdir;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.catalog.site.classes.SiteStore;
import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.TransferJob;
import edu.isi.pegasus.planner.partitioner.graph.GraphNode;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * The interface that defines how the cleanup job is invoked and created.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public abstract class AbstractStrategy implements Strategy {

    /** Constant suffix for the names of the create directory nodes. */
    public static final String CREATE_DIR_SUFFIX = "_cdir";

    /** Constant prefix for the names of the create directory nodes. */
    public static final String CREATE_DIR_PREFIX = "create_dir_";

    /** The handle to the logging object, that is used to log the messages. */
    protected LogManager mLogger;

    /** The job prefix that needs to be applied to the job file basenames. */
    protected String mJobPrefix;

    /** Whether we want to use dirmanager or mkdir directly. */
    protected boolean mUseMkdir;

    /** The implementation instance that is used to create a create dir job. */
    protected Implementation mImpl;

    /** The Site Store handle. */
    protected SiteStore mSiteStore;

    /**
     * Intializes the class.
     *
     * @param bag bag of initialization objects
     * @param impl the implementation instance that creates create dir job
     */
    public void initialize(PegasusBag bag, Implementation impl) {
        mImpl = impl;
        mJobPrefix = bag.getPlannerOptions().getJobnamePrefix();
        mLogger = bag.getLogger();
        mSiteStore = bag.getHandleToSiteStore();

        // in case of staging of executables/worker package
        // we use mkdir directly
        mUseMkdir = bag.getPegasusProperties().transferWorkerPackage();
    }

    /**
     * It returns the name of the create directory job, that is to be assigned. The name takes into
     * account the workflow name while constructing it, as that is thing that can guarentee
     * uniqueness of name in case of deferred planning.
     *
     * @param dag the workflow to which the create dir jobs are being added.
     * @param pool the execution pool for which the create directory job is responsible.
     * @return String corresponding to the name of the job.
     */
    public String getCreateDirJobName(ADag dag, String pool) {

        // sanity check
        if (pool == null) {
            return null;
        }

        StringBuffer sb = new StringBuffer();

        sb.append(AbstractStrategy.CREATE_DIR_PREFIX);

        // append the job prefix if specified in options at runtime
        if (mJobPrefix != null) {
            sb.append(mJobPrefix);
        }

        sb.append(dag.getLabel()).append("_").append(dag.getIndex()).append("_");

        // sb.append(pool).append(this.CREATE_DIR_SUFFIX);
        sb.append(pool);

        return sb.toString();
    }

    /**
     * Retrieves the sites for which the create dir jobs need to be created. It returns all the
     * sites where the compute jobs have been scheduled.
     *
     * @return a Set containing a list of siteID's of the sites where the dag has to be run.
     */
    public static Set getCreateDirSites(ADag dag) {
        Set set = new HashSet();

        for (Iterator<GraphNode> it = dag.jobIterator(); it.hasNext(); ) {
            GraphNode node = it.next();
            Job job = (Job) node.getContent();

            if (job.getJobType() == Job.CHMOD_JOB) {
                // PM-845 staging site is always set to the associated
                // compute jobs execution site
                set.add(job.getStagingSiteHandle());
                // they are only created in the shared fs mode.
                continue;
            }

            // add to the set only if the job is
            // being run in the work directory
            // this takes care of local site create dir
            if (job.runInWorkDirectory()) {

                String site = job.getStagingSiteHandle();
                // sanity check for remote transfer jobs
                if (job instanceof TransferJob) {
                    site = ((TransferJob) job).getNonThirdPartySite();
                }

                // System.out.println( "Job staging site handle " + job.getID() + " " + site );
                set.add(site);
            }
        }

        // remove the stork pool
        set.remove("stork");

        return set;
    }
}
