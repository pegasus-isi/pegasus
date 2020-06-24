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
package edu.isi.pegasus.planner.refiner;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.common.logging.LoggingKeys;
import edu.isi.pegasus.common.util.Separator;
import edu.isi.pegasus.planner.catalog.site.classes.SiteCatalogEntry;
import edu.isi.pegasus.planner.catalog.transformation.Mapper;
import edu.isi.pegasus.planner.catalog.transformation.TransformationCatalogEntry;
import edu.isi.pegasus.planner.catalog.transformation.classes.Container;
import edu.isi.pegasus.planner.catalog.transformation.classes.TCType;
import edu.isi.pegasus.planner.catalog.transformation.classes.TransformationStore;
import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.DataFlowJob;
import edu.isi.pegasus.planner.classes.FileTransfer;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.PegasusFile;
import edu.isi.pegasus.planner.cluster.JobAggregator;
import edu.isi.pegasus.planner.cluster.aggregator.Decaf;
import edu.isi.pegasus.planner.code.CodeGeneratorFactory;
import edu.isi.pegasus.planner.code.generator.Stampede;
import edu.isi.pegasus.planner.common.PegRandom;
import edu.isi.pegasus.planner.common.PegasusConfiguration;
import edu.isi.pegasus.planner.estimate.Estimator;
import edu.isi.pegasus.planner.estimate.EstimatorFactory;
import edu.isi.pegasus.planner.namespace.Globus;
import edu.isi.pegasus.planner.namespace.Hints;
import edu.isi.pegasus.planner.namespace.Pegasus;
import edu.isi.pegasus.planner.partitioner.graph.GraphNode;
import edu.isi.pegasus.planner.selector.SiteSelector;
import edu.isi.pegasus.planner.selector.TransformationSelector;
import edu.isi.pegasus.planner.selector.site.SiteSelectorFactory;
import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

/**
 * This engine calls out to the Site Selector selected by the user and maps the jobs in the workflow
 * to the execution pools.
 *
 * @author Karan Vahi
 * @author Gaurang Mehta
 * @version $Revision$
 */
public class InterPoolEngine extends Engine implements Refiner {

    /** The name of the refiner for purposes of error logging */
    public static final String REFINER_NAME = "InterPoolEngine";

    /** ADag object corresponding to the Dag whose jobs we want to schedule. */
    private ADag mDag;

    /** Set of the execution pools which the user has specified. */
    private Set mExecPools;

    /** Handle to the site selector. */
    private SiteSelector mSiteSelector;

    /**
     * The handle to the transformation selector, that ends up selecting what transformations to
     * pick up.
     */
    private TransformationSelector mTXSelector;

    /**
     * The handle to the transformation catalog mapper object that caches the queries to the
     * transformation catalog, and indexes them according to lfn's. There is no purge policy in the
     * TC Mapper, so per se it is not a classic cache.
     */
    private Mapper mTCMapper;

    /** handle to PegasusConfiguration */
    private PegasusConfiguration mPegasusConfiguration;

    /**
     * Handle to the transformation store that stores the transformation catalog user specifies in
     * the DAX
     */
    protected TransformationStore mDAXTransformationStore;

    /** Handle to the estimator. */
    private Estimator mEstimator;

    /**
     * Default constructor.
     *
     * @param bag the bag of initialization objects.
     */
    public InterPoolEngine(PegasusBag bag) {
        super(bag);
        mDag = new ADag();
        mExecPools = new java.util.HashSet();

        // initialize the transformation mapper
        mTCMapper = Mapper.loadTCMapper(mProps.getTCMapperMode(), mBag);
        mBag.add(PegasusBag.TRANSFORMATION_MAPPER, mTCMapper);
        mTXSelector = null;
        mPegasusConfiguration = new PegasusConfiguration(bag.getLogger());
    }

    /**
     * Overloaded constructor.
     *
     * @param dag the <code>ADag</code> object corresponding to the Dag for which we want to
     *     determine on which pools to run the nodes of the Dag.
     * @param bag the bag of initialization objects
     */
    public InterPoolEngine(ADag dag, PegasusBag bag) {
        this(bag);
        mDag = dag;
        mExecPools = (Set) mPOptions.getExecutionSites();
        mLogger.log("List of executions sites is " + mExecPools, LogManager.DEBUG_MESSAGE_LEVEL);

        this.mDAXTransformationStore = dag.getTransformationStore();
        this.mEstimator = EstimatorFactory.loadEstimator(dag, bag);
    }

    /**
     * Returns the bag of intialization objects.
     *
     * @return PegasusBag
     */
    public PegasusBag getPegasusBag() {
        return mBag;
    }

    /**
     * Returns a reference to the workflow that is being refined by the refiner.
     *
     * @return ADAG object.
     */
    public ADag getWorkflow() {
        return this.mDag;
    }

    /**
     * This is where the callout to the Partitioner should take place, that partitions the workflow
     * into clusters and sends to the site selector only those list of jobs that are ready to be
     * scheduled.
     */
    public void determineSites() {
        // at present we schedule the whole workflow at once
        List pools = convertToList(mExecPools);

        // going through all the jobs making up the Adag, to do the physical mapping
        scheduleJobs(mDag, pools);
    }

    /**
     * It schedules a list of jobs on the execution pools by calling out to the site selector
     * specified. It is upto to the site selector to determine if the job can be run on the list of
     * sites passed.
     *
     * @param dag the abstract workflow.
     * @param sites the list of execution sites, specified by the user.
     */
    public void scheduleJobs(ADag dag, List sites) {

        // we iterate through the DAX Transformation Store and update
        // the transformation catalog with any transformation specified.
        for (TransformationCatalogEntry entry : this.mDAXTransformationStore.getAllEntries()) {
            try {
                // insert an entry into the transformation catalog
                // for the mapper to pick up later on
                mLogger.log(
                        "Addding entry into transformation catalog " + entry,
                        LogManager.DEBUG_MESSAGE_LEVEL);

                if (mTCHandle.insert(entry, false) != 1) {
                    mLogger.log(
                            "Unable to add entry to transformation catalog " + entry,
                            LogManager.WARNING_MESSAGE_LEVEL);
                }
            } catch (Exception ex) {
                throw new RuntimeException(
                        "Exception while inserting into TC in Interpool Engine " + ex);
            }
        }

        mSiteSelector = SiteSelectorFactory.loadInstance(mBag);
        mSiteSelector.mapWorkflow(dag, sites);

        int i = 0;

        // Iterate through the jobs and hand them to
        // the site selector if required
        for (Iterator<GraphNode> it = dag.jobIterator(); it.hasNext(); i++) {
            GraphNode node = it.next();
            Job job = (Job) node.getContent();

            if (job instanceof DataFlowJob) {
                // associate the job with decaf job aggregator
                // hardcoded for time being
                DataFlowJob dflow = (DataFlowJob) job;
                JobAggregator decaf = new Decaf();
                decaf.initialize(dag, mBag);
                dflow.setJobAggregator(decaf);

                // PM-1205 datalfows are clustered jobs
                // we map the constitutent jobs not the datalfow job itself.
                for (Iterator consIT = dflow.nodeIterator(); consIT.hasNext(); ) {
                    GraphNode n = (GraphNode) consIT.next();
                    Job j = (Job) n.getContent();
                    incorporateSiteMapping(j, sites);
                }
            }
            incorporateSiteMapping(job, sites);
        } // end of mapping all jobs

        // PM-916 write out all the metadata related events for the
        // mapped workflow
        generateStampedeMetadataEvents(dag);
    }

    /**
     * Incorporates hints and checks to ensure a job has been mapped correctly.
     *
     * @param job
     * @param sites
     */
    protected void incorporateSiteMapping(Job job, List<String> sites) {
        StringBuilder error = null;
        // check if the user has specified any hints in the dax
        incorporateHint(job, Hints.EXECUTION_SITE_KEY);

        String site = job.getSiteHandle();
        mLogger.log(
                "Setting up site mapping for job " + job.getName(), LogManager.DEBUG_MESSAGE_LEVEL);

        if (site == null) {
            complainForFailedSiteMapping(job, sites);
            mLogger.log(error.toString(), LogManager.ERROR_MESSAGE_LEVEL);
            throw new RuntimeException(error.toString());
        }

        if (site.length() == 0 || site.equalsIgnoreCase(SiteSelector.SITE_NOT_FOUND)) {
            error = new StringBuilder();
            error.append("Site Selector (")
                    .append(mSiteSelector.description())
                    .append(") could not map job ")
                    .append(job.getCompleteTCName())
                    .append(" with id ")
                    .append(job.getID())
                    .append(" to any site");
            mLogger.log(error.toString(), LogManager.ERROR_MESSAGE_LEVEL);
            throw new RuntimeException(error.toString());
        }

        mLogger.log(
                "Job was mapped to " + job.jobName + " to site " + site,
                LogManager.DEBUG_MESSAGE_LEVEL);

        // incorporate the profiles and
        // do transformation selection
        // set the staging site for the job
        TransformationCatalogEntry entry = lookupTC(job);
        incorporateProfiles(job, entry);

        // PM-810 assign data configuration for the job if
        // not already incorporated from profiles and properites
        if (!job.vdsNS.containsKey(Pegasus.DATA_CONFIGURATION_KEY)) {
            job.setDataConfiguration(PegasusConfiguration.DEFAULT_DATA_CONFIGURATION_VALUE);
        }
        job.setStagingSiteHandle(determineStagingSite(job));
        handleExecutableFileTransfers(job, entry);

        // PM-882 incorporate estimates on runtimes of the jobs
        // after the site selection has been done
        incorporateEstimates(job);
    }

    /**
     * Returns the staging site to be used for a job. The determination is made on the basis of the
     * following - data configuration value for job - from planner command line options - If a
     * staging site is not determined from the options it is set to be the execution site for the
     * job
     *
     * @param job the job for which to determine the staging site
     * @return the staging site
     */
    private String determineStagingSite(Job job) {
        return mPegasusConfiguration.determineStagingSite(job, mPOptions);
    }

    /**
     * Incorporates the profiles from the various sources into the job. The profiles are
     * incorporated in the order pool, transformation catalog, and properties file, with the
     * profiles from the properties file having the highest priority. It is here where the
     * transformation selector is called to select amongst the various transformations returned by
     * the TC Mapper.
     *
     * @param job the job into which the profiles have been incorporated.
     * @param tcEntry the transformation catalog entry to be associated with the job
     * @return true profiles were successfully incorporated. false otherwise
     */
    private boolean incorporateProfiles(Job job, TransformationCatalogEntry tcEntry) {
        String siteHandle = job.getSiteHandle();

        mLogger.log(
                "For job " + job.getName() + " updating profiles from site " + job.getSiteHandle(),
                LogManager.TRACE_MESSAGE_LEVEL);

        // the profile information from the pool catalog needs to be
        // assimilated into the job.
        job.updateProfiles(mSiteStore.lookup(siteHandle).getProfiles());

        /* PM-810
        TransformationCatalogEntry tcEntry = lookupTC( job );

        FileTransfer fTx = handleFileTransfersForMainExecutable( job, tcEntry );
        */

        // add any notifications specified in the transformation
        // catalog for the job. JIRA PM-391
        job.addNotifications(tcEntry);

        // the profile information from the transformation
        // catalog needs to be assimilated into the job
        // overriding the one from pool catalog.
        job.updateProfiles(tcEntry);

        // the profile information from the properties file
        // is assimilated overidding the one from transformation
        // catalog.
        job.updateProfiles(mProps);

        /* PM-810
        //handle dependant executables
        handleFileTransfersForDependantExecutables( job );
        if( fTx != null ){
            //add the main executable back as input
            job.addInputFile( fTx);
        }
        */

        return true;
    }

    /**
     * Returns the main executable to be associated with the job.
     *
     * @param job the job
     * @return
     */
    private TransformationCatalogEntry lookupTC(Job job) {

        TransformationCatalogEntry tcEntry = null;
        List tcEntries = null;
        String siteHandle = job.getSiteHandle();

        // we now query the TCMapper only if there is no hint available
        // by the user in the DAX 3.0 .
        if (job.getRemoteExecutable() == null || job.getRemoteExecutable().length() == 0) {

            // query the TCMapper and get hold of all the valid TC
            // entries for that site
            tcEntries =
                    mTCMapper.getTCList(job.namespace, job.logicalName, job.version, siteHandle);

            StringBuffer error;
            if (tcEntries != null && tcEntries.size() > 0) {
                // select a tc entry calling out to
                // the transformation selector
                tcEntry = selectTCEntry(tcEntries, job, mProps.getTXSelectorMode());
                if (tcEntry == null) {
                    error = new StringBuffer();
                    error.append("Transformation selection operation for job  ")
                            .append(job.getCompleteTCName())
                            .append(" for site ")
                            .append(job.getSiteHandle())
                            .append(" unsuccessful.");
                    mLogger.log(error.toString(), LogManager.ERROR_MESSAGE_LEVEL);
                    throw new RuntimeException(error.toString());
                }
            } else {
                // mismatch. should be unreachable code!!!
                // as error should have been thrown in the site selector
                throw new RuntimeException(
                        "Site selector mapped job "
                                + job.getCompleteTCName()
                                + " to pool "
                                + job.executionPool
                                + " for which no mapping exists in "
                                + "transformation mapper.");
            }
        } else {
            // create a transformation catalog entry object
            // corresponding to the executable set
            String executable = job.getRemoteExecutable();
            tcEntry = new TransformationCatalogEntry();
            tcEntry.setLogicalTransformation(
                    job.getTXNamespace(), job.getTXName(), job.getTXVersion());
            tcEntry.setResourceId(job.getSiteHandle());
            tcEntry.setPhysicalTransformation(executable);
            // hack to determine whether an executable is
            // installed or static binary
            tcEntry.setType(executable.startsWith("/") ? TCType.INSTALLED : TCType.STAGEABLE);
        }

        return tcEntry;
    }

    /**
     * Handles any file transfer related to staging of executables required by the job.
     *
     * @param job
     * @param entry
     */
    private void handleExecutableFileTransfers(Job job, TransformationCatalogEntry entry) {
        FileTransfer fTx = handleFileTransfersForMainExecutable(job, entry);

        // PM-1525 starting 5.0, the parsing of dax and tc in yaml formats sets
        // compound transformation for the transformation catalog entry object
        // need to add dependent executables as input files for the job
        for (PegasusFile pf : entry.getDependantFiles()) {
            job.addInputFile(pf);
        }

        // handle dependant executables
        handleFileTransfersForDependantExecutables(job);

        // PM-1195 check if any container transfers need to be done
        FileTransfer cTx = handleFileTransfersForAssociatedContainer(job, entry);

        if (fTx != null) {
            // add the main executable back as input
            job.addInputFile(fTx);
        }
        if (cTx != null) {
            mLogger.log(
                    "Container Executable "
                            + cTx.getLFN()
                            + " for job "
                            + job.getID()
                            + " being staged from "
                            + cTx.getSourceURL(),
                    LogManager.DEBUG_MESSAGE_LEVEL);
            job.addInputFile(cTx);
        }
    }

    /**
     * Handles any file transfer related to the main executable for the job, and also maps the
     * executable for the job
     *
     * @param job the job
     * @param entry the transformation catalog entry
     * @return FileTransfer if required for the staging the main executable
     */
    private FileTransfer handleFileTransfersForMainExecutable(
            Job job, TransformationCatalogEntry entry) {
        FileTransfer fTx = null;
        String stagingSiteHandle = job.getStagingSiteHandle();

        if (entry.getType().equals(TCType.STAGEABLE)) {
            SiteCatalogEntry site = mSiteStore.lookup(stagingSiteHandle);

            if (site == null) {
                throw new RuntimeException(
                        "Unable to find site catalog entry for staging site "
                                + stagingSiteHandle
                                + " for job "
                                + job.getID());
            }

            // construct a file transfer object and add it
            // as an input file to the job in the dag
            fTx = new FileTransfer(job.getStagedExecutableBaseName(), job.jobName);
            fTx.setType(FileTransfer.EXECUTABLE_FILE);

            // PM-1617 grab any checksums if available
            fTx.assimilateChecksum(entry);

            // the physical transformation points to
            // guc or the user specified transfer mechanism
            // accessible url
            fTx.addSource(entry.getResourceId(), entry.getPhysicalTransformation());

            // PM-833 for executable staging set the executable only to basename only
            // should work in all data configurations
            job.setRemoteExecutable("." + File.separator + job.getStagedExecutableBaseName());

            // setting the job type of the job to
            // denote the executable is being staged
            // job.setJobType(Job.STAGED_COMPUTE_JOB);
            job.setExecutableStagingForJob(true);
        } else {
            // the executable needs to point to the physical
            // path gotten from the selected transformantion
            // entry
            job.executable = entry.getPhysicalTransformation();
        }

        return fTx;
    }

    /**
     * Updates job and associates any container related file transfer
     *
     * @param job
     */
    private FileTransfer handleFileTransfersForAssociatedContainer(
            Job job, TransformationCatalogEntry entry) {
        Container c = entry.getContainer();
        if (c == null) {
            // nothing to do
            return null;
        }

        // associate container with the job
        job.setContainer(c);
        if (c.getType() == Container.TYPE.shifter) {
            // PM-1345 we don't transfer any shifter containers
            return null;
        }
        mLogger.log(
                "Job " + job.getID() + " associated with container " + c.getLFN(),
                LogManager.DEBUG_MESSAGE_LEVEL);

        FileTransfer fTx = new FileTransfer(c.getLFN(), job.jobName);
        fTx.setType(c.getType());

        String site = c.getImageSite();
        if (site == null) {
            site = "CONTAINER_SITE";
        }
        // construct logical name of the container with the suffix if
        // it exists in the URL?

        fTx.addSource(site, c.getImageURL().getURL());

        return fTx;
    }

    /**
     * Handles the dependant executables that need to be staged.
     *
     * @param job Job
     */
    private void handleFileTransfersForDependantExecutables(Job job) {
        String siteHandle = job.getSiteHandle();
        String stagingSiteHandle = job.getStagingSiteHandle();
        boolean installedTX = !(job.userExecutablesStagedForJob());

        List dependantExecutables = new ArrayList();
        for (Iterator it = job.getInputFiles().iterator(); it.hasNext(); ) {
            PegasusFile input = (PegasusFile) it.next();

            if (input.getType() == PegasusFile.EXECUTABLE_FILE) {

                // if the main executable is installed, just remove the executable
                // file requirement from the input files
                if (installedTX) {
                    it.remove();
                    continue;
                }

                // query the TCMapper and get hold of all the valid TC
                // entries for that site
                String lfn[] = Separator.split(input.getLFN());
                List tcEntries = mTCMapper.getTCList(lfn[0], lfn[1], lfn[2], siteHandle);

                StringBuffer error;
                if (tcEntries != null && tcEntries.size() > 0) {
                    // select a tc entry calling out to
                    // the transformation selector , we only should stage
                    // never pick any installed one.
                    TransformationCatalogEntry tcEntry = selectTCEntry(tcEntries, job, "Staged");
                    if (tcEntry == null) {
                        error = new StringBuffer();
                        error.append("Transformation selection operation for job  ")
                                .append(job.getCompleteTCName())
                                .append(" for site ")
                                .append(job.getSiteHandle())
                                .append(" unsuccessful.");
                        mLogger.log(error.toString(), LogManager.ERROR_MESSAGE_LEVEL);
                        throw new RuntimeException(error.toString());
                    }

                    if (tcEntry.getType().equals(TCType.STAGEABLE)) {

                        SiteCatalogEntry site = mSiteStore.lookup(stagingSiteHandle);
                        // construct a file transfer object and add it
                        // as an input file to the job in the dag

                        // a disconnect between the basename and the input lfn.
                        String basename = Job.getStagedExecutableBaseName(lfn[0], lfn[1], lfn[2]);

                        FileTransfer fTx = new FileTransfer(basename, job.jobName);
                        fTx.setType(FileTransfer.EXECUTABLE_FILE);

                        // PM-1617 grab any checksums if available
                        fTx.assimilateChecksum(tcEntry);

                        // the physical transformation points to
                        // guc or the user specified transfer mechanism
                        // accessible url
                        fTx.addSource(tcEntry.getResourceId(), tcEntry.getPhysicalTransformation());

                        dependantExecutables.add(fTx);

                        // the jobs executable is the path to where
                        // the executable is going to be staged
                        // job.executable = externalStagedPath;
                        mLogger.log(
                                "Dependant Executable "
                                        + input.getLFN()
                                        + " being staged from "
                                        + fTx.getSourceURL(),
                                LogManager.DEBUG_MESSAGE_LEVEL);
                    }
                }
                it.remove();
            } // end of if file is exectuable
        }

        // add all the dependant executable FileTransfers back as input files
        for (Iterator it = dependantExecutables.iterator(); it.hasNext(); ) {
            FileTransfer file = (FileTransfer) it.next();
            job.addInputFile(file);
        }
    }

    /**
     * Calls out to the transformation selector to select an entry from a list of valid
     * transformation catalog entries.
     *
     * @param entries list of <code>TransformationCatalogEntry</code> objects.
     * @param job the job.
     * @param selectors the selector to be called
     * @return the selected <code>TransformationCatalogEntry</code> object null when transformation
     *     selector is unable to select any transformation
     */
    private TransformationCatalogEntry selectTCEntry(List entries, Job job, String selector) {

        // load the transformation selector. different
        // selectors may end up being loaded for different jobs.
        mTXSelector = TransformationSelector.loadTXSelector(selector);
        entries = mTXSelector.getTCEntry(entries, job.getSiteHandle());
        return (entries == null || entries.size() == 0)
                ? null
                : entries.size() > 1
                        ?
                        // select a random entry
                        (TransformationCatalogEntry)
                                entries.get(PegRandom.getInteger(entries.size() - 1))
                        :
                        // return the first one
                        (TransformationCatalogEntry) entries.get(0);
    }

    /**
     * It incorporates a hint in the namespace to the job. After the hint is incorporated the key is
     * deleted from the hint namespace for that job.
     *
     * @param job the job that needs the hint to be incorporated.
     * @param key the key in the hint namespace.
     * @return true the hint was successfully incorporated. false the hint was not set in job or was
     *     not successfully incorporated.
     */
    private boolean incorporateHint(Job job, String key) {
        // sanity check
        if (key.length() == 0) {
            return false;
        }

        switch (key.charAt(0)) {
            case 'e':
                if (key.equals(Hints.EXECUTION_SITE_KEY) && job.hints.containsKey(key)) {
                    // user has overridden in the dax which execution Pool to use
                    job.executionPool = (String) job.hints.removeKey(Hints.EXECUTION_SITE_KEY);

                    incorporateHint(job, Hints.PFN_HINT_KEY);
                    return true;
                }
                break;

            case 'p':
                if (key.equals(Hints.PFN_HINT_KEY)) {
                    job.setRemoteExecutable(
                            job.hints.containsKey(Hints.PFN_HINT_KEY)
                                    ? (String) job.hints.removeKey(Hints.PFN_HINT_KEY)
                                    : null);

                    return true;
                }
                break;

            default:
                break;
        }
        return false;
    }

    /**
     * Incorporate estimates
     *
     * @param job
     */
    protected void incorporateEstimates(Job job) {
        Map<String, String> estimates = mEstimator.getAllEstimates(job);

        for (Map.Entry<String, String> entry : estimates.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            // each estimates is incorporated as a metadata attribute for the job
            job.getMetadata().construct(key, value);
        }

        String runtime = estimates.get("runtime");
        if (runtime != null) {
            // add to both Pegasus and metadata profiles in addition to globus
            job.vdsNS.checkKeyInNS(Pegasus.RUNTIME_KEY, runtime);
            job.addMetadata(Pegasus.MAX_WALLTIME, runtime);
        }

        String memory = estimates.get("memory");
        if (memory != null) {
            // add to both Pegasus and metadata profiles in addition to globus
            job.vdsNS.checkKeyInNS(Pegasus.MEMORY_KEY, memory);
            job.addMetadata(Globus.MAX_MEMORY_KEY, memory);
        }
    }

    /**
     * Converts a Vector to a List. It only copies by reference.
     *
     * @param v Vector
     * @return a ArrayList
     */
    public List convertToList(Vector v) {
        return new java.util.ArrayList(v);
    }

    /**
     * Converts a Set to a List. It only copies by reference.
     *
     * @param s Set
     * @return a ArrayList
     */
    public List convertToList(Set s) {
        return new java.util.ArrayList(s);
    }

    /**
     * Generates events for the mapped workflow.
     *
     * @param workflow the parsed dax
     * @param bag the initialized object bag
     */
    private void generateStampedeMetadataEvents(ADag workflow) {
        Stampede codeGenerator =
                (Stampede)
                        CodeGeneratorFactory.loadInstance(
                                mBag, CodeGeneratorFactory.STAMPEDE_EVENT_GENERATOR_CLASS);

        mLogger.logEventStart(
                LoggingKeys.EVENTS_PEGASUS_STAMPEDE_GENERATION,
                LoggingKeys.DAX_ID,
                workflow.getAbstractWorkflowName());

        try {
            Collection<File> result = codeGenerator.generateMetadataEventsForWF(workflow);
            for (Iterator it = result.iterator(); it.hasNext(); ) {
                mLogger.log(
                        "Written out stampede metadata events for the mapped workflow to "
                                + it.next(),
                        LogManager.DEBUG_MESSAGE_LEVEL);
            }
        } catch (Exception e) {
            throw new RuntimeException(
                    "Unable to generate stampede metadata events for mapped workflow", e);
        }

        mLogger.logEventCompletion();
    }

    /**
     * Complain with an informative error for the user in site selector fails.
     *
     * @param job
     * @param sites
     * @throws RuntimeException
     */
    private void complainForFailedSiteMapping(Job job, List<String> sites) throws RuntimeException {
        StringBuilder error = new StringBuilder();
        error.append("Site Selector could not map the job ")
                .append(job.getCompleteTCName())
                .append(" with id ")
                .append(job.getID())
                .append("\n")
                .append("to any of the execution sites ")
                .append(sites)
                .append("\n")
                .append("using the Transformation Mapper (")
                .append(this.mTCMapper.getMode())
                .append(")")
                .append("\n")
                .append("This error is most likely due to an error in the transformation catalog.")
                .append("\n")
                .append("Make sure that the ")
                .append(job.getCompleteTCName())
                .append(" transformation exists with matching system information for sites ")
                .append("\n")
                .append(sites)
                .append(" you are trying to plan for ")
                .append(mSiteStore.getSysInfos(sites))
                .append("\n");

        // add more information if possible
        try {
            List<TransformationCatalogEntry> entries =
                    mTCHandle.lookup(
                            job.getTXNamespace(),
                            job.getTXName(),
                            job.getTXVersion(),
                            (List) null,
                            null);
            if (entries != null && !entries.isEmpty()) {
                error.append("Candidate Entries found were " + entries);
            }
        } catch (Exception e) {
            // ignore
        }
        throw new RuntimeException(error.toString());
    }
}
