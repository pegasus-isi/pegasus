/**
 * This file or a portion of this file is licensed under the terms of
 * the Globus Toolkit Public License, found at $PEGASUS_HOME/GTPL or
 * http://www.globus.org/toolkit/download/license.html.
 * This notice must appear in redistributions of this file
 * with or without modification.
 *
 * Redistributions of this Software, with or without modification, must reproduce
 * the GTPL in:
 * (1) the Software, or
 * (2) the Documentation or
 * some other similar material which is provided with the Software (if any).
 *
 * Copyright 1999-2004
 * University of Chicago and The University of Southern California.
 * All rights reserved.
 */

package org.griphyn.cPlanner.engine;

import org.griphyn.cPlanner.classes.ADag;
import org.griphyn.cPlanner.classes.FileTransfer;
import org.griphyn.cPlanner.classes.PegasusFile;
import org.griphyn.cPlanner.classes.JobManager;
import org.griphyn.cPlanner.classes.PlannerOptions;
import org.griphyn.cPlanner.classes.SiteInfo;
import org.griphyn.cPlanner.classes.SubInfo;

import org.griphyn.cPlanner.common.LogManager;
import org.griphyn.cPlanner.common.PegasusProperties;

import org.griphyn.cPlanner.selector.SiteSelector;
import org.griphyn.cPlanner.selector.TransformationSelector;

import org.griphyn.cPlanner.provenance.pasoa.XMLProducer;
import org.griphyn.cPlanner.provenance.pasoa.producer.XMLProducerFactory;

import org.griphyn.cPlanner.provenance.pasoa.PPS;
import org.griphyn.cPlanner.provenance.pasoa.pps.PPSFactory;


import org.griphyn.common.catalog.TransformationCatalogEntry;

import org.griphyn.common.catalog.transformation.TCMode;

import org.griphyn.common.classes.TCType;

import org.griphyn.common.catalog.transformation.Mapper;

import org.griphyn.common.util.Separator;



import java.io.File;

import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.Vector;

/**
 * This engine calls out to the Site Selector selected by the user and maps the
 * jobs in the workflow to the execution pools.
 *
 * @author Karan Vahi
 * @author Gaurang Mehta
 * @version $Revision$
 *
 */
public class InterPoolEngine extends Engine implements Refiner {

    /**
     * ADag object corresponding to the Dag whose jobs we want to schedule.
     *
     */
    private ADag mDag;

    /**
     * Set of the execution pools which the user has specified.
     */
    private Set mExecPools;

    /**
     * Handle to the site selector.
     */
    private SiteSelector mSiteSelector;

    /**
     * The handle to the transformation selector, that ends up selecting
     * what transformations to pick up.
     */
    private TransformationSelector mTXSelector;

    /**
     * The mode with which the transformation catalog mapper needs to be called.
     */
    private String mTCMapperMode;

    /**
     * The handle to the transformation catalog mapper object that caches the
     * queries to the transformation catalog, and indexes them according to
     * lfn's. There is no purge policy in the TC Mapper, so per se it is not a
     * classic cache.
     */
    private Mapper mTCMapper;

    /**
     * The XML Producer object that records the actions.
     */
    private XMLProducer mXMLStore;


    /**
     * Default constructor.
     *
     *
     * @param props   the properties to be used.
     */
    public InterPoolEngine( PegasusProperties props ) {
        super( props );
        mDag        = new ADag();
        mExecPools  = new java.util.HashSet();
        mTCMapper   = Mapper.loadTCMapper(mProps.getTCMapperMode());
        mTXSelector = null;
        mXMLStore        = XMLProducerFactory.loadXMLProducer( props );
    }

    /**
     * Overloaded constructor.
     *
     * @param aDag      the <code>ADag</code> object corresponding to the Dag
     *                  for which we want to determine on which pools to run
     *                  the nodes of the Dag.
     * @param props   the properties to be used.
     * @param options   The options specified by the user to run the planner.
     *
     */
    public InterPoolEngine( ADag aDag, PegasusProperties props, PlannerOptions options) {
        super( props );
        mDag = aDag;
        mPOptions = options;
        mExecPools = (Set)options.getExecutionSites();
        mTCHandle = TCMode.loadInstance();

        mSiteSelector = SiteSelector.loadSiteSelector(
            mProps.getSiteSelectorMode(),
            mProps.getSiteSelectorPath());
        mSiteSelector.setAbstractDag(aDag);
        //initialize the transformation mapper and pass
        //them to the site selector loaded
        mTCMapper   = Mapper.loadTCMapper(mProps.getTCMapperMode());
        mSiteSelector.setTCMapper(mTCMapper);

        mTXSelector = null;
        mXMLStore        = XMLProducerFactory.loadXMLProducer( props );
    }

    /**
     * Returns a reference to the workflow that is being refined by the refiner.
     *
     *
     * @return ADAG object.
     */
    public ADag getWorkflow(){
        return this.mDag;
    }

    /**
     * Returns a reference to the XMLProducer, that generates the XML fragment
     * capturing the actions of the refiner. This is used for provenace
     * purposes.
     *
     * @return XMLProducer
     */
    public XMLProducer getXMLProducer(){
        return this.mXMLStore;
    }


    /**
     * This is where the callout to the Partitioner should take place, that
     * partitions the workflow into clusters and sends to the site selector only
     * those list of jobs that are ready to be scheduled.
     *
     */
    public void determinePools() {
        SubInfo job;

        //at present we schedule the whole workflow at once
        List jobs = convertToList(mDag.vJobSubInfos);
        List pools = convertToList(mExecPools);

        //going through all the jobs making up the Adag, to do the physical mapping
        scheduleJobs(jobs, pools);
    }

    /**
     * It schedules a list of jobs on the execution pools by calling out to the
     * site selector specified. It is upto to the site selector to determine if
     * the job can be run on the list of pools passed.
     *
     * @param jobs  the list of jobs to be scheduled.
     * @param pools the list of execution pools, specified by the user.
     *
     */
    public void scheduleJobs(List jobs, List pools) {
        String[] mappings = mSiteSelector.mapJob2ExecPool(jobs, pools);
        int i = 0;
        StringBuffer error;

        //load the PPS implementation
        PPS pps = PPSFactory.loadPPS( this.mProps );

        mXMLStore.add( "<workflow url=\"" + mPOptions.getDAX() + "\">" );

        //call the begin workflow method
        try{
            pps.beginWorkflowRefinementStep( this, "Site Selection", false );
        }
        catch( Exception e ){
            throw new RuntimeException( "PASOA Exception", e );
        }

        //clear the XML store
        mXMLStore.clear();


        //Iterate through the jobs and hand them to
        //the site selector if required
        for(Iterator it = jobs.iterator();it.hasNext();i++){
            SubInfo job = (SubInfo) it.next();
            String res = mappings[i];

            //check if the user has specified any hints in the dax
            incorporateHint(job, "pfnUniverse");
            if (incorporateHint(job, "executionPool")) {
                //i++;
                incorporateProfiles(job);
                continue;
            }

            if (res == null) {
                error = new StringBuffer();
                error.append( "Site Selector could not map the job " ).
                      append( job.getCompleteTCName() ).
                      append( "\nMost likely an error occured in site selector." );
                mLogger.log( error.toString(),
                            LogManager.ERROR_MESSAGE_LEVEL );
                throw new RuntimeException( error.toString() );
            }
            String pool = res.substring(0, res.indexOf(":"));
            String jm = res.substring(res.indexOf(":") + 1);
            jm = ( (jm == null) || jm.length() == 0 ||
                  jm.equalsIgnoreCase("null")) ?
                null : jm;

            if (pool.length() == 0 ||
                pool.equalsIgnoreCase(SiteSelector.POOL_NOT_FOUND)) {
                error = new StringBuffer();
                error.append( "Site Selector (" ).append( mSiteSelector.description() ).
                      append( ") could not map job " ).append( job.getCompleteTCName() ).
                      append( " to any site" );
                mLogger.log( error.toString(), LogManager.ERROR_MESSAGE_LEVEL );
                throw new RuntimeException( error.toString() );
            }
            job.executionPool = pool;
            job.globusScheduler = (jm == null) ?
                getJobManager(pool, job.condorUniverse) : jm;

            if (job.globusScheduler == null) {
                error = new StringBuffer();
                error.append( "Could not find a jobmanager at pool (").
                      append( pool ).append( ") for universe " ).
                      append( job.condorUniverse );
                mLogger.log( error.toString(), LogManager.ERROR_MESSAGE_LEVEL );
                throw new RuntimeException( error.toString() );

            }

            mLogger.log("Mapped job " + job.jobName + " to pool " + pool,
                        LogManager.DEBUG_MESSAGE_LEVEL);
            //incorporate the profiles and
            //do transformation selection
            if ( !incorporateProfiles(job) ){
                error = new StringBuffer();
                error.append( "Profiles incorrectly incorporated for ").
                      append( job.getCompleteTCName());

               mLogger.log( error.toString(), LogManager.ERROR_MESSAGE_LEVEL );
               throw new RuntimeException( error.toString() );

            }

            //log actions as XML fragment
            try{
                logRefinerAction(job);
                pps.siteSelectionFor( job.getName(), job.getName() );
            }
            catch( Exception e ){
                throw new RuntimeException( "PASOA Exception", e );
            }

        }//end of mapping all jobs

        try{
            pps.endWorkflowRefinementStep( this );
        }
        catch( Exception e ){
            throw new RuntimeException( "PASOA Exception", e );
        }

    }

    /**
     * Incorporates the profiles from the various sources into the job.
     * The profiles are incorporated in the order pool, transformation catalog,
     * and properties file, with the profiles from the properties file having
     * the highest priority.
     * It is here where the transformation selector is called to select
     * amongst the various transformations returned by the TC Mapper.
     *
     * @param job  the job into which the profiles have been incorporated.
     *
     * @return true profiles were successfully incorporated.
     *         false otherwise
     */
    private boolean incorporateProfiles(SubInfo job){
        TransformationCatalogEntry tcEntry = null;
        List tcEntries = null;
        String siteHandle = job.getSiteHandle();

        //the profile information from the pool catalog needs to be
        //assimilated into the job.
        job.updateProfiles(mPoolHandle.getPoolProfile(siteHandle));


        //query the TCMapper and get hold of all the valid TC
        //entries for that site
        tcEntries = mTCMapper.getTCList(job.namespace,job.logicalName,
                                        job.version,siteHandle);

        StringBuffer error;
        FileTransfer fTx = null;
        if(tcEntries != null && tcEntries.size() > 0){
            //select a tc entry calling out to
            //the transformation selector
            tcEntry = selectTCEntry(tcEntries,job,mProps.getTXSelectorMode());
            if(tcEntry == null){
                error = new StringBuffer();
                error.append( "Transformation selection operation for job  ").
                      append( job.getCompleteTCName() ).append(" for site " ).
                      append( job.getSiteHandle() ).append( " unsuccessful." );
                mLogger.log( error.toString(), LogManager.ERROR_MESSAGE_LEVEL );
                throw new RuntimeException( error.toString() );
            }

            //something seriously wrong in this code line below.
            //Need to verify further after more runs. (Gaurang 2-7-2006).
//            tcEntry = (TransformationCatalogEntry) tcEntries.get(0);
            if(tcEntry.getType().equals(TCType.STATIC_BINARY)){
                SiteInfo site = mPoolHandle.getPoolEntry(siteHandle,"vanilla");
                //construct a file transfer object and add it
                //as an input file to the job in the dag
                fTx = new FileTransfer(job.getStagedExecutableBaseName(),
                                                    job.jobName);
                fTx.setType(FileTransfer.EXECUTABLE_FILE);
                //the physical transformation points to
                //guc or the user specified transfer mechanism
                //accessible url
                fTx.addSource(tcEntry.getResourceId(),
                              tcEntry.getPhysicalTransformation());
                //the destination url is the working directory for
                //pool where it needs to be staged to
                //always creating a third party transfer URL
                //for the destination.
                String stagedPath =  mPoolHandle.getExecPoolWorkDir(job)
                    + File.separator + job.getStagedExecutableBaseName();
                fTx.addDestination(siteHandle,
                                   site.getURLPrefix(false) + stagedPath);

                //added in the end now after dependant executables
                //have been handled Karan May 31 2007
                //job.addInputFile(fTx);

                //the jobs executable is the path to where
                //the executable is going to be staged
                job.executable = stagedPath;
                //setting the job type of the job to
                //denote the executable is being staged
                job.setJobType(SubInfo.STAGED_COMPUTE_JOB);
            }
            else{
                //the executable needs to point to the physical
                //path gotten from the selected transformantion
                //entry
                job.executable = tcEntry.getPhysicalTransformation();
            }
        }
        else{
            //mismatch. should be unreachable code!!!
            //as error should have been thrown in the site selector
            mLogger.log(
                "Site selector mapped job " +
                job.getCompleteTCName() + " to pool " +
                job.executionPool + " for which no mapping exists in " +
                "transformation mapper.",LogManager.FATAL_MESSAGE_LEVEL);
            return false;
        }

        //the profile information from the transformation
        //catalog needs to be assimilated into the job
        //overriding the one from pool catalog.
        job.updateProfiles(tcEntry);

        //the profile information from the properties file
        //is assimilated overidding the one from transformation
        //catalog.
        job.updateProfiles(mProps);

        //handle dependant executables
        handleDependantExecutables( job );
        if( fTx != null ){
            //add the main executable back as input
            job.addInputFile( fTx);
        }

        return true;
    }

    /**
     * Handles the dependant executables that need to be staged.
     *
     * @param job SubInfo
     *
     */
    private void handleDependantExecutables( SubInfo job ){
        String siteHandle = job.getSiteHandle();
        boolean installedTX =  !(job.getJobType() == SubInfo.STAGED_COMPUTE_JOB );

        List dependantExecutables = new ArrayList();
        for (Iterator it = job.getInputFiles().iterator(); it.hasNext(); ) {
            PegasusFile input = (PegasusFile) it.next();

            if (input.getType() == PegasusFile.EXECUTABLE_FILE) {

                //if the main executable is installed, just remove the executable
                //file requirement from the input files
                if( installedTX ){
                    it.remove();
                    continue;
                }

                //query the TCMapper and get hold of all the valid TC
                //entries for that site
                String lfn[] = Separator.split( input.getLFN() );
                List tcEntries = mTCMapper.getTCList( lfn[0], lfn[1], lfn[2],
                                                     siteHandle);

                StringBuffer error;
                if (tcEntries != null && tcEntries.size() > 0) {
                    //select a tc entry calling out to
                    //the transformation selector , we only should stage
                    //never pick any installed one.
                    TransformationCatalogEntry tcEntry = selectTCEntry(tcEntries, job,
                                            "Staged" );
                    if (tcEntry == null) {
                        error = new StringBuffer();
                        error.append("Transformation selection operation for job  ").
                            append(job.getCompleteTCName()).append(" for site ").
                            append(job.getSiteHandle()).append(" unsuccessful.");
                        mLogger.log(error.toString(),
                                    LogManager.ERROR_MESSAGE_LEVEL);
                        throw new RuntimeException(error.toString());
                    }

                    //            tcEntry = (TransformationCatalogEntry) tcEntries.get(0);
                    if (tcEntry.getType().equals(TCType.STATIC_BINARY)) {
                        SiteInfo site = mPoolHandle.getPoolEntry(siteHandle,
                            "vanilla");
                        //construct a file transfer object and add it
                        //as an input file to the job in the dag

                        //a disconnect between the basename and the input lfn.
                        String basename = SubInfo.getStagedExecutableBaseName(  lfn[0], lfn[1], lfn[2] );

                        FileTransfer fTx = new FileTransfer( basename,
                                                             job.jobName );
                        fTx.setType(FileTransfer.EXECUTABLE_FILE);
                        //the physical transformation points to
                        //guc or the user specified transfer mechanism
                        //accessible url
                        fTx.addSource(tcEntry.getResourceId(),
                                      tcEntry.getPhysicalTransformation());
                        //the destination url is the working directory for
                        //pool where it needs to be staged to
                        //always creating a third party transfer URL
                        //for the destination.
                        String stagedPath = mPoolHandle.getExecPoolWorkDir(job)
                            + File.separator + basename;
                        fTx.addDestination(siteHandle,
                                           site.getURLPrefix(false) + stagedPath);

                        dependantExecutables.add( fTx );

                        //the jobs executable is the path to where
                        //the executable is going to be staged
                        //job.executable = stagedPath;
                        mLogger.log( "Dependant Executable " + input.getLFN() + " being staged from " +
                                     fTx.getSourceURL(), LogManager.DEBUG_MESSAGE_LEVEL );

                    }

                }
                it.remove();
            } //end of if file is exectuable
        }

        //add all the dependant executable FileTransfers back as input files
        for( Iterator it = dependantExecutables.iterator(); it.hasNext(); ){
            FileTransfer file = (FileTransfer)it.next();
            job.addInputFile( file );
        }
    }

    /**
     * Recursively ends up calling the transformation selector accordxing to
     * a chain of selections that need to be performed on the list of valid
     * transformation catalog
     *
     * @param entries    list of <code>TransformationCatalogEntry</code> objects.
     * @param job        the job.
     * @param selectors  comma separated list of selectors.
     *
     * @return the selected <code>TransformationCatalogEntry</code> object
     *         null when transformation selector is unable to select any
     *         transformation
     */
    private TransformationCatalogEntry selectTCEntry(List entries, SubInfo job,
                                                     String selectors){
        //at present there is only one selector
        //operation performed on the selector
        String selector = selectors;

        //load the transformation selector. different
        //selectors may end up being loaded for different jobs.
        mTXSelector = TransformationSelector.loadTXSelector(selector);
        entries    = mTXSelector.getTCEntry(entries);
        return (entries == null || entries.size() == 0)?
                null:
                 entries.size() > 1?
                      //call the selector again
                      selectTCEntry(entries,job,selectors):
                      (TransformationCatalogEntry) entries.get(0);
    }

    /**
     * It returns a jobmanager for the given pool.
     *
     * @param pool      the name of the pool.
     * @param universe  the universe for which you need the scheduler on that
     *                  particular pool.
     *
     * @return the jobmanager for that pool and universe.
     *         null if not found.
     */
    private String getJobManager(String pool, String universe) {
        SiteInfo p = mPoolHandle.getPoolEntry(pool, universe);
        JobManager jm = (p == null)? null : p.selectJobManager(universe,true);
        return (jm == null) ? null : jm.getInfo(JobManager.URL);
    }

    /**
     * It incorporates a hint in the namespace to the job. After the hint
     * is incorporated the key is deleted from the hint namespace for that
     * job.
     *
     * @param job  the job that needs the hint to be incorporated.
     * @param key  the key in the hint namespace.
     *
     * @return true  the hint was successfully incorporated.
     *         false the hint was not set in job or was not successfully
     *               incorporated.
     */
    private boolean incorporateHint(SubInfo job, String key) {
        //sanity check
        if (key.length() == 0) {
            return false;
        }

        switch (key.charAt(0)) {
            case 'e':
                if (key.equals("executionPool") && job.hints.containsKey(key)) {
                    //user has overridden in the dax which execution Pool to use
                    job.executionPool = (String) job.hints.removeKey(
                        "executionPool");

                    incorporateHint(job, "globusScheduler");
                    return true;
                }
                break;

            case 'g':
                if (key.equals("globusScheduler")) {
                    job.globusScheduler = (job.hints.containsKey(
                        "globusScheduler")) ?
                        //take the globus scheduler that the user
                        //specified in the DAX
                        (String) job.hints.removeKey("globusScheduler") :
                        //select one from the pool handle
                        mPoolHandle.getPoolEntry(job.executionPool,
                                                 job.condorUniverse).
                        selectJobManager(job.condorUniverse,true).getInfo(JobManager.URL);

                    return true;
                }
                break;

            case 'p':
                if (key.equals("pfnUniverse")) {
                    job.condorUniverse = job.hints.containsKey("pfnUniverse") ?
                        (String) job.hints.removeKey("pfnUniverse") :
                        job.condorUniverse;

                    return true;

                }
                break;

            default:
                break;

        }
        return false;
    }

    /**
     * Converts a Vector to a List. It only copies by reference.
     * @param v Vector
     * @return a ArrayList
     */
    public List convertToList(Vector v) {
        return  new java.util.ArrayList(v);
    }

    /**
     * Converts a Set to a List. It only copies by reference.
     * @param s Set
     * @return a ArrayList
     */
    public List convertToList(Set s) {
        return new java.util.ArrayList(s);
    }


    /**
     * Logs the action taken by the refiner on a job as a XML fragment in
     * the XML Producer.
     *
     * @param job  the <code>SubInfo</code> containing the job that was mapped
     *             to a site.
     */
    protected void logRefinerAction( SubInfo job ){
        StringBuffer sb = new StringBuffer();
        sb.append( "\t<siteselection job=\"" ).append( job.getName() ).append( "\">" );
        sb.append( "\n" ).append( "\t\t" );
        sb.append( "<logicalsite>" ).append( job.getSiteHandle() ).append( "</logicalsite>" );
        sb.append( "\n" ).append( "\t\t" );
        sb.append( "<jobmanager>" ).append( job.getJobManager() ).append( "</jobmanager>" );
        sb.append( "\n" );
        sb.append( "\t</siteselection>" );
        sb.append( "\n" );
        mXMLStore.add( sb.toString() );
    }

}
