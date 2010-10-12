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


package edu.isi.pegasus.planner.cluster.aggregator;

import edu.isi.pegasus.planner.catalog.site.classes.SiteCatalogEntry;

import edu.isi.pegasus.planner.code.GridStartFactory;
import edu.isi.pegasus.planner.code.gridstart.PegasusExitCode;

import org.griphyn.cPlanner.classes.ADag;
import org.griphyn.cPlanner.classes.AggregatedJob;
import org.griphyn.cPlanner.classes.SubInfo;

import edu.isi.pegasus.common.logging.LogManager;

import edu.isi.pegasus.planner.namespace.Pegasus;
import edu.isi.pegasus.planner.namespace.Dagman;


import edu.isi.pegasus.planner.code.GridStart;
import org.griphyn.cPlanner.classes.PegasusBag;


import java.util.List;

/**
 * This class aggregates the smaller jobs in a manner such that
 * they are launched at remote end, sequentially on a single node using
 * seqexec. The executable seqexec is a Pegasus tool distributed in the Pegasus worker
 * package, and can be usually found at $PEGASUS_HOME/bin/seqexec.
 *
 * @author Karan Vahi vahi@isi.edu
 * @version $Revision$
 */

public class SeqExec extends Abstract {

    /**
     * The logical name of the transformation that is able to run multiple
     * jobs sequentially.
     */
    public static final String COLLAPSE_LOGICAL_NAME = "seqexec";


    /**
     * The suffix to be applied to seqexec progress report file.
     */
    public static final String SEQEXEC_PROGRESS_REPORT_SUFFIX = ".prg";

    /**
     * Flag indicating whether a global log file or per job file.
     */
    private boolean mGlobalLog;

    /**
     * Flag indicating whether to fail on first hard error or not.
     */
    private boolean mFailOnFirstError;
    
    /**
     * Flag to indicate whether to log progress or not.
     */
    private boolean mLogProgress;

    /**
     * The default constructor.
     */
    public SeqExec(){
        super();
    }

    /**
     *Initializes the JobAggregator impelementation
     *
     * @param dag  the workflow that is being clustered.
     * @param bag   the bag of objects that is useful for initialization.
     *
     */
    public void initialize( ADag dag , PegasusBag bag  ){
        super.initialize( dag, bag );
        mGlobalLog = mProps.logJobAggregatorProgressToGlobal();
        mLogProgress = mProps.logJobAggregatorProgress();
        //set abort of first job failure
        this.setAbortOnFirstJobFailure( mProps.abortOnFirstJobFailure() );

    }

    /**
     * Constructs a new aggregated job that contains all the jobs passed to it.
     * The new aggregated job, appears as a single job in the workflow and
     * replaces the jobs it contains in the workflow.
     * <p>
     * The seqexec uses kickstart to invoke each of the smaller constituent
     * jobs. The kickstart output appears on the stdout of the seqexec. Hence,
     * the seqexec itself is not being kickstarted. At the same time, appropriate
     * postscript is constructed to be invoked on the job.
     *
     * @param jobs the list of <code>SubInfo</code> objects that need to be
     *             collapsed. All the jobs being collapsed should be scheduled
     *             at the same pool, to maintain correct semantics.
     * @param name  the logical name of the jobs in the list passed to this
     *              function.
     * @param id   the id that is given to the new job.
     *
     *
     * @return  the <code>AggregatedJob</code> object corresponding to the aggregated
     *          job containing the jobs passed as List in the input,
     *          null if the list of jobs is empty
     */
    public AggregatedJob construct(List jobs,String name, String id) {
        AggregatedJob mergedJob = super.construct(jobs,name,id);
        //ensure that AggregatedJob is invoked via NoGridStart
        mergedJob.vdsNS.construct( Pegasus.GRIDSTART_KEY,
                                   GridStartFactory.GRIDSTART_SHORT_NAMES[
                                                          GridStartFactory.NO_GRIDSTART_INDEX] );

        SubInfo firstJob = (SubInfo)jobs.get(0);
        StringBuffer message = new StringBuffer();
        message.append( " POSTScript for merged job " ).
                append( mergedJob.getName() ).append( " " );

        //should we tinker with the postscript for this job
        if( mergedJob.dagmanVariables.containsKey( Dagman.POST_SCRIPT_KEY ) ){
            //no merged job has been set to have a specific post script
            //no tinkering
        }
        else{
            //we need to tinker
            //gridstart is always populated
            String gridstart = (String) firstJob.vdsNS.get(Pegasus.GRIDSTART_KEY);
            if (gridstart.equalsIgnoreCase( GridStartFactory.
                                            GRIDSTART_SHORT_NAMES[
                                            GridStartFactory.KICKSTART_INDEX]) ||
                 gridstart.equalsIgnoreCase( GridStartFactory.
                                            GRIDSTART_SHORT_NAMES[
                                            GridStartFactory.SEQEXEC_INDEX]) ) {
                //ensure $PEGASUS_HOME/bin/exitpost is invoked
                //as the baby jobs are being invoked by kickstart
                mergedJob.dagmanVariables.construct( Dagman.POST_SCRIPT_KEY,
                                                     PegasusExitCode.SHORT_NAME );
            }
        }
        message.append( mergedJob.dagmanVariables.get( Dagman.POST_SCRIPT_KEY ) );
        mLogger.log( message.toString(), LogManager.DEBUG_MESSAGE_LEVEL );

        return mergedJob;
    }


    /**
     * Enables the constitutent jobs that make up a aggregated job.
     *
     * @param mergedJob   the clusteredJob
     * @param jobs         the constitutent jobs
     *
     * @return AggregatedJob
     */
    protected AggregatedJob  enable(  AggregatedJob mergedJob, List jobs  ){
        SubInfo firstJob = (SubInfo)jobs.get(0);
        
//        SiteInfo site = mSiteHandle.getPoolEntry( firstJob.getSiteHandle(),
//                                                  Condor.VANILLA_UNIVERSE);

        SiteCatalogEntry site = mSiteStore.lookup( firstJob.getSiteHandle() );
        
        //NEEDS TO BE FIXED AS CURRENTLY NO PLACEHOLDER FOR Kickstart 
        //PATH IN THE NEW SITE CATALOG Karan July 10, 2008
        GridStart gridStart = mGridStartFactory.loadGridStart( firstJob,
                                                               site.getKickstartPath() );

        //explicitly set the gridstart key
        //so as to enable the correct generation of the postscript for
        //the aggregated job
        firstJob.vdsNS.construct( Pegasus.GRIDSTART_KEY,
                                  gridStart.getVDSKeyValue() );


        return gridStart.enable( mergedJob, jobs );
    }


    /**
     * Returns the logical name of the transformation that is used to
     * collapse the jobs.
     *
     * @return the the logical name of the collapser executable.
     * @see #COLLAPSE_LOGICAL_NAME
     */
    public String getCollapserLFN(){
        return COLLAPSE_LOGICAL_NAME;
    }

    /**
     * Determines whether there is NOT an entry in the transformation catalog
     * for the job aggregator executable on a particular site.
     *
     * @param site       the site at which existence check is required.
     *
     * @return boolean  true if an entry does not exists, false otherwise.
     */
    public boolean entryNotInTC(String site) {
        return this.entryNotInTC( this.TRANSFORMATION_NAMESPACE,
                                  COLLAPSE_LOGICAL_NAME,
                                  this.TRANSFORMATION_VERSION,
                                  site);
    }


    /**
     * Returns the arguments with which the <code>AggregatedJob</code>
     * needs to be invoked with.
     *
     * @param job  the <code>AggregatedJob</code> for which the arguments have
     *             to be constructed.
     *
     * @return argument string
     */
    public String aggregatedJobArguments( AggregatedJob job ){
        StringBuffer arguments = new StringBuffer();

        //do we need to fail hard on first error
        if( this.abortOnFristJobFailure()){
            arguments.append( " -f " );
        }

        //track the progress of the seqexec job
        //if specified in properties
        if( mLogProgress ){
            arguments.append( " -R ").append( logFile(job) );
        }
        
        return arguments.toString();
    }


    /**
     * Setter method to indicate , failure on first consitutent job should
     * result in the abort of the whole aggregated job. Ignores any value
     * passed, as MPIExec does not handle it for time being.
     *
     * @param fail  indicates whether to abort or not .
     */
    public void setAbortOnFirstJobFailure( boolean fail){
        mFailOnFirstError = fail;
    }

    /**
     * Returns a boolean indicating whether to fail the aggregated job on
     * detecting the first failure during execution of constituent jobs.
     *
     * @return boolean indicating whether to fail or not.
     */
    public boolean abortOnFristJobFailure(){
        return mFailOnFirstError;
    }

    /**
     * Returns the name of the log file to used on the remote site, for the
     * seqexec job. Depending upon the property settings, either assigns a
     * common
     *
     *
     * @param job the <code>AggregatedJob</code>
     *
     * @return the path to the log file.
     */
    protected String logFile( AggregatedJob job ){
        StringBuffer sb = new StringBuffer( 32 );
        if ( mGlobalLog ){
            //the basename of the log file is derived from the dag name
            sb.append( this.mClusteredADag.dagInfo.getLabel() );
        }
        else{
            //per seqexec job name
            sb.append( job.getName() );
        }
        sb.append( this.SEQEXEC_PROGRESS_REPORT_SUFFIX);
        return sb.toString();
    }
}
