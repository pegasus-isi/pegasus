/**
 * This file or a portion of this file is licensed under the terms of
 * the Globus Toolkit Public License, found in file GTPL, or at
 * http://www.globus.org/toolkit/download/license.html. This notice must
 * appear in redistributions of this file, with or without modification.
 *
 * Redistributions of this Software, with or without modification, must
 * reproduce the GTPL in: (1) the Software, or (2) the Documentation or
 * some other similar material which is provided with the Software (if
 * any).
 *
 * Copyright 1999-2004 University of Chicago and The University of
 * Southern California. All rights reserved.
 */

package org.griphyn.cPlanner.cluster.aggregator;

import org.griphyn.cPlanner.cluster.JobAggregator;

import org.griphyn.cPlanner.code.gridstart.GridStartFactory;
import org.griphyn.cPlanner.code.gridstart.ExitPOST;

import org.griphyn.cPlanner.classes.ADag;
import org.griphyn.cPlanner.classes.AggregatedJob;
import org.griphyn.cPlanner.classes.SubInfo;

import org.griphyn.cPlanner.common.PegasusProperties;
import org.griphyn.cPlanner.common.LogManager;

import org.griphyn.cPlanner.namespace.VDS;
import org.griphyn.cPlanner.namespace.Dagman;

import java.util.List;

import java.io.File;

/**
 * This class aggregates the smaller jobs in a manner such that
 * they are launched at remote end, sequentially on a single node using
 * seqexec. The executable seqexec is a VDS tool distributed in the VDS worker
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
     * The overloaded constructor, that is called by load method.
     *
     * @param properties the <code>PegasusProperties</code> object containing all
     *                   the properties required by Pegasus.
     * @param submitDir  the submit directory where the submit file for the job
     *                   has to be generated.
     * @param dag        the workflow that is being clustered.
     *
     * @see JobAggregatorFactory#loadInstance(String,PegasusProperties,String,ADag)
     *
     */
    public SeqExec(PegasusProperties properties, String submitDir, ADag dag){
        super(properties,submitDir,dag);
        mFailOnFirstError = false;
        mGlobalLog = properties.jobAggregatorLogGlobal();
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
        mergedJob.vdsNS.construct( VDS.GRIDSTART_KEY,
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
            String gridstart = (String) firstJob.vdsNS.get(VDS.GRIDSTART_KEY);
            if (gridstart.equalsIgnoreCase( GridStartFactory.
                                            GRIDSTART_SHORT_NAMES[
                                            GridStartFactory.KICKSTART_INDEX]) ) {
                //ensure $PEGASUS_HOME/bin/exitpost is invoked
                //as the baby jobs are being invoked by kickstart
                mergedJob.dagmanVariables.construct( Dagman.POST_SCRIPT_KEY,
                                                     ExitPOST.SHORT_NAME );
            }
        }
        message.append( mergedJob.dagmanVariables.get( Dagman.POST_SCRIPT_KEY ) );
        mLogger.log( message.toString(), LogManager.DEBUG_MESSAGE_LEVEL );

        return mergedJob;
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
        return this.entryNotInTC(null,COLLAPSE_LOGICAL_NAME,null,site);
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
        arguments.append( " -R ").append( logFile(job) );


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