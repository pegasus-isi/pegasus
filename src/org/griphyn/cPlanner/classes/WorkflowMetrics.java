/*
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

package org.griphyn.cPlanner.classes;

/**
 * A Workflow metrics class that stores the metrics about the workflow.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class WorkflowMetrics extends Data {


    /**
     * The total number of  jobs in the executable workflow.
     */
    private int mNumTotalJobs;

    /**
     * The number of compute jobs.
     */
    private int mNumComputeJobs;

    /**
     * The number of stage in transfer jobs.
     */
    private int mNumSITxJobs;

    /**
     * The number of stage-out transfer jobs.
     */
    private int mNumSOTxJobs;

    /**
     * The number of inter-site transfer jobs.
     */
    private int mNumInterTxJobs;

    /**
     * The number of registration jobs.
     */
    private int mNumRegJobs;

    /**
     * The number of cleanup jobs.
     */
    private int mNumCleanupJobs;

    /**
     * The label of the dax.
     */
    private String mDAXLabel;

    /**
     * The default constructor.
     */
    public WorkflowMetrics() {
        reset();
    }

    /**
     * Resets the internal counters to zero.
     */
    public void reset(){
        mNumTotalJobs    = 0;
        mNumComputeJobs  = 0;
        mNumSITxJobs     = 0;
        mNumSOTxJobs     = 0;
        mNumInterTxJobs  = 0;
        mNumRegJobs      = 0;
        mNumCleanupJobs  = 0;
    }


    /**
     * Sets the DAXlabel.
     *
     * @param label  the dax label
     */
    public void setLabel( String label ){
        mDAXLabel = label;
    }


    /**
     * Returns the DAXlabel.
     *
     * @return the dax label
     */
    public String getLabel(  ){
        return mDAXLabel;
    }

    /**
     * Increment the metrics when on the basis of type of job.
     *
     * @param job  the job being added.
     */
    public void increment( SubInfo job ){
        //sanity check
        if( job == null ){
            return;
        }

        //increment the total
        mNumTotalJobs++;

        //increment on basis of type of job
        int type = job.getJobType();
        switch( type ){

            //treating compute and staged compute as same
            case SubInfo.COMPUTE_JOB:
            case SubInfo.STAGED_COMPUTE_JOB:
                mNumComputeJobs++;
                break;

            case SubInfo.STAGE_IN_JOB:
                mNumSITxJobs++;
                break;

            case SubInfo.STAGE_OUT_JOB:
                mNumSOTxJobs++;
                break;

            case SubInfo.INTER_POOL_JOB:
                mNumInterTxJobs++;
                break;

            case SubInfo.REPLICA_REG_JOB:
                mNumRegJobs++;
                break;

            case SubInfo.CLEANUP_JOB:
                mNumCleanupJobs++;
                break;
        }

    }

    /**
     * Decrement the metrics when on the basis of type of job.
     *
     * @param job  the job being removed.
     */
    public void decrement( SubInfo job ){
        //sanity check
        if( job == null ){
            return;
        }

        //increment the total
        mNumTotalJobs--;

        //increment on basis of type of job
        int type = job.getJobType();
        switch( type ){

            //treating compute and staged compute as same
            case SubInfo.COMPUTE_JOB:
            case SubInfo.STAGED_COMPUTE_JOB:
                mNumComputeJobs--;
                break;

            case SubInfo.STAGE_IN_JOB:
                mNumSITxJobs--;
                break;

            case SubInfo.STAGE_OUT_JOB:
                mNumSOTxJobs--;
                break;

            case SubInfo.INTER_POOL_JOB:
                mNumInterTxJobs--;
                break;

            case SubInfo.REPLICA_REG_JOB:
                mNumRegJobs--;
                break;

            case SubInfo.CLEANUP_JOB:
                mNumCleanupJobs--;
                break;


        }

    }

    /**
     * Returns a textual description of the object.
     *
     * @return Object
     */
    public String toString(){
        StringBuffer sb = new StringBuffer();

        append( sb, "dax-label", this.mDAXLabel );
        append( sb, "compute-jobs.count", this.mNumComputeJobs );
        append( sb, "si-jobs.count", this.mNumSITxJobs );
        append( sb, "so-jobs.count", this.mNumSOTxJobs );
        append( sb, "inter-jobs.count", this.mNumInterTxJobs );
        append( sb, "reg-jobs.count", this.mNumRegJobs );
        append( sb, "cleanup-jobs.count", this.mNumCleanupJobs );
        append( sb, "total-jobs.count", this.mNumTotalJobs );

        return sb.toString();
    }

    /**
     * Appends a key=value pair to the StringBuffer.
     *
     * @param buffer    the StringBuffer that is to be appended to.
     * @param key   the key.
     * @param value the value.
     */
    protected void append( StringBuffer buffer, String key, String value ){
        buffer.append( key ).append( " = " ).append( value ).append( "\n" );
    }

    /**
     * Appends a key=value pair to the StringBuffer.
     *
     * @param buffer    the StringBuffer that is to be appended to.
     * @param key   the key.
     * @param value the value.
     */
    protected void append( StringBuffer buffer, String key, int value ){
        buffer.append( key ).append( " = " ).append( value ).append( "\n" );
    }


    /**
     * Returns the clone of the object.
     *
     * @return the clone
     */
    public Object clone(){
        WorkflowMetrics wm;
        try {
            wm = (WorkflowMetrics)super.clone();
        }
        catch (CloneNotSupportedException e) {
            //somewhere in the hierarch chain clone is not implemented
            throw new RuntimeException( "Clone not implemented in the base class of " +
                                        this.getClass().getName(),
                                        e);
        }

        wm.mNumCleanupJobs = this.mNumCleanupJobs;
        wm.mNumComputeJobs = this.mNumComputeJobs;
        wm.mNumInterTxJobs = this.mNumInterTxJobs;
        wm.mNumRegJobs     = this.mNumRegJobs;
        wm.mNumSITxJobs    = this.mNumSITxJobs;
        wm.mNumSOTxJobs    = this.mNumSOTxJobs;
        wm.mNumTotalJobs   = this.mNumTotalJobs;
        wm.mDAXLabel       = this.mDAXLabel;

        return wm;
    }


}
