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


package edu.isi.pegasus.planner.classes;

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
     * The number of clustered compute jobs.
     */
    private int mNumClusteredJobs;

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
     * The number of create dir jobs.
     */
    private int mNumCreateDirJobs;

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
        mNumCreateDirJobs = 0;
        mNumClusteredJobs = 0;
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
    public void increment( Job job ){
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
            case Job.COMPUTE_JOB:
            case Job.STAGED_COMPUTE_JOB:
                if( job instanceof AggregatedJob ){
                    mNumClusteredJobs++;
                }
                else{
                    mNumComputeJobs++;
                }
                break;

            case Job.STAGE_IN_JOB:
                mNumSITxJobs++;
                break;

            case Job.STAGE_OUT_JOB:
                mNumSOTxJobs++;
                break;

            case Job.INTER_POOL_JOB:
                mNumInterTxJobs++;
                break;

            case Job.REPLICA_REG_JOB:
                mNumRegJobs++;
                break;

            case Job.CLEANUP_JOB:
                mNumCleanupJobs++;
                break;

            case Job.CREATE_DIR_JOB:
                mNumCreateDirJobs++;
                break;

            default:
                throw new RuntimeException( "Unknown or Unassigned job " + job.getID() + " of type " + type );

        }

    }

    /**
     * Decrement the metrics when on the basis of type of job.
     *
     * @param job  the job being removed.
     */
    public void decrement( Job job ){
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
            case Job.COMPUTE_JOB:
            case Job.STAGED_COMPUTE_JOB:
                if( job instanceof AggregatedJob ){
                    mNumClusteredJobs--;
                }
                else{
                    mNumComputeJobs--;
                }
                break;

            case Job.STAGE_IN_JOB:
                mNumSITxJobs--;
                break;

            case Job.STAGE_OUT_JOB:
                mNumSOTxJobs--;
                break;

            case Job.INTER_POOL_JOB:
                mNumInterTxJobs--;
                break;

            case Job.REPLICA_REG_JOB:
                mNumRegJobs--;
                break;

            case Job.CLEANUP_JOB:
                mNumCleanupJobs--;
                break;

             case Job.CREATE_DIR_JOB:
                mNumCreateDirJobs--;
                break;

            default:
                throw new RuntimeException( "Unknown or Unassigned job " + job.getID() + " of type " + type );

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
        append( sb, "createdir-jobs.count", this.mNumCreateDirJobs );
        append( sb, "compute-jobs.count", this.mNumComputeJobs );
        append( sb, "clustered-compute-jobs.count", this.mNumClusteredJobs );
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
        wm.mNumCreateDirJobs = this.mNumCreateDirJobs;
        wm.mNumClusteredJobs = this.mNumClusteredJobs;

        return wm;
    }


}
