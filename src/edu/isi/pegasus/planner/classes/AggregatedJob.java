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

import edu.isi.pegasus.planner.cluster.JobAggregator;

import java.util.Collection;
import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * This class holds all the specifics of an aggregated job. An aggregated job
 * or a clustered job is a job, that contains a collection of smaller jobs.
 * An aggregated job during execution may explode into n smaller job executions.
 * At present it does not store information about the dependencies between the
 * jobs.
 *
 * @author Karan Vahi
 * @version $Revision$
 */

public class AggregatedJob extends Job {

    /**
     * The collection of jobs that are contained in the aggregated job.
     */
    private List mConstituentJobs;

    /**
     * Boolean indicating whether a job has been fully rendered to an executable
     * job or not i.e the aggregated job has been mapped to the aggregator and
     * the constituent jobs have been gridstarted or not.
     */
    private boolean mHasBeenRenderedToExecutableForm;

    /**
     * Handle to the JobAggregator that created this job.
     */
    private JobAggregator mJobAggregator;

    /**
     * The default constructor.
     */
    public AggregatedJob() {
        super();
        mConstituentJobs = new ArrayList(3);
        mHasBeenRenderedToExecutableForm = false;
        this.mJobAggregator = null;
    }

    /**
     * The overloaded constructor.
     *
     * @param num  the number of constituent jobs
     */
    public AggregatedJob(int num) {
        this();
        mConstituentJobs = new ArrayList(num);
    }

    /**
     * The overloaded constructor.
     *
     * @param job the job whose shallow copy is created, and is the main job.
     * @param num the number of constituent jobs.
     */
    public AggregatedJob(Job job,int num) {
        super((Job)job.clone());
        mConstituentJobs = new ArrayList(num);
        mHasBeenRenderedToExecutableForm = false;
        this.mJobAggregator = null;
    }

    /**
     * Returns a boolean indicating whether a job has been rendered to an executable
     * form or not
     *
     * @return boolean
     */
    public boolean renderedToExecutableForm( ){
        return this.mHasBeenRenderedToExecutableForm;
    }


    /**
     * Returns a boolean indicating whether a job has been rendered to an executable
     * form or not
     *
     * @param value boolean to set to.
     */
    public void setRenderedToExecutableForm( boolean value ){
        this.mHasBeenRenderedToExecutableForm = value;
    }

    /**
     * Sets the JobAggregator that created this aggregated job.
     * Useful for rendering the job to an executable form later on.
     *
     * @param aggregator   handle to the JobAggregator used for aggregating the job
     */
    public void setJobAggregator( JobAggregator aggregator ){
        this.mJobAggregator = aggregator;
    }

    /**
     * Returns the JobAggregator that created this aggregated job.
     * Useful for rendering the job to an executable form later on.
     *
     * @return  JobAggregator
     */
    public JobAggregator getJobAggregator( ){
        return this.mJobAggregator;
    }

    /**
     * Adds a job to the aggregated job.
     *
     * @param job  the job to be added.
     */
    public void add(Job job){
        mConstituentJobs.add(job);
    }

    /**
     * Returns a new copy of the Object. The constituent jobs are also cloned.
     *
     * @return Object
     */
    public Object clone(){
        AggregatedJob newJob = new AggregatedJob((Job)super.clone(),
                                              mConstituentJobs.size());
        for(Iterator it = this.mConstituentJobs.iterator();it.hasNext();){
            newJob.add( (Job)(((Job)it.next()).clone()));
        }
        newJob.mHasBeenRenderedToExecutableForm = this.mHasBeenRenderedToExecutableForm;
        newJob.mJobAggregator = this.mJobAggregator;

        return newJob;
    }


    /**
     * Returns an iterator to the constituent jobs of the AggregatedJob.
     *
     * @return Iterator
     */
    public Iterator constituentJobsIterator(){
        return mConstituentJobs.iterator();
    }
    
    /**
     * Returns a job from a particular position in the list of constituent jobs
     * 
     * @param index    the index to retrieve from
     *
     * @return   a constituent job.
     */
    public Job getConstituentJob( int index ){
        return (Job) this.mConstituentJobs.get( index );
    }
    
    /**
     * Returns the number of constituent jobs.
     *
     * @return Iterator
     */
    public int numberOfConsitutentJobs(){
        return mConstituentJobs.size();
    }
    /**
     * Returns a textual description of the object.
     *
     * @return textual description of the job.
     */
    public String toString(){
        StringBuffer sb = new StringBuffer(32);
        sb.append("\n").append("[MAIN JOB]").append(super.toString());
        sb.append("\n").append("[CONSTITUENT JOBS]");
        int num = 0;
        for(Iterator it = mConstituentJobs.iterator();it.hasNext();++num){
            sb.append("\n").append("[CONSTITUENT JOB] :").append(num);
            sb.append(it.next());
        }
        return sb.toString();
    }

}
