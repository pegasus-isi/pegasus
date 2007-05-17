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
 * @version $Revision: 1.2 $
 */

public class AggregatedJob extends SubInfo {

    /**
     * The collection of jobs that are contained in the aggregated job.
     */
    private Collection mConstituentJobs;


    /**
     * The default constructor.
     */
    public AggregatedJob() {
        super();
        mConstituentJobs = new ArrayList(3);
    }

    /**
     * The overloaded constructor.
     *
     * @param num  the number of constituent jobs
     */
    public AggregatedJob(int num) {
        super();
        mConstituentJobs = new ArrayList(num);
    }

    /**
     * The overloaded constructor.
     *
     * @param job the job whose shallow copy is created, and is the main job.
     * @param num the number of constituent jobs.
     */
    public AggregatedJob(SubInfo job,int num) {
        super((SubInfo)job.clone());
        mConstituentJobs = new ArrayList(num);
    }

    /**
     * Adds a job to the aggregated job.
     *
     * @param job  the job to be added.
     */
    public void add(SubInfo job){
        mConstituentJobs.add(job);
    }

    /**
     * Returns a new copy of the Object. The constituent jobs are also cloned.
     *
     * @return Object
     */
    public Object clone(){
        AggregatedJob newJob = new AggregatedJob((SubInfo)super.clone(),
                                              mConstituentJobs.size());
        for(Iterator it = this.mConstituentJobs.iterator();it.hasNext();){
            newJob.add( (SubInfo)(((SubInfo)it.next()).clone()));
        }
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