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
package org.griphyn.vdl.workflow;

import edu.isi.pegasus.common.util.Currently;
import java.util.*;

/**
 * This class is the container for a Workflow record. The record itself contains information about
 * the job or jobs that ran.
 *
 * @author Jens-S. Vöckler
 * @author Mike Wilde
 * @version $Revision$
 */
public class WorkEntry implements Workflow, Cloneable {
    /** primary key: unique workflow identifier. */
    private long m_id = -1;

    /** secondary key, part 1: base directory */
    private String m_basedir;

    /** secondary key, part 2: VO group */
    private String m_vogroup;

    /** secondary key, part 3: workflow label */
    private String m_label;

    /** secondary key, part 4: run directory */
    private String m_run;

    /** workflow creator as an account name. */
    private String m_creator;

    /** workflow creation time as time stamp. */
    private Date m_ctime;

    /** workflow state as integer to represent future, current or past workflows. */
    private int m_state;

    /** last modification time of workflow state as time stamp. */
    private Date m_mtime;

    /** list of jobs associated with the workflow. */
    private Map m_jobMap;

    /**
     * Creates and returns a copy of this object.
     *
     * @return a new instance with a deep copy of everything.
     */
    public Object clone() {
        WorkEntry result =
                new WorkEntry(
                        getID(),
                        getBaseDirectory(),
                        getVOGroup(),
                        getWorkflowLabel(),
                        getRunDirectory());
        result.setCreator(getCreator());
        result.setCreationTime(getCreationTime());
        result.setState(getState());
        result.setModificationTime(getModificationTime());

        // now the tricky portion -- deep clone jobs
        for (Iterator i = iterateJob(); i.hasNext(); ) {
            result.addJob((JobStateEntry) ((JobStateEntry) i.next()).clone());
        }
        return result;
    }

    /** Default constructor, should not be used. */
    private WorkEntry() {
        m_jobMap = new TreeMap();
    }

    /**
     * Constructs another empty class.
     *
     * @param id is the workflow primary key
     */
    public WorkEntry(long id) {
        m_jobMap = new TreeMap();
        m_id = id;
    }

    /**
     * Constructs another empty class.
     *
     * @param id is the workflow primary key
     * @param basedir is the base directory
     * @param vogroup is the VO group identifier
     * @param label is the workflow label
     * @param run is the workflow run directory
     */
    public WorkEntry(long id, String basedir, String vogroup, String label, String run) {
        m_jobMap = new TreeMap();
        m_id = id;
        m_basedir = basedir;
        m_vogroup = vogroup;
        m_label = label;
        m_run = run;
    }

    /**
     * Accessor
     *
     * @see #setID(long)
     */
    public long getID() {
        return this.m_id;
    }

    /**
     * Accessor.
     *
     * @param id
     * @see #getID()
     */
    public void setID(long id) {
        this.m_id = id;
    }

    /**
     * Accessor
     *
     * @see #setBaseDirectory(String)
     */
    public String getBaseDirectory() {
        return this.m_basedir;
    }

    /**
     * Accessor.
     *
     * @param basedir
     * @see #getBaseDirectory()
     */
    public void setBaseDirectory(String basedir) {
        this.m_basedir = basedir;
    }

    /**
     * Accessor
     *
     * @see #setVOGroup(String)
     */
    public String getVOGroup() {
        return this.m_vogroup;
    }

    /**
     * Accessor.
     *
     * @param vogroup
     * @see #getVOGroup()
     */
    public void setVOGroup(String vogroup) {
        this.m_vogroup = vogroup;
    }

    /**
     * Accessor
     *
     * @see #setWorkflowLabel(String)
     */
    public String getWorkflowLabel() {
        return this.m_label;
    }

    /**
     * Accessor.
     *
     * @param label
     * @see #getWorkflowLabel()
     */
    public void setWorkflowLabel(String label) {
        this.m_label = label;
    }

    /**
     * Accessor
     *
     * @see #setRunDirectory(String)
     */
    public String getRunDirectory() {
        return this.m_run;
    }

    /**
     * Accessor.
     *
     * @param run
     * @see #getRunDirectory()
     */
    public void setRunDirectory(String run) {
        this.m_run = run;
    }

    /**
     * Accessor
     *
     * @see #setCreator(String)
     */
    public String getCreator() {
        return this.m_creator;
    }

    /**
     * Accessor.
     *
     * @param creator
     * @see #getCreator()
     */
    public void setCreator(String creator) {
        this.m_creator = creator;
    }

    /**
     * Accessor
     *
     * @see #setCreationTime(Date)
     */
    public Date getCreationTime() {
        return this.m_ctime;
    }

    /**
     * Accessor.
     *
     * @param ctime
     * @see #getCreationTime()
     */
    public void setCreationTime(Date ctime) {
        this.m_ctime = ctime;
    }

    /**
     * Accessor
     *
     * @see #setState(int)
     */
    public int getState() {
        return this.m_state;
    }

    /**
     * Accessor.
     *
     * @param state
     * @see #getState()
     */
    public void setState(int state) {
        this.m_state = state;
    }

    /**
     * Accessor
     *
     * @see #setModificationTime(Date)
     */
    public Date getModificationTime() {
        return this.m_mtime;
    }

    /**
     * Accessor.
     *
     * @param mtime
     * @see #getModificationTime()
     */
    public void setModificationTime(Date mtime) {
        this.m_mtime = mtime;
    }

    /**
     * Accessor: Adds a job to the bag of jobs.
     *
     * @param job is the job to add.
     * @see JobStateEntry
     */
    public void addJob(JobStateEntry job) {
        this.m_jobMap.put(job.getID(), job);
    }

    /**
     * Accessor: Obtains an job by the job id.
     *
     * @param id is the job identifier in the workflow.
     * @return a reference to the job of this name.
     * @see JobStateEntry
     */
    public JobStateEntry getJob(String id) {
        return (JobStateEntry) this.m_jobMap.get(id);
    }

    /**
     * Accessor: Counts the number of jobs in this workflow.
     *
     * @return the number of jobs in the internal bag.
     */
    public int getJobCount() {
        return this.m_jobMap.size();
    }

    /**
     * Accessor: A list of all jobs known to this workflow. This list is read-only.
     *
     * @return an array with <code>JobStateEntry</code> elements.
     * @see JobStateEntry
     */
    public java.util.List getJobList() {
        return Collections.unmodifiableList(new ArrayList(this.m_jobMap.values()));
    }

    /**
     * Accessor: Obtains all jobs references by their id. The map is a read-only map to avoid
     * modifications outside the API.
     *
     * @return a map with all jobs known to this workflow
     * @see JobStateEntry
     */
    public java.util.Map getJobMap() {
        return Collections.unmodifiableMap(this.m_jobMap);
    }

    /**
     * Accessor: Provides an iterator over the jobs of this workflow.
     *
     * @return an iterator to walk the <code>JobStateEntry</code> list with.
     * @see JobStateEntry
     */
    public Iterator iterateJob() {
        return this.m_jobMap.values().iterator();
    }

    /** Accessor: Removes all jobs. Effectively empties the bag. */
    public void removeAllJob() {
        this.m_jobMap.clear();
    }

    /**
     * Accessor: Removes a specific job.
     *
     * @param name is the job identifier.
     * @return the object that was removed, or null, if not found.
     * @see JobStateEntry
     */
    public JobStateEntry removeJob(String name) {
        return (JobStateEntry) this.m_jobMap.remove(name);
    }

    /**
     * Accessor: Adds a new or overwrites an existing actual argument.
     *
     * @param job is a new complete job to enter.
     * @see JobStateEntry
     */
    public void setJob(JobStateEntry job) {
        this.m_jobMap.put(job.getID(), job);
    }

    /**
     * Accessor: Replaces all jobs with the new collection of jobs. Note that a collection can be
     * anything in a list or set.
     *
     * @param jobs is a collection of jobs.
     * @see JobStateEntry
     */
    public void setJob(Collection jobs) {
        this.m_jobMap.clear();
        for (Iterator i = jobs.iterator(); i.hasNext(); ) {
            JobStateEntry j = (JobStateEntry) i.next();
            this.m_jobMap.put(j.getID(), j);
        }
    }

    /**
     * Accessor: Replaces all job mappings with new job mappings.
     *
     * @param jobs is a map with job id to job mappings.
     * @see JobStateEntry
     */
    public void setJob(Map jobs) {
        this.m_jobMap.clear();
        this.m_jobMap.putAll(jobs);
    }

    /**
     * Constructs a sensible line of all internal data points
     *
     * @return a line containing all internal data.
     */
    public String toString() {
        String newline = System.getProperty("line.separator", "\r\n");
        StringBuffer result = new StringBuffer(80 * (1 + m_jobMap.size()));

        // the workflow stuff
        result.append(getID()).append('|');
        result.append(getBaseDirectory()).append('|');
        result.append(getVOGroup()).append('|');
        result.append(getWorkflowLabel()).append('|');
        result.append(getRunDirectory()).append('|');
        result.append(getCreator()).append('|');
        ;
        result.append(Currently.iso8601(false, false, false, getCreationTime()));
        result.append('|');
        result.append(Integer.toString(getState())).append('|');
        ;
        result.append(Currently.iso8601(false, false, false, getModificationTime()));
        result.append(newline);

        // add all jobs with space ahead
        for (Iterator i = iterateJob(); i.hasNext(); ) {
            JobStateEntry j = (JobStateEntry) i.next();
            result.append("  ").append(j).append(newline);
        }

        return result.toString();
    }
}
