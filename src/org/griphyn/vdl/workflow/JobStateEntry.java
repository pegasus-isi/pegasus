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
 * This class is the container for a job state record.
 *
 * <p>When constructing a job from piece-meal, please note that setting the job's state will
 * automatically set the job's last modification time to the current time. If this is not desired,
 * you must set the modification time after setting the state. However, none of the constructors
 * will set the modification to the current time (yet).
 *
 * @author Jens-S. Vöckler
 * @author Mike Wilde
 * @version $Revision$
 */
public class JobStateEntry implements Workflow, Cloneable {
    /** primary key: which workflow do we belong to. If -1 then unknown. */
    private long m_wfid = -1;

    /** primary key: unique job identifier within the workflow */
    private String m_id;

    /** the Condor state of the job. */
    private String m_state;

    /** the last modification time of the job state. */
    private Date m_mtime;

    /** the resource where the job ran, may be unspecified. */
    private String m_site = null;

    /**
     * Creates and returns a copy of this object.
     *
     * @return a new instance.
     */
    public Object clone() {
        JobStateEntry result = new JobStateEntry(getWorkflowID(), getID());
        result.setState(getState());
        result.setModificationTime(getModificationTime());
        result.setSite(getSite());
        return result;
    }

    /** Default contructor. */
    public JobStateEntry() {
        // empty
    }

    /**
     * Constructs an other empty class.
     *
     * @param wfid is the workflow primary key
     */
    public JobStateEntry(long wfid) {
        m_wfid = wfid;
    }

    /**
     * Constructs an other empty class.
     *
     * @param wfid is the workflow primary key
     * @param jobid is the job identifier within the workflow
     */
    public JobStateEntry(long wfid, String jobid) {
        m_wfid = wfid;
        m_id = jobid;
    }

    /**
     * Constructs an other empty class. It will set the workflow identifier to -1 to indicate no
     * connection.
     *
     * @param jobid is the job identifier within the workflow
     */
    public JobStateEntry(String jobid) {
        m_wfid = -1;
        m_id = jobid;
    }

    /**
     * Accessor
     *
     * @see #setWorkflowID(long)
     * @return this job's workflow identifier.
     */
    public long getWorkflowID() {
        return this.m_wfid;
    }

    /**
     * Accessor.
     *
     * @param wfid is the new workflow id as positive number.
     * @see #getWorkflowID()
     */
    public void setWorkflowID(long wfid) {
        if (wfid < 0) throw new RuntimeException("negative workflow id");
        this.m_wfid = wfid;
    }

    /**
     * Accessor
     *
     * @see #setID(String)
     * @return the job identifier
     */
    public String getID() {
        return this.m_id;
    }

    /**
     * Accessor.
     *
     * @param id is the new job id, must not be <code>null</code>.
     * @see #getID()
     */
    public void setID(String id) {
        if (id == null) throw new NullPointerException();
        this.m_id = id;
    }

    /**
     * Accessor
     *
     * @see #setState(String)
     * @return the Condor job state string with some extensions.
     */
    public String getState() {
        return this.m_state;
    }

    /**
     * Accessor. As a side effect, setting the job state will set the current modification time to
     * the current time.
     *
     * @param state is the new Condor job state, must not be <code>null</code>.
     * @see #getState()
     */
    public void setState(String state) {
        if (state == null) throw new NullPointerException();
        this.m_state = state;
        this.m_mtime = new Date();
    }

    /**
     * Accessor
     *
     * @see #setModificationTime(Date)
     * @return the last modification time of any of this job's state.
     */
    public Date getModificationTime() {
        return this.m_mtime;
    }

    /**
     * Accessor.
     *
     * @param mtime is the new last modification time of this job, must not be <code>null</code>.
     * @see #getModificationTime()
     */
    public void setModificationTime(Date mtime) {
        if (mtime == null) throw new NullPointerException();
        this.m_mtime = mtime;
    }

    /**
     * Accessor
     *
     * @see #setSite(String)
     */
    public String getSite() {
        return this.m_site;
    }

    /**
     * Accessor.
     *
     * @param site is the new remote site, may be <code>null</code>.
     * @see #getSite()
     */
    public void setSite(String site) {
        this.m_site = site;
    }

    /**
     * Constructs a simple line of all internal data points. Adjust to your requirements - this is
     * an example, only.
     *
     * @return a line containing all internal data.
     */
    public String toString() {
        StringBuffer result = new StringBuffer(80);

        result.append(Long.toString(m_wfid)).append('|');
        result.append(m_id).append('|');
        result.append(m_state).append('|');
        result.append(Currently.iso8601(false, true, false, m_mtime)).append('|');
        result.append(m_site == null ? "NULL" : m_site);

        return result.toString();
    }
}
