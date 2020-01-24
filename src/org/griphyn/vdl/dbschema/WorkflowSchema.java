/*
 * This file or a portion of this file is licensed under the terms of
 * the Globus Toolkit Public License, found in file ../GTPL, or at
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
package org.griphyn.vdl.dbschema;

import java.io.*;
import java.lang.reflect.*;
import java.sql.*;
import java.util.*;
import org.griphyn.vdl.util.Logging;
import org.griphyn.vdl.workflow.*;

/**
 * This class provides basic functionalities to interact with the backend database for workflow
 * records. Currently, only searches that fill the workflow class are implemented.
 *
 * @author Jens-S. Vöckler
 * @author Mike Wilde
 * @version $Revision$
 */
public class WorkflowSchema extends DatabaseSchema implements WF {
    /**
     * Default constructor for access to the WF set of tables.
     *
     * @param dbDriverName is the database driver name
     */
    public WorkflowSchema(String dbDriverName)
            throws ClassNotFoundException, NoSuchMethodException, InstantiationException,
                    IllegalAccessException, InvocationTargetException, SQLException, IOException {
        // load the driver from the properties
        super(dbDriverName, PROPERTY_PREFIX);
        Logging.instance().log("dbschema", 3, "done with parent schema c'tor");

        // Note: Does not rely on optional JDBC3 features
        this.m_dbdriver.insertPreparedStatement("work.select.all", "SELECT * FROM wf_work");
        this.m_dbdriver.insertPreparedStatement(
                "work.select.mtime", "SELECT * FROM wf_work WHERE mtime >= ?");
        this.m_dbdriver.insertPreparedStatement(
                "work.select.sk",
                "SELECT * FROM wf_work WHERE basedir=? AND vogroup=? "
                        + "AND workflow=? AND run=?");

        this.m_dbdriver.insertPreparedStatement("job.select.all", "SELECT * FROM wf_jobstate");
        this.m_dbdriver.insertPreparedStatement(
                "job.select.mtime",
                "SELECT * FROM wf_jobstate WHERE wfid IN "
                        + "( SELECT id FROM wf_work WHERE mtime >= ? )");
        this.m_dbdriver.insertPreparedStatement(
                "job.select.sk", "SELECT * FROM wf_jobstate WHERE wfid=? AND jobid=?");
    }

    /**
     * Converts a regular datum into an SQL timestamp.
     *
     * @param date is a regular Java date
     * @return a SQL timestamp obtained from the Date.
     */
    protected java.sql.Timestamp toStamp(java.util.Date date) {
        return new java.sql.Timestamp(date.getTime());
    }

    /**
     * Converts a SQL timestamp into a regular datum.
     *
     * @param date is SQL timestamp from the database
     * @return a regular Java date
     */
    protected java.util.Date fromStamp(java.sql.Timestamp date) {
        return new java.util.Date(date.getTime());
    }

    /**
     * Obtains all jobs that belong to a particular workflow.
     *
     * @param wfid is the workflow identifier for jobs.
     * @return a list of all jobs
     */
    private java.util.List getAllJobs(long wfid) throws SQLException {
        java.util.List result = new ArrayList();
        Logging.instance().log("xaction", 1, "START load jobs for work");
        PreparedStatement ps = m_dbdriver.getPreparedStatement("job.select.work");

        if (m_dbdriver.preferString()) ps.setString(1, Long.toString(wfid));
        else ps.setLong(1, wfid);
        ResultSet rs = ps.executeQuery();
        Logging.instance().log("xaction", 1, "INTER load jobs for work");

        while (rs.next()) {
            JobStateEntry j = new JobStateEntry(wfid, rs.getString("id"));
            j.setState(rs.getString("state"));
            j.setModificationTime(fromStamp(rs.getTimestamp("mtime")));
            j.setSite(rs.getString("site"));
            result.add(j);
        }
        rs.close();

        Logging.instance().log("xaction", 1, "FINAL load jobs for work");
        return result;
    }

    /**
     * Converts the output of a result set into a workflow
     *
     * @param rs is the result set of a query, which is better valid.
     * @param withJobs if true, also add the jobs, if false, no jobs.
     * @return a workflow instance created from the result set.
     */
    protected WorkEntry convertResultSet(ResultSet rs, boolean withJobs) throws SQLException {
        WorkEntry result =
                new WorkEntry(
                        rs.getLong("id"),
                        rs.getString("basedir"),
                        rs.getString("vogroup"),
                        rs.getString("workflow"),
                        rs.getString("run"));
        result.setCreator(rs.getString("creator"));
        result.setCreationTime(fromStamp(rs.getTimestamp("ctime")));
        result.setState(rs.getInt("state"));
        result.setModificationTime(fromStamp(rs.getTimestamp("mtime")));

        if (withJobs) result.setJob(getAllJobs(rs.getLong("id")));

        return result;
    }

    /**
     * Load a single workflow from the backend database into a Java object. The identification is
     * based on the secondary key quadruple.
     *
     * @param basedir is the base directory
     * @param vogroup is the VO group identifier
     * @param label is the workflow label
     * @param run is the workflow run directory
     * @return the Workflow that was matched by the id, which may be null
     */
    public WorkEntry getWorkflow(String basedir, String vogroup, String label, String run)
            throws SQLException {
        WorkEntry result = null;
        Logging.instance().log("xaction", 1, "START load work sk");

        PreparedStatement ps = m_dbdriver.getPreparedStatement("work.select.sk");
        int i = 1;
        ps.setString(i++, basedir);
        ps.setString(i++, vogroup);
        ps.setString(i++, label);
        ps.setString(i++, run);

        ResultSet rs = ps.executeQuery();
        Logging.instance().log("xaction", 1, "INTER load work sk");

        if (rs.next()) {
            result = convertResultSet(rs, true);
        } else {
            Logging.instance().log("wf", 0, "No workflows found");
        }
        rs.close();

        Logging.instance().log("xaction", 1, "FINAL load work sk");
        return result;
    }

    /**
     * Loads all workflows that are fresh enough and returns a map of workflows matching. The list
     * is indexed by the primary key of the WF table, which is the unique workflow id.
     *
     * @param mtime is the oldest last modification time. Use <code>null</code> for all.
     * @return a map of workflows, indexed by their workflow id.
     */
    public java.util.Map getWorkflows(java.util.Date mtime) throws SQLException {
        java.util.Map result = new TreeMap();
        Logging.instance().log("xaction", 1, "START load all work");

        PreparedStatement ps = null;
        if (mtime == null) {
            ps = m_dbdriver.getPreparedStatement("work.select.all");
        } else {
            ps = m_dbdriver.getPreparedStatement("work.select.mtime");
            ps.setTimestamp(1, toStamp(mtime));
        }

        ResultSet rs = ps.executeQuery();
        Logging.instance().log("xaction", 1, "INTER load all work");

        while (rs.next()) {
            // insert workflows without job state
            result.put(new Long(rs.getLong("id")), convertResultSet(rs, false));
        }
        rs.close();

        if (result.size() > 0) {
            // now add job state
            Logging.instance().log("xaction", 1, "START load all jobs");
            if (mtime == null) {
                ps = m_dbdriver.getPreparedStatement("job.select.all");
            } else {
                ps = m_dbdriver.getPreparedStatement("job.select.mtime");
                ps.setTimestamp(1, toStamp(mtime));
            }

            rs = ps.executeQuery();
            Logging.instance().log("xaction", 1, "INTER load all jobs");

            while (rs.next()) {
                Long key = new Long(rs.getLong("wfid"));
                JobStateEntry job = new JobStateEntry(rs.getLong("wfid"), rs.getString("jobid"));
                job.setState(rs.getString("state"));
                job.setModificationTime(fromStamp(rs.getTimestamp("mtime")));
                job.setSite(rs.getString("site"));

                if (result.containsKey(key)) {
                    ((WorkEntry) (result.get(key))).addJob(job);
                }
            }
            Logging.instance().log("xaction", 1, "FINAL load all jobs");
        } else {
            Logging.instance().log("wf", 0, "No workflows found");
        }

        Logging.instance().log("xaction", 1, "FINAL load all work");
        return result;
    }
}
