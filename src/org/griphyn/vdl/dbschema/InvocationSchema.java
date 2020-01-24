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

import edu.isi.pegasus.common.util.Separator;
import edu.isi.pegasus.planner.invocation.Architecture;
import edu.isi.pegasus.planner.invocation.Arguments;
import edu.isi.pegasus.planner.invocation.HasDescriptor;
import edu.isi.pegasus.planner.invocation.HasFilename;
import edu.isi.pegasus.planner.invocation.HasText;
import edu.isi.pegasus.planner.invocation.InvocationRecord;
import edu.isi.pegasus.planner.invocation.Job;
import edu.isi.pegasus.planner.invocation.JobStatus;
import edu.isi.pegasus.planner.invocation.StatCall;
import edu.isi.pegasus.planner.invocation.StatInfo;
import edu.isi.pegasus.planner.invocation.Status;
import edu.isi.pegasus.planner.invocation.Usage;
import java.io.*;
import java.lang.reflect.*;
import java.net.InetAddress;
import java.sql.*;
import java.util.*;
import org.griphyn.vdl.util.Logging;

/**
 * This class provides basic functionalities to interact with the backend database for invocation
 * records, such as insertion, deletion, and search.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 */
public class InvocationSchema extends DatabaseSchema implements PTC {
    /**
     * Default constructor for the provenance tracking.
     *
     * @param dbDriverName is the database driver name
     */
    public InvocationSchema(String dbDriverName)
            throws ClassNotFoundException, NoSuchMethodException, InstantiationException,
                    IllegalAccessException, InvocationTargetException, SQLException, IOException {
        // load the driver from the properties
        super(dbDriverName, PROPERTY_PREFIX);
        Logging.instance().log("dbschema", 3, "done with parent schema c'tor");

        // Note: Does not rely on optional JDBC3 features
        this.m_dbdriver.insertPreparedStatement(
                "stmt.save.uname",
                "INSERT INTO ptc_uname(id,archmode,sysname,os_release,machine) "
                        + "VALUES (?,?,?,?,?)");
        this.m_dbdriver.insertPreparedStatement(
                "stmt.save.rusage",
                "INSERT INTO ptc_rusage(id,utime,stime,minflt,majflt,nswaps,"
                        + "nsignals,nvcsw,nivcsw) VALUES (?,?,?,?,?,?,?,?,?)");
        this.m_dbdriver.insertPreparedStatement(
                "stmt.save.stat",
                "INSERT INTO ptc_stat(id,errno,fname,fdesc,size,mode,inode,atime,"
                        + "ctime,mtime,uid,gid) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)");
        this.m_dbdriver.insertPreparedStatement(
                "stmt.save.ivr",
                "INSERT INTO ptc_invocation(id,creator,creationtime,wf_label,"
                        + "wf_time,version,start,duration,tr_namespace,tr_name,tr_version,"
                        + "dv_namespace,dv_name,dv_version,resource,host,pid,"
                        + "uid,gid,cwd,arch,total)"
                        + "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");

        this.m_dbdriver.insertPreparedStatement(
                "stmt.save.job",
                "INSERT INTO ptc_job(id,type,start,duration,pid,rusage,stat,"
                        + "exitcode,exit_msg,args) VALUES (?,?,?,?,?,?,?,?,?,?)");
        this.m_dbdriver.insertPreparedStatement(
                "stmt.save.lfn", "INSERT INTO ptc_lfn(id,stat,initial,lfn) VALUES (?,?,?,?)");

        this.m_dbdriver.insertPreparedStatement(
                "stmt.select.ivr.sk",
                "SELECT id FROM ptc_invocation WHERE start=? AND host=? AND pid=?");
        this.m_dbdriver.insertPreparedStatement(
                "stmt.select.uname.sk",
                "SELECT id FROM ptc_uname WHERE archmode=? AND sysname=? "
                        + "AND os_release=? AND machine=?");
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
     * Checks the existence of an invocation record in the database. The information is based on the
     * (start,host,pid) tuple, although with private networks, cases may arise that have this tuple
     * identical, yet are different.
     *
     * @param start is the start time of the grid launcher
     * @param host is the address of the host it ran upon
     * @param pid is the process id of the grid launcher itself.
     * @return the id of the existing record, or -1
     */
    public long getInvocationID(java.util.Date start, InetAddress host, int pid)
            throws SQLException {
        long result = -1;
        Logging.instance().log("xaction", 1, "START select invocation id");

        PreparedStatement ps = m_dbdriver.getPreparedStatement("stmt.select.ivr.sk");

        int i = 1;
        ps.setTimestamp(i++, toStamp(start));
        ps.setString(i++, host.getHostAddress());
        ps.setInt(i++, pid);

        Logging.instance().log("chunk", 2, "SELECT id FROM invocation");
        ResultSet rs = ps.executeQuery();
        if (rs.next()) result = rs.getLong(1);
        rs.close();
        Logging.instance().log("xaction", 1, "FINAL select invocation id");
        return result;
    }

    /**
     * Determines the id of an existing identical architecture, or creates a new entry.
     *
     * @param arch is the architecture description
     * @return the id of the architecture, either new or existing.
     */
    public long saveArchitecture(Architecture arch) throws SQLException {
        long result = -1;
        Logging.instance().log("xaction", 1, "START select uname id");

        int i = 1;
        PreparedStatement ps = m_dbdriver.getPreparedStatement("stmt.select.uname.sk");
        stringOrNull(ps, i++, arch.getArchMode());
        stringOrNull(ps, i++, arch.getSystemName());
        stringOrNull(ps, i++, arch.getRelease());
        stringOrNull(ps, i++, arch.getMachine());

        Logging.instance().log("chunk", 2, "SELECT id FROM uname");
        ResultSet rs = ps.executeQuery();
        if (rs.next()) result = rs.getLong(1);
        rs.close();
        Logging.instance().log("xaction", 1, "FINAL select uname id");

        if (result == -1) {
            // nothing found, need to really insert things
            Logging.instance().log("xaction", 1, "START save uname");

            try {
                result = m_dbdriver.sequence1("uname_id_seq");
            } catch (SQLException e) {
                Logging.instance()
                        .log("app", 0, "During rusage sequence number: " + e.toString().trim());
                Logging.instance().log("xaction", 1, "START rollback");
                m_dbdriver.rollback();
                Logging.instance().log("xaction", 1, "FINAL rollback");
                throw e; // re-throw
            }

            // add ID explicitely from sequence to insertion
            ps = m_dbdriver.getPreparedStatement("stmt.save.uname");
            i = 1;
            longOrNull(ps, i++, result);

            stringOrNull(ps, i++, arch.getArchMode());
            stringOrNull(ps, i++, arch.getSystemName());
            stringOrNull(ps, i++, arch.getRelease());
            stringOrNull(ps, i++, arch.getMachine());

            // save prepared values
            Logging.instance().log("chunk", 2, "INSERT INTO uname");
            try {
                int rc = ps.executeUpdate();
                if (result == -1) result = m_dbdriver.sequence2(ps, "uname_id_seq", 1);
            } catch (SQLException e) {
                // race condition possibility: try once more to find info
                result = -1;
                Logging.instance().log("xaction", 1, "START select uname id");

                i = 1;
                ps = m_dbdriver.getPreparedStatement("stmt.select.uname.sk");
                stringOrNull(ps, i++, arch.getArchMode());
                stringOrNull(ps, i++, arch.getSystemName());
                stringOrNull(ps, i++, arch.getRelease());
                stringOrNull(ps, i++, arch.getMachine());

                Logging.instance().log("chunk", 2, "SELECT id FROM uname");
                rs = ps.executeQuery();
                if (rs.next()) result = rs.getLong(1);
                rs.close();
                Logging.instance().log("xaction", 1, "FINAL select uname id");

                if (result == -1) {
                    Logging.instance()
                            .log("app", 0, "While inserting into rusage: " + e.toString().trim());
                    // rollback in saveInvocation()
                    m_dbdriver.cancelPreparedStatement("stmt.save.uname");
                    throw e; // re-throw
                }
            }
            Logging.instance().log("xaction", 1, "FINAL save uname: ID=" + result);
        }

        // done
        return result;
    }

    /**
     * Inserts an invocation record into the database.
     *
     * @param ivr is the invocation record to store.
     * @return true, if insertion was successful, false otherwise.
     */
    public boolean saveInvocation(InvocationRecord ivr) throws SQLException {
        // big outer try
        try {
            long id = -1;
            try {
                id = m_dbdriver.sequence1("invocation_id_seq");
            } catch (SQLException e) {
                Logging.instance()
                        .log("app", 0, "During IVR sequence number: " + e.toString().trim());
                throw e; // re-throw
            }

            // add ID explicitely from sequence to insertion
            Logging.instance().log("xaction", 1, "START save invocation");
            PreparedStatement ps = m_dbdriver.getPreparedStatement("stmt.save.ivr");
            int i = 1;
            longOrNull(ps, i++, id);

            // current_user()
            stringOrNull(ps, i++, System.getProperty("user.name"));

            // now()
            ps.setTimestamp(i++, toStamp(new java.util.Date()));

            // wf_label, wf_time: not available at the moment...
            if (ivr.getWorkflowLabel() == null) ps.setNull(i++, Types.VARCHAR);
            else ps.setString(i++, ivr.getWorkflowLabel());

            if (ivr.getWorkflowTimestamp() == null) ps.setNull(i++, Types.TIMESTAMP);
            else ps.setTimestamp(i++, toStamp(ivr.getWorkflowTimestamp()));

            // version
            ps.setString(i++, ivr.getVersion());

            // start, duration
            ps.setTimestamp(i++, toStamp(ivr.getStart()));
            ps.setDouble(i++, ivr.getDuration());

            // TR
            i = splitDefinition(ps, ivr.getTransformation(), i);

            // DV: not available at the moment
            i = splitDefinition(ps, ivr.getDerivation(), i);

            // resource (site handle)
            if (ivr.getResource() == null) ps.setNull(i++, Types.VARCHAR);
            else ps.setString(i++, ivr.getResource());

            // host
            ps.setString(i++, ivr.getHostAddress().getHostAddress());

            // [pug]id
            ps.setInt(i++, ivr.getPID());
            ps.setInt(i++, ivr.getUID());
            ps.setInt(i++, ivr.getGID());

            // cwd
            stringOrNull(ps, i++, ivr.getWorkingDirectory().getValue());

            // uname
            ps.setLong(i++, saveArchitecture(ivr.getArchitecture()));

            // save usage and remember id
            ps.setLong(i++, saveUsage(ivr.getUsage()));

            // save prepared values
            Logging.instance().log("chunk", 2, "INSERT INTO invocation");

            int rc = ps.executeUpdate();
            if (id == -1) id = m_dbdriver.sequence2(ps, "invocation_id_seq", 1);
            Logging.instance().log("xaction", 1, "FINAL save invocation: ID=" + id);

            // save jobs belonging to invocation
            for (Iterator j = ivr.iterateJob(); j.hasNext(); ) {
                saveJob(id, ((Job) j.next()));
            }

            // jsv 20050815: more stat info for Prophesy
            for (Iterator j = ivr.iterateStatCall(); j.hasNext(); ) {
                StatCall s = (StatCall) j.next();
                String sch = s.getHandle().toLowerCase();
                if (sch.equals("initial") || sch.equals("final")) {
                    saveLFN(id, s);
                }
            }

            // done
            m_dbdriver.commit();
            return true;
        } catch (SQLException e) {
            // show complete exception chain
            for (SQLException walk = e; walk != null; walk = walk.getNextException()) {
                Logging.instance()
                        .log(
                                "app",
                                0,
                                walk.getSQLState()
                                        + ": "
                                        + walk.getErrorCode()
                                        + ": "
                                        + walk.getMessage().trim());

                StackTraceElement[] ste = walk.getStackTrace();
                for (int n = 0; n < 5 && n < ste.length; ++n) {
                    Logging.instance().log("app", 0, ste[n].toString());
                }
            }

            Logging.instance().log("xaction", 1, "START rollback");
            m_dbdriver.cancelPreparedStatement("stmt.save.ivr");
            m_dbdriver.rollback();
            Logging.instance().log("xaction", 1, "FINAL rollback");
            throw e; // re-throw
        }
    }

    /**
     * Splits the canonical FQDN of a definition into its components, and save each component into
     * the database.
     *
     * @param in is the canonical form.
     * @param i is the current position in the invocation prepared stmt.
     * @return the updated position (usually i+3)
     */
    private int splitDefinition(PreparedStatement ps, String in, int i) throws SQLException {
        if (in == null) {
            // no input, insert all NULL values
            ps.setNull(i++, Types.VARCHAR);
            ps.setNull(i++, Types.VARCHAR);
            ps.setNull(i++, Types.VARCHAR);
        } else {
            // there is input after all, insert appropriate values
            String ns = null;
            String vs = null;

            // separate namespace
            int p1 = in.indexOf(Separator.NAMESPACE);
            if (p1 == -1) {
                // no namespace
                p1 = 0;
            } else {
                // there is a namespace
                ns = in.substring(0, p1);
                p1 += Separator.NAMESPACE.length();
            }

            // separate version
            int p2 = in.indexOf(Separator.NAME, p1);
            if (p2 == -1) {
                // no version attached
                p2 = in.length();
            } else {
                vs = in.substring(p2 + Separator.NAME.length());
            }

            // separate identifier -- this is a must-have
            String nm = in.substring(p1, p2);

            stringOrNull(ps, i++, ns);
            stringOrNull(ps, i++, nm);
            stringOrNull(ps, i++, vs);
        }

        // done
        return i;
    }

    /**
     * Helper function to insert a chunk of the invocation record. This piece deals with the rusage
     * information.
     *
     * @param u is the usage record to insert into the database
     * @return the sequence number under which it was inserted.
     * @exception SQLException if something goes awry during insertion.
     */
    protected long saveUsage(Usage u) throws SQLException {
        if (u == null) throw new RuntimeException("usage record is null");

        long id = -1;
        try {
            id = m_dbdriver.sequence1("rusage_id_seq");
        } catch (SQLException e) {
            Logging.instance()
                    .log("app", 0, "During rusage sequence number: " + e.toString().trim());
            Logging.instance().log("xaction", 1, "START rollback");
            m_dbdriver.rollback();
            Logging.instance().log("xaction", 1, "FINAL rollback");
            throw e; // re-throw
        }

        // add ID explicitely from sequence to insertion
        Logging.instance().log("xaction", 1, "START save rusage");
        PreparedStatement ps = m_dbdriver.getPreparedStatement("stmt.save.rusage");
        int i = 1;
        longOrNull(ps, i++, id);

        // add rest of rusage record
        ps.setDouble(i++, u.getUserTime());
        ps.setDouble(i++, u.getSystemTime());
        ps.setInt(i++, u.getMinorFaults());
        ps.setInt(i++, u.getMajorFaults());
        ps.setInt(i++, u.getSwaps());
        ps.setInt(i++, u.getSignals());
        ps.setInt(i++, u.getVoluntarySwitches());
        ps.setInt(i++, u.getInvoluntarySwitches());

        // save prepared values
        Logging.instance().log("chunk", 2, "INSERT INTO rusage");
        try {
            ps.executeUpdate();
            if (id == -1) id = m_dbdriver.sequence2(ps, "rusage_id_seq", 1);
        } catch (SQLException e) {
            Logging.instance().log("app", 0, "While inserting into rusage: " + e.toString().trim());
            // rollback in saveInvocation()
            this.m_dbdriver.cancelPreparedStatement("stmt.save.rusage");
            throw e; // re-throw
        }

        // done
        Logging.instance().log("xaction", 1, "FINAL save rusage: ID=" + id);
        return id;
    }

    /**
     * Helper function to insert a chunk of the invocation record. This piece deals with the stat
     * and fstat information.
     *
     * @param s is the stat record to insert into the database
     * @return the sequence number under which it was inserted.
     * @exception SQLException if something goes awry during insertion.
     */
    protected long saveStat(StatCall s) throws SQLException {

        long id = -1;
        try {
            id = m_dbdriver.sequence1("stat_id_seq");
        } catch (SQLException e) {
            Logging.instance().log("app", 0, "During stat sequence number: " + e.toString().trim());
            Logging.instance().log("xaction", 1, "START rollback");
            m_dbdriver.rollback();
            Logging.instance().log("xaction", 1, "FINAL rollback");
            throw e; // re-throw
        }

        // add ID explicitely from sequence to insertion
        Logging.instance().log("xaction", 1, "START save stat");
        PreparedStatement ps = this.m_dbdriver.getPreparedStatement("stmt.save.stat");
        int i = 1;
        longOrNull(ps, i++, id);

        // add rest of stat record, as appropriate. Many things will stay
        // NULL during the fill-in of incomplete (failed) stat info
        ps.setInt(i++, s.getError());
        edu.isi.pegasus.planner.invocation.File f = s.getFile();
        if (f != null) {
            if (f instanceof HasFilename && ((HasFilename) f).getFilename() != null)
                ps.setString(i++, ((HasFilename) f).getFilename());
            else ps.setNull(i++, Types.VARCHAR);

            if (f instanceof HasDescriptor && ((HasDescriptor) f).getDescriptor() != -1)
                ps.setInt(i++, ((HasDescriptor) f).getDescriptor());
            else ps.setNull(i++, Types.INTEGER);
        } else {
            i += 2;
        }

        StatInfo si = s.getStatInfo();
        if (si != null) {
            ps.setLong(i++, si.getSize());
            ps.setInt(i++, si.getMode());
            ps.setLong(i++, si.getINode());

            ps.setTimestamp(i++, toStamp(si.getAccessTime()));
            ps.setTimestamp(i++, toStamp(si.getCreationTime()));
            ps.setTimestamp(i++, toStamp(si.getModificationTime()));

            ps.setInt(i++, si.getUID());
            ps.setInt(i++, si.getGID());
        } else {
            // bug fixed 20040908 jsv:
            // we don't know anything about those, so fill in NULL for rDBMS
            // that don't automagically default empty columns to NULL (sigh).
            ps.setNull(i++, Types.BIGINT);
            ps.setNull(i++, Types.INTEGER);
            ps.setNull(i++, Types.BIGINT);

            ps.setNull(i++, Types.TIMESTAMP);
            ps.setNull(i++, Types.TIMESTAMP);
            ps.setNull(i++, Types.TIMESTAMP);

            ps.setNull(i++, Types.INTEGER);
            ps.setNull(i++, Types.INTEGER);
        }

        // save prepared values
        Logging.instance().log("chunk", 2, "INSERT INTO stat");
        try {
            ps.executeUpdate();
            if (id == -1) id = m_dbdriver.sequence2(ps, "stat_id_seq", 1);
        } catch (SQLException e) {
            Logging.instance().log("app", 0, "While inserting into stat: " + e.toString().trim());
            // rollback in safeInvocation()
            this.m_dbdriver.cancelPreparedStatement("stmt.save.stat");
            throw e; // re-throw
        }

        // done
        Logging.instance().log("xaction", 1, "FINAL save stat: ID=" + id);
        return id;
    }

    /**
     * Helper function to insert a LFN PFN mapping stat call into the stat information records.
     *
     * @param iid is the invocation record id to which this job belongs.
     * @param s is an instance of a stat call from the initial or final list
     * @exception SQLException if something goes awry during insertion.
     */
    protected void saveLFN(long iid, StatCall s) throws SQLException {
        Logging.instance().log("xaction", 1, "START save lfn");

        PreparedStatement ps = m_dbdriver.getPreparedStatement("stmt.save.lfn");
        int i = 1;

        // add foreign ID explicitely
        ps.setLong(i++, iid);

        // stat (foreign key)
        ps.setLong(i++, saveStat(s));

        // set the modifier before or after
        String id = s.getHandle().toLowerCase();
        if (id.equals("initial") || id.equals("final")) {
            ps.setString(i++, id.substring(0, 1));
        } else {
            ps.setNull(i++, Types.CHAR);
        }

        // set the LFN
        stringOrNull(ps, i++, s.getLFN());

        // save prepared values
        Logging.instance().log("chunk", 2, "INSERT INTO lfn");
        try {
            ps.executeUpdate();
        } catch (SQLException e) {
            Logging.instance().log("app", 0, "While inserting into lfn: " + e.toString().trim());
            // rollback in safeInvocation()
            m_dbdriver.cancelPreparedStatement("stmt.save.lfn");
            throw e; // re-throw
        }

        // done
        Logging.instance().log("xaction", 1, "FINAL save lfn");
    }

    /**
     * Helper function to insert a chunk of the invocation record. This piece deals with the jobs
     * themselves
     *
     * @param iid is the invocation record id to which this job belongs.
     * @param job is the job to insert.
     * @exception SQLException if something goes awry during insertion.
     */
    protected void saveJob(long iid, Job job) throws SQLException {
        Logging.instance().log("xaction", 1, "START save job");

        PreparedStatement ps = m_dbdriver.getPreparedStatement("stmt.save.job");
        int i = 1;

        // add foreign ID explicitely
        ps.setLong(i++, iid);

        // type
        String tag = job.getTag();
        if (tag.equals("mainjob")) {
            ps.setString(i++, "M");
        } else if (tag.equals("prejob")) {
            ps.setString(i++, "P");
        } else if (tag.equals("postjob")) {
            ps.setString(i++, "p");
        } else if (tag.equals("cleanup")) {
            ps.setString(i++, "c");
        } else if (tag.equals("setup")) {
            ps.setString(i++, "S");
        } else {
            throw new SQLException("illegal job type \"" + tag + "\"");
        }

        // start, duration
        ps.setTimestamp(i++, toStamp(job.getStart()));
        ps.setDouble(i++, job.getDuration());

        // pid
        ps.setInt(i++, job.getPID());

        // usage (foreign key)
        ps.setLong(i++, saveUsage(job.getUsage()));

        // stat (foreign key)
        ps.setLong(i++, saveStat(job.getExecutable()));

        // exitcode, exit_msg
        Status status = job.getStatus();
        ps.setInt(i++, status.getStatus());
        JobStatus js = status.getJobStatus();
        String msg = null;
        if (js instanceof HasText) msg = ((HasText) js).getValue();
        stringOrNull(ps, i++, msg);

        // args
        Arguments args = job.getArguments();
        stringOrNull(ps, i++, args.getValue());

        // save prepared values
        Logging.instance().log("chunk", 2, "INSERT INTO job");
        try {
            ps.executeUpdate();
        } catch (SQLException e) {
            Logging.instance().log("app", 0, "While inserting into job: " + e.toString().trim());
            // rollback in safeInvocation()
            m_dbdriver.cancelPreparedStatement("stmt.save.job");
            throw e; // re-throw
        }

        // done
        Logging.instance().log("xaction", 1, "FINAL save job");
    }
}
