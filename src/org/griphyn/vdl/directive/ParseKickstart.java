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

package org.griphyn.vdl.directive;

import edu.isi.pegasus.planner.invocation.Architecture;
import edu.isi.pegasus.planner.invocation.InvocationRecord;
import edu.isi.pegasus.planner.invocation.Job;
import edu.isi.pegasus.planner.invocation.JobStatus;
import edu.isi.pegasus.planner.invocation.JobStatusFailure;
import edu.isi.pegasus.planner.invocation.JobStatusRegular;
import edu.isi.pegasus.planner.invocation.JobStatusSignal;
import edu.isi.pegasus.planner.invocation.JobStatusSuspend;
import edu.isi.pegasus.planner.invocation.Status;
import edu.isi.pegasus.planner.parser.InvocationParser;
import java.io.*;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.MissingResourceException;
import org.griphyn.vdl.dbschema.*;
import org.griphyn.vdl.toolkit.FriendlyNudge;
import org.griphyn.vdl.util.ChimeraProperties;
import org.griphyn.vdl.util.Logging;

/**
 * Main objective of this class is to extract the exit status from the invocation record returned by
 * kickstart. The expected usage is another Java class passing a filename, and obtaining the cooked
 * exit status for the parse. All other details, like removing non-XML header and tailers,
 * de-concatenation, are handled internally.
 *
 * <p>Usage of the class is divided into typically three steps. The first step is to obtain an
 * instance of the the parser, and configure it to fit your needs.
 *
 * <p>
 *
 * <pre>
 * ParseKickstart pks = new ParseKickstart();
 * ... // set flags
 * pks.setDatabaseSchema( ptcschema );
 * </pre>
 *
 * The next step can be executed multiple times, and parse one or more kickstart output files.
 *
 * <p>
 *
 * <pre>
 * List result = null;
 * try {
 *   result = pks.parseFile( file );
 * } catch ( FriendlyNudge fn ) {
 *   // handle failures
 * }
 * </pre>
 *
 * Once you are definitely done, it is recommend to dis-associate yourself from the active database
 * connection.
 *
 * <p>
 *
 * <pre>
 * pks.close();
 * pks = null;
 * </pre>
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 * @see org.griphyn.vdl.toolkit.ExitCode
 * @see org.griphyn.vdl.parser.InvocationParser
 */
public class ParseKickstart extends Directive {
    /**
     * Determines, if an empty output record constitutes a failure or success. In old Globus 2.0,
     * empty output frequently occurred. With the NFS bug alleviation, while not fixed, it occurs a
     * lot less frequently.
     */
    private boolean m_emptyFail = true;

    /** Determines, if the invocation records go back into the VDC or not. */
    private boolean m_noDBase = false;

    /**
     * Determines, if the invocation records, when incurring a database failure, will fail the
     * application or not.
     */
    private boolean m_ignoreDBFail = false;

    /** The database schema driver used to connect to the PTC. */
    private DatabaseSchema m_dbschema = null;

    /**
     * Semi-singleton, dynamically instantiated once for the lifetime. The properties determine
     * which Xerces parser is being used.
     */
    private InvocationParser m_ip = null;

    /** Attaches a workflow label (tag) to all workflows passing thru. */
    private String m_wf_label = null;

    /** Attaches a workflow mtime to all workflows passing thru. */
    private Date m_wf_mtime = null;

    /** Default c'tor. */
    public ParseKickstart() throws IOException, MissingResourceException {
        super();
    }

    /**
     * C'tor which permits the setting of a PTC connection.
     *
     * @param dbschema is the database schema to use for the PTC.
     */
    public ParseKickstart(DatabaseSchema dbschema) throws IOException, MissingResourceException {
        super();
        if ((m_dbschema = dbschema) == null) m_noDBase = true;
    }

    /**
     * C'tor which permits the setting of a PTC connection.
     *
     * @param dbschema is the database schema to use for the PTC.
     * @param emptyFail determines, if empty input files are error or OK.
     */
    public ParseKickstart(DatabaseSchema dbschema, boolean emptyFail)
            throws IOException, MissingResourceException {
        super();
        if ((m_dbschema = dbschema) == null) m_noDBase = true;
        m_emptyFail = emptyFail;
    }

    /**
     * Sets the database schema.
     *
     * @param dbschema is a database schema instance for the PTC.
     */
    public void setDatabaseSchema(DatabaseSchema dbschema) {
        m_dbschema = dbschema;
    }

    /** Closes the associated database backend and invalidates the schema. */
    public void close() throws SQLException {
        if (m_dbschema != null) m_dbschema.close();
        m_dbschema = null;
        m_ip = null;
    }

    /**
     * Obtains the fail-on-empty-file value.
     *
     * @return true, if to fail on empty files.
     * @see #setEmptyFail( boolean )
     */
    public boolean getEmptyFail() {
        return m_emptyFail;
    }

    /**
     * Sets the fail-on-empty-file value.
     *
     * @param emptyFail contains the new value, if to fail on empty files.
     * @see #getEmptyFail()
     */
    public void setEmptyFail(boolean emptyFail) {
        m_emptyFail = emptyFail;
    }

    /**
     * Gets the variable to permit connections to the PTC, or use parse-only mode.
     *
     * @return true, if the PTC is intended to be used, false for parse-only mode.
     * @see #setNoDBase(boolean)
     */
    public boolean getNoDBase() {
        return this.m_noDBase;
    }

    /**
     * Sets the parse-only versus PTC mode.
     *
     * @param noDBase is true to use the parse-only mode.
     * @see #getNoDBase()
     */
    public void setNoDBase(boolean noDBase) {
        this.m_noDBase = noDBase;
    }

    /**
     * Obtains a dont-fail-on-database-errors mode.
     *
     * @return true, if database failures are not fatal.
     * @see #setIgnoreDBFail(boolean)
     */
    public boolean getIgnoreDBFail() {
        return this.m_ignoreDBFail;
    }

    /**
     * Sets the dont-fail-on-dbase-errors mode.
     *
     * @param ignore is true to render database error non-fatal.
     * @see #getIgnoreDBFail()
     */
    public void setIgnoreDBFail(boolean ignore) {
        this.m_ignoreDBFail = ignore;
    }

    /**
     * Obtains the current value of the workflow label to use.
     *
     * @return current workflow label to use, may be <code>null</code>.
     * @see #setWorkflowLabel(String)
     */
    public String getWorkflowLabel() {
        return this.m_wf_label;
    }

    /**
     * Sets the workflow label.
     *
     * @param label is the (new) workflow label.
     * @see #getWorkflowLabel()
     */
    public void setWorkflowLabel(String label) {
        this.m_wf_label = label;
    }

    /**
     * Obtains the current value of the workflow modification time to use.
     *
     * @return current workflow mtime, may be <code>null</code>.
     * @see #setWorkflowTimestamp(Date)
     */
    public Date getWorkflowTimestamp() {
        return this.m_wf_mtime;
    }

    /**
     * Sets the workflow modification time to record.
     *
     * @param mtime is the (new) workflow mtime.
     * @see #getWorkflowTimestamp()
     */
    public void setWorkflowTimestamp(Date mtime) {
        this.m_wf_mtime = mtime;
    }

    /**
     * Determines the exit code of an invocation record. Currently, we will determine the exit code
     * from all jobs until failure or no more jobs. However, set-up and clean-up jobs are ignored.
     *
     * @param ivr is the invocation record to put into the database
     * @return the status code as exit code to signal failure etc.
     *     <pre>
     *   0   regular exit with exit code 0
     *   1   regular exit with exit code > 0
     *   2   failure to run program from kickstart
     *   3   application had died on signal
     *   4   application was suspended (should not happen)
     *   5   failure in exit code parsing
     *   6   impossible case
     * </pre>
     */
    public int determineExitStatus(InvocationRecord ivr) {
        boolean seen = false;
        for (Iterator i = ivr.iterateJob(); i.hasNext(); ) {
            Job job = (Job) i.next();

            // set-up/clean-up jobs don't count in failure modes
            if (job.getTag().equals("cleanup")) continue;
            if (job.getTag().equals("setup")) continue;

            // obtains status from job
            Status status = job.getStatus();
            if (status == null) return 6;

            JobStatus js = status.getJobStatus();
            if (js == null) {
                // should not happen
                return 6;
            } else if (js instanceof JobStatusRegular) {
                // regular exit code - success or failure?
                int exitcode = ((JobStatusRegular) js).getExitCode();
                if (exitcode != 0) return 1;
                else seen = true;
                // continue, if exitcode of 0 to implement chaining !!!!
            } else if (js instanceof JobStatusFailure) {
                // kickstart failure
                return 2;
            } else if (js instanceof JobStatusSignal) {
                // died on signal
                return 3;
            } else if (js instanceof JobStatusSuspend) {
                // suspended???
                return 4;
            } else {
                // impossible/unknown case
                return 6;
            }
        }

        // success, or no [matching] jobs
        return seen ? 0 : 5;
    }

    /**
     * Extracts records from the given input file. Since there may be more than one record per file,
     * especially in the case of MPI, multiple results are possible, though traditionally only one
     * will be used.
     *
     * @param input is the name of the file that contains the records
     * @return a list of strings, each representing one invocation record. The result should not be
     *     empty (exception will be thrown).
     * @throws FriendlyNudge, if the input format was invalid. The caller has to assume failure to
     *     parse the record provided.
     */
    public List extractToMemory(java.io.File input) throws FriendlyNudge {
        List result = new ArrayList();
        StringWriter out = null;
        Logging log = getLogger();

        // open the files
        int p1, p2, state = 0;
        try {
            BufferedReader in = new BufferedReader(new FileReader(input));
            out = new StringWriter(4096);

            String line = null;
            while ((line = in.readLine()) != null) {
                if ((state & 1) == 0) {
                    // try to copy the XML line in any case
                    if ((p1 = line.indexOf("<?xml")) > -1)
                        if ((p2 = line.indexOf("?>", p1)) > -1) {
                            out.write(line, p1, p2 + 2);
                            log.log("parser", 2, "state=" + state + ", seen <?xml ...?>");
                        }
                    // start state with the correct root element
                    if ((p1 = line.indexOf("<invocation")) > -1) {
                        if (p1 > 0) line = line.substring(p1);
                        log.log("parser", 2, "state=" + state + ", seen <invocation>");
                        ++state;
                    }
                }
                if ((state & 1) == 1) {
                    out.write(line);
                    if ((p1 = line.indexOf("</invocation>")) > -1) {
                        log.log("parser", 2, "state=" + state + ", seen </invocation>");
                        ++state;

                        out.flush();
                        out.close();
                        result.add(out.toString());
                        out = new StringWriter(4096);
                    }
                }
            }

            in.close();
            out.close();
        } catch (IOException ioe) {
            throw new FriendlyNudge(
                    "While copying " + input.getPath() + " into temp. file: " + ioe.getMessage(),
                    5);
        }

        // some sanity checks
        if (state == 0)
            throw new FriendlyNudge(
                    "File "
                            + input.getPath()
                            + " does not contain invocation records,"
                            + " assuming failure",
                    5);
        if ((state & 1) == 1)
            throw new FriendlyNudge(
                    "File "
                            + input.getPath()
                            + " contains an incomplete invocation record,"
                            + " assuming failure",
                    5);

        // done
        return result;
    }

    /**
     * Parses the contents of a kickstart output file, and returns a list of exit codes obtains from
     * the records.
     *
     * @param arg0 is the name of the file to read
     * @return a list with one or more exit code, one for each record.
     * @throws FriendlyNudge, if parsing of the file goes hay-wire.
     * @throws IOException if something happens while reading properties to instantiate the XML
     *     parser.
     * @throws SQLException if accessing the database fails.
     */
    public List parseFile(String arg0) throws FriendlyNudge, IOException, SQLException {
        List result = new ArrayList();
        Logging me = getLogger();
        me.log("kickstart", 2, "working with file " + arg0);

        // get access to the invocation parser
        if (m_ip == null) {
            ChimeraProperties props = ChimeraProperties.instance();
            String psl = props.getPTCSchemaLocation();
            me.log("kickstart", 2, "using XML schema location " + psl);
            m_ip = new InvocationParser(psl);
        }

        // check input file
        java.io.File check = new java.io.File(arg0);

        // test 1: file exists
        if (!check.exists()) {
            me.log("kickstart", 2, "file does not exist, fail with 5");
            throw new FriendlyNudge("file does not exist " + arg0 + ", assuming failure", 5);
        }

        // test 2: file is readable
        if (!check.canRead()) {
            me.log("kickstart", 2, "file not readable, fail with 5");
            throw new FriendlyNudge("unable to read file " + arg0 + ", assuming failure", 5);
        }

        // test 3: file has nonzero size
        // FIXME: Actually need to check the record size
        me.log("kickstart", 2, "file has size " + check.length());
        if (check.length() == 0) {
            // deal with 0-byte file
            if (getEmptyFail()) {
                me.log("kickstart", 2, "zero size file, fail with 5");
                throw new FriendlyNudge("file has zero length " + arg0 + ", assuming failure", 5);
            } else {
                me.log("kickstart", 2, "zero size file, succeed with 0");
                me.log("app", 1, "file has zero length " + arg0 + ", assuming success");
                result.add(new Integer(0));
                return result;
            }
        }

        // test 4: extract XML into tmp file
        me.log("kickstart", 2, "about to extract content into memory");
        List extract = extractToMemory(check);
        me.log("kickstart", 2, extract.size() + " records extracted");

        // testme: for each record obtained, work on it
        Architecture cachedUname = null;
        for (int j = 1; j - 1 < extract.size(); ++j) {
            String temp = (String) extract.get(j - 1);
            me.log("kickstart", 2, "content[" + j + "] extracted, length " + temp.length());

            // test 5: try to parse XML
            me.log("app", 2, "starting to parse invocation");
            me.log("kickstart", 2, "about to parse invocation record");
            InvocationRecord invocation = m_ip.parse(new StringReader(temp));
            me.log("kickstart", 2, "done parsing invocation");

            if (invocation == null) {
                me.log("kickstart", 2, "result record " + j + " is invalid (null), fail with 5");
                throw new FriendlyNudge(
                        "invalid XML invocation record " + j + " in " + arg0 + ", assuming failure",
                        5);
            } else {
                me.log("kickstart", 2, "result record " + j + " appears valid");
                me.log("app", 1, "invocation " + j + " was parsed successfully");
            }

            // NEW: attached workflow tag and mtime
            if (m_wf_label != null) invocation.setWorkflowLabel(m_wf_label);
            if (m_wf_mtime != null) invocation.setWorkflowTimestamp(m_wf_mtime);

            // Fix for Pegasus Bug 39
            // the machine information tag is created only once for a cluster
            // the -H flag disables the generation of machine information
            Architecture uname = invocation.getArchitecture();
            if (uname == null) {
                // attempt to update with cachedUname
                invocation.setArchitecture(cachedUname);
            } else {
                cachedUname = uname;
            }

            // insert into database -- iff it is available
            if (!m_noDBase && m_dbschema != null && m_dbschema instanceof PTC) {
                PTC ptc = (PTC) m_dbschema;

                try {
                    // FIXME: (start,host,pid) may not be a sufficient secondary key
                    me.log("kickstart", 2, "about to obtain secondary key triple");
                    if (ptc.getInvocationID(
                                    invocation.getStart(),
                                    invocation.getHostAddress(),
                                    invocation.getPID())
                            == -1) {
                        me.log("kickstart", 2, "new invocation, adding");
                        me.log("app", 1, "adding invocation to database");
                        // may throw SQLException
                        ptc.saveInvocation(invocation);
                    } else {
                        me.log("kickstart", 2, "existing invocation, skipping");
                        me.log("app", 1, "invocation already exists, skipping!");
                    }
                } catch (SQLException sql) {
                    if (m_ignoreDBFail) {
                        // if dbase errors are non-fatal, just protocol what is going on.
                        for (int n = 0; sql != null; ++n) {
                            me.log(
                                    "default",
                                    0,
                                    "While inserting PTR ["
                                            + j
                                            + "]:"
                                            + n
                                            + ": "
                                            + sql.getMessage()
                                            + ", ignoring");
                            sql = sql.getNextException();
                        }
                    } else {
                        // rethrow, if dbase errors are fatal (default)
                        throw sql;
                    }
                } // catch
            } // if use dbase

            // determine result code
            int status = 0;
            me.log("kickstart", 2, "about to determine exit status");
            status = determineExitStatus(invocation);
            me.log("kickstart", 2, "exit status is " + status);
            result.add(new Integer(status));
        } // for

        // done
        return result;
    }
}
