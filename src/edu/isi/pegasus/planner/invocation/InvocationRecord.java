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
package edu.isi.pegasus.planner.invocation;

import edu.isi.pegasus.common.util.Currently;
import java.io.IOException;
import java.io.Writer;
import java.net.InetAddress;
import java.util.*;

/**
 * This class is the container for an invocation record. The record itself contains information
 * about the job or jobs that ran, the total usage, and information about central files that were
 * used.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 */
public class InvocationRecord extends Invocation // implements Cloneable
{
    /** The "official" namespace URI of the invocation record schema. */
    public static final String SCHEMA_NAMESPACE = "http://pegasus.isi.edu/schema/invocation";

    /** The "not-so-official" location URL of the invocation record definition. */
    public static final String SCHEMA_LOCATION = "http://pegasus.isi.edu/schema/iv-2.1.xsd";

    /** protocol version information. */
    private String m_version;

    /** start of gridlaunch timestamp. */
    private Date m_start;

    /** total duration of call. */
    private double m_duration;

    /** Name of the: Transformation that produced this invocation. */
    private String m_transformation;

    /** Name of the Derivation that produced this invocation. */
    private String m_derivation;

    /** Records the physical memory on the remote machine, if available. */
    private long m_pmem = -1;

    /** process id of gridlaunch itself. */
    private int m_pid;

    /** host address where gridlaunch ran (primary interface). */
    private InetAddress m_hostaddr;

    /** Symbolic hostname where gridlaunch ran (primary interface). */
    private String m_hostname;

    /** Symbolic name of primary interface we used to determine host-name and -address. */
    private String m_interface;

    /** numerical user id of the effective user. */
    private int m_uid;

    /** symbolical user name of the effective user. */
    private String m_user;

    /** numerical group id of the effective user. */
    private int m_gid;

    /** symbolical group name of the effective user. */
    private String m_group;

    /** Working directory at startup. */
    private WorkingDir m_cwd;

    /** Architectural information. */
    private Architecture m_uname;

    /** Total resource consumption by gridlaunch and all siblings. */
    private Usage m_usage;

    /** Job records: prejob, main job, postjob */
    private List m_job;

    /** Array with stat() and fstat() information about various files. */
    private List m_stat;

    /** Resource, site or pool at which the jobs was run. */
    private String m_resource;

    /** Workflow label, currently optional? */
    private String m_wf_label;

    /** Workflow timestamp to make the label more unique. */
    private Date m_wf_stamp;

    /** Environment settings. */
    private Environment m_environment;

    /**
     * Currently active umask while kickstart was executing. This is available with new kickstart,
     * older version will have -1 at this API point.
     */
    private int m_umask = -1;

    /** The Machine object capturing machine information. */
    private Machine m_machine;

    /**
     * Accessor
     *
     * @see #setUMask(int)
     */
    public int getUMask() {
        return this.m_umask;
    }

    /**
     * Accessor.
     *
     * @param umask
     * @see #getUMask()
     */
    public void setUMask(int umask) {
        this.m_umask = umask;
    }

    /** Default c'tor: Construct a hollow shell and allow further information to be added later. */
    public InvocationRecord() {
        m_stat = new ArrayList(5);
        m_job = new ArrayList(3);
        m_environment = null;
    }

    /**
     * Accessor
     *
     * @see #setVersion(String)
     */
    public String getVersion() {
        return this.m_version;
    }

    /**
     * Accessor.
     *
     * @param version
     * @see #getVersion()
     */
    public void setVersion(String version) {
        this.m_version = version;
    }

    /**
     * Accessor
     *
     * @see #setStart(Date)
     */
    public Date getStart() {
        return this.m_start;
    }

    /**
     * Accessor.
     *
     * @param start
     * @see #getStart()
     */
    public void setStart(Date start) {
        this.m_start = start;
    }

    /**
     * Accessor
     *
     * @see #setDuration(double)
     */
    public double getDuration() {
        return this.m_duration;
    }

    /**
     * Accessor.
     *
     * @param duration
     * @see #getDuration()
     */
    public void setDuration(double duration) {
        this.m_duration = duration;
    }

    /**
     * Accessor.
     *
     * @param machine
     * @see #getMachine()
     */
    public void setMachine(Machine machine) {
        this.m_machine = machine;
    }

    /**
     * Accessor.
     *
     * @return machine
     * @see #setMachine(org.griphyn.vdl.invocation.Machine)
     */
    public Machine getMachine() {
        return this.m_machine;
    }

    /**
     * Accessor
     *
     * @see #setTransformation(String)
     */
    public String getTransformation() {
        return this.m_transformation;
    }

    /**
     * Accessor.
     *
     * @param transformation
     * @see #getTransformation()
     */
    public void setTransformation(String transformation) {
        this.m_transformation = transformation;
    }

    /**
     * Accessor
     *
     * @see #setDerivation(String)
     */
    public String getDerivation() {
        return this.m_derivation;
    }

    /**
     * Accessor.
     *
     * @param derivation
     * @see #getDerivation()
     */
    public void setDerivation(String derivation) {
        this.m_derivation = derivation;
    }

    /**
     * Accessor
     *
     * @see #setPID(int)
     */
    public int getPID() {
        return this.m_pid;
    }

    /**
     * Accessor.
     *
     * @param pid
     * @see #getPID()
     */
    public void setPID(int pid) {
        this.m_pid = pid;
    }

    /**
     * Accessor
     *
     * @see #setHostAddress(InetAddress)
     */
    public InetAddress getHostAddress() {
        return this.m_hostaddr;
    }

    /**
     * Accessor.
     *
     * @param hostaddr
     * @see #getHostAddress()
     */
    public void setHostAddress(InetAddress hostaddr) {
        this.m_hostaddr = hostaddr;
    }

    /**
     * Accessor
     *
     * @see #setHostname(String)
     */
    public String getHostname() {
        return this.m_hostname;
    }

    /**
     * Accessor.
     *
     * @param hostname
     * @see #getHostname()
     */
    public void setHostname(String hostname) {
        this.m_hostname = hostname;
    }

    /**
     * Accessor.
     *
     * @see #setInterface(String)
     */
    public String getInterface() {
        return this.m_interface;
    }

    /**
     * Accessor.
     *
     * @param p_interface
     * @see #getInterface()
     */
    public void setInterface(String p_interface) {
        this.m_interface = p_interface;
    }

    /**
     * Accessor
     *
     * @see #setUID(int)
     */
    public int getUID() {
        return this.m_uid;
    }

    /**
     * Accessor.
     *
     * @param uid
     * @see #getUID()
     */
    public void setUID(int uid) {
        this.m_uid = uid;
    }

    /**
     * Accessor
     *
     * @see #setUser(String)
     */
    public String getUser() {
        return this.m_user;
    }

    /**
     * Accessor.
     *
     * @param user
     * @see #getUser()
     */
    public void setUser(String user) {
        this.m_user = user;
    }

    /**
     * Accessor
     *
     * @see #setGID(int)
     */
    public int getGID() {
        return this.m_gid;
    }

    /**
     * Accessor.
     *
     * @param gid
     * @see #getGID()
     */
    public void setGID(int gid) {
        this.m_gid = gid;
    }

    /**
     * Accessor
     *
     * @see #setGroup(String)
     */
    public String getGroup() {
        return this.m_group;
    }

    /**
     * Accessor.
     *
     * @param group
     * @see #getGroup()
     */
    public void setGroup(String group) {
        this.m_group = group;
    }

    /**
     * Accessor
     *
     * @see #setUsage(Usage)
     */
    public Usage getUsage() {
        return this.m_usage;
    }

    /**
     * Accessor.
     *
     * @param usage
     * @see #getUsage()
     */
    public void setUsage(Usage usage) {
        this.m_usage = usage;
    }

    /**
     * Accessor
     *
     * @see #setArchitecture(Architecture)
     */
    public Architecture getArchitecture() {
        return this.m_uname;
    }

    /**
     * Accessor.
     *
     * @param uname
     * @see #getArchitecture()
     */
    public void setArchitecture(Architecture uname) {
        this.m_uname = uname;
    }

    /**
     * Accessor
     *
     * @see #setResource( String )
     */
    public String getResource() {
        return this.m_resource;
    }

    /**
     * Accessor.
     *
     * @param resource
     * @see #getResource()
     */
    public void setResource(String resource) {
        this.m_resource = resource;
    }

    /**
     * Accessor
     *
     * @see #setWorkflowLabel( String )
     */
    public String getWorkflowLabel() {
        return this.m_wf_label;
    }

    /**
     * Accessor.
     *
     * @param label
     * @see #getWorkflowLabel()
     */
    public void setWorkflowLabel(String label) {
        this.m_wf_label = label;
    }

    /**
     * Accessor
     *
     * @see #setWorkflowTimestamp( Date )
     */
    public Date getWorkflowTimestamp() {
        return this.m_wf_stamp;
    }

    /**
     * Accessor.
     *
     * @param stamp
     * @see #getResource()
     */
    public void setWorkflowTimestamp(Date stamp) {
        this.m_wf_stamp = stamp;
    }

    /**
     * Accessor
     *
     * @see #setEnvironment(Environment)
     */
    public Environment getEnvironment() {
        return this.m_environment;
    }

    /**
     * Accessor.
     *
     * @param environment
     * @see #getEnvironment()
     */
    public void setEnvironment(Environment environment) {
        this.m_environment = environment;
    }

    //   /**
    //    * Parses an ISO 8601 timestamp?
    //    *
    //    * @param stamp
    //    * @see #getResource()
    //    */
    //   public void setWorkflowTimestamp( String stamp )
    //   { this.m_wf_stamp = stamp; }

    /**
     * Accessor: Appends a job to the list of jobs.
     *
     * @param job is the job to append to the list.
     */
    public void addJob(Job job) {
        this.m_job.add(job);
    }

    /**
     * Accessor: Inserts a Job into a specific position of the job list.
     *
     * @param index is the position to insert the item into
     * @param job is the job to insert into the list.
     */
    public void addJob(int index, Job job) {
        this.m_job.add(index, job);
    }

    /**
     * Accessor: Obtains a job at a certain position in the job list.
     *
     * @param index is the position in the list to obtain a job from
     * @return the job at that position.
     * @throws IndexOutOfBoundsException if the index points to an element in the list that does not
     *     contain any elments.
     */
    public Job getJob(int index) throws IndexOutOfBoundsException {
        // -- check bound for index
        if ((index < 0) || (index >= this.m_job.size())) throw new IndexOutOfBoundsException();

        return (Job) this.m_job.get(index);
    }

    /**
     * Accessor: Obtains the size of the job list.
     *
     * @return number of elements that an external array needs to be sized to.
     */
    public int getJobCount() {
        return this.m_job.size();
    }

    /**
     * Accessor: Gets an array of all values that constitute the current content. This list is
     * read-only.
     *
     * @return a list of jobs.
     */
    public java.util.List getJobList() {
        return Collections.unmodifiableList(this.m_job);
    }

    /**
     * Accessor: Enumerates the internal values that constitute the content of the job list.
     *
     * @return an iterator to walk the list with.
     */
    public Iterator iterateJob() {
        return this.m_job.iterator();
    }

    /**
     * Accessor: Enumerates the internal values that constitute the content of the job list.
     *
     * @return a list iterator to walk the list with.
     */
    public ListIterator listIterateJob() {
        return this.m_job.listIterator();
    }

    /** Accessor: Removes all values from the job list. */
    public void removeAllJob() {
        this.m_job.clear();
    }

    /**
     * Accessor: Removes a specific job from the job list.
     *
     * @param index is the position at which an element is to be removed.
     * @return the job that was removed.
     */
    public Job removeJob(int index) {
        return (Job) this.m_job.remove(index);
    }

    /**
     * Accessor: Overwrites a job at a certain position.
     *
     * @param index position to overwrite an elment in.
     * @param job is the Job to replace with.
     * @throws IndexOutOfBoundsException if the position pointed to is invalid.
     */
    public void setJob(int index, Job job) throws IndexOutOfBoundsException {
        // -- check bounds for index
        if ((index < 0) || (index >= this.m_job.size())) {
            throw new IndexOutOfBoundsException();
        }
        this.m_job.set(index, job);
    }

    /**
     * Accessor: Overwrites internal list with an external list representing jobs.
     *
     * @param jobs is the external list of job to overwrite with.
     */
    public void setJob(Collection jobs) {
        this.m_job.clear();
        this.m_job.addAll(jobs);
    }

    /**
     * Accessor: Appends a stat to the list of stats.
     *
     * @param stat is the stat to append to the list.
     */
    public void addStatCall(StatCall stat) {
        this.m_stat.add(stat);
    }

    /**
     * Accessor: Inserts a StatCall into a specific position of the stat list.
     *
     * @param index is the position to insert the item into
     * @param stat is the stat to insert into the list.
     */
    public void addStatCall(int index, StatCall stat) {
        this.m_stat.add(index, stat);
    }

    /**
     * Accessor: Obtains a stat at a certain position in the stat list.
     *
     * @param index is the position in the list to obtain a stat from
     * @return the stat at that position.
     * @throws IndexOutOfBoundsException if the index points to an element in the list that does not
     *     contain any elments.
     */
    public StatCall getStatCall(int index) throws IndexOutOfBoundsException {
        // -- check bound for index
        if ((index < 0) || (index >= this.m_stat.size())) throw new IndexOutOfBoundsException();

        return (StatCall) this.m_stat.get(index);
    }

    /**
     * Accessor: Obtains the size of the stat list.
     *
     * @return number of elements that an external array needs to be sized to.
     */
    public int getStatCount() {
        return this.m_stat.size();
    }

    /**
     * Accessor: Gets an array of all values that constitute the current content. This list is
     * read-only.
     *
     * @return a list of stats.
     */
    public java.util.List getStatList() {
        return Collections.unmodifiableList(this.m_stat);
    }

    /**
     * Accessor: Enumerates the internal values that constitute the content of the stat list.
     *
     * @return an iterator to walk the list with.
     */
    public Iterator iterateStatCall() {
        return this.m_stat.iterator();
    }

    /**
     * Accessor: Enumerates the internal values that constitute the content of the stat list.
     *
     * @return a list iterator to walk the list with.
     */
    public ListIterator listIterateStatCall() {
        return this.m_stat.listIterator();
    }

    /** Accessor: Removes all values from the stat list. */
    public void removeAllStatCall() {
        this.m_stat.clear();
    }

    /**
     * Accessor: Removes a specific stat from the stat list.
     *
     * @param index is the position at which an element is to be removed.
     * @return the stat that was removed.
     */
    public StatCall removeStatCall(int index) {
        return (StatCall) this.m_stat.remove(index);
    }

    /**
     * Accessor: Overwrites a stat at a certain position.
     *
     * @param index position to overwrite an elment in.
     * @param stat is the StatCall to replace with.
     * @throws IndexOutOfBoundsException if the position pointed to is invalid.
     */
    public void setStatCall(int index, StatCall stat) throws IndexOutOfBoundsException {
        // -- check bounds for index
        if ((index < 0) || (index >= this.m_stat.size())) {
            throw new IndexOutOfBoundsException();
        }
        this.m_stat.set(index, stat);
    }

    /**
     * Accessor: Overwrites internal list with an external list representing stats.
     *
     * @param stats is the external list of stat to overwrite with.
     */
    public void setStatCall(Collection stats) {
        this.m_stat.clear();
        this.m_stat.addAll(stats);
    }

    /**
     * Accessor
     *
     * @see #setWorkingDirectory(WorkingDir)
     * @see #setWorkingDirectory(String)
     */
    public WorkingDir getWorkingDirectory() {
        return this.m_cwd;
    }

    /**
     * Accessor.
     *
     * @param cwd
     * @see #getWorkingDirectory()
     * @see #setWorkingDirectory(WorkingDir)
     */
    public void setWorkingDirectory(String cwd) {
        this.m_cwd = new WorkingDir(cwd);
    }

    /**
     * Accessor.
     *
     * @param cwd
     * @see #getWorkingDirectory()
     * @see #setWorkingDirectory(String)
     */
    public void setWorkingDirectory(WorkingDir cwd) {
        this.m_cwd = cwd;
    }

    /**
     * Accessor.
     *
     * @return the recorded physical memory in byte, or -1 if not available.
     * @see #setPhysicalMemory( long )
     */
    public long getPhysicalMemory() {
        return this.m_pmem;
    }

    /**
     * Accessor.
     *
     * @param pmem
     * @see #getPhysicalMemory()
     */
    public void setPhysicalMemory(long pmem) {
        this.m_pmem = pmem;
    }

    /**
     * Converts the active state into something meant for human consumption. The method will be
     * called when recursively traversing the instance tree.
     *
     * @param stream is a stream opened and ready for writing. This can also be a string stream for
     *     efficient output.
     */
    public void toString(Writer stream) throws IOException {
        throw new IOException("method not implemented, please contact vds-support@griphyn.org");
    }

    /**
     * Writes the header of the XML output. The output contains the special strings to start an XML
     * document, some comments, and the root element. The latter points to the XML schema via XML
     * Instances.
     *
     * @param stream is a stream opened and ready for writing. This can also be a string stream for
     *     efficient output.
     * @param indent is a <code>String</code> of spaces used for pretty printing. The initial amount
     *     of spaces should be an empty string. The parameter is used internally for the recursive
     *     traversal.
     * @param namespace is the XML schema namespace prefix. If neither empty nor null, each element
     *     will be prefixed with this prefix, and the root element will map the XML namespace.
     * @exception IOException if something fishy happens to the stream.
     */
    public void writeXMLHeader(Writer stream, String indent, String namespace) throws IOException {
        String newline = System.getProperty("line.separator", "\r\n");

        // intro
        if (indent != null && indent.length() > 0) stream.write(indent);
        stream.write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
        stream.write(newline);

        // when was this document generated
        if (indent != null && indent.length() > 0) stream.write(indent);
        stream.write("<!-- generated: ");
        stream.write(Currently.iso8601(false));
        stream.write(" -->");
        stream.write(newline);

        // who generated this document
        if (indent != null && indent.length() > 0) stream.write(indent);
        stream.write("<!-- generated by: ");
        stream.write(System.getProperties().getProperty("user.name", "unknown"));
        stream.write(" [");
        stream.write(System.getProperties().getProperty("user.region", "??"));
        stream.write("] -->");
        stream.write(newline);

        // root element with elementary attributes
        if (indent != null && indent.length() > 0) stream.write(indent);
        stream.write('<');
        if (namespace != null && namespace.length() > 0) {
            stream.write(namespace);
            stream.write(':');
        }
        stream.write("invocation xmlns");
        if (namespace != null && namespace.length() > 0) {
            stream.write(':');
            stream.write(namespace);
        }
        stream.write("=\"");
        stream.write(SCHEMA_NAMESPACE);
        stream.write(
                "\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:schemaLocation=\"");
        stream.write(SCHEMA_NAMESPACE);
        stream.write(' ');
        stream.write(SCHEMA_LOCATION);
        stream.write("\"");

        writeAttribute(stream, " version=\"", this.m_version);
        writeAttribute(stream, " start=\"", Currently.iso8601(false, true, true, this.m_start));
        writeAttribute(stream, " duration=\"", Double.toString(this.m_duration));
        if (this.m_transformation != null && this.m_transformation.length() > 0)
            writeAttribute(stream, " transformation=\"", this.m_transformation);
        if (this.m_derivation != null && this.m_derivation.length() > 0)
            writeAttribute(stream, " derivation=\"", this.m_derivation);
        if (this.m_pmem != -1) writeAttribute(stream, " ram=\"", Long.toString(this.m_pmem));
        writeAttribute(stream, " pid=\"", Integer.toString(this.m_pid));
        if (this.m_resource != null && this.m_resource.length() > 0)
            writeAttribute(stream, " resource=\"", this.m_resource);
        if (this.m_wf_label != null && this.m_wf_label.length() > 0)
            writeAttribute(stream, " wf-label=\"", this.m_wf_label);
        if (this.m_wf_stamp != null)
            writeAttribute(
                    stream, " wf-stamp=\"", Currently.iso8601(false, true, true, this.m_wf_stamp));
        writeAttribute(stream, " hostaddr=\"", this.m_hostaddr.getHostAddress());
        if (this.m_hostname != null && this.m_hostname.length() > 0)
            writeAttribute(stream, " hostname=\"", this.m_hostname);
        writeAttribute(stream, " uid=\"", Integer.toString(this.m_uid));
        if (this.m_user != null && this.m_user.length() > 0)
            writeAttribute(stream, " user=\"", this.m_user);
        writeAttribute(stream, " gid=\"", Integer.toString(this.m_gid));
        if (this.m_group != null && this.m_group.length() > 0)
            writeAttribute(stream, " group=\"", this.m_group);

        stream.write('>');
        if (indent != null) stream.write(newline);
    }

    /**
     * Dump the state of the current element as XML output. This function traverses all sibling
     * classes as necessary, and converts the data into pretty-printed XML output. The stream
     * interface should be able to handle large output efficiently.
     *
     * @param stream is a stream opened and ready for writing. This can also be a string stream for
     *     efficient output.
     * @param indent is a <code>String</code> of spaces used for pretty printing. The initial amount
     *     of spaces should be an empty string. The parameter is used internally for the recursive
     *     traversal.
     * @param namespace is the XML schema namespace prefix. If neither empty nor null, each element
     *     will be prefixed with this prefix, and the root element will map the XML namespace.
     * @exception IOException if something fishy happens to the stream.
     */
    public void toXML(Writer stream, String indent, String namespace) throws IOException {
        // write prefix
        writeXMLHeader(stream, indent, namespace);

        // part 1: jobs
        String newindent = indent == null ? null : indent + "  ";
        for (Iterator i = this.m_job.iterator(); i.hasNext(); ) {
            ((Job) i.next()).toXML(stream, newindent, namespace);
        }

        // part 2: cwd and total usage
        m_cwd.toXML(stream, newindent, namespace);
        m_usage.toXML(stream, newindent, namespace);
        m_uname.toXML(stream, newindent, namespace);
        // machine if not null
        if (m_machine != null) {
            m_machine.toXML(stream, indent, namespace);
        }

        // part 3: statcall records
        for (Iterator i = this.m_stat.iterator(); i.hasNext(); ) {
            ((StatCall) i.next()).toXML(stream, newindent, namespace);
        }

        // part 4: environment and resourcs
        if (m_environment != null) m_environment.toXML(stream, newindent, namespace);

        // close tag
        if (indent != null && indent.length() > 0) stream.write(indent);
        stream.write("</");
        if (namespace != null && namespace.length() > 0) {
            stream.write(namespace);
            stream.write(':');
        }
        stream.write("invocation>");
        stream.write(System.getProperty("line.separator", "\r\n"));
        stream.flush(); // this is the only time we flush
    }
}
