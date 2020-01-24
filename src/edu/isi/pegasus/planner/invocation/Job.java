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
import java.text.DecimalFormat;
import java.util.*;

/**
 * This class is contains the record from each jobs that ran in every invocation.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 */
public class Job extends Invocation // implements Cloneable
{
    /**
     * This is the tag to produce the job for. Usually, it is one of "mainjob", "prejob", "postjob",
     * or "cleanup".
     */
    private String m_tag;

    /** Start time of this job. */
    private Date m_start;

    /** Duration of the job. */
    private double m_duration;

    /** Process id assigned to the job. */
    private int m_pid;

    /** Resource usage of this job. */
    private Usage m_usage;

    /** Exit condition of the job. */
    private Status m_status;

    /** Stat call of the executable. */
    private StatCall m_executable;

    /** Command-line arguments. */
    private Arguments m_arguments;

    public Job(String tag) {
        m_tag = tag;
        m_usage = null;
        m_status = null;
        m_executable = null;
        m_arguments = null;
    }

    /**
     * Accessor
     *
     * @see #setTag(String)
     */
    public String getTag() {
        return this.m_tag;
    }

    /**
     * Accessor.
     *
     * @param tag
     * @see #getTag()
     */
    public void setTag(String tag) {
        this.m_tag = tag;
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
     * @see #setStatus(Status)
     */
    public Status getStatus() {
        return this.m_status;
    }

    /**
     * Accessor.
     *
     * @param status
     * @see #getStatus()
     */
    public void setStatus(Status status) {
        this.m_status = status;
    }

    /**
     * Accessor
     *
     * @see #setExecutable(StatCall)
     */
    public StatCall getExecutable() {
        return this.m_executable;
    }

    /**
     * Accessor.
     *
     * @param executable
     * @see #getExecutable()
     */
    public void setExecutable(StatCall executable) {
        this.m_executable = executable;
    }

    /**
     * Accessor
     *
     * @see #setArguments(Arguments)
     */
    public Arguments getArguments() {
        return this.m_arguments;
    }

    /**
     * Accessor.
     *
     * @param arguments
     * @see #getArguments()
     */
    public void setArguments(Arguments arguments) {
        this.m_arguments = arguments;
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
     * Dumps the state of the current element as XML output. This function traverses all sibling
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
        String newline = System.getProperty("line.separator", "\r\n");
        DecimalFormat d = new DecimalFormat("#.###");
        String tag =
                (namespace != null && namespace.length() > 0) ? namespace + ":" + m_tag : m_tag;

        // open tag
        if (indent != null && indent.length() > 0) stream.write(indent);
        stream.write('<');
        stream.write(tag);
        writeAttribute(stream, " start=\"", Currently.iso8601(false, true, true, m_start));
        writeAttribute(stream, " duration=\"", d.format(m_duration));
        writeAttribute(stream, " pid=\"", Integer.toString(m_pid));
        stream.write('>');
        if (indent != null) stream.write(newline);

        // dump content
        String newindent = indent == null ? null : indent + "  ";
        m_usage.toXML(stream, newindent, namespace);
        m_status.toXML(stream, newindent, namespace);
        m_executable.toXML(stream, newindent, namespace);
        m_arguments.toXML(stream, newindent, namespace);

        // close tag
        if (indent != null && indent.length() > 0) stream.write(indent);
        stream.write("</");
        stream.write(tag);
        stream.write('>');
        if (indent != null) stream.write(newline);
    }
}
