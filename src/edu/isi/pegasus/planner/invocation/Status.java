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

import java.io.IOException;
import java.io.Writer;
import java.util.*;

/**
 * This class encapsulates the exit code or reason of termination for a given job. The class itself
 * contains the raw exit code. It also aggregates an instance of the JobStatus interface, which
 * describes more clearly failure, regular execution, signal and suspension.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 */
public class Status extends Invocation // implements Cloneable
{
    /**
     * The raw exit code, unparsed and unprepared. There are several interpretation of the value.
     * Usually, it is interpreted as unsigned 16 bit value. The high byte contains the exit code.
     * The low byte has the core dump flag as MSB, and the rest denote the signal number. A value of
     * -1 denotes a failure from the grid launcher before starting the job.
     */
    private int m_status;

    /** This member variable contains the real status of the job. */
    private JobStatus m_jobStatus;

    /** Default c'tor: Construct a hollow shell and allow further information to be added later. */
    public Status() {
        m_status = 0;
        m_jobStatus = null;
    }

    /**
     * Constructs a layer with the raw exit code.
     *
     * @param raw is the raw exit code to store.
     */
    public Status(int raw) {
        m_status = raw;
        m_jobStatus = null;
    }

    /**
     * Constructs the complete class with raw exit code and a status child describing the exit code.
     *
     * @param raw is the raw exit status
     * @param status is the description of the kind of exit.
     */
    public Status(int raw, JobStatus status) {
        m_status = raw;
        m_jobStatus = status;
    }

    /**
     * Accessor
     *
     * @see #setStatus(int)
     */
    public int getStatus() {
        return this.m_status;
    }

    /**
     * Accessor.
     *
     * @param status
     * @see #getStatus()
     */
    public void setStatus(int status) {
        this.m_status = status;
    }

    /**
     * Accessor
     *
     * @see #setJobStatus( JobStatus )
     */
    public JobStatus getJobStatus() {
        return this.m_jobStatus;
    }

    /**
     * Accessor.
     *
     * @param jobStatus is an instance of the class describing the real reason for program
     *     termination on the remote end.
     * @see #getJobStatus()
     */
    public void setJobStatus(JobStatus jobStatus) {
        this.m_jobStatus = jobStatus;
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
     * Dump the state of the current element as XML output. This function traverses all sibling
     * classes as necessary, and converts the data into pretty-printed XML output. The stream
     * interface should be able to handle large output efficiently.
     *
     * @param stream is a stream opened and ready for writing. This can also be a string stream for
     *     efficient output.
     * @param indent is a <code>String</code> of spaces used for pretty printing. The initial amount
     *     of spaces should be an empty string. The parameter is used internally for the recursive
     *     traversal. If a <code>null</code> value is specified, no indentation nor linefeeds will
     *     be generated.
     * @param namespace is the XML schema namespace prefix. If neither empty nor null, each element
     *     will be prefixed with this prefix, and the root element will map the XML namespace.
     * @exception IOException if something fishy happens to the stream.
     */
    public void toXML(Writer stream, String indent, String namespace) throws IOException {
        String tag =
                (namespace != null && namespace.length() > 0) ? namespace + ":status" : "status";

        // open tag
        if (indent != null && indent.length() > 0) stream.write(indent);
        stream.write('<');
        stream.write(tag);
        writeAttribute(stream, " raw=\"", Integer.toString(m_status));
        stream.write(">");

        // dump content
        String newindent = indent == null ? null : indent + "  ";
        if (m_jobStatus != null) m_jobStatus.toXML(stream, newindent, namespace);
        else throw new RuntimeException("unknown state of job status");

        // close tag
        stream.write("</");
        stream.write(tag);
        stream.write('>');
        if (indent != null) stream.write(System.getProperty("line.separator", "\r\n"));
    }
}
