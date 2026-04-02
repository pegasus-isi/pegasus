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
package edu.isi.pegasus.planner.invocation;

import java.io.IOException;
import java.io.Writer;
import java.util.*;

/**
 * This class is the container for a complete call to stat() or fstat(). It contains information
 * about the file or descriptor. Optionally, it may also contain some data from the file or
 * descriptor.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 */
public class StatCall extends Invocation // implements Cloneable
{
    /** optional handle for stat calls of the invocation record */
    private String m_handle;

    /** optional logical filename associated with this stat call */
    private String m_lfn;

    /** value of errno after calling any stat function, or -1 for failure. */
    private int m_errno;

    /** the object (fn,fd) that the stat call was taken on. */
    private File m_file;

    /** the stat information itself, only present for unfailed calls. */
    private StatInfo m_statinfo;

    /** Optional data gleaned from stdout or stderr. */
    private Data m_data;

    /** Default c'tor: Construct a hollow shell and allow further information to be added later. */
    public StatCall() {
        m_handle = m_lfn = null;
        m_file = null;
        m_statinfo = null;
        m_data = null;
    }

    /**
     * Construct a specific but empty stat call object.
     *
     * @param handle is the identifier to give this specific stat call.
     */
    public StatCall(String handle) {
        m_handle = handle;
        m_lfn = null;
        m_file = null;
        m_statinfo = null;
        m_data = null;
    }

    /**
     * Accessor
     *
     * @see #setHandle(String)
     */
    public String getHandle() {
        return this.m_handle;
    }

    /**
     * Accessor.
     *
     * @param handle
     * @see #getHandle()
     */
    public void setHandle(String handle) {
        this.m_handle = handle;
    }

    /**
     * Accessor
     *
     * @see #setLFN(String)
     */
    public String getLFN() {
        return this.m_lfn;
    }

    /**
     * Accessor.
     *
     * @param lfn
     * @see #getLFN()
     */
    public void setLFN(String lfn) {
        this.m_lfn = lfn;
    }

    /**
     * Accessor
     *
     * @see #setError(int)
     */
    public int getError() {
        return this.m_errno;
    }

    /**
     * Accessor.
     *
     * @param errno
     * @see #getError()
     */
    public void setError(int errno) {
        this.m_errno = errno;
    }

    /**
     * Accessor
     *
     * @see #setFile(File)
     */
    public File getFile() {
        return this.m_file;
    }

    /**
     * Accessor.
     *
     * @param file
     * @see #getFile()
     */
    public void setFile(File file) {
        this.m_file = file;
    }

    /**
     * Accessor
     *
     * @see #setStatInfo(StatInfo)
     */
    public StatInfo getStatInfo() {
        return this.m_statinfo;
    }

    /**
     * Accessor.
     *
     * @param statinfo
     * @see #getStatInfo()
     */
    public void setStatInfo(StatInfo statinfo) {
        this.m_statinfo = statinfo;
    }

    /**
     * Accessor
     *
     * @see #setData(String)
     */
    public Data getData() {
        return this.m_data;
    }

    /**
     * Accessor.
     *
     * @param data
     * @see #getData()
     */
    public void setData(String data) {
        this.m_data = new Data(data);
    }

    /**
     * Conversion accessor.
     *
     * @param data
     * @see #getData()
     * @see #setData( String )
     */
    public void setData(Data data) {
        this.m_data = data;
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
        String newline = System.getProperty("line.separator", "\r\n");
        String tag =
                (namespace != null && namespace.length() > 0)
                        ? namespace + ":statcall"
                        : "statcall";

        // open tag
        if (indent != null && indent.length() > 0) stream.write(indent);
        stream.write('<');
        stream.write(tag);
        writeAttribute(stream, " error=\"", Integer.toString(m_errno));
        if (m_handle != null) writeAttribute(stream, " id=\"", m_handle);
        if (m_lfn != null) writeAttribute(stream, " lfn=\"", m_lfn);
        stream.write('>');
        if (indent != null) stream.write(newline);

        // dump content
        String newindent = indent == null ? null : indent + "  ";
        m_file.toXML(stream, newindent, namespace);
        if (m_statinfo != null) m_statinfo.toXML(stream, newindent, namespace);
        if (m_data != null) m_data.toXML(stream, newindent, namespace);

        // close tag
        if (indent != null && indent.length() > 0) stream.write(indent);
        stream.write("</");
        stream.write(tag);
        stream.write('>');
        if (indent != null) stream.write(newline);
    }
}
