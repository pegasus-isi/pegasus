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
 * This class is transient for XML parsing. The data value will be incorporated into the job status
 * classes.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 * @see StatCall
 */
public class JobStatusSignal extends JobStatus implements HasText {
    /** This is the signal number that led to the process termination. */
    private short m_signo;

    /** This flag signals, if a core file was generated. Not all systems support core files. */
    private boolean m_core;

    /** This is the signal name or message contained between the tags. */
    private String m_value;

    /** Default c'tor: Construct a hollow shell and allow further information to be added later. */
    public JobStatusSignal() {
        m_signo = 0;
        m_core = false;
        m_value = null;
    }

    /**
     * Constructs an error number without reason text.
     *
     * @param signo is the error number
     */
    public JobStatusSignal(short signo) {
        m_signo = signo;
        m_core = false;
        m_value = null;
    }

    /**
     * Constructs a piece of data.
     *
     * @param signo is the error number.
     * @param core is the core flag.
     */
    public JobStatusSignal(short signo, boolean core) {
        m_signo = signo;
        m_core = core;
        m_value = null;
    }

    /**
     * Constructs a piece of data.
     *
     * @param signo is the error number.
     * @param core is the core flag.
     * @param value is the textual error reason.
     */
    public JobStatusSignal(short signo, boolean core, String value) {
        m_signo = signo;
        m_value = value;
    }

    /**
     * Appends a piece of text to the existing text.
     *
     * @param fragment is a piece of text to append to existing text. Appending <code>null</code> is
     *     a noop.
     */
    public void appendValue(String fragment) {
        if (fragment != null) {
            if (this.m_value == null) this.m_value = new String(fragment);
            else this.m_value += fragment;
        }
    }

    /**
     * Accessor
     *
     * @see #setSignalNumber(short)
     */
    public short getSignalNumber() {
        return this.m_signo;
    }

    /**
     * Accessor.
     *
     * @param signo
     * @see #getSignalNumber()
     */
    public void setSignalNumber(short signo) {
        this.m_signo = signo;
    }

    /**
     * Accessor
     *
     * @see #setCoreFlag(boolean)
     */
    public boolean getCoreFlag() {
        return this.m_core;
    }

    /**
     * Accessor.
     *
     * @param core
     * @see #getCoreFlag()
     */
    public void setCoreFlag(boolean core) {
        this.m_core = core;
    }

    /**
     * Accessor
     *
     * @see #setValue(String)
     */
    public String getValue() {
        return this.m_value;
    }

    /**
     * Accessor.
     *
     * @param value is the new value to set.
     * @see #getValue()
     */
    public void setValue(String value) {
        this.m_value = value;
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
     * Dumps the state of the current element as XML output. This function can return the necessary
     * data more efficiently, thus overwriting the inherited method.
     *
     * @param indent is a <code>String</code> of spaces used for pretty printing. The initial amount
     *     of spaces should be an empty string. The parameter is used internally for the recursive
     *     traversal.
     * @return a String which contains the state of the current class and its siblings using XML.
     *     Note that these strings might become large.
     */
    public String toXML(String indent) {
        StringBuffer result = new StringBuffer(40); // good for no content

        result.append("<signalled signal=\"");
        result.append(Short.toString(m_signo));
        result.append("\" corefile=\"");
        result.append(Boolean.toString(m_core));

        if (m_value == null) {
            // no content
            result.append("\"/>");
        } else {
            // yes, content
            result.append("\">");
            result.append(quote(m_value, false));
            result.append("</signalled>");
        }

        return result.toString();
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
                (namespace != null && namespace.length() > 0)
                        ? namespace + ":signalled"
                        : "signalled";

        stream.write('<');
        stream.write(tag);
        writeAttribute(stream, " signal=\"", Short.toString(m_signo));
        writeAttribute(stream, " corefile=\"", Boolean.toString(m_core));

        if (m_value == null) {
            // no content
            stream.write("/>");
        } else {
            // yes, content
            stream.write('>');
            stream.write(quote(m_value, false));

            // close tag
            stream.write("</");
            stream.write(tag);
            stream.write('>');
        }
    }
}
