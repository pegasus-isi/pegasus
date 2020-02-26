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
 * @see Job
 * @see JobStatus
 */
public class JobStatusFailure extends JobStatus implements HasText {
    /** This is the errno value with the reason that the invocation of the application failed. */
    private int m_errno;

    /** This is the data contained between the tags. A <code>null</code> value is not valid. */
    private String m_value;

    /** Default c'tor: Construct a hollow shell and allow further information to be added later. */
    public JobStatusFailure() {
        m_errno = 0;
        m_value = null;
    }

    /**
     * Constructs an error number without reason text.
     *
     * @param errno is the error number
     */
    public JobStatusFailure(int errno) {
        m_errno = errno;
        m_value = null;
    }

    /**
     * Constructs a piece of data.
     *
     * @param errno is the error number .
     * @param value is the textual error reason.
     */
    public JobStatusFailure(int errno, String value) {
        m_errno = errno;
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
        StringBuffer result = new StringBuffer(32); // good for no content

        result.append("<failure error=\"");
        result.append(Integer.toString(m_errno));

        if (m_value == null) {
            // no content
            result.append("\"/>");
        } else {
            // yes, content
            result.append("\">");
            result.append(quote(m_value, false));
            result.append("</failure>");
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
                (namespace != null && namespace.length() > 0) ? namespace + ":failure" : "failure";

        // open tag
        stream.write('<');
        stream.write(tag);
        writeAttribute(stream, " error=\"", Integer.toString(m_errno));

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
