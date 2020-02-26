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
public class JobStatusRegular extends JobStatus // implements Cloneable
{
    /** This is exit code returned by the application. */
    private short m_exitcode;

    /** Default c'tor: Construct a hollow shell and allow further information to be added later. */
    public JobStatusRegular() {
        m_exitcode = 0;
    }

    /**
     * Constructs an exit code.
     *
     * @param exitcode is the exit code of the application.
     */
    public JobStatusRegular(short exitcode) {
        m_exitcode = exitcode;
    }

    /**
     * Accessor
     *
     * @see #setExitCode(short)
     */
    public short getExitCode() {
        return this.m_exitcode;
    }

    /**
     * Accessor.
     *
     * @param exitcode
     * @see #getExitCode()
     */
    public void setExitCode(short exitcode) {
        this.m_exitcode = exitcode;
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

        result.append("<regular exitcode=\"");
        result.append(Short.toString(m_exitcode));
        result.append("\"/>");

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
        stream.write('<');
        if (namespace != null && namespace.length() > 0) {
            stream.write(namespace);
            stream.write(':');
        }
        stream.write("regular");
        writeAttribute(stream, " exitcode=\"", Short.toString(m_exitcode));
        stream.write("/>");
    }
}
