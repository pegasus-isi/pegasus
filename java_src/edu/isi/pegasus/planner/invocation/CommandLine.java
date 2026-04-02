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
 * This class maintains the application that was run, and the arguments to the commandline that were
 * actually passed on to the application.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 * @see Job
 */
public class CommandLine extends Invocation implements HasText {
    /** This is the executable that was run. */
    private String m_executable;

    /** This is the data contained between the tags. There may be no data. */
    private StringBuffer m_value;

    /** Default c'tor: Construct a hollow shell and allow further information to be added later. */
    public CommandLine() {
        m_executable = null;
        m_value = null;
    }

    /**
     * Constructs an applications without arguments.
     *
     * @param executable is the name of the application.
     */
    public CommandLine(String executable) {
        m_executable = executable;
        m_value = null;
    }

    /**
     * Constructs an applications with arguments.
     *
     * @param executable is the name of the application.
     * @param value represents the argument line passed.
     */
    public CommandLine(String executable, String value) {
        m_executable = executable;
        m_value = new StringBuffer(value);
    }

    /**
     * Appends a piece of text to the existing text.
     *
     * @param fragment is a piece of text to append to existing text. Appending <code>null</code> is
     *     a noop.
     */
    public void appendValue(String fragment) {
        if (fragment != null) {
            if (this.m_value == null) this.m_value = new StringBuffer(fragment);
            else this.m_value.append(fragment);
        }
    }

    /**
     * Accessor
     *
     * @see #setExecutable(String)
     */
    public String getExecutable() {
        return this.m_executable;
    }

    /**
     * Accessor.
     *
     * @param executable
     * @see #getExecutable()
     */
    public void setExecutable(String executable) {
        this.m_executable = executable;
    }

    /**
     * Accessor
     *
     * @see #setValue(String)
     */
    public String getValue() {
        return (m_value == null ? null : m_value.toString());
    }

    /**
     * Accessor.
     *
     * @param value is the new value to set.
     * @see #getValue()
     */
    public void setValue(String value) {
        this.m_value = (value == null ? null : new StringBuffer(value));
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
        StringBuffer result = new StringBuffer(32 + m_executable.length());

        result.append("<command-line executable=\"");
        result.append(quote(m_executable, true));

        if (m_value == null) {
            // no content
            result.append("\"/>");
        } else {
            // yes, content
            result.append("\">");
            result.append(quote(getValue(), false));
            result.append("</command-line>");
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
                        ? namespace + ":command-line"
                        : "command-line";

        // open tag
        if (indent != null && indent.length() > 0) stream.write(indent);
        stream.write('<');
        stream.write(tag);
        writeAttribute(stream, " executable=\"", m_executable);

        if (m_value != null) {
            // yes, content
            stream.write('>');
            stream.write(quote(getValue(), false));
            stream.write("</");
            stream.write(tag);
            stream.write('>');
        } else {
            // no content
            stream.write("/>");
        }
        if (indent != null) stream.write(System.getProperty("line.separator", "\r\n"));
    }
}
