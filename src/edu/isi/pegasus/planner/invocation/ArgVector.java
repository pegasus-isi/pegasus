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
public class ArgVector extends Arguments {
    /**
     * This is the (new) alternative explicit argument vector. The reason for using a map is that I
     * cannot random access an ArrayList.
     */
    private Map m_argv;

    /** Default c'tor: Construct a hollow shell and allow further information to be added later. */
    public ArgVector() {
        super();
        m_argv = new TreeMap();
    }

    /**
     * Constructs an applications without arguments.
     *
     * @param executable is the name of the application.
     */
    public ArgVector(String executable) {
        super(executable);
        m_argv = new TreeMap();
    }

    /**
     * Returns the full argument vector as one single string.
     *
     * @return a single string joining all arguments with a single space.
     * @see #setValue(int,String)
     */
    public String getValue() {
        StringBuffer result = new StringBuffer(128);

        if (m_argv.size() > 0) {
            boolean flag = false;
            for (Iterator i = m_argv.keySet().iterator(); i.hasNext(); ) {
                Integer key = (Integer) i.next();
                String value = (String) m_argv.get(key);

                if (value != null) {
                    if (flag) result.append(' ');
                    else flag = true;
                    // FIXME: Use single quotes around value, if it contains ws.
                    // FIXME: escape contained apostrophes and esc characters.
                    result.append(value);
                }
            }
        }

        return result.toString();
    }

    /**
     * Sets the argument vector at the specified location.
     *
     * @param position is the position at which to set the entry.
     * @param entry is the argument vector position Setting <code>null</code> is a noop.
     */
    public void setValue(int position, String entry) {
        if (position >= 0) {
            if (entry == null) {
                m_argv.put(new Integer(position), new String());
            } else {
                m_argv.put(new Integer(position), entry);
            }
        }
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
        StringBuffer result = new StringBuffer(64);
        String newline = System.getProperty("line.separator", "\r\n");

        result.append("<argument-vector");
        if (m_executable != null) {
            result.append(" executable=\"");
            result.append(quote(m_executable, true));
            result.append('"');
        }

        if (m_argv.size() == 0) {
            // no content
            result.append("/>");
        } else {
            // yes, content
            String newindent = indent == null ? null : indent + "  ";

            result.append('>');
            if (indent != null) result.append(newline);

            for (Iterator i = m_argv.keySet().iterator(); i.hasNext(); ) {
                Integer key = (Integer) i.next();
                String entry = (String) m_argv.get(key);
                if (entry != null) {
                    if (newindent != null) result.append(newindent);
                    result.append("<arg nr=\"").append(key).append("\">");
                    result.append(quote(entry, false));
                    result.append("</arg>");
                    if (newindent != null) result.append(newline);
                }
            }
            result.append("</argument-vector>");
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
        String newline = System.getProperty("line.separator", "\r\n");
        String tag =
                (namespace != null && namespace.length() > 0)
                        ? namespace + ":argument-vector"
                        : "argument-vector";

        // open tag
        if (indent != null && indent.length() > 0) stream.write(indent);
        stream.write('<');
        stream.write(tag);
        if (m_executable != null) writeAttribute(stream, " executable=\"", m_executable);

        if (m_argv.size() > 0) {
            // yes, new content
            String newindent = indent == null ? null : indent + "  ";
            String newtag =
                    (namespace != null && namespace.length() > 0) ? namespace + ":arg" : "arg";

            stream.write('>');
            if (indent != null) stream.write(newline);
            for (Iterator i = m_argv.keySet().iterator(); i.hasNext(); ) {
                String key = (String) i.next();
                String entry = (String) m_argv.get(key);
                if (entry != null) {
                    if (newindent != null && newindent.length() > 0) stream.write(newindent);
                    stream.write('<');
                    stream.write(newtag);
                    writeAttribute(stream, " nr=\"", key);
                    stream.write('>');
                    stream.write(quote(entry, false));
                    stream.write("</");
                    stream.write(newtag);
                    stream.write('>');
                    if (indent != null) stream.write(newline);
                }
            }

            if (indent != null && indent.length() > 0) stream.write(indent);
            stream.write("</");
            stream.write(tag);
            stream.write('>');

        } else {
            // no content
            stream.write("/>");
        }

        if (indent != null) stream.write(newline);
    }
}
