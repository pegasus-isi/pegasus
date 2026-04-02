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
public class Environment extends Invocation {
    /** Mappings of keys to values */
    private Map m_environment;

    /** Default c'tor: Construct a hollow shell and allow further information to be added later. */
    public Environment() {
        m_environment = new HashMap();
    }

    /**
     * Adds an environment entry, effectively a key value pair, to the current environment settings.
     *
     * @param entry is the environment entry to add
     * @return the old entry including <code>null</code>.
     * @see #addEntry( String, String )
     */
    public String addEntry(EnvEntry entry) {
        String key = entry.getKey();
        if (key != null) {
            String value = entry.getValue();
            if (value == null) value = new String();
            return (String) m_environment.put(entry.getKey(), entry.getValue());
        } else {
            return null; // evil!
        }
    }

    /**
     * Adds an environment entry, effectively a key value pair, to the current environment settings.
     *
     * @param key is the identifier for the environment setting.
     * @param value is the value associated with the key.
     * @return the old entry including <code>null</code>.
     * @see #addEntry( EnvEntry )
     */
    public String addEntry(String key, String value) {
        if (key != null) {
            if (value == null) value = new String();
            return (String) m_environment.put(key, value);
        } else {
            return null;
        }
    }

    /**
     * Retrieves the value for a given key
     *
     * @param key is the identifier in the map to retrieve the key for
     * @return the value for the given, which may include <code>null</code>.
     */
    public String get(String key) {
        return (String) m_environment.get(key);
    }

    /**
     * Creates a sorted iterator
     *
     * @return an iterator over sorted keys
     */
    public Iterator iterator() {
        Set result = new TreeSet(m_environment.keySet());
        return result.iterator();
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
                        ? namespace + ":environment"
                        : "environment";

        // open tag
        if (indent != null && indent.length() > 0) stream.write(indent);
        stream.write('<');
        stream.write(tag);
        if (m_environment.size() == 0) {
            // no content
            stream.write("/>");
            if (indent != null) stream.write(newline);
        } else {
            // yes, content
            String newindent = (indent == null) ? null : indent + "  ";
            String envtag =
                    (namespace != null && namespace.length() > 0) ? namespace + ":env" : "env";
            stream.write('>');
            if (indent != null) stream.write(newline);

            for (Iterator i = this.iterator(); i.hasNext(); ) {
                String key = (String) i.next();
                String value = this.get(key);

                if (newindent != null && newindent.length() > 0) stream.write(newindent);
                stream.write('<');
                stream.write(envtag);
                writeAttribute(stream, " key=\"", key);
                stream.write('>');

                if (value != null) stream.write(quote(value, false));

                stream.write("</");
                stream.write(envtag);
                stream.write('>');
                if (indent != null) stream.write(newline);
            }

            stream.write("</");
            stream.write(tag);
            stream.write('>');
        }
        if (indent != null) stream.write(newline);
    }
}
