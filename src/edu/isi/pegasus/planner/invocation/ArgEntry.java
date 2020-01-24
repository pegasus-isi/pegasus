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
 * This class carries an argument vector entry for the argument vector. This calls is expected to be
 * transient to the parsing process only.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 */
public class ArgEntry extends Invocation implements HasText {
    /** argument vector position */
    private int m_position;

    /** argument vector value */
    private StringBuffer m_value;

    /** Default c'tor: Construct a hollow shell and allow further information to be added later. */
    public ArgEntry() {
        m_position = -1;
        m_value = null;
    }

    /**
     * C'tor: Prepares a given key for accepting a value later on.
     *
     * @param position is the location to use.
     */
    public ArgEntry(int position) {
        m_position = position;
        m_value = null;
    }

    /**
     * C'tor: Fully initializes the class
     *
     * @param position is the location to use.
     * @param value is the value to remember
     */
    public ArgEntry(int position, String value) {
        m_position = position;
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
     * @return the position of this entry in the argument vector.
     * @see #setPosition(int)
     */
    public int getPosition() {
        return this.m_position;
    }

    /**
     * Accessor.
     *
     * @param position
     * @see #getPosition()
     */
    public void setPosition(int position) {
        this.m_position = position;
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
        stream.write('[');
        stream.write(m_position);
        stream.write("]=");
        stream.write(getValue());
    }

    /**
     * Dumps the state of the current element as XML output. However, for the given instance, this
     * class is ludicrious.
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
        String tag = (namespace != null && namespace.length() > 0) ? namespace + ":arg" : "arg";

        // open tag
        if (indent != null && indent.length() > 0) stream.write(indent);
        stream.write('<');
        stream.write(tag);
        writeAttribute(stream, " id=\"", Integer.toString(m_position));
        stream.write('>');

        stream.write(quote(getValue(), false));

        stream.write("</");
        stream.write(tag);
        stream.write('>');
        if (indent != null) stream.write(System.getProperty("line.separator", "\r\n"));
    }
}
