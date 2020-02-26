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
 * This class is the container for a regular file object. A regular file object contains just a
 * filename.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 */
public class Regular extends File implements HasFilename {
    /** Name of the file. */
    protected String m_filename;

    /** Default c'tor: Construct a hollow shell and allow further information to be added later. */
    public Regular() {
        super();
        m_filename = null;
    }

    /**
     * Constructs a regular file.
     *
     * @param filename is the name of the file that stat was invoked
     */
    public Regular(String filename) {
        super();
        m_filename = filename;
    }

    /**
     * Accessor
     *
     * @see #setFilename(String)
     */
    public String getFilename() {
        return this.m_filename;
    }

    /**
     * Accessor.
     *
     * @param filename
     * @see #getFilename()
     */
    public void setFilename(String filename) {
        this.m_filename = filename;
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
        String tag = (namespace != null && namespace.length() > 0) ? namespace + ":file" : "file";

        if (indent != null && indent.length() > 0) stream.write(indent);
        stream.write('<');
        stream.write(tag);
        writeAttribute(stream, " name=\"", m_filename);

        if (m_hexbyte != null && m_hexbyte.length() > 0) {
            // yes, content
            stream.write('>');
            stream.write(m_hexbyte);
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
