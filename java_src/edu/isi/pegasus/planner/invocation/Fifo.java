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
 * This class is the container for a FIFO object. A FIFO, also known as named pipe, does not consume
 * space on the filesystem except for an inode.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 */
public class Fifo extends Temporary {
    /** optional message count for the FIFO. */
    protected int m_count;

    /** optional number of bytes read from FIFO. */
    protected long m_rsize;

    /**
     * optional number of bytes written - but not to the FIFO. This has to do with the message size
     * that was created from the original input message.
     */
    protected long m_wsize;

    /** Default c'tor: Construct a hollow shell and allow further information to be added later. */
    public Fifo() {
        super();
        m_count = 0;
        m_rsize = m_wsize = 0;
    }

    /**
     * Constructs a FIFO object.
     *
     * @param filename is the name of the file that stat was invoked
     * @param descriptor is a valid file descriptor number.
     */
    public Fifo(String filename, int descriptor) {
        super(filename, descriptor);
        m_count = 0;
        m_rsize = m_wsize = 0;
    }

    /**
     * Accessor
     *
     * @see #setCount(int)
     */
    public int getCount() {
        return this.m_count;
    }

    /**
     * Accessor.
     *
     * @param count
     * @see #getCount()
     */
    public void setCount(int count) {
        this.m_count = count;
    }

    /**
     * Accessor
     *
     * @see #setInputSize(long)
     */
    public long getInputSize() {
        return this.m_rsize;
    }

    /**
     * Accessor.
     *
     * @param rsize
     * @see #getInputSize()
     */
    public void setInputSize(long rsize) {
        this.m_rsize = rsize;
    }

    /**
     * Accessor
     *
     * @see #setOutputSize(long)
     */
    public long getOutputSize() {
        return this.m_wsize;
    }

    /**
     * Accessor.
     *
     * @param wsize
     * @see #getOutputSize()
     */
    public void setOutputSize(long wsize) {
        this.m_wsize = wsize;
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
        String tag = (namespace != null && namespace.length() > 0) ? namespace + ":fifo" : "fifo";

        // open tag
        if (indent != null && indent.length() > 0) stream.write(indent);
        stream.write('<');
        stream.write(tag);
        writeAttribute(stream, " name=\"", m_filename);
        writeAttribute(stream, " descriptor=\"", Integer.toString(m_descriptor));
        writeAttribute(stream, " count=\"", Integer.toString(m_count));
        writeAttribute(stream, " rsize=\"", Long.toString(m_rsize));
        writeAttribute(stream, " wsize=\"", Long.toString(m_wsize));

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
