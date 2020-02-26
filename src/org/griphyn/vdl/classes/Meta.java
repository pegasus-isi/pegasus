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

package org.griphyn.vdl.classes;

import java.io.IOException;
import java.io.Serializable;
import java.io.Writer;
import java.util.*;

/**
 * This class implements nothing for meta data.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 */
public class Meta extends VDL implements Serializable {
    /** Stores the content of whatever. */
    private String m_content;

    /** Default ctor. Calls the parent initialization. */
    public Meta() {
        super();
        this.m_content = new String();
    }

    /** Ctor to initialize the content while constructing the class. This is a convenience ctor. */
    public Meta(String content) {
        super();
        this.m_content = content;
    }

    /**
     * Appends text to the current internal state. The text may contain other elements which are not
     * quoted or changed in any way.
     *
     * @param content is the new text to append to the current content.
     * @see #getContent()
     */
    public void addContent(String content) {
        this.m_content += content;
    }

    /**
     * Gets the content state of this object. The text may contain other elements which are not
     * quoted or changed in any way.
     *
     * @return The current state of content. The text may be null.
     * @see #setContent(String)
     */
    public String getContent() {
        return this.m_content;
    }

    /**
     * Overwrites the internal state with new content. The supplied content will become effectively
     * the active state of the object. Usually, this method will be called during SAX assembly of
     * the instance structure.
     *
     * @param content is the new state to register.
     * @see #getContent()
     */
    public void setContent(String content) {
        this.m_content = content;
    }

    /**
     * Converts the active state into something meant for human consumption. This method overwrites
     * the base class default as it can be more efficiently implemented.
     *
     * @return There is not textual representation of metadata in VDLt.
     */
    public String toString() {
        return new String();
    }

    /**
     * Prints the current content onto the stream. This is a no-op, because there is not textual
     * representation of metadata in VDLt.
     *
     * @param stream is a stream opened and ready for writing. This can also be a string stream for
     *     efficient output.
     * @throws IOException if something happens to the stream.
     */
    public void toString(Writer stream) throws IOException {
        // do nothing
    }

    /**
     * Dump the state of the current element as XML output. The stream interface should be able to
     * handle large output efficiently, if you use a buffered writer.
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
        String tag = (namespace != null && namespace.length() > 0) ? namespace + ":meta" : "meta";

        // just one tag
        if (indent != null && indent.length() > 0) stream.write(indent);
        stream.write('<');
        stream.write(tag);
        if (this.m_content.length() > 0) {
            stream.write('>');
            stream.write(quote(this.m_content, false));
            stream.write("</");
            stream.write(tag);
            stream.write('>');
        } else {
            stream.write("/>");
        }
        if (indent != null) stream.write(System.getProperty("line.separator", "\r\n"));
    }
}
