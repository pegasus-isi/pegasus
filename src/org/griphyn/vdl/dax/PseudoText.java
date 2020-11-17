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
package org.griphyn.vdl.dax;

import java.io.IOException;
import java.io.Writer;
import java.util.*;

/**
 * This class extends the base class <code>Leaf</code> by adding an attribute to store the content
 * of a the textual data of mixed content. The <code>PseudoText</code> is not an element!
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 * @see Leaf
 */
public class PseudoText extends Leaf implements Cloneable {
    /** Stores the content of the textual element. */
    private String m_content;

    /**
     * Creates and returns a copy of this object.
     *
     * @return a new instance.
     */
    public Object clone() {
        // java.lang.String implements inherently copy-on-write.
        return new PseudoText(this.m_content);
    }

    /** Default ctor. */
    public PseudoText() {
        super();
    }

    /** Ctor to initialize the content while constructing the class. This is a convenience ctor. */
    public PseudoText(String content) {
        super();
        this.m_content = content;
    }

    /**
     * Gets the content state of this object. The text may contain other elements which are not
     * quoted or changed in any way, because the text element is designed to be a leaf node.
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
     * Converts the active state into something meant for human consumption. The method will be
     * called when recursively traversing the instance tree. This method overwrites the default
     * method, since it appears to be faster to do directly.
     *
     * @return The current content enclosed in quotes.
     */
    public String toString() {
        StringBuffer result = new StringBuffer(this.m_content.length() + 2);
        result.append('"');
        if (this.m_content != null) result.append(escape(this.m_content));
        result.append('"');
        return result.toString();
    }

    /**
     * Converts the active state into something meant for human consumption. The method will be
     * called when recursively traversing the instance tree.
     *
     * @param stream is a stream opened and ready for writing. This can also be a string stream for
     *     efficient output.
     */
    public void toString(Writer stream) throws IOException {
        stream.write('"');
        if (this.m_content != null) stream.write(escape(this.m_content));
        stream.write('"');
    }

    //  static private final String empty_element = "</text>";

    /**
     * Converts the active state into something meant for computer consumption. The method will be
     * called when recursively traversing the instance tree. This method overwrites the inherited
     * methods since it appears to be faster to do it this way.
     *
     * <p>FIXME: Contents will not be properly XML quoted.
     *
     * @param indent is an arbitrary string to prefix a line with for pretty printing. Usually, an
     *     initial amount of zero spaces are used. Unused in this case.
     * @param namespace is the XML schema namespace prefix. If neither empty nor null, each element
     *     will be prefixed with this prefix, and the root element will map the XML namespace.
     * @return a string which contains the current string. Since the string is part of mixed
     *     content, no element tags are supplied, nor any additional whitespaces.
     */
    public String toXML(String indent, String namespace) {
        //    if ( m_content != null && m_content.length() > 0 ) {
        //      StringBuffer result = new StringBuffer( m_content.length() + 16 );
        //      result.append( "<text>"). append( quote(this.m_content,false) ).append("</text>");
        //      return result.toString();
        //    } else {
        //      return empty_element;
        //    }
        if (this.m_content != null) {
            return new String(quote(this.m_content, false));
        } else {
            return new String();
        }
    }

    /**
     * Dump the state of the current element as XML output. This function traverses all sibling
     * classes as necessary, and converts the data into pretty-printed XML output. The stream
     * interface should be able to handle large output efficiently.
     *
     * <p>FIXME: Contents will not be properly XML quoted.
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
        //    if ( m_content != null && m_content.length() > 0 ) {
        //      stream.write( "<text>");
        //      stream.write( quote(this.m_content,false) );
        //      stream.write("</text>");
        //    } else {
        //      stream.write(empty_element);
        //    }
        if (this.m_content != null) {
            stream.write(quote(this.m_content, false));
        }
    }
}
