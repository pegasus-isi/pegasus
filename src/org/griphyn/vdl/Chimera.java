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
package org.griphyn.vdl;

import java.io.*;
import java.text.*;
import org.griphyn.vdl.util.Logging;

/**
 * This abstract class defines a common base for all JAPI Chimera objects. All VDL-related classes
 * must conform to this interface, in order to make various instances available as a reference to
 * this class.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 */
public abstract class Chimera {
    /**
     * Escapes certain characters inappropriate for textual output.
     *
     * @param original is a string that needs to be quoted
     * @return a string that is "safe" to print.
     */
    public static String escape(String original) {
        if (original == null) return null;
        StringBuffer result = new StringBuffer(2 * original.length());
        StringCharacterIterator i = new StringCharacterIterator(original);
        for (char ch = i.first(); ch != i.DONE; ch = i.next()) {
            if (ch == '\r') {
                result.append("\\r");
            } else if (ch == '\n') {
                result.append("\\n");
            } else if (ch == '\t') {
                result.append("\\t");
            } else {
                // Chimera bugzilla bug#21
                // Do not escape apostrophe unless it is required to escape
                // it in the input.
                if (ch == '\"' || ch == '\\') result.append('\\');
                result.append(ch);
            }
        }

        return result.toString();
    }

    /**
     * Escapes certain characters inappropriate for XML content output. FIXME: Quotes within
     * attribute values are still not handled correctly.
     *
     * @param original is a string that needs to be quoted
     * @param isAttribute denotes an attributes value, if set to true. If false, it denotes regular
     *     XML content outside of attributes.
     * @return a string that is "safe" to print as XML.
     */
    public static String quote(String original, boolean isAttribute) {
        if (original == null) return null;
        StringBuffer result = new StringBuffer(2 * original.length());
        StringCharacterIterator i = new StringCharacterIterator(original);
        for (char ch = i.first(); ch != i.DONE; ch = i.next()) {
            switch (ch) {
                case '<':
                    result.append("&lt;");
                    break;
                case '&':
                    result.append("&amp;");
                    break;
                case '>':
                    result.append("&gt;");
                    break;
                case '\'':
                    result.append("&apos;");
                    break;
                case '\"':
                    result.append("&quot;");
                    break;
                default:
                    result.append(ch);
                    break;
            }
        }

        return result.toString();
    }

    /**
     * Dumps content of the given element into a string. This function traverses all sibling classes
     * as necessary and converts the data into textual output.
     *
     * <p>Sibling classes which represent small leaf objects, and can return the necessary data more
     * efficiently, are encouraged to overwrite this method.
     *
     * @return a textual description of the element and its sub-classes. Be advised that these
     *     strings might become large.
     */
    public String toString() {
        StringWriter sw = new StringWriter();
        try {
            this.toString(sw);
            sw.flush();
        } catch (IOException ioe) {
            Logging.instance().log("default", 0, ioe.toString());
        }
        return sw.toString();
    }

    /**
     * Dumps the content of the given element into a stream. This function traverses all sibling
     * classes as necessary and converts the data into textual output.
     *
     * @param s is a stream opened and ready for writing. This can also be a string stream for
     *     efficient output. The stream interface should be able to handle large elements
     *     efficiently.
     * @exception IOException if something fishy happens to the stream.
     */
    public abstract void toString(Writer s) throws IOException;

    /**
     * XML write helper method writes a quoted attribute onto a stream. The terminating quote will
     * be appended automatically. Values will be XML-escaped. No action will be taken, if the value
     * is null.
     *
     * @param stream is the stream to append to
     * @param key is the attribute including initial space, attribute name, equals sign, and opening
     *     quote.
     * @param value is a string value, which will be put within the quotes and which will be
     *     escaped. If the value is null, no action will be taken
     * @exception IOException for stream errors.
     */
    public void writeAttribute(Writer stream, String key, String value) throws IOException {
        if (value != null) {
            stream.write(key);
            stream.write(quote(value, true));
            stream.write('"');
        }
    }

    /**
     * Dumps the state of the current element as XML output. This function traverses all sibling
     * classes as necessary, and converts the data into pretty-printed XML output.
     *
     * <p>Sibling classes which represent small leaf objects, and can return the necessary data more
     * efficiently, are encouraged to overwrite this method.
     *
     * @param indent is a <code>String</code> of spaces used for pretty printing. The initial amount
     *     of spaces should be an empty string. The parameter is used internally for the recursive
     *     traversal. If null, avoidable whitespaces in the output will be avoided.
     * @param namespace is the XML schema namespace prefix. If neither empty nor null, each element
     *     will be prefixed with this prefix, and the root element will map the XML namespace.
     * @return a String which contains the state of the current class and its siblings using XML.
     *     Note that these strings might become large.
     */
    public String toXML(String indent, String namespace) {
        StringWriter sw = new StringWriter();
        try {
            this.toXML(sw, indent, namespace);
            sw.flush();
        } catch (IOException ioe) {
            Logging.instance().log("default", 0, ioe.toString());
        }
        return sw.toString();
    }

    /**
     * Provides backward compatibility.
     *
     * <pre>
     * toXML( stream, indent, (String) null );
     * </pre>
     *
     * @param stream is a stream opened and ready for writing. This can also be a string stream for
     *     efficient output.
     * @param indent is a <code>String</code> of spaces used for pretty printing. The initial amount
     *     of spaces should be an empty string. The parameter is used internally for the recursive
     *     traversal. If a <code>null</code> value is specified, no indentation nor linefeeds will
     *     be generated.
     * @exception IOException if something fishy happens to the stream.
     * @see #toXML( Writer, String, String )
     */
    public void toXML(Writer stream, String indent) throws IOException {
        toXML(stream, indent, (String) null);
    }

    /**
     * Dump the state of the current element as XML output. This function traverses all sibling
     * classes as necessary, and converts the data into pretty-printed XML output. The stream
     * interface should be able to handle large output efficiently, if you used a buffered writer.
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
     * @see java.io.BufferedWriter
     */
    public abstract void toXML(Writer stream, String indent, String namespace) throws IOException;
}
