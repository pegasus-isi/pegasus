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
 * <code>Use</code> is employed to reference bound actual arguments. Note that actual arguments are
 * either of type <code>Scalar</code> or of type <code>List</code>. Each argument has a preferred
 * linkage that is optionally repeated in this usage class. <code>Use</code> extends the base class
 * <code>Leaf</code> by adding most attributes of all siblings.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 * @see Leaf
 * @see Text
 * @see LFN
 * @see Value
 * @see Scalar
 * @see List
 */
public class Use extends Leaf implements Cloneable, Serializable {
    /**
     * The linkage type when refering to an argument that contains a filename. Legal values range
     * from <code>LFN#NONE</code> to <code>LFN#INOUT</code>. The initial value is used to flag the
     * non-initialized state.
     *
     * @see LFN#NONE
     * @see LFN#INPUT
     * @see LFN#OUTPUT
     * @see LFN#INOUT
     * @see LFN#isInRange(int)
     */
    private int m_link = -1;

    /**
     * Stores the name of the bound variable from the actual argument. This must not be empty. A
     * value must be filled in to reach a valid object state.
     */
    private String m_name;

    /**
     * Stores the prefix string to be used when rendering a <code>List</code>. Unused for <code>
     * Scalar</code> content.
     *
     * @see Value
     * @see Scalar
     * @see List
     */
    private String m_prefix;

    /**
     * Stores the separator string used when rendering a <code>List</code>. Unused for <code>Scalar
     * </code> content.
     *
     * @see Value
     * @see Scalar
     * @see List
     */
    private String m_separator;

    /**
     * Stores the suffix string to terminate a <code>List</code> rendering with. Unused for <code>
     * Scalar</code> content.
     *
     * @see Value
     * @see Scalar
     * @see List
     */
    private String m_suffix;

    /**
     * Creates and returns a copy of this object.
     *
     * @return a new instance.
     */
    public Object clone() {
        Use result = new Use(this.m_name, this.m_prefix, this.m_separator, this.m_suffix);
        result.setAnyLink(this.m_link);
        return result;
    }

    /**
     * Default ctor. Creates an empty object that is not valid due to the lack of a bound variable
     * name. To be used by the SAX parser.
     */
    public Use() {
        super();
        this.m_separator = " "; // default per XML Schema
        this.m_prefix = this.m_suffix = "";
    }

    /**
     * Convenience ctor. Creates an empty object with a bound argument name. This ctor should be
     * used by outside applications to assure proper initialization of the bound argument name.
     *
     * @param name is the name of the bound argument to remember.
     */
    public Use(String name) {
        super();
        this.m_name = name;
        this.m_separator = " "; // default per XML Schema
        this.m_prefix = this.m_suffix = "";
    }

    /**
     * Convenience ctor. Creates an object with a bound argument name. This ctor should be used by
     * outside applications to assure proper initialization of the bound argument name.
     *
     * @param name is the name of the bound argument to remember.
     * @param prefix is a prefix when rendering list content into a string.
     * @param separator is a string to be placed between list elements when rendering a list.
     * @param suffix is a suffix when rendering list content into a string.
     * @see Scalar
     * @see List
     */
    public Use(String name, String prefix, String separator, String suffix) {
        super();
        this.m_name = name;
        this.m_prefix = prefix;
        this.m_separator = separator;
        this.m_suffix = suffix;
    }

    /**
     * Convenience ctor. Creates an object with a bound argument name. This ctor should be used by
     * outside applications to assure proper initialization of the bound argument name.
     *
     * @param name is the name of the bound argument to remember.
     * @param link is the linkage type of the bound argument for type checking.
     * @throws IllegalArgumentException if the linkage is not within the legal range between {@link
     *     LFN#NONE} and {@link LFN#INOUT}.
     */
    public Use(String name, int link) throws IllegalArgumentException {
        super();
        this.m_name = name;
        this.m_separator = " "; // default per XML Schema
        this.m_prefix = this.m_suffix = "";
        if (LFN.isInRange(link)) this.m_link = link;
        else throw new IllegalArgumentException();
    }

    /**
     * Accessor: Obtains the current state of the linkage.
     *
     * @return the linkage value. The returned value might be -1 to indicate that the linkage was
     *     not initialized. Note that -1 is an out of range value for linkage.
     * @see #setLink(int)
     */
    public int getLink() {
        return this.m_link;
    }

    /**
     * Accessor: Obtains the name of the bound actual argument.
     *
     * @return the bound name. A misconfigured object might return an empty or null string.
     * @see #setName(String)
     */
    public String getName() {
        return this.m_name;
    }

    /**
     * Accessor: Obtains the current prefix rendering information. The prefix is used in {@link
     * List} rendering as front bracket.
     *
     * @return the prefix rendering string, which might be null or empty.
     * @see #setPrefix(String)
     */
    public String getPrefix() {
        return this.m_prefix;
    }

    /**
     * Accessor: Obtains the current separator rendering information. The separator is used in
     * {@link List} rendering as element separator.
     *
     * @return the separator rendering string, which might be null or empty.
     * @see #setSeparator(String)
     */
    public String getSeparator() {
        return this.m_separator;
    }

    /**
     * Accessor: Obtains the current suffix rendering information. The suffix is used in {@link
     * List} rendering as rear bracket.
     *
     * @return the suffix rendering string, which might be null or empty.
     * @see #setSuffix(String)
     */
    public String getSuffix() {
        return this.m_suffix;
    }

    /**
     * Accessor: Sets the linkage of the bound argument.
     *
     * @param link is the linkage value as integer within the range.
     * @throws IllegalArgumentException if the linkage is not within the legal range between {@link
     *     LFN#NONE} and {@link LFN#INOUT}.
     * @see #getLink()
     * @see LFN#NONE
     * @see LFN#INPUT
     * @see LFN#OUTPUT
     * @see LFN#INOUT
     * @see LFN#isInRange(int)
     */
    public void setLink(int link) throws IllegalArgumentException {
        if (LFN.isInRange(link)) this.m_link = link;
        else throw new IllegalArgumentException();
    }

    private void setAnyLink(int link) {
        this.m_link = link;
    }

    /**
     * Accessor: Sets or overwrites the name of the bound argument. Do not use empty or null strings
     * here.
     *
     * @param name is the new variable name to remember.
     * @see #getName()
     */
    public void setName(String name) {
        this.m_name = name;
    }

    /**
     * Accessor: Sets or overwrites the current prefix rendering information. The prefix is used in
     * {@link List} rendering as front bracket.
     *
     * @param prefix is a rendering string, which might be null or empty.
     * @see #getPrefix()
     */
    public void setPrefix(String prefix) {
        this.m_prefix = prefix;
    }

    /**
     * Accessor: Sets or overwrites the current separator rendering information. The separator is
     * used between {@link List} element during rendering.
     *
     * @param separator is a rendering string, which might be null or empty.
     * @see #getSeparator()
     */
    public void setSeparator(String separator) {
        this.m_separator = separator;
    }

    /**
     * Accessor: Sets or overwrites the current suffix rendering information. The suffix is used in
     * {@link List} rendering as rear bracket.
     *
     * @param suffix is a rendering string, which might be null or empty.
     * @see #getSuffix()
     */
    public void setSuffix(String suffix) {
        this.m_suffix = suffix;
    }

    /**
     * Dump content of this instance representation into a stream.
     *
     * <p>FIXME: The rendering information is not dumped into the non-XML output.
     *
     * @param stream is a stream opened and ready for writing. This can also be a string stream for
     *     efficient output. The stream interface should be able to handle large elements
     *     efficiently.
     * @exception IOException if something fishy happens to the stream.
     */
    public void toString(Writer stream) throws IOException {
        boolean has_fix =
                (this.m_prefix != null && this.m_prefix.length() > 0
                        || this.m_suffix != null && this.m_suffix.length() > 0);
        boolean has_sep = (this.m_separator == null || !this.m_separator.equals(" "));

        if (has_fix || has_sep) {
            // must use the tedious version
            stream.write("${");
            if (has_fix) {
                // this is the ${pre:sep:suf|link:id} version
                stream.write('"');
                if (this.m_prefix != null) stream.write(escape(this.m_prefix));
                stream.write("\":\"");
                if (this.m_separator != null) stream.write(escape(this.m_separator));
                stream.write("\":\"");
                if (this.m_suffix != null) stream.write(escape(this.m_suffix));
                stream.write("\"|");
            } else if (has_sep) {
                // this is the ${sep|link:id} version, mind that " " is IMPLIED!
                // thus, ${""|link:id} is the output for any null separator, while
                // ${link:id} will be the output for a space separator.
                stream.write('"');
                if (this.m_separator != null) stream.write(escape(this.m_separator));
                stream.write("\"|");
            }

            if (LFN.isInRange(this.m_link)) {
                stream.write(LFN.toString(this.m_link)); // no need to escape()
                stream.write(':');
            }
            stream.write(escape(this.m_name));
            stream.write('}');
        } else if (LFN.isInRange(this.m_link)) {
            // use the type-casting version
            stream.write('(');
            stream.write(LFN.toString(this.m_link)); // no need to escape()
            stream.write(')');
            stream.write(escape(this.m_name));
        } else {
            // can use minimal version
            stream.write(escape(this.m_name));
        }
    }

    /**
     * Dump the state of the current element as XML output. This function traverses all sibling
     * classes as necessary, and converts the data into pretty-printed XML output. The stream
     * interface should be able to handle large output efficiently, if you use a buffered writer.
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
        if (indent != null && indent.length() > 0) stream.write(indent);
        stream.write('<');
        if (namespace != null && namespace.length() > 0) {
            stream.write(namespace);
            stream.write(':');
        }
        stream.write("use");
        writeAttribute(stream, " name=\"", this.m_name);
        if (LFN.isInRange(this.m_link))
            writeAttribute(stream, " link=\"", LFN.toString(this.m_link));
        if (this.m_prefix != null && this.m_prefix.length() > 0)
            writeAttribute(stream, " prefix=\"", this.m_prefix);

        // If the separator is empty, write it. We may not need to write it,
        // if the separator is a space.
        if (this.m_separator == null || !this.m_separator.equals(" ")) {
            stream.write(" separator=\"");
            if (this.m_separator != null) stream.write(quote(this.m_separator, true));
            stream.write('\"');
        }

        if (this.m_suffix != null && this.m_suffix.length() > 0)
            writeAttribute(stream, " suffix=\"", this.m_suffix);

        stream.write("/>");
        if (indent != null) stream.write(System.getProperty("line.separator", "\r\n"));
    }
}
