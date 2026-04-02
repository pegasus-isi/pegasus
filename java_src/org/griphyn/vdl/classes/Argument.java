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
 * A class to encapsulate a command line argument line. The command line is separated into a list of
 * distinct fragments. Each fragment can only be of type <code>Use</code> or <code>Text</code>.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 * @see Leaf
 * @see Text
 * @see Use
 */
public class Argument extends VDL implements Serializable {
    /**
     * Throws this message, if neither &lt;text&gt; nor &lt;use&gt; elements are tried to be added.
     */
    private static final String c_error_message =
            "Only \"text\" and \"use\" elements are allowed inside an \"argument\".";

    /**
     * The command line consists of an ordered list of <code>Leaf</code> pieces, which in their sum
     * create the commandline. Any value passed down is an arbitrary mix of the three potential
     * <code>Leaf</code> types. Each element only allows for <code>Text</code> and <code>Use</code>
     * children in arbitrary number and order.
     *
     * @see Leaf
     * @see Text
     * @see Use
     */
    private ArrayList m_leafList;

    /**
     * Each <code>Argument</code> is a fragment of the complete command line. Each such group (of
     * fragments) can be given a name. Special names of the stdio handles refer to these handles.
     */
    private String m_name;

    /** Array ctor. */
    public Argument() {
        this.m_leafList = new ArrayList();
    }

    /**
     * Standard ctor: Constructs a named <code>Argument</code> group.
     *
     * @param name is the identifier for the argument group.
     */
    public Argument(String name) {
        this.m_name = name;
        this.m_leafList = new ArrayList();
    }

    /**
     * Convenience ctor: Constructs a name argument group, and enters the first (and possibly only)
     * fragment into the group.
     *
     * @param name is the unique identifier for the argument group.
     * @param firstChild is the element to place into the argument group. Only <code>Leaf</code>s of
     *     type <code>Use</code> or <code>Text</code> are permissable.
     * @see Leaf
     * @see Use
     * @see Text
     */
    public Argument(String name, Leaf firstChild) {
        this.m_name = name;
        this.m_leafList = new ArrayList();
        this.m_leafList.add(firstChild);
    }

    /**
     * Accessor: Appends a commandline fragment to the current group.
     *
     * @param vLeaf is the fragment to add. Note that only leaf values of <code>Use</code> or <code>
     *     Text</code> are allowed.
     * @throws IndexOutOfBoundsException if the value cannot be added.
     * @throws IllegalArgumentException if the value type is neither <code>Use</code> nor <code>Text
     *     </code>.
     * @see Leaf
     * @see Text
     * @see Use
     */
    public void addLeaf(Leaf vLeaf) throws IndexOutOfBoundsException, IllegalArgumentException {
        if (vLeaf instanceof Text || vLeaf instanceof Use) this.m_leafList.add(vLeaf);
        else throw new java.lang.IllegalArgumentException(c_error_message);
    }

    /**
     * Accessor: Inserts a <code>Leaf</code> value into a specific position of this commandline
     * group.
     *
     * @param index is the position to insert the item into
     * @param vLeaf is the value to append to the list. Note that only leaf values of <code>Use
     *     </code> or <code>Text</code> are allowed.
     * @throws IndexOutOfBoundsException if the value cannot be added.
     * @throws IllegalArgumentException if the value type is neither <code>Use</code> nor <code>Text
     *     </code>.
     * @see Text
     * @see Use
     */
    public void addLeaf(int index, Leaf vLeaf)
            throws IndexOutOfBoundsException, IllegalArgumentException {
        if (vLeaf instanceof Text || vLeaf instanceof Use) this.m_leafList.add(index, vLeaf);
        else throw new java.lang.IllegalArgumentException(c_error_message);
    }

    /**
     * Accessor: Enumerates the internal values that constitute the content this commandline group.
     *
     * @return the iterator to the commandline group internal list.
     * @deprecated Use the new Collection based interfaces
     */
    public Enumeration enumerateLeaf() {
        return Collections.enumeration(this.m_leafList);
    }

    /**
     * Accessor: Obtains the <code>Leaf</code> at a certain position in the commandline argument
     * group.
     *
     * @param index is the position in the list to obtain a value from
     * @return The <code>Use</code> or <code>Text</code> at the position.
     * @throws IndexOutOfBoundsException if the index points to an element in the list that does not
     *     contain any elments.
     * @see Use
     * @see Text
     */
    public Leaf getLeaf(int index) throws IndexOutOfBoundsException {
        // -- check bound for index
        if ((index < 0) || (index >= this.m_leafList.size())) throw new IndexOutOfBoundsException();

        return (Leaf) this.m_leafList.get(index);
    }

    /**
     * Accessor: Gets an array of all values that constitute the current value content of this
     * commandline group. This array is a copy to avoid write-through modifications.
     *
     * @return an array with a mixture of either <code>Text</code> or <code>Use</code> values.
     * @see Use
     * @see Text
     * @deprecated Use the new Collection based interfaces
     */
    public Leaf[] getLeaf() {
        int size = this.m_leafList.size();
        Leaf[] mLeaf = new Leaf[size];
        System.arraycopy(this.m_leafList.toArray(new Leaf[0]), 0, mLeaf, 0, size);
        return mLeaf;
    }

    /**
     * Accessor: Obtains the size of the commandline group.
     *
     * @return number of elements that an external array needs to be sized to.
     */
    public int getLeafCount() {
        return this.m_leafList.size();
    }

    /**
     * Accessor: Gets an array of all values that constitute the current content. This list is
     * read-only.
     *
     * @return an array with a mixture of either <code>Text</code> or <code>LFN</code> values.
     * @see LFN
     * @see Text
     */
    public java.util.List getLeafList() {
        return Collections.unmodifiableList(this.m_leafList);
    }

    /**
     * Accessor: Obtains the current name of this commandline group.
     *
     * @return the name of this commandline group.
     * @see #setName(java.lang.String)
     */
    public String getName() {
        return this.m_name;
    }

    /**
     * Accessor: Enumerates the internal values that constitute the content of the <code>Scalar
     * </code> element.
     *
     * @return an iterator to walk the list with.
     */
    public Iterator iterateLeaf() {
        return this.m_leafList.iterator();
    }

    /**
     * Accessor: Enumerates the internal values that constitute the content of the <code>Scalar
     * </code> element.
     *
     * @return an enumeration to walk the list with.
     */
    public ListIterator listIterateLeaf() {
        return this.m_leafList.listIterator();
    }

    /**
     * Accessor: Enumerates the internal values that constitute the content of the <code>Scalar
     * </code> element.
     *
     * @param start is the start index
     * @return an enumeration to walk the list with.
     */
    public ListIterator listIterateLeaf(int start) {
        return this.m_leafList.listIterator(start);
    }

    /** Accessor: Removes all values from this commandline group. */
    public void removeAllLeaf() {
        this.m_leafList.clear();
    }

    /**
     * Accessor: Removes a specific fragment from this commandline group.
     *
     * @param index is the position at which an element is to be removed.
     * @return the object that was removed. The removed item is either an <code>Use</code> or a
     *     <code>Text</code>.
     * @see Use
     * @see Text
     */
    public Leaf removeLeaf(int index) {
        return (Leaf) this.m_leafList.remove(index);
    }

    /**
     * Accessor: Overwrites a <code>Use</code> or <code>Text</code> value fragment at a certain
     * position in this command line group.
     *
     * @param index position to overwrite an elment in.
     * @param vLeaf is either a <code>Use</code> or <code>Text</code> object.
     * @throws IndexOutOfBoundsException if the position pointed to is invalid.
     * @throws IllegalArgumentException if the added element is of the incorrect <code>Leaf</code>
     *     type.
     * @see Use
     * @see Text
     */
    public void setLeaf(int index, Leaf vLeaf)
            throws IndexOutOfBoundsException, IllegalArgumentException {
        // -- check bounds for index
        if ((index < 0) || (index >= this.m_leafList.size())) {
            throw new IndexOutOfBoundsException();
        }
        if (vLeaf instanceof Text || vLeaf instanceof Use) this.m_leafList.set(index, vLeaf);
        else throw new IllegalArgumentException(c_error_message);
    } // -- void setLeaf(int, Leaf)

    /**
     * Accessor: Replaces the commandline group with another group value. Warning: The replacements
     * are not checked for being of the correct leaf types.
     *
     * @param leafArray is the external list of <code>Text</code> or <code>Use</code> objects used
     *     to overwrite things.
     * @see Text
     * @see Use
     * @deprecated Use the new Collection based interfaces
     */
    public void setLeaf(Leaf[] leafArray) {
        this.m_leafList.clear();
        this.m_leafList.addAll(Arrays.asList(leafArray));
    }

    /**
     * Accessor: Overwrites internal list with an external list representing a <code>Scalar</code>
     * value.
     *
     * @param leaves is the external list of <code>Text</code> or <code>LFN</code> objects used to
     *     overwrite things.
     * @see Text
     * @see LFN
     */
    public void setLeaf(Collection leaves) {
        this.m_leafList.clear();
        this.m_leafList.addAll(leaves);
    }

    /**
     * Accessor: Replaces or sets the current identifier for this commandline group.
     *
     * @param name is the new identifier to use for this commandline group.
     * @see #getName()
     */
    public void setName(String name) {
        this.m_name = name;
    }

    /**
     * Converts the commandline group into textual format for human consumption.
     *
     * @param stream is a stream opened and ready for writing. This can also be a string stream for
     *     efficient output.
     * @exception IOException if something fishy happens to the stream.
     */
    public void toString(Writer stream) throws IOException {
        stream.write("argument");
        if (this.m_name != null) {
            stream.write(' ');
            stream.write(escape(this.m_name));
        }

        stream.write(" = ");
        for (Iterator i = this.m_leafList.iterator(); i.hasNext(); ) {
            ((Leaf) i.next()).toString(stream);
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
        String newline = System.getProperty("line.separator", "\r\n");
        String tag =
                (namespace != null && namespace.length() > 0)
                        ? namespace + ":argument"
                        : "argument";

        // open tag
        if (indent != null && indent.length() > 0) stream.write(indent);
        stream.write('<');
        stream.write(tag);
        writeAttribute(stream, " name=\"", this.m_name); // null-safe
        stream.write('>');
        if (indent != null) stream.write(newline);

        // dump content
        String newindent = indent == null ? null : indent + "  ";
        for (Iterator i = this.m_leafList.iterator(); i.hasNext(); ) {
            ((Leaf) i.next()).toXML(stream, newindent, namespace);
        }

        // close tag
        if (indent != null && indent.length() > 0) stream.write(indent);
        stream.write("</");
        stream.write(tag);
        stream.write('>');
        if (indent != null) stream.write(newline);
    }
}
