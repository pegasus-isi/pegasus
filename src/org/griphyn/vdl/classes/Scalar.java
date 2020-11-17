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
 * This class implements one of the argument types for parameters passed to transformations from
 * derivations.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 * @see Value
 * @see List
 */
public class Scalar extends Value implements Cloneable, Serializable {
    /**
     * Any value passed down is an arbitrary mix of the three potential {@link Leaf} types.
     *
     * @see Leaf
     * @see Text
     * @see LFN
     */
    private ArrayList m_leafList;

    /**
     * Creates and returns a copy of this object.
     *
     * @return a new instance.
     */
    public Object clone() {
        Scalar result = new Scalar();
        for (int index = 0; index < this.m_leafList.size(); ++index) {
            result.addLeaf((Leaf) this.getLeaf(index).clone());
        }
        return result;
    }

    /** Default ctor. */
    public Scalar() {
        super();
        this.m_leafList = new ArrayList();
    }

    /**
     * Convenience ctor: Initializes the object with the first child to be put into the list of
     * values.
     *
     * @param firstChild is either a <code>LFN</code> or <code>Text</code> object.
     * @see Leaf
     * @see LFN
     * @see Text
     */
    public Scalar(Leaf firstChild) {
        super();
        this.m_leafList = new ArrayList();
        this.m_leafList.add(firstChild);
    }

    /**
     * Accessor: Obtains the value type of this class. By using the abstract method in the parent
     * class, <code>Scalar</code> objects can be distinguished from <code>List</code> objects
     * without using the <code>instanceof</code> operator.
     *
     * @return the fixed value of being a scalar.
     * @see Value#SCALAR
     */
    public int getContainerType() {
        // always
        return Value.SCALAR;
    }

    /**
     * This method determines which container is being used in the abstract base class in order to
     * kludgy statements when printing debug info.
     *
     * @return the symblic identifier for the type of the Value.
     */
    public String getSymbolicType() {
        // always
        return new String("Scalar");
    }

    /**
     * Accessor: Adds a <code>Leaf</code> value to the list of values gathered as the content of a
     * <code>Scalar</code>.
     *
     * @param vLeaf is the value to append to the list. Note that only leaf values of <code>LFN
     *     </code> or <code>Text</code> are allowed.
     * @throws IndexOutOfBoundsException if the value cannot be added.
     * @see Leaf
     * @see Text
     * @see LFN
     */
    public void addLeaf(Leaf vLeaf) throws IndexOutOfBoundsException {
        this.m_leafList.add(vLeaf);
    }

    /**
     * Accessor: Inserts a <code>Leaf</code> value into a specific position of the list of gathered
     * values.
     *
     * @param index is the position to insert the item into
     * @param vLeaf is the value to append to the list. Note that only leaf values of <code>LFN
     *     </code> or <code>Text</code> are allowed.
     * @throws IndexOutOfBoundsException if the value cannot be added.
     * @see Text
     * @see LFN
     */
    public void addLeaf(int index, Leaf vLeaf) throws IndexOutOfBoundsException {
        this.m_leafList.add(index, vLeaf);
    }

    /**
     * Accessor: Enumerates the internal values that constitute the content of the <code>Scalar
     * </code> element.
     *
     * @return an enumeration to walk the list with.
     * @deprecated Use the new Collection based interfaces
     */
    public Enumeration enumerateLeaf() {
        return Collections.enumeration(this.m_leafList);
    }

    /**
     * Determines all LFN instances of a given scalar that match the specified linkage. This is a
     * higher-level method employing the given API.
     *
     * @param linkage is the linkage to check for, -1 for all filenames.
     * @return a set of logical filename instances that match the linkage and were part of the
     *     scalar. The result may be an empty set, if no such result were to be found.
     * @see LFN
     */
    public java.util.List getAllLFN(int linkage) {
        java.util.List result = new ArrayList();

        for (Iterator i = iterateLeaf(); i.hasNext(); ) {
            Leaf leaf = (Leaf) i.next();
            if (leaf instanceof LFN) {
                LFN lfn = (LFN) leaf;
                if (linkage == -1 || lfn.getLink() == linkage)
                    result.add(lfn); // add *all* information about this file
            }
        }
        return result;
    }

    /**
     * Determines all LFN instances of a given scalar that match the specified linkage. This is a
     * higher-level method employing the given API. Note that also linkage of NONE will not be found
     * in wildcard search mode.
     *
     * @param linkage is the linkage to check for, -1 for all filenames.
     * @return a set of all logical filenames that match the linkage and were part of the scalar.
     *     The result may be an empty set, if no such result were to be found. For a linkage of -1,
     *     complete LFNs will be returned, for any other linkage, just the filename will be
     *     returned.
     * @see Derivation#getLFNList( int )
     * @see LFN
     */
    public java.util.List getLFNList(int linkage) {
        java.util.List result = new ArrayList();

        for (Iterator i = iterateLeaf(); i.hasNext(); ) {
            Leaf leaf = (Leaf) i.next();
            if (leaf instanceof LFN) {
                LFN local = (LFN) leaf;
                if (linkage == -1 && local.getLink() != LFN.NONE) {
                    result.add(local); // we need *all* information about this file
                } else if (local.getLink() == linkage) {
                    result.add(local.getFilename()); // we may know some things
                }
            }
        }
        return result;
    }

    /**
     * Determines if the scalar contains an LFN of the specified linkage. The logic uses
     * short-circuit evaluation, thus finding things is faster than not finding things.
     *
     * @param filename is the name of the LFN
     * @param linkage is the linkage to check for, -1 for any linkage type.
     * @return true if the LFN is contained in the scalar, false otherwise.
     * @see org.griphyn.vdl.classes.LFN
     */
    public boolean containsLFN(String filename, int linkage) {
        // sanity checks
        if (filename == null)
            throw new NullPointerException("You are searching for a non-existing filename");

        for (Iterator i = this.iterateLeaf(); i.hasNext(); ) {
            Leaf leaf = (Leaf) i.next();
            if (leaf instanceof LFN) {
                int l_link = ((LFN) leaf).getLink();
                String l_name = ((LFN) leaf).getFilename();
                if (linkage == -1 && l_link != LFN.NONE) {
                    if (filename.equals(l_name)) return true;
                } else if (l_link == linkage && filename.equals(l_name)) {
                    return true;
                }
            }
        }

        // not found
        return false;
    }

    /**
     * Accessor: Obtains the <code>Leaf</code> at a certain position in the list of leaf values.
     *
     * @param index is the position in the list to obtain a value from
     * @return The <code>LFN</code> or <code>Text</code> at the position.
     * @throws IndexOutOfBoundsException if the index points to an elment in the list that does not
     *     contain any elments.
     * @see LFN
     * @see Text
     */
    public Leaf getLeaf(int index) throws IndexOutOfBoundsException {
        // -- check bound for index
        if ((index < 0) || (index >= this.m_leafList.size())) throw new IndexOutOfBoundsException();

        return (Leaf) this.m_leafList.get(index);
    }

    /**
     * Accessor: Gets an array of all values that constitute the current content. This array is a
     * copy to avoid write-through modifications.
     *
     * @return an array with a mixture of either <code>Text</code> or <code>LFN</code> values.
     * @see LFN
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
     * Accessor: Obtains the size of the internal list of {@link Leaf}s.
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
     * @return an iterator to walk the list with.
     */
    public ListIterator listIterateLeaf() {
        return this.m_leafList.listIterator();
    }

    /**
     * Accessor: Enumerates the internal values that constitute the content of the <code>Scalar
     * </code> element.
     *
     * @param start is the start index
     * @return a list iterator to walk the list with.
     */
    public ListIterator listIterateLeaf(int start) {
        return this.m_leafList.listIterator(start);
    }

    /** Accessor: Removes the content of the <code>Scalar</code>. */
    public void removeAllLeaf() {
        this.m_leafList.clear();
    }

    /**
     * Accessor: Remove a single item from the list of nodes. The list is shrunken in the process.
     *
     * @param index is the position at which an element is to be removed.
     * @return the object that was removed. The object is either a <code>LFN</code> or a <code>Text
     *     </code>.
     * @see LFN
     * @see Text
     */
    public Leaf removeLeaf(int index) {
        return (Leaf) this.m_leafList.remove(index);
    }

    /**
     * Accessor: Overwrites a <code>LFN</code> or <code>Text</code> value at a certain position in
     * the content-constituting list.
     *
     * @param index position to overwrite an elment in.
     * @param vLeaf is either a <code>LFN</code> or <code>Text</code> object.
     * @throws IndexOutOfBoundsException if the position pointed to is invalid.
     * @see LFN
     * @see Text
     */
    public void setLeaf(int index, Leaf vLeaf) throws IndexOutOfBoundsException {
        // -- check bounds for index
        if ((index < 0) || (index >= this.m_leafList.size())) {
            throw new IndexOutOfBoundsException();
        }
        this.m_leafList.set(index, vLeaf);
    }

    /**
     * Accessor: Overwrites internal list with an external list representing a <code>Scalar</code>
     * value.
     *
     * @param leafArray is the external list of <code>Text</code> or <code>LFN</code> objects used
     *     to overwrite things.
     * @see Text
     * @see LFN
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
     * Converts the object state into textual format for human consumption.
     *
     * @return a textual description of the element and its sub-classes. Be advised that these
     *     strings might become large.
     */
    public String toString() {
        StringBuffer result = new StringBuffer(40);
        for (Iterator i = this.m_leafList.iterator(); i.hasNext(); ) {
            result.append(((Leaf) i.next()).toString());
        }
        return result.toString();
    }

    /**
     * Converts the object state into textual format for human consumption.
     *
     * @param stream is a stream opened and ready for writing. This can also be a string stream for
     *     efficient output.
     */
    public void toString(Writer stream) throws IOException {
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
     *     traversal.
     * @param namespace is the XML schema namespace prefix. If neither empty nor null, each element
     *     will be prefixed with this prefix, and the root element will map the XML namespace.
     * @exception IOException if something fishy happens to the stream.
     */
    public void toXML(Writer stream, String indent, String namespace) throws IOException {
        String tag =
                (namespace != null && namespace.length() > 0) ? namespace + ":scalar" : "scalar";
        String newline = System.getProperty("line.separator", "\r\n");

        // open tag
        if (indent != null && indent.length() > 0) stream.write(indent);
        stream.write('<');
        stream.write(tag);
        stream.write('>');
        if (indent != null) stream.write(newline);

        // dump content
        String newindent = indent == null ? null : indent + "  ";
        for (Iterator i = this.m_leafList.iterator(); i.hasNext(); ) {
            ((Leaf) i.next()).toXML(stream, newindent, namespace);
        }

        // finalize
        if (indent != null && indent.length() > 0) stream.write(indent);
        stream.write("</");
        stream.write(tag);
        stream.write('>');
        if (indent != null) stream.write(newline);
    }
}
