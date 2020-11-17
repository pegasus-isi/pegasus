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
 * This class implements the list argument type used for parameters passed to transformations from
 * derivations.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 * @see Value
 * @see Scalar
 */
public class List extends Value implements Cloneable, Serializable {
    /** A list is just an ordered bunch of {@link Scalar}. */
    private ArrayList m_scalarList;

    /**
     * Creates and returns a copy of this object.
     *
     * @return a new instance.
     */
    public Object clone() {
        List result = new List();
        for (int index = 0; index < this.m_scalarList.size(); ++index) {
            result.addScalar((Scalar) this.getScalar(index).clone());
        }
        return result;
    }

    /** Default ctor. */
    public List() {
        super();
        this.m_scalarList = new ArrayList();
    }

    /**
     * Convenience ctor: Initializes the list, and stores the given {@link Scalar} as first child
     * into the list.
     *
     * @param firstChild is the first element in the list
     */
    public List(Scalar firstChild) {
        super();
        this.m_scalarList = new ArrayList();
        this.m_scalarList.add(firstChild);
    }

    /**
     * Accessor: Obtains the value type of this class. By using the abstract method in the parent
     * class, <code>List</code> objects can be distinguished from <code>Scalar</code> objects
     * without using the <code>instanceof</code> operator.
     *
     * @return the fixed value of being a scalar.
     * @see Value#LIST
     */
    public int getContainerType() {
        return Value.LIST;
    }

    /**
     * This method determines which container is being used in the abstract base class in order to
     * kludgy statements when printing debug info.
     *
     * @return the symblic identifier for the type of the Value.
     */
    public String getSymbolicType() {
        // always
        return new String("List");
    }

    /**
     * Accessor: Appends as <code>Scalar</code> value to the list.
     *
     * @param vScalar is the <code>Scalar</code> to append to the list.
     * @throws IndexOutOfBoundsException if the value cannot be added.
     * @see Scalar
     */
    public void addScalar(Scalar vScalar) throws IndexOutOfBoundsException {
        this.m_scalarList.add(vScalar);
    }

    /**
     * Accessor: Insert a <code>Scalar</code> at a specific position.
     *
     * @param index is the position to insert the item into
     * @param vScalar is the <code>Scalar</code> to append to the list.
     * @throws IndexOutOfBoundsException if the value cannot be added.
     * @see Scalar
     */
    public void addScalar(int index, Scalar vScalar) throws IndexOutOfBoundsException {
        this.m_scalarList.add(index, vScalar);
    }

    /**
     * Accessor: constructs the iterator for the <code>List</code> items.
     *
     * @return an enumeration to walk the list with.
     * @deprecated Use the new Collection based interfaces
     */
    public Enumeration enumerateScalar() {
        return Collections.enumeration(this.m_scalarList);
    }

    /**
     * Determines all LFN instances of a given scalar that match the specified linkage. This is a
     * higher-level method employing the given API.
     *
     * @param linkage is the linkage to check for, -1 for all filenames.
     * @return a set of logical filename instances that match the linkage and were part of the
     *     scalar. The result may be an empty set, if no such result were to be found.
     * @see Scalar#getAllLFN( int )
     * @see LFN
     */
    public java.util.List getAllLFN(int linkage) {
        java.util.List result = new ArrayList();

        for (Iterator i = this.iterateScalar(); i.hasNext(); )
            result.addAll(((Scalar) i.next()).getAllLFN(linkage));

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
     * @see Scalar#getLFNList( int )
     * @see Derivation#getLFNList( int )
     * @see LFN
     */
    public java.util.List getLFNList(int linkage) {
        java.util.List result = new ArrayList();

        for (Iterator i = this.iterateScalar(); i.hasNext(); )
            result.addAll(((Scalar) i.next()).getLFNList(linkage));

        return result;
    }

    /**
     * Determines if the list contains an LFN of the specified linkage. The logic uses short-circuit
     * evaluation, thus finding things is faster than not finding things. Searching a list is a
     * potentially expensive method.
     *
     * @param filename is the name of the LFN
     * @param linkage is the linkage to check for, -1 for any linkage type.
     * @return true if the LFN is contained in the scalar, false otherwise.
     * @see org.griphyn.vdl.classes.LFN
     * @see Scalar#containsLFN( String, int )
     */
    public boolean containsLFN(String filename, int linkage) {
        for (Iterator i = this.iterateScalar(); i.hasNext(); )
            if (((Scalar) i.next()).containsLFN(filename, linkage)) return true;

        // not found
        return false;
    }

    /**
     * Accessor: Obtains the value of a specific item in the list.
     *
     * @param index is the position of which to obtain the value of.
     * @return The {@link Scalar} at the specified position.
     * @throws IndexOutOfBoundsException if the index points to an element that is beyond the list
     *     boundaries.
     */
    public Scalar getScalar(int index) throws IndexOutOfBoundsException {
        // -- check bound for index
        if ((index < 0) || (index >= this.m_scalarList.size()))
            throw new IndexOutOfBoundsException();

        return (Scalar) this.m_scalarList.get(index);
    }

    /**
     * Accessor: Gets an array of all <code>Scalar</code>s in the list.
     *
     * @return an array of <code>Scalar</code>s.
     * @see Scalar
     * @deprecated Use the new Collection based interfaces
     */
    public Scalar[] getScalar() {
        int size = this.m_scalarList.size();
        Scalar[] mScalar = new Scalar[size];
        System.arraycopy(this.m_scalarList.toArray(new Scalar[0]), 0, mScalar, 0, size);
        return mScalar;
    }

    /**
     * Accessor: Obtains the element count of the internal list
     *
     * @return number of elements in the internal list
     */
    public int getScalarCount() {
        return this.m_scalarList.size();
    }

    /**
     * Accessor: Gets an array of all <code>Scalar</code>s in the list. This list is read-only.
     *
     * @return an array of <code>Scalar</code>s.
     * @see Scalar
     */
    public java.util.List getScalarList() {
        return Collections.unmodifiableList(this.m_scalarList);
    }

    /**
     * Accessor: constructs the iterator for the <code>List</code> items.
     *
     * @return an enumeration to walk the list with.
     */
    public Iterator iterateScalar() {
        return this.m_scalarList.iterator();
    }

    /**
     * Accessor: constructs the iterator for the <code>List</code> items.
     *
     * @return an enumeration to walk the list with.
     */
    public ListIterator listIterateScalar() {
        return this.m_scalarList.listIterator();
    }

    /**
     * Accessor: constructs the iterator for the <code>List</code> items.
     *
     * @param start is the starting position for the sub-iteration.
     * @return an enumeration to walk the list with.
     */
    public ListIterator listIterateScalar(int start) {
        return this.m_scalarList.listIterator(start);
    }

    /** Accessor: Removes all elements in the <code>List</code>. */
    public void removeAllScalar() {
        this.m_scalarList.clear();
    }

    /**
     * Accessor: Removes a single element from the <code>List</code>. Each component in this vector
     * with an index greater or equal to the specified index is shifted downward to have an index
     * one smaller than the value it had previously. The size of this vector is decreased by 1.
     *
     * @param index is the position at which an element is to be removed.
     * @return the {@link Scalar} that was removed.
     * @throws ArrayIndexOutOfBoundsException if the index was invalid.
     */
    public Scalar removeScalar(int index) {
        return (Scalar) this.m_scalarList.remove(index);
    }

    /**
     * Accessor: Overwrite an element at a given position.
     *
     * @param index is the position to use. It must be within the list.
     * @param vScalar is the new value to replace the element with.
     * @throws IndexOutOfBoundsException if the position is outside the list.
     */
    public void setScalar(int index, Scalar vScalar) throws IndexOutOfBoundsException {
        // -- check bounds for index
        if ((index < 0) || (index >= this.m_scalarList.size())) {
            throw new IndexOutOfBoundsException();
        }
        this.m_scalarList.set(index, vScalar);
    } // -- void setScalar(int, Scalar)

    /**
     * Accessor: Replaces all elements with a new list of {@link Scalar}s.
     *
     * @param scalarArray is the list to replace the original list with.
     * @deprecated Use the new Collection based interfaces
     */
    public void setScalar(Scalar[] scalarArray) {
        // -- copy array
        this.m_scalarList.clear();
        this.m_scalarList.addAll(Arrays.asList(scalarArray));
    }

    /**
     * Accessor: Replaces all elements with a new list of {@link Scalar}s.
     *
     * @param scalars is the list to replace the original list with.
     */
    public void setScalar(Collection scalars) {
        this.m_scalarList.clear();
        this.m_scalarList.addAll(scalars);
    }

    /**
     * Dumps the list and all its contents into a string. The list will be terminated by brackets,
     * elements separated by komma, space. Elements itself will be dumped by recursive calls to the
     * element specific method of the same name.
     *
     * @param stream is a stream opened and ready for writing. This can also be a string stream for
     *     efficient output.
     * @exception IOException if something fishy happens to the stream.
     */
    public void toString(Writer stream) throws IOException {
        stream.write("[ ");
        for (Iterator i = this.m_scalarList.iterator(); i.hasNext(); ) {
            ((Scalar) i.next()).toString(stream);
            if (i.hasNext()) stream.write(", ");
        }
        stream.write(" ]");
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
        String newline = System.getProperty("line.separator", "\r\n");
        String tag = (namespace != null && namespace.length() > 0) ? namespace + ":list" : "list";

        // open tag
        if (indent != null && indent.length() > 0) stream.write(indent);
        stream.write('<');
        stream.write(tag);
        stream.write('>');
        if (indent != null) stream.write(newline);

        // dump content
        String newindent = indent == null ? null : indent + "  ";
        for (Iterator i = this.m_scalarList.iterator(); i.hasNext(); ) {
            // FIXME: If we cast to Value, we can have lists in lists
            ((Scalar) i.next()).toXML(stream, newindent, namespace);
        }

        // close tag
        if (indent != null && indent.length() > 0) stream.write(indent);
        stream.write("</");
        stream.write(tag);
        stream.write('>');
        if (indent != null) stream.write(newline);
    }
}
