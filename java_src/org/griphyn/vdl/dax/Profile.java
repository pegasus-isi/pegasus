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
 * A <code>Profile</code> captures scheduler system and application environment specific stuff in a
 * uniform fashion. Each profile declaration assigns a value to a key within a namespace. As of this
 * writing, valid namespaces are
 *
 * <dl>
 *   <dt>vds
 *   <dd>Virtual Data System specific material.
 *   <dt>condor
 *   <dd>If the job runs in using the <a href="http://www.cs.wisc.edu/condor/">Condor</a/>
 *       scheduler, certain items like the "universe" or "requirments" can be set. Please note that
 *       currently the universe is provided as a hint to the logical transformation itself.
 *   <dt>dagman
 *   <dd>The job graph will usually be run by Condor DAGMan. Some issues, e.g. the number of
 *       retries, are specific to DAGMan and not Condor.
 *   <dt>env
 *   <dd>The Unix environment variables that are required for the job.
 * </dl>
 *
 * In the future, more namespaces may be added.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 */
public class Profile extends DAX implements Cloneable {
    /**
     * The namespace of a profile. All profiles must mention their namespace in order to generate
     * acceptable behaviour.
     */
    private String m_namespace;

    /**
     * The identifier within a namespace. The meaning of the key can differ between namespaces.
     * Within the unix namespace, it is the name of an environment variable. Within the condor
     * namespace, it is a Condor submit file key.
     */
    private String m_key;

    /**
     * The value to above keys. Any value passed down is an arbitrary mix of the three potential
     * {@link Leaf} types. A profile value element only allows for {@link PseudoText} and {@link
     * Filename} children in arbitrary number and order.
     *
     * @see Leaf
     * @see PseudoText
     * @see Filename
     */
    private ArrayList m_leafList;

    /**
     * This value records which entity was responsible for setting the profile in the first place.
     */
    private String m_origin;

    /**
     * Creates and returns a copy of this object.
     *
     * @return a new instance.
     */
    public Object clone() {
        Profile result = new Profile(this.m_namespace, this.m_key);
        for (int index = 0; index < this.m_leafList.size(); ++index) {
            result.setLeaf(index, (Leaf) this.getLeaf(index).clone());
        }
        result.setOrigin(this.m_origin);
        return result;
    }

    /** Array ctor. */
    public Profile() {
        this.m_leafList = new ArrayList();
        this.m_origin = null;
    }

    /**
     * Standard ctor: set up everything except a value of the ns.key pair.
     *
     * @param namespace is the namespace within which to operate.
     * @param key is an identifier unique within the namespace.
     */
    public Profile(String namespace, String key) {
        this.m_leafList = new ArrayList();
        this.m_namespace = namespace;
        this.m_key = key;
        this.m_origin = null;
    }

    /**
     * Convenience ctor: set up the first piece of the value in one go.
     *
     * @param namespace is the namespace within which to operate.
     * @param key is an identifier unique within the namespace.
     * @param firstChild is the first fragment of the value. Only <code>Leaf</code>s of type <code>
     *     Filename</code> or <code>PseudoText</code> are permissable.
     * @see Leaf
     * @see Filename
     * @see PseudoText
     */
    public Profile(String namespace, String key, Leaf firstChild) {
        this.m_leafList = new ArrayList();
        this.m_leafList.add(firstChild);
        this.m_namespace = namespace;
        this.m_key = key;
        this.m_origin = null;
    }

    /**
     * Convenience ctor: set up the first piece of the value in one go.
     *
     * @param namespace is the namespace within which to operate.
     * @param key is an identifier unique within the namespace.
     * @param children is a collection of fragments for the value. Only <code>Leaf</code>s of type
     *     <code>Filename</code> or <code>PseudoText</code> are permissable.
     * @see Leaf
     * @see Filename
     * @see PseudoText
     */
    public Profile(String namespace, String key, Collection children) {
        this.m_leafList = new ArrayList();
        this.m_leafList.addAll(children);
        this.m_namespace = namespace;
        this.m_key = key;
        this.m_origin = null;
    }

    /**
     * Accessor: Append a value fragment to this profile instance.
     *
     * @param vLeaf is the fragment to add. Note that only leaf values of <code>Filename</code> or
     *     <code>PseudoText</code> are allowed.
     * @see Leaf
     * @see PseudoText
     * @see Filename
     */
    public void addLeaf(Leaf vLeaf) {
        this.m_leafList.add(vLeaf);
    }

    /**
     * Accessor: Inserts a <code>Leaf</code> value into a specific position of the list of gathered
     * values.
     *
     * @param index is the position to insert the item into
     * @param vLeaf is the value to append to the list. Note that only leaf values of <code>Filename
     *     </code> or <code>PseudoText</code> are allowed.
     * @throws IndexOutOfBoundsException if the value cannot be added.
     * @see Leaf
     * @see PseudoText
     * @see Filename
     */
    public void addLeaf(int index, Leaf vLeaf) throws IndexOutOfBoundsException {
        this.m_leafList.add(index, vLeaf);
    }

    /**
     * Accessor: Enumerates the internal values that constitute the content of the <code>Profile
     * </code> value.
     *
     * @return the iterator to the value fragment list.
     * @deprecated Use the new Collection based interfaces
     */
    public Enumeration enumerateLeaf() {
        return Collections.enumeration(this.m_leafList);
    }

    /**
     * Accessor: Obtains the <code>Leaf</code> at a certain position in the list of profile value
     * fragments.
     *
     * @param index is the position in the list to obtain a value from
     * @return The <code>Filename</code> or <code>PseudoText</code> at the position.
     * @throws IndexOutOfBoundsException if the index points to an element in the list that does not
     *     contain any elments.
     * @see Filename
     * @see PseudoText
     */
    public Leaf getLeaf(int index) throws IndexOutOfBoundsException {
        // -- check bound for index
        if ((index < 0) || (index >= this.m_leafList.size())) throw new IndexOutOfBoundsException();

        return (Leaf) this.m_leafList.get(index);
    }

    /**
     * Accessor: Gets an array of all values that constitute the current value content of a profile.
     *
     * @return an array with a mixture of either <code>PseudoText</code> or <code>Filename</code>
     *     values.
     * @see Filename
     * @see PseudoText
     * @deprecated Use the new Collection based interfaces
     */
    public Leaf[] getLeaf() {
        int size = this.m_leafList.size();
        Leaf[] mLeaf = new Leaf[size];
        System.arraycopy(this.m_leafList.toArray(new Leaf[0]), 0, mLeaf, 0, size);
        return mLeaf;
    }

    /**
     * Accessor: Obtains the number of profile value fragments.
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
     * @return an array with a mixture of either <code>PseudoText</code> or <code>Filename</code>
     *     values.
     * @see PseudoText
     * @see Filename
     */
    public java.util.List getLeafList() {
        return Collections.unmodifiableList(this.m_leafList);
    }

    /**
     * Accessor: Gets the namespace value for the profile.
     *
     * @return the currently active namespace for this instance.
     * @see #setNamespace(java.lang.String)
     */
    public String getNamespace() {
        return this.m_namespace;
    }

    /**
     * Accessor: Obtains the originator for a profile.
     *
     * @return an arbitrary string at the moment.
     * @see #setOrigin( java.lang.String )
     */
    public String getOrigin() {
        return this.m_origin;
    }

    /**
     * Accessor: Gets the key identifier for the profile.
     *
     * @return the currently active key for this instance.
     * @see #setKey(java.lang.String)
     */
    public String getKey() {
        return this.m_key;
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
     * Accessor: Enumerates the internal values that constitute the content of the <code>Leaf</code>
     * element.
     *
     * @return a list iterator to walk the list with.
     */
    public ListIterator listIterateLeaf() {
        return this.m_leafList.listIterator();
    }

    /**
     * Accessor: Enumerates the internal values that constitute the content of the <code>Leaf</code>
     * element.
     *
     * @param start is the start index
     * @return a list iterator to walk the list with.
     */
    public ListIterator listIterateLeaf(int start) {
        return this.m_leafList.listIterator(start);
    }

    /** Accessor: Removes all value fragments from the profile. */
    public void removeAllLeaf() {
        this.m_leafList.clear();
    }

    /**
     * Accessor: Remove a single fragment from the list of value fragments.
     *
     * @param index is the position at which an element is to be removed.
     * @return the object that was removed. The removed item is either an <code>Filename</code> or a
     *     <code>PseudoText</code>.
     * @see Filename
     * @see PseudoText
     */
    public Leaf removeLeaf(int index) {
        return (Leaf) this.m_leafList.remove(index);
    }

    /**
     * Accessor: Overwrites a <code>Filename</code> or <code>PseudoText</code> value fragment at a
     * certain position in the profile value fragment list.
     *
     * @param index position to overwrite an elment in.
     * @param vLeaf is either a <code>Filename</code> or <code>PseudoText</code> object.
     * @throws IndexOutOfBoundsException if the position pointed to is invalid.
     * @see Filename
     * @see PseudoText
     */
    public void setLeaf(int index, Leaf vLeaf) throws IndexOutOfBoundsException {
        // -- check bounds for index
        if ((index < 0) || (index >= this.m_leafList.size())) {
            throw new IndexOutOfBoundsException();
        }
        this.m_leafList.set(index, vLeaf);
    }

    /**
     * Accessor: Overwrites internal value fragments list with an external list representing a
     * profile value.
     *
     * @param leafArray is the external list of <code>PseudoText</code> or <code>Filename</code>
     *     objects used to overwrite things.
     * @see PseudoText
     * @see Filename
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
     * @param leaves is the external list of <code>PseudoText</code> or <code>Filename</code>
     *     objects used to overwrite things.
     * @see PseudoText
     * @see Filename
     */
    public void setLeaf(Collection leaves) {
        this.m_leafList.clear();
        this.m_leafList.addAll(leaves);
    }

    /**
     * Accessor: Sets the originator for a profile.
     *
     * @param origin is the new originator to use, may be <code>null</code>.
     * @see #getOrigin()
     */
    public void setOrigin(String origin) {
        this.m_origin = origin;
    }

    /**
     * Accessor: Adjusts a namespace value to a new state.
     *
     * @param namespace is the new namespace to use.
     * @see #getNamespace()
     */
    public void setNamespace(String namespace) {
        this.m_namespace = namespace;
    }

    /**
     * Accessor: Adjusts the identifier within a namespace.
     *
     * @param key is the new identifier to use from now on.
     * @see #getKey()
     */
    public void setKey(String key) {
        this.m_key = key;
    }

    /**
     * Converts the active state into something meant for human consumption. The method will be
     * called when recursively traversing the instance tree.
     *
     * @param stream is a stream opened and ready for writing. This can also be a string stream for
     *     efficient output.
     */
    public void toString(Writer stream) throws IOException {
        stream.write("profile ");
        stream.write(escape(this.m_namespace));
        stream.write("::");
        stream.write(escape(this.m_key));
        stream.write(" = ");

        for (int i = 0; i < this.m_leafList.size(); ++i) {
            ((Leaf) this.m_leafList.get(i)).toString(stream);
        }
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
     *     traversal.
     * @param namespace is the XML schema namespace prefix. If neither empty nor null, each element
     *     will be prefixed with this prefix, and the root element will map the XML namespace.
     * @exception IOException if something fishy happens to the stream.
     */
    public void toXML(Writer stream, String indent, String namespace) throws IOException {
        String newline = System.getProperty("line.separator", "\r\n");
        String tag =
                (namespace != null && namespace.length() > 0) ? namespace + ":profile" : "profile";

        // open tag
        if (indent != null && indent.length() > 0) stream.write(indent);
        stream.write('<');
        stream.write(tag);
        writeAttribute(stream, " origin=\"", this.m_origin);
        writeAttribute(stream, " namespace=\"", this.m_namespace);
        writeAttribute(stream, " key=\"", this.m_key);
        stream.write('>');
        // NO: if ( indent != null ) stream.write( newline );

        // dump content
        // NO: String newindent = indent==null ? null : indent+"  ";
        for (Iterator i = this.m_leafList.iterator(); i.hasNext(); ) {
            // NO: ((Leaf) i.next()).shortXML( stream, newindent, namespace, 0x00 );
            ((Leaf) i.next()).shortXML(stream, "", namespace, 0x00);
        }

        // close tag
        // NO: if ( indent != null && indent.length() > 0 ) stream.write( indent );
        stream.write("</");
        stream.write(tag);
        stream.write('>');
        if (indent != null) stream.write(newline);
    }
}
