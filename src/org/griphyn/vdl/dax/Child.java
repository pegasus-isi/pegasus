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
 * This class captures the parent-child relationship between any two nodes in a directed acyclic
 * graph. For ease of external transportation, the graph is flattened into this two-level form.
 * Please note that this presentation is slightly less powerful than the true DAGMan form, because
 * for each child, there can be multiple parents, but multiple children cannot be grouped.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 */
public class Child extends DAX implements Cloneable {
    /** Captures the list of parent nodes for this child node. */
    private HashSet m_parentSet;

    /** Captures the element for which we are constructing dependencies. */
    private String m_thisChild = null;

    /**
     * Creates and returns a copy of this object.
     *
     * @return a new instance.
     */
    public Object clone() {
        Child result = new Child(this.m_thisChild);
        for (Iterator i = this.m_parentSet.iterator(); i.hasNext(); ) {
            result.addParent((String) i.next());
        }

        return result;
    }

    /** Default ctor: Constructs a child node w/o any parents */
    public Child() {
        this.m_parentSet = new HashSet();
    }

    /**
     * Ctor: Constructs a child node.
     *
     * @param child is the job ID of the child node.
     */
    public Child(String child) {
        this.m_thisChild = child;
        this.m_parentSet = new HashSet();
    }

    /**
     * Ctor: Constructs a child node.
     *
     * @param child is the job reference of the child node.
     */
    public Child(Job child) {
        this.m_thisChild = child.getID();
        this.m_parentSet = new HashSet();
    }

    /** Convenience ctor: Constructs a child node with one parent. */
    public Child(String child, String parent) {
        this.m_thisChild = child;
        this.m_parentSet = new HashSet();
        this.m_parentSet.add(parent);
    }

    /** Convenience ctor: Constructs a child node with one parent. */
    public Child(Job child, Job parent) {
        this.m_thisChild = child.getID();
        this.m_parentSet = new HashSet();
        this.m_parentSet.add(parent.getID());
    }

    /**
     * Accessor: Adds a parent job id as dependency to the list of parents.
     *
     * @param parent is the parent id to add, <b>not</b> the parent reference.
     * @see Job
     */
    public void addParent(String parent) {
        this.m_parentSet.add(parent);
    }

    /**
     * Accessor: Adds a parent job id as dependency to the list of parents.
     *
     * @param parent is the parent reference to add
     * @see Job
     */
    public void addParent(Job parent) {
        this.m_parentSet.add(parent.getID());
    }

    /**
     * Accessor: Provides an iterator for the parent list.
     *
     * @return the iterator for all dependencies.
     * @see Job
     */
    public Iterator iterateParent() {
        return this.m_parentSet.iterator();
    }

    /**
     * Accessor: Obtains the child identifier.
     *
     * @return the name of the current child, or <code>null</code>, if the element is hollow.
     * @see #setChild( String )
     */
    public String getChild() {
        return this.m_thisChild;
    }

    /**
     * Accessor: Obtains a parent, iff it is in the bag.
     *
     * @param name is the parent id to look up.
     * @return true if the parent is know, false otherwise.
     */
    public boolean getParent(String name) {
        return this.m_parentSet.contains(name);
    }

    //  /**
    //   * Accessor: Obtains the complete parental dependencies (one level).
    //   *
    //   * @return an array with all parent IDs inside.
    //   * @see Job
    //   */
    //  public String[] getParent()
    //  {
    //    int size = this.m_parentSet.size();
    //    String[] mArray = new String[size];
    //    for (Iterator i=this.m_parentSet.iterator(); i.hasNext(); ) {
    //      mArray[index] = (String) i.next();
    //    }
    //    return mArray;
    //  }

    /**
     * Accessor: Obtains the count of parental dependencies.
     *
     * @return the number of parents.
     * @see Job
     */
    public int getParentCount() {
        return this.m_parentSet.size();
    }

    /**
     * Accessor: Removes all parental dependencies.
     *
     * @see Job
     */
    public void removeAllParent() {
        this.m_parentSet.clear();
    }

    /**
     * Accessor: Removes a parent name from the bag.
     *
     * @param name is the name of the parent ID to remove.
     * @return true, if the parent was removed, false, if it was not present.
     * @see Job
     * @see java.util.HashSet#remove(Object)
     */
    public boolean removeParent(String name) {
        return this.m_parentSet.remove(name);
    }

    /**
     * Accessor: Sets the identifier for this dependency child.
     *
     * @param id is the job identifier.
     * @see #getChild()
     */
    public void setChild(String id) {
        this.m_thisChild = id;
    }

    /**
     * Accessor: Sets the identifier for this dependency child.
     *
     * @param job is a job reference.
     * @see #getChild()
     */
    public void setChild(Job job) {
        this.m_thisChild = job.getID();
    }

    /**
     * Updates the identifiers for child and parents from a mapping.
     *
     * @param mapping is the mapping between old and new identifier
     * @return a new instance with mapped identifiers. If none of the old identifiers in mapping are
     *     in the child, the result is the same as a {@link #clone()}.
     */
    public Child updateChild(java.util.Map mapping) {
        Child result = null;
        if (mapping.containsKey(this.m_thisChild)) {
            // child name itself needs mapping
            result = new Child((String) mapping.get(this.m_thisChild));
        } else {
            // child name can be copied
            result = new Child(this.m_thisChild);
        }

        for (Iterator i = this.m_parentSet.iterator(); i.hasNext(); ) {
            String parent = (String) i.next();
            if (mapping.containsKey(parent)) {
                result.addParent((String) mapping.get(parent));
            } else {
                result.addParent(parent);
            }
        }

        return result;
    }

    /**
     * Converts the active state into something meant for human consumption. The method will be
     * called when recursively traversing the instance tree.
     *
     * @param stream is a stream opened and ready for writing. This can also be a string stream for
     *     efficient output.
     */
    public void toString(Writer stream) throws IOException {
        // only do anything for children with parents!
        if (this.m_parentSet.size() > 0) {
            stream.write("    ");
            stream.write("CHILD ");
            stream.write(escape(this.m_thisChild));
            stream.write(" PARENT");
            for (Iterator i = this.m_parentSet.iterator(); i.hasNext(); ) {
                stream.write(' ');
                stream.write(escape((String) i.next()));
            }
            stream.write(System.getProperty("line.separator", "\r\n"));
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
        // only do anything for children with parents!
        if (this.m_parentSet.size() > 0) {
            String newline = System.getProperty("line.separator", "\r\n");
            String tag =
                    (namespace != null && namespace.length() > 0) ? namespace + ":child" : "child";
            String tag2 =
                    (namespace != null && namespace.length() > 0)
                            ? namespace + ":parent"
                            : "parent";

            // open tag
            if (indent != null && indent.length() > 0) stream.write(indent);
            stream.write('<');
            stream.write(tag);
            writeAttribute(stream, " ref=\"", this.m_thisChild);
            stream.write('>');
            if (indent != null) stream.write(newline);

            String newindent = indent == null ? null : indent + "  ";
            for (Iterator i = this.m_parentSet.iterator(); i.hasNext(); ) {
                if (indent != null && indent.length() > 0) stream.write(newindent);
                stream.write('<');
                stream.write(tag2);
                writeAttribute(stream, " ref=\"", (String) i.next());
                stream.write("/>");
                if (indent != null) stream.write(newline);
            }

            if (indent != null && indent.length() > 0) stream.write(indent);
            stream.write("</");
            stream.write(tag);
            stream.write('>');
            if (indent != null) stream.write(newline);
        }
    }
}
