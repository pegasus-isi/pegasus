/**
 * Copyright 2007-2008 University Of Southern California
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.isi.pegasus.planner.classes;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;

/**
 * Captures the parent child relationship between the jobs in the ADAG.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class PCRelation extends Data /*implements Comparable*/ {

    /** the parent making up the parent child relationship pair in a dag */
    public String parent;

    /** the child making up the parent child relationship pair in a dag */
    public String child;

    /**
     * this is used for collapsing the dag during the reduction algorithm on the basis of the
     * results returned from the Replica Catalog.
     */
    public boolean isDeleted;

    /** The abstract id for the parent node. required for stampede events. */
    private String mAbstractParentID;

    /** The abstract id for the child node. required for stampede events. */
    private String mAbstractChildID;

    /** the default constructor */
    public PCRelation() {
        parent = "";
        child = "";
        mAbstractParentID = "";
        mAbstractChildID = "";
        isDeleted = false;
    }

    /** the overloaded constructor */
    public PCRelation(String parentName, String childName, boolean deleted) {
        this();
        parent = parentName;
        child = childName;
        isDeleted = deleted;
    }

    /** the overloaded constructor */
    public PCRelation(String parentName, String childName) {
        this();
        parent = parentName;
        child = childName;
        isDeleted = false;
    }

    /**
     * Returns the parent in the edge.
     *
     * @return parent
     */
    public String getParent() {
        return parent;
    }

    /**
     * Returns the child in the edge.
     *
     * @return child
     */
    public String getChild() {
        return child;
    }

    /**
     * Sets the abstract parent id associated with the edge.
     *
     * @param id the abstract id
     */
    public void setAbstractParentID(String id) {
        mAbstractParentID = id;
    }

    /**
     * Sets the abstract child id associated with the edge.
     *
     * @param id the abstract id
     */
    public void setAbstractChildID(String id) {
        mAbstractChildID = id;
    }

    /**
     * Returns the abstract parent id associated with the edge.
     *
     * @return parent
     */
    public String getAbstractParentID() {
        return mAbstractParentID;
    }

    /**
     * Returns the abstract child id associated with the edge.
     *
     * @return child
     */
    public String getAbstractChildID() {
        return mAbstractChildID;
    }

    /** returns a new copy of the Object */
    public Object clone() {
        PCRelation pc = new PCRelation();
        pc.parent = this.parent;
        pc.child = this.child;
        pc.isDeleted = this.isDeleted;
        pc.mAbstractChildID = this.mAbstractChildID;
        pc.mAbstractParentID = this.mAbstractParentID;
        return pc;
    }

    /**
     * Checks if an object is similar to the one referred to by this class. We compare the primary
     * key to determine if it is the same or not.
     *
     * @return true if the primary key (parent,child) match. else false.
     */
    public boolean equals(Object o) {
        PCRelation rel = (PCRelation) o;

        return (rel.parent.equals(this.parent) && rel.child.equals(this.child)) ? true : false;
    }

    public int compareTo(Object o) {
        return (this.equals(o)) ? 0 : 1;
    }

    /**
     * Returns the textual description.
     *
     * @return textual description.
     */
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append("{")
                .append(parent)
                .append(" [")
                .append(this.getAbstractParentID())
                .append("]")
                .append(" -> ")
                .append(child)
                .append(" [")
                .append(this.getAbstractChildID())
                .append("]")
                .append(",")
                .append(this.isDeleted)
                .append("}");

        return sb.toString();
    }

    /**
     * Returns the DOT description of the object. This is used for visualizing the workflow.
     *
     * @return String containing the Partition object in XML.
     * @exception IOException if something fishy happens to the stream.
     */
    public String toDOT() throws IOException {
        Writer writer = new StringWriter(32);
        toDOT(writer, "");
        return writer.toString();
    }

    /**
     * Returns the DOT description of the object. This is used for visualizing the workflow.
     *
     * @param stream is a stream opened and ready for writing. This can also be a StringWriter for
     *     efficient output.
     * @param indent is a <code>String</code> of spaces used for pretty printing. The initial amount
     *     of spaces should be an empty string. The parameter is used internally for the recursive
     *     traversal.
     * @exception IOException if something fishy happens to the stream.
     */
    public void toDOT(Writer stream, String indent) throws IOException {
        String newLine = System.getProperty("line.separator", "\r\n");

        // write out the edge
        stream.write(indent);
        stream.write("\"");
        stream.write(getParent());
        stream.write("\"");
        stream.write(" -> ");
        stream.write("\"");
        stream.write(getChild());
        stream.write("\"");
        stream.write(newLine);
        stream.flush();
    }

    /**
     * Sets the child of the edge.
     *
     * @param childNode the child node
     */
    public void setChild(String childNode) {
        this.child = childNode;
    }

    /**
     * Sets the parent of the edge.
     *
     * @param parentNode the parent node
     */
    public void setParent(String parentNode) {
        this.parent = parentNode;
    }
}
