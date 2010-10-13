/**
 *  Copyright 2007-2008 University Of Southern California
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.griphyn.cPlanner.partitioner;

import edu.isi.pegasus.planner.classes.Data;

import java.util.Iterator;
import java.util.List;

/**
 * Data class that allows us to construct information about the nodes
 * in the abstract graph. Contains for each node the references to it's
 * parents and children.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class GraphNode extends Data {

    //the constants for the color of the nodes
    public static final int WHITE_COLOR = 0;
    public static final int GRAY_COLOR  = 1;
    public static final int BLACK_COLOR = 2;

    /**
     * The logical id of the job as identified in the dax.
     */
    private String mLogicalID;

    /**
     * The logical name of the node as identified in the dax.
     */
    private String mLogicalName;

    /**
     * The depth of the node from the root or any arbitary node.
     */
    private int mDepth;

    /**
     * The color the node is colored.
     */
    private int mColor;

    /**
     * The list of parents of the job/node in the abstract graph. Each element
     * of the list is a <code>GraphNode</code> object.
     */
    private List mParents;

    /**
     * The list of children of the job/node in the abstract graph. Each element
     * of the list is a <code>GraphNode</code> object.
     */
    private List mChildren;

    /**
     * A Bag of objects that maybe associated with the node.
     *
     * @see Bag
     */
    private Bag mBag;

    /**
     * The default constructor.
     */
    public GraphNode() {
        mLogicalID = new String();
        mParents = new java.util.LinkedList();
        mChildren = null;
        mDepth = -1;
        mLogicalName = new String();
        mColor = this.WHITE_COLOR;
        mBag   = null;
    }

    /**
     * The overloaded constructor.
     *
     * @param id    the logical id of the node.
     * @param name  the name of the node.
     */
    public GraphNode(String id, String name) {
        mLogicalID = id;
        mParents = new java.util.LinkedList();
        mChildren = new java.util.LinkedList();
        mDepth = -1;
        mLogicalName = name;
        mColor = this.WHITE_COLOR;
    }

    /**
     * Sets the bag of objects associated with the node.
     */
    public void setBag(Bag bag) {
        mBag = bag;
    }


    /**
     * It adds the parents to the node. It ends up overwriting all the existing
     * parents if some already exist.
     */
    public void setParents(List parents) {
        mParents = parents;
    }

    /**
     * It sets the children to the node. It ends up overwriting all the existing
     * parents if some already exist.
     */
    public void setChildren(List children) {
        mChildren = children;
    }

    /**
     * Sets the depth associated with the node.
     */
    public void setDepth(int depth) {
        mDepth = depth;
    }


    /**
     * Returns the bag of objects associated with the node.
     *
     * @return the bag or null if no bag associated
     */
    public Bag getBag(){
        return mBag;
    }

    /**
     * Returns a reference to the parents of the node.
     */
    public List getParents() {
        return mParents;
    }

    /**
     * Returns a reference to the parents of the node.
     */
    public List getChildren() {
        return mChildren;
    }

    /**
     * Adds a child to end of the child list.
     */
    public void addChild(GraphNode child) {
        mChildren.add(child);
    }

    /**
     * Returns the logical id of the graph node.
     */
    public String getID() {
        return mLogicalID;
    }

    /**
     * Returns the logical name of the graph node.
     */
    public String getName() {
        return mLogicalName;
    }

    /**
     * Returns the depth of the node in the graph.
     */
    public int getDepth() {
        return mDepth;
    }


    /**
     * Returns if the color of the node is as specified.
     *
     * @param color  color that node should be.
     */
    public boolean isColor(int color){
        return (mColor == color)?true:false;
    }

    /**
     * Sets the color of the node to the color specified
     *
     * @param color  color that node should be.
     */
    public void setColor(int color){
        mColor = color;
    }


    /**
     * Returns if all the parents of that node have the color that is specified.
     *
     * @param color the color of the node.
     *
     * @return  true if there are no parents or all parents are of the color.
     *          false in all other  cases.
     */
    public boolean parentsColored(int color) {
        boolean colored = true;
        GraphNode par;
        if (mParents == null) {
            return colored;
        }

        Iterator it = mParents.iterator();
        while (it.hasNext() && colored) {
            par = (GraphNode) it.next();
            colored = par.isColor(color);
        }

        return colored;
    }


    /**
     * The textual representation of the graph node.
     */
    public String toString() {
        StringBuffer sb = new StringBuffer();
        Iterator it;

        sb.append("ID->").append(mLogicalID).append(" name->").
            append(mLogicalName).append(" parents->{");
        if (mParents != null) {
            it = mParents.iterator();
            while (it.hasNext()) {
                sb.append( ( (GraphNode) it.next()).getID()).append(',');
            }

        }
        sb.append("} children->{");
        it = mChildren.iterator();
        while (it.hasNext()) {
            sb.append( ( (GraphNode) it.next()).getID()).append(',');
        }
        sb.append("}");
        sb.append(" Bag-{").append(getBag()).append("}");
        return sb.toString();
    }


    /**
     * Returns a copy of the object.
     */
    public Object clone(){
        return new java.lang.CloneNotSupportedException(
            "Clone() not implemented in GraphNode");
    }
}
