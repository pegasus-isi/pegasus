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
package org.griphyn.cPlanner.partitioner.graph;

import org.griphyn.cPlanner.classes.Data;

import java.util.Iterator;
import java.util.List;

/**
 * Data class that allows us to construct information about the nodes
 * in the abstract graph. Contains for each node the references to it's
 * parents and children. The direction of the edges is usually following the
 * children from a node. Parents are kept to facilitate bottom up traversals.
 *
 * @author Karan Vahi
 * @version $Revision: 1.2 $
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
     * The content associated with this node.
     */
    private GraphNodeContent mContent;

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
        mChildren = new java.util.LinkedList();
        mDepth = -1;
        mLogicalName = new String();
        mColor = this.WHITE_COLOR;
        mBag   = null;
    }

    /**
     * The overloaded constructor.
     *
     * @param id       the id of the node in the graph.
     * @param content  the content to be associated with the node.
     */
    public GraphNode( String id, GraphNodeContent content ){
        this();
        mLogicalID = id;
        mContent = content;
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
     * Sets the bag of objects associated with the node. Overwrite the previous
     * bag if existing.
     *
     * @param bag  the <code>Bag</code> to be associated with the node.
     */
    public void setBag( Bag bag ) {
        mBag = bag;
    }

    /**
     * Sets the content associated with the node. Overwrites the previous
     * content if existing.
     *
     * @param content  the <code>GraphNodeContent</code> to be associated with the node.
     */
    public void setContent( GraphNodeContent content ) {
        mContent = content;
    }


    /**
     * It adds the parents to the node. It ends up overwriting all the existing
     * parents if some already exist.
     */
    public void setParents( List parents ) {
        mParents = parents;
    }

    /**
     * It sets the children to the node. It ends up overwriting all the existing
     * parents if some already exist.
     */
    public void setChildren( List children ) {
        mChildren = children;
    }

    /**
     * Sets the depth associated with the node.
     */
    public void setDepth( int depth ) {
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
     * Returns the content associated with the node.
     *
     * @return the content or null if no content associated
     */
    public GraphNodeContent getContent(){
        return mContent;
    }


    /**
     * Returns a list of <code>GraphNode</code> objects that are parents of the node.
     *
     * @return list of <code>GraphNode</code> objects.
     */
    public List getParents() {
        return mParents;
    }



    /**
     * Returns a list of <code>GraphNode</code> objects that are children of the
     * node.
     *
     * @return list of <code>GraphNode</code> objects.
     */
    public List getChildren() {
        return mChildren;
    }

    /**
     * Adds a child to end of the child list.
     *
     * @param child  adds a child to the node.
     */
    public void addChild( GraphNode child ) {
        mChildren.add( child );
    }

    /**
     * Adds a parent to end of the parent list.
     *
     * @param parent  adds a parent to the node.
     */
    public void addParent( GraphNode parent ) {
        mParents.add( parent );
    }

    /**
     * Removes a child linkage to the node.
     *
     * @param child  child to be removed.
     */
    public void removeChild( GraphNode child ){
        mChildren.remove( child );
    }

    /**
     * Removes a parent linkage to the node.
     *
     * @param parent  parent to be removed.
     */
    public void removeParent( GraphNode parent ){
        mParents.remove( parent );
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
    public boolean isColor( int color ){
        return (mColor == color)?true:false;
    }

    /**
     * Sets the color of the node to the color specified
     *
     * @param color  color that node should be.
     */
    public void setColor( int color ){
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
    public boolean parentsColored( int color ) {
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
     *
     * @return textual description.
     */
    public String toString() {
        StringBuffer sb = new StringBuffer();
        Iterator it;

        sb.append( "ID->" ).append(mLogicalID).append( " name->" ).
            append( mLogicalName ).append( " parents->{" );
        if (mParents != null) {
            it = mParents.iterator();
            while (it.hasNext()) {
                sb.append( ( (GraphNode) it.next()).getID()).append(',');
            }

        }
        sb.append( "} children->{" );
        it = mChildren.iterator();
        while (it.hasNext()) {
            sb.append( ( (GraphNode) it.next()).getID()).append(',');
        }
        sb.append( "}" );
        sb.append( " Content-{" ).append( getContent() ).append( "}" );
        sb.append( " Bag-{" ).append( getBag() ).append( "}" );
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