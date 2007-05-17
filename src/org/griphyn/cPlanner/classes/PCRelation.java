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

package org.griphyn.cPlanner.classes;

import java.util.Iterator;

import java.io.Writer;
import java.io.StringWriter;
import java.io.IOException;

/**
 * Captures the parent child relationship between the  jobs in the ADAG.
 *
 * @author Karan Vahi
 * @version $Revision: 1.6 $
 */

public class PCRelation extends Data /*implements Comparable*/{

    /**
     * the parent making up the
     * parent child relationship pair
     * in a dag
     */
    public String parent;

    /**
     * the child making up the
     * parent child relationship pair
     * in a dag
     */
    public String child;

    /**
     * this is used for collapsing the dag
     * during the reduction algorithm
     * on the basis of the results returned
     * from the Replica Catalog.
     */
    public boolean isDeleted;

    /**
     * the default constructor
     */
    public PCRelation(){
        parent = new String();
        child = new String();
        isDeleted = false;
    }

    /**
     * the overloaded constructor
     */
    public PCRelation(String parentName,String childName,boolean deleted){
        parent = new String(parentName);
        child  = new String(childName);
        isDeleted = deleted;

    }

    /**
     * the overloaded constructor
     */
    public PCRelation(String parentName,String childName){
        parent = new String(parentName);
        child  = new String(childName);
        isDeleted = false;

    }

    /**
     * Returns the parent in the edge.
     *
     * @return parent
     */
    public String getParent(){
        return parent;
    }


    /**
     * Returns the child in the edge.
     *
     * @return child
     */
    public String getChild(){
        return child;
    }


    /**
     * returns a new copy of the
     * Object
     */
    public Object clone(){
        PCRelation pc = new PCRelation();
        pc.parent     = this.parent;
        pc.child      = this.child;
        pc.isDeleted  = this.isDeleted;

        return pc;
    }

    /**
     * Checks if an object is similar to the one referred to by this class.
     * We compare the primary key to determine if it is the same or not.
     *
     * @return true if the primary key (parent,child) match.
     *         else false.
     */
    public boolean equals(Object o){
        PCRelation rel = (PCRelation)o;

        return (rel.parent.equals(this.parent) &&
                rel.child.equals(this.child))?
               true:
               false;
    }

    public int compareTo(Object o){
        return (this.equals(o))?0:
            1;
    }

    /**
     * Returns the textual description.
     *
     * @return textual description.
     */
    public String toString(){
        StringBuffer sb = new StringBuffer();
        sb.append( "{" ).append( parent ).append( " -> " ).append( child ).
           append( "," ).append( this.isDeleted ).append( "}" );

        return sb.toString();
    }

    /**
     * Returns the DOT description of the object. This is used for visualizing
     * the workflow.
     *
     * @return String containing the Partition object in XML.
     *
     * @exception IOException if something fishy happens to the stream.
     */
    public String toDOT() throws IOException{
        Writer writer = new StringWriter(32);
        toDOT( writer, "" );
        return writer.toString();
    }

    /**
     * Returns the DOT description of the object. This is used for visualizing
     * the workflow.
     *
     * @param stream is a stream opened and ready for writing. This can also
     *               be a StringWriter for efficient output.
     * @param indent  is a <code>String</code> of spaces used for pretty
     *                printing. The initial amount of spaces should be an empty
     *                string. The parameter is used internally for the recursive
     *                traversal.
     *
     *
     * @exception IOException if something fishy happens to the stream.
     */
    public void toDOT( Writer stream, String indent ) throws IOException {
        String newLine = System.getProperty( "line.separator", "\r\n" );

        //write out the edge
        stream.write( indent );
        stream.write( "\"" );
        stream.write( getParent());
        stream.write( "\"" );
        stream.write( " -> ");
        stream.write( "\"" );
        stream.write( getChild() );
        stream.write( "\"" );
        stream.write( newLine );
        stream.flush();
    }


}