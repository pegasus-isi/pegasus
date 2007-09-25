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

import java.util.Enumeration;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;
import java.util.Vector;


import java.io.Writer;
import java.io.StringWriter;
import java.io.IOException;


/**
 *  This class object contains the info about a Dag.
 *  DagInfo object contains the information to create the .dax file.
 *  vJobSubInfos is a Vector containing SubInfo objects of jobs making
 *  the Dag.
 *  Each subinfo object contains information needed to generate a submit
 *  file for that job.
 *
 * @author Karan Vahi
 * @author Gaurang Mehta
 * @version $Revision$
 *
 * @see DagInfo
 * @see SubInfo
 */

public class ADag extends Data {

    /**
     * The DagInfo object which contains the information got from parsing the
     * dax file.
     */
    public DagInfo dagInfo;

    /**
     * Vector of <code>SubInfo</code> objects. Each SubInfo object contains
     * information corresponding to the submit file for one job.
     */
    public Vector vJobSubInfos;

    /**
     * The root of the submit directory hierarchy for the DAG. This is the
     * directory where generally the DAG related files like the log files,
     * .dag and dagman output files reside.
     */
    private String mSubmitDirectory;



    /**
     * Initialises the class member variables.
     */
    public ADag() {
        dagInfo          = new DagInfo();
        vJobSubInfos     = new Vector();
        mSubmitDirectory = ".";
    }

    /**
     * Overloaded constructor.
     *
     * @param dg     the <code>DagInfo</code>
     * @param vSubs  the jobs in the workflow.
     */
    public ADag (DagInfo dg, Vector vSubs){
        this.dagInfo      = (DagInfo)dg.clone();
        this.vJobSubInfos = (Vector)vSubs.clone();
        mSubmitDirectory  = ".";
    }

    /**
     * Returns a new copy of the Object.
     *
     * @return the clone of the object.
     */
    public Object clone(){
        ADag newAdag        = new ADag();
        newAdag.dagInfo     = (DagInfo)this.dagInfo.clone();
        newAdag.vJobSubInfos= (Vector)this.vJobSubInfos.clone();
        newAdag.setBaseSubmitDirectory( this.mSubmitDirectory );
        return newAdag;
    }


    /**
     * Returns the String description of the dag associated with this object.
     *
     * @return textual description.
     */
    public String toString(){
        String st = "\n Submit Directory " + this.mSubmitDirectory +
                    "\n" + this.dagInfo.toString() +
                    vectorToString("\n Jobs making the DAG ",this.vJobSubInfos);
        return st;
    }

    /**
     * This adds a new job to the ADAG object. It ends up adding both the job name
     * and the job description to the internal structure.
     *
     * @param job  the new job that is to be added to the ADag.
     */
    public void add(SubInfo job){
        //add to the dagInfo
        dagInfo.addNewJob(job );
        vJobSubInfos.addElement(job);
    }


    /**
     * Removes all the jobs from the workflow, and all the edges between
     * the workflows. The only thing that remains is the meta data about the
     * workflow.
     *
     *
     */
    public void clearJobs(){
        vJobSubInfos.clear();
        dagInfo.dagJobs.clear();
        dagInfo.relations.clear();
        dagInfo.lfnMap.clear();
        //reset the workflow metrics also
        this.getWorkflowMetrics().reset();
    }

    /**
     * Returns whether the workflow is empty or not.
     * @return boolean
     */
    public boolean isEmpty(){
        return vJobSubInfos.isEmpty();
    }

    /**
     * Removes a particular job from the workflow. It however does not
     * delete the relations the edges that refer to the job.
     *
     * @param job  the <code>SubInfo</code> object containing the job description.
     *
     * @return boolean indicating whether the removal was successful or not.
     */
    public boolean remove(SubInfo job){
	boolean a = dagInfo.remove( job );
	boolean b = vJobSubInfos.remove(job);
	return a && b;
    }

    /**
     * Returns the number of jobs in the dag on the basis of number of elements
     * in the <code>dagJobs</code> Vector.
     *
     * @return the number of jobs.
     */
    public int getNoOfJobs(){
        return this.dagInfo.getNoOfJobs();
    }



    /**
     * Adds a new PCRelation pair to the Vector of <code>PCRelation</code>
     * pairs. For the new relation the isDeleted parameter is set to false.
     *
     * @param parent    The parent in the relation pair
     * @param child     The child in the relation pair
     *
     * @see org.griphyn.cPlanner.classes.PCRelation
     */
    public void addNewRelation(String parent, String child){
        PCRelation newRelation = new PCRelation(parent,child);
        this.dagInfo.relations.addElement(newRelation);
    }


    /**
     * Adds a new PCRelation pair to the Vector of <code>PCRelation</code>
     * pairs.
     *
     * @param parent    The parent in the relation pair
     * @param child     The child in the relation pair
     * @param isDeleted Whether the relation has been deleted due to the reduction
     *                  algorithm or not.
     *
     * @see org.griphyn.cPlanner.classes.PCRelation
     */
    public void addNewRelation(String parent, String child, boolean isDeleted){
        PCRelation newRelation = new PCRelation(parent,child,isDeleted);
        this.dagInfo.relations.addElement(newRelation);
    }

    /**
     * Sets the submit directory for the workflow.
     *
     * @param dir   the submit directory.
     */
    public void setBaseSubmitDirectory(String dir){
        this.mSubmitDirectory = dir;
    }

    /**
     * Returns the label of the workflow, that was specified in the DAX.
     *
     * @return the label of the workflow.
     */
    public String getLabel(){
        return this.dagInfo.getLabel();
    }

    /**
     * Returns the last modified time for the file containing the workflow
     * description.
     *
     * @return the MTime
     */
    public String getMTime(){
        return this.dagInfo.getMTime();
    }


    /**
     * Returns the root of submit directory hierarchy for the workflow.
     *
     * @return the directory.
     */
    public String getBaseSubmitDirectory(){
        return this.mSubmitDirectory;
    }


    /**
     * Gets all the parents of a particular node
     *
     * @param node the name of the job whose parents are to be found.
     *
     * @return    Vector corresponding to the parents of the node
     */
    public Vector getParents(String node){
        return this.dagInfo.getParents(node);
    }

    /**
     * Get all the children of a particular node.
     *
     * @param node  the name of the node whose children we want to find.
     *
     * @return  Vector containing the
     *          children of the node
     *
     */
    public Vector getChildren(String node){
       return this.dagInfo.getChildren(node);
    }


     /**
     * Returns all the leaf nodes of the dag. The way the structure of Dag is
     * specified, in terms of the parent child relationship pairs, the
     * determination of the leaf nodes can be computationally intensive. The
     * complexity is of order n^2
     *
     * @return Vector of <code>String</code> corresponding to the job names of
     *         the leaf nodes.
     *
     * @see org.griphyn.cPlanner.classes.PCRelation
     * @see org.griphyn.cPlanner.classes.DagInfo#relations
     */
    public Vector getLeafNodes(){
        return this.dagInfo.getLeafNodes();
    }

    /**
     * It returns the a unique list of the execution sites that the Planner
     * has mapped the dax to so far in it's stage of planning . This is a
     * subset of the pools specified by the user at runtime.
     *
     * @return  a TreeSet containing a list of siteID's of the sites where the
     *          dag has to be run.
     */
    public Set getExecutionSites(){
        Set set = new TreeSet();
        SubInfo sub = null;

        for(Iterator it = this.vJobSubInfos.iterator();it.hasNext();){
            sub = (SubInfo)it.next();
            set.add(sub.executionPool);
        }

        //remove the stork pool
        set.remove("stork");

        return set;
    }

    /**
     * It determines the root Nodes for the ADag looking at the relation pairs
     * of the adag. The way the structure of Dag is specified in terms
     * of the parent child relationship pairs, the determination of the leaf
     * nodes can be computationally intensive. The complexity if of order n^2.
     *
     *
     * @return the root jobs of the Adag
     *
     * @see org.griphyn.cPlanner.classes.PCRelation
     * @see org.griphyn.cPlanner.classes.DagInfo#relations
     */
    public Vector getRootNodes(){
        return this.dagInfo.getRootNodes();
    }


    /**
     * Returns an iterator for traversing through the jobs in the workflow.
     *
     * @return Iterator
     */
    public Iterator jobIterator(){
        return this.vJobSubInfos.iterator();
    }

    /**
     * This returns a SubInfo object corresponding to the job by looking through
     * all the subInfos.
     *
     *
     *@param job   jobName of the job for which we need the subInfo object.
     *
     *@return      the <code>SubInfo</code> objects corresponding to the job
     */
    public SubInfo getSubInfo(String job){

        SubInfo sub = null;

        //System.out.println("Job being considered is " + job);
        for ( Enumeration e = this.vJobSubInfos.elements(); e.hasMoreElements(); ){
            sub = (SubInfo)e.nextElement();
            if(job.equalsIgnoreCase(sub.jobName)){
                return sub;
            }

        }

        throw new RuntimeException("Can't find the sub info object for job " + job);

    }

    /**
     * Returns the metrics about the workflow.
     *
     * @return the WorkflowMetrics
     */
    public WorkflowMetrics getWorkflowMetrics(){
        return this.dagInfo.getWorkflowMetrics();
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

        String newIndent = (indent == null ) ? "\t" : indent + "\t";

        //write out the dot header
        writeDOTHeader( stream, null );

        //traverse through the jobs
        for( Iterator it = jobIterator(); it.hasNext(); ){
            ( (SubInfo)it.next() ).toDOT( stream, newIndent );
        }

        stream.write( newLine );

        //traverse through the edges
        for( Iterator it = dagInfo.relations.iterator(); it.hasNext(); ){
            ( (PCRelation)it.next() ).toDOT( stream, newIndent );
        }

        //write out the tail
        stream.write( "}" );
        stream.write( newLine );
    }


    /**
     * Writes out the static DOT Header.
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
    public void writeDOTHeader( Writer stream, String indent ) throws IOException {
        String newLine = System.getProperty( "line.separator", "\r\n" );

        String newIndent = ( indent == null ) ? null : indent + "\t";

        //write out the header and static stuff for now
        if ( indent != null && indent.length() > 0 ) {stream.write( indent ) ;};
        stream.write( "digraph E {");
        stream.write( newLine );

        //write out the size of the image
        if ( newIndent != null && newIndent.length() > 0 ) { stream.write( newIndent );}
        stream.write( "size=\"8.0,10.0\"");
        stream.write( newLine );

        //write out the ratio
        if ( newIndent != null && newIndent.length() > 0 ) { stream.write( newIndent );};
        stream.write( "ratio=fill");
        stream.write( newLine );

        //write out what the shape of the nodes need to be like
        if ( newIndent != null && newIndent.length() > 0 ) { stream.write( newIndent );};
        stream.write( "node [shape=ellipse]");
        stream.write( newLine );

        //write out how edges are to be rendered.
        if ( newIndent != null && newIndent.length() > 0 ) { stream.write( newIndent );};
        stream.write( "edge [arrowhead=normal, arrowsize=1.0]");
        stream.write( newLine );

    }

}
