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

package org.griphyn.cPlanner.engine;


import org.griphyn.cPlanner.classes.ADag;
import org.griphyn.cPlanner.classes.DagInfo;
import org.griphyn.cPlanner.classes.PCRelation;
import org.griphyn.cPlanner.classes.PegasusFile;
import org.griphyn.cPlanner.classes.PlannerOptions;
import org.griphyn.cPlanner.classes.SubInfo;

import org.griphyn.cPlanner.common.LogManager;
import org.griphyn.cPlanner.common.PegasusProperties;

import java.util.Enumeration;
import java.util.List;
import java.util.Set;
import java.util.HashSet;
import java.util.Vector;
import java.util.Iterator;

/**
 *
 * Reduction  engine for Planner 2.
 * Given a ADAG it looks up the replica catalog and
 * determines which output files are in the
 * Replica Catalog, and on the basis of these
 * ends up reducing the dag.
 *
 * @author Karan Vahi
 * @author Gaurang Mehta
 * @version $Revision: 1.18 $
 *
 */

public class ReductionEngine extends Engine{



    /**
     * the original dag object which
     * needs to be reduced on the basis of
     * the results returned from the
     * Replica Catalog
     */
    private ADag mOriginalDag;

    /**
     * the dag relations of the
     * orginal dag
     */
    private Vector mOrgDagRelations;

    /**
     * the reduced dag object which is
     * returned.
     */
    private ADag mReducedDag;


    /**
     * the jobs which are found to be in
     * the Replica Catalog. These are
     * the jobs whose output files are at
     * some location in the Replica Catalog.
     * This does not include the jobs which
     * are deleted by applying the reduction
     * algorithm
     */
    private Vector mOrgJobsInRC ;

    /**
     * the jobs which are deleted due
     * to the application of the
     * Reduction algorithm. These do
     * not include the jobs whose output
     * files are in the RC. These are
     * the ones which are deleted due
     * to cascade delete
     */
    private Vector mAddJobsDeleted;

    /**
     * all deleted jobs. This
     * is mOrgJobsInRC + mAddJobsDeleted.
     */
    private Vector mAllDeletedJobs;


    /**
     * the files whose locations are
     * returned from the ReplicaCatalog
     */
    private Set mFilesInRC;

    /**
     * The constructor
     *
     * @param orgDag    The original Dag object
     * @param properties the <code>PegasusProperties</code> to be used.
     * @param options   The options specified by
     *                  the user to run the
     *                  planner
     */
    public ReductionEngine( ADag orgDag, PegasusProperties properties, PlannerOptions options ){
        super( properties );
        this.mPOptions   = options;
        mOriginalDag     = orgDag;
        mOrgDagRelations = mOriginalDag.dagInfo.relations;
        mOrgJobsInRC     = new Vector();
        mAddJobsDeleted  = new Vector();
        mAllDeletedJobs  = new Vector();

    }




    /**
     * Reduces the workflow on the basis of the existence of lfn's in the
     * replica catalog. The existence of files, is determined via the bridge.
     *
     * @param rcb instance of the replica catalog bridge.
     *
     */
    public ADag reduceDag( ReplicaCatalogBridge rcb ){

        //search for the replicas of
        //the files. The search list
        //is already present in Replica
        //Mechanism classes
        mFilesInRC = rcb.getFilesInReplica();

        //we reduce the dag only if the
        //force option is not specified.
        if(mPOptions.getForce())
            return mOriginalDag;

        mLogger.log("Reducing the workflow",LogManager.DEBUG_MESSAGE_LEVEL);
        mOrgJobsInRC =
            getJobsInRC(mOriginalDag.vJobSubInfos,mFilesInRC);
        mAllDeletedJobs = (Vector)mOrgJobsInRC.clone();
        firstPass(mOrgJobsInRC);
        secondPass();
        firstPass(mAddJobsDeleted);

        mLogMsg = "Nodes/Jobs Deleted from the Workflow during reduction ";
        mLogger.log(mLogMsg,LogManager.DEBUG_MESSAGE_LEVEL);
        for(Enumeration e = mAllDeletedJobs.elements();e.hasMoreElements();){
            mLogger.log("\t" + e.nextElement(),LogManager.DEBUG_MESSAGE_LEVEL);
        }
        mLogger.logCompletion(mLogMsg,LogManager.DEBUG_MESSAGE_LEVEL);
        mReducedDag = makeRedDagObject(mOriginalDag,mAllDeletedJobs);

        mLogger.logCompletion("Reducing the workflow",
                              LogManager.DEBUG_MESSAGE_LEVEL);
        return mReducedDag;
    }





    /**
     * This determines the jobs which are in
     * the RC corresponding to the files found
     * in the Replica Catalog. A job is said to
     * be in the RC if all the outfiles for
     * that job are found to be in the RC.
     * A job in RC can be removed from the Dag
     * and the Dag correspondingly reduced.
     *
     * @param vSubInfos    Vector of <code>SubInfo</code>
     *                    objects corresponding to  all
     *                    the jobs of a Abstract Dag
     *
     * @param filesInRC   Set of <code>String</code>
     *                    objects corresponding to the
     *                    logical filenames of files
     *                    which are found to be in the
     *                    Replica Catalog
     *
     * @return a Vector of jobNames (Strings)
     *
     * @see org.griphyn.cPlanner.classes.ReplicaLocations
     * @see org.griphyn.cPlanner.classes.SubInfo
     */
    private Vector getJobsInRC(Vector vSubInfos,Set filesInRC){
        SubInfo subInfo;
        Set vJobOutputFiles;
        String jobName;
        Vector vJobsInReplica = new Vector();
        int noOfOutputFilesInJob = 0;
        int noOfSuccessfulMatches = 0;

        if( vSubInfos.isEmpty() ){
            String msg = "ReductionEngine: The set of jobs in the workflow " +
                         "\n is empty.";
            mLogger.log( msg, LogManager.DEBUG_MESSAGE_LEVEL );
            return new Vector();
        }

        Enumeration e = vSubInfos.elements();
        mLogger.log("Jobs whose o/p files already exist",
                    LogManager.DEBUG_MESSAGE_LEVEL);
        while(e.hasMoreElements()){
            //getting submit information about each submit file of a job
            subInfo = (SubInfo)e.nextElement();
            jobName = subInfo.jobName;
            //System.out.println(jobName);

            if(!subInfo.outputFiles.isEmpty()){
                vJobOutputFiles = subInfo.getOutputFiles();
            }else{
                vJobOutputFiles = new HashSet();
            }

            /* Commented on Oct10. This ended up making the
            Planner doing duplicate transfers
            if(subInfo.stdOut.length()>0)
                vJobOutputFiles.addElement(subInfo.stdOut);
            */

            //determine the no of output files for that job
            if(vJobOutputFiles.isEmpty()){
                mLogger.log("Job "  + subInfo.getName() + " has no o/p files",
                            LogManager.DEBUG_MESSAGE_LEVEL);
            }

            for( Iterator temp = vJobOutputFiles.iterator(); temp.hasNext(); ){
                temp.next();
                noOfOutputFilesInJob++;
            }

            //traversing through the output files of that particular job
            for( Iterator en = vJobOutputFiles.iterator(); en.hasNext(); ){
                PegasusFile pf = (PegasusFile)en.next();
                //jobName = pf.getLFN();
                //if(stringInList(jobName,filesInRC)){
                if(filesInRC.contains(pf.getLFN())){
                    noOfSuccessfulMatches++;
                }
            }

            //if the noOfOutputFilesInJob and noOfSuccessfulMatches are equal
            //this means that all files required by job is in RC
            if(noOfOutputFilesInJob == noOfSuccessfulMatches){
                mLogger.log("\t" + subInfo.jobName,
                            LogManager.DEBUG_MESSAGE_LEVEL);
                vJobsInReplica.addElement(subInfo.jobName);
            }
            //reinitialise the variables
            noOfSuccessfulMatches = 0;
            noOfOutputFilesInJob = 0;
        }
        mLogger.logCompletion("Jobs whose o/p files already exist",
                              LogManager.DEBUG_MESSAGE_LEVEL);
        return vJobsInReplica;

    }



    /**
     * If a job is deleted it marks
     * all the relations related to that
     * job as deleted
     *
     * @param vDelJobs  the vector containing the names
     *                  of the deleted jobs whose relations
     *                  we want to nullify
     */
    private void firstPass(Vector vDelJobs){
        Enumeration edeljobs = vDelJobs.elements();
        while(edeljobs.hasMoreElements()){
            String deljob = (String)edeljobs.nextElement();

            Enumeration epcrel = mOrgDagRelations.elements();
            while( epcrel.hasMoreElements()){
                PCRelation pcrc = (PCRelation)epcrel.nextElement();
                if((pcrc.child.equalsIgnoreCase(deljob))||
                      (pcrc.parent.equalsIgnoreCase(deljob))){
                    pcrc.isDeleted=true;
                }
             }
        }

    }

    /**
     * In the second pass we find all the
     * parents of the nodes which have been
     * found to be in the RC.
     * Corresponding to each parent, we find
     * the corresponding siblings for that
     * deleted job.
     * If all the siblings are deleted, we
     * can delete that parent.
     */
    private void secondPass(){
        Enumeration eDelJobs = mAllDeletedJobs.elements();
        Enumeration ePcRel;
        Enumeration eParents;
        String node;
        String parentNodeName;
        PCRelation currentRelPair;

        Vector vParents = new Vector();//all parents of a particular node
        Vector vSiblings = new Vector();

        while(eDelJobs.hasMoreElements()){
            node = (String)eDelJobs.nextElement();

            //getting the parents of that node
            vParents = this.getNodeParents(node);

            //now for each parent checking if the siblings are deleted
            //if yes then delete the node
            eParents = vParents.elements();
            while(eParents.hasMoreElements()){
                parentNodeName = (String)eParents.nextElement();

                //getting all the siblings for parentNodeName
                vSiblings = this.getChildren(parentNodeName);

                //now we checking if all the siblings are in vdeljobs
                Enumeration temp = vSiblings.elements();
                boolean siblingsDeleted = true;
                while(temp.hasMoreElements()){
                    if(stringInVector( (String)temp.nextElement(),mAllDeletedJobs)){
                        //means the sibling has been marked deleted
                    }
                    else{
                        siblingsDeleted = false;
                    }
                }

                //if all siblings are deleted add the job to vdeljobs
                if(siblingsDeleted){

                    //only add if the parentNodeName is not already in the list
                    if(!stringInVector(parentNodeName,mAllDeletedJobs)){
                        String msg = "Deleted Node :" + parentNodeName;
                        mLogger.log(msg,LogManager.DEBUG_MESSAGE_LEVEL);
                        mAddJobsDeleted.addElement(new String (parentNodeName));
                        mAllDeletedJobs.addElement(new String (parentNodeName));

                    }
                }

                //clearing the siblings vector for that parent
                vSiblings.clear();

            }//end of while(eParents.hasMoreElements()){

            //clearing the parents Vector for that job
            vParents.clear();

        }//end of while(eDelJobs.hasMoreElements)
    }


    /**
     * Gets all the parents of a particular node.
     *
     * @param node  the name of the job whose  parents are to be found.
     *
     * @return    Vector corresponding to the parents of the node.
     */
    private Vector getNodeParents(String node){
        //getting the parents of that node
        return mOriginalDag.getParents(node);
    }

    /**
     * Gets all the children of a particular node.
     *
     * @param node  the name of the node whose children we want to find.
     *
     * @return  Vector containing the children of the node.
     */
    private Vector getChildren(String node){
        return mOriginalDag.getChildren(node);
    }



    /**
     * All the deleted jobs which
     * happen to be leaf nodes. This
     * entails that the output files
     * of these jobs be transferred
     * from the location returned
     * by the Replica Catalog to the
     * pool specified.
     * This is a subset of mAllDeletedJobs
     * Also to determine the deleted
     * leaf jobs it refers the original
     * dag, not the reduced dag.
     *
     * @return  Vector containing the
     *          names of the deleted
     *          leaf jobs.
     */
    public Vector getDeletedLeafJobs(){
        Vector delLeafJobs = new Vector();

        Enumeration e = mAllDeletedJobs.elements();
        mLogger.log("Finding deleted leaf jobs",LogManager.DEBUG_MESSAGE_LEVEL);
        while(e.hasMoreElements()){
            String job = (String)e.nextElement();
            if(getChildren(job).isEmpty()){
                //means a leaf job
                String msg = "Found deleted leaf job :" + job;
                mLogger.log(msg,LogManager.DEBUG_MESSAGE_LEVEL);
                delLeafJobs.addElement(job);

            }
        }
        mLogger.logCompletion("Finding deleted leaf jobs",
                              LogManager.DEBUG_MESSAGE_LEVEL);
        return delLeafJobs;
    }


    /**
     * makes the Reduced Dag object which
     * corresponding to the deleted jobs
     * which are specified.
     *
     * Note : We are plainly copying the
     * inputFiles and the outputFiles. After
     * reduction this changes but since we
     * need those only to look up the RC,
     * which we have done.
     *
     * @param orgDag    the original Dag
     * @param vDelJobs  the Vector containing the
     *                  names of the jobs whose
     *                  SubInfos and Relations we
     *                  want to remove.
     *
     * @return  the reduced dag, which doesnot
     *          have the deleted jobs
     *
     */
    public ADag makeRedDagObject(ADag orgDag, Vector vDelJobs){
        ADag redDag = new ADag();
        redDag.dagInfo =
              constructNewDagInfo(mOriginalDag.dagInfo,vDelJobs);
        redDag.vJobSubInfos =
              constructNewSubInfos(mOriginalDag.vJobSubInfos,vDelJobs);
        return redDag;
    }




    /**
     * Constructs a DagInfo object for the
     * decomposed Dag on the basis of the jobs
     * which are deleted from the DAG by the
     * reduction algorithm
     *
     * Note : We are plainly copying the
     * inputFiles and the outputFiles. After reduction
     * this changes but since we need those
     * only to look up the RC, which we have done.
     *
     * @param dagInfo   the object which is reduced on
     *                  the basis of vDelJobs
     *
     * @param vDelJobs  Vector containing the logical file
     *                  names of jobs which are to
     *                  be deleted
     *
     * @return          the DagInfo object corresponding
     *                  to the Decomposed Dag
     *
     */

    private DagInfo constructNewDagInfo(DagInfo dagInfo,Vector vDelJobs){
        DagInfo newDagInfo = (DagInfo)dagInfo.clone();
        String jobName;

        PCRelation currentRelation;
        String parentName;
        String childName;
        boolean deleted;

        //populating DagJobs
        newDagInfo.dagJobs = new Vector();
        Enumeration e = dagInfo.dagJobs.elements();
        while(e.hasMoreElements()){
            jobName = (String)e.nextElement();
            if(!stringInVector( jobName,vDelJobs) ){
                //that job is to be executed so we add it
                newDagInfo.dagJobs.addElement(new String(jobName));
            }
        }


        //populating PCRelation Vector
        newDagInfo.relations = new Vector();
        e = dagInfo.relations.elements();
        while(e.hasMoreElements()){
            currentRelation = (PCRelation)e.nextElement();
            parentName = new String(currentRelation.parent);
            childName  = new String(currentRelation.child);


            if( !(currentRelation.isDeleted) ){//the pair has not been marked deleted
                newDagInfo.relations.addElement(new PCRelation(parentName,childName,false));
            }
        }

        return newDagInfo;

    }//end of function



    /**
     * constructs the Vector of subInfo objects
     * corresponding to the reduced ADAG.
     *
     * It also modifies the strargs to remove
     * them up of markup and display correct paths
     * to the filenames
     *
     *
     * @param vSubInfos    the SubInfo object including
     *                     the jobs which are not needed
     *                     after the execution of the
     *                     reduction algorithm
     *
     * @param vDelJobs     the jobs which are deleted by
     *                     the reduction algo as their
     *                     output files are in the Replica Catalog
     *
     * @return             the SubInfo objects except the ones
     *                     for the deleted jobs
     *
     */

    private Vector constructNewSubInfos(Vector vSubInfos,Vector vDelJobs){
        Vector vNewSubInfos = new Vector();
        SubInfo newSubInfo;
        SubInfo currentSubInfo;
        String jobName;

        Enumeration e = vSubInfos.elements();

        while(e.hasMoreElements()){
            currentSubInfo = (SubInfo)e.nextElement();
            jobName = currentSubInfo.jobName;
            //we add only if the jobName is not in vDelJobs
            if(!stringInVector(jobName,vDelJobs)){
                newSubInfo = (SubInfo)currentSubInfo.clone();
                //adding to Vector
                vNewSubInfos.addElement(newSubInfo);
            }

        }//end of while
        return vNewSubInfos;
    }


}