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
package org.griphyn.cPlanner.partitioner;

import org.griphyn.cPlanner.common.LogManager;

import org.griphyn.vdl.classes.LFN;

import org.griphyn.vdl.dax.ADAG;
import org.griphyn.vdl.dax.Filename;
import org.griphyn.vdl.dax.Job;

import org.griphyn.vdl.euryale.Callback;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This class ends up writing a partitioned dax, that corresponds to one
 * partition as defined by the Partitioner. It looks up the dax once when
 * it is initialized, stores it in memory and then refers the memory to look
 * up the job details for the jobs making up a particular partition.
 *
 * @author Karan Vahi
 * @version $Revision: 1.9 $
 */
public class SingleLook extends DAXWriter{

    /**
     * The set of job id's in the partition.
     */
    private Set mNodeSet;

    /**
     * A map containing the relations between the jobs making up the partition.
     */
    private Map mRelationsMap;

    /**
     * The ADAG object containing the partitioned dax.
     */
    private ADAG mPartADAG;

    /**
     * The number of jobs that are in the partition.
     */
    private int mNumOfJobs;

    /**
     * The number of jobs about which the callback interface has knowledge.
     */
    private int mCurrentNum;

    /**
     * The flag to identify that dax is in memory.
     */
    private boolean mDAXInMemory;

    /**
     * The map containing all the jobs in the dax indexed by the job id.
     */
    private Map mJobMap;


    /**
     * The overloaded constructor.
     *
     * @param daxFile   the path to the dax file that is being partitioned.
     * @param directory the directory in which the partitioned daxes are to be
     *                  generated.
     */
    public SingleLook(String daxFile, String directory){
        super(daxFile,directory);
        mDAXInMemory = false;
        mJobMap      = null;
    }


    /**
     * It writes out a dax consisting of the jobs as specified in the partition.
     *
     * @param partition  the partition object containing the relations and id's
     *                   of the jobs making up the partition.
     * @param index      the index of the partition.
     *
     * @return boolean  true if dax successfully generated and written.
     *                  false in case of error.
     */
    public boolean writePartitionDax(Partition partition, int index){
        Iterator it;
        List fileList = null;
        List parentIDs = null;

        //do the cleanup from the previous partition write
        mPartADAG = null;
        mNodeSet  = null;
        mRelationsMap = null;


        //get from the partition object the set of jobs
        //and relations between them
        mNodeSet      = partition.getNodeIDs();
        mRelationsMap = partition.getRelations();
        mNumOfJobs    = mNodeSet.size();

        //set the current number of jobs whose information we have
        mCurrentNum = 0;
        if(!mDAXInMemory){
            mLogger.log("Parsing the original DAX file",
                        LogManager.DEBUG_MESSAGE_LEVEL);
            //dax is not in memory.
            mJobMap = new java.util.HashMap();
            //Callback takes care of putting dax in memory
            Callback callback = new MyCallBackHandler();
            org.griphyn.vdl.euryale.DAXParser d =
                new org.griphyn.vdl.euryale.DAXParser(null);
            d.setCallback(callback);

            //start the parsing of the dax
            d.parse(mDaxFile);
            mDAXInMemory = true;
            mLogger.logCompletion("Parsing the original DAX file",
                                  LogManager.DEBUG_MESSAGE_LEVEL);
        }

        mPartADAG      = new ADAG(0,index,mPartitionName);

        //get the job information for the jobs in the partiton.
        it = mNodeSet.iterator();
        while(it.hasNext()){
            String id = (String)it.next();
            Job job = (Job)mJobMap.get(id);
            if(job == null){
                throw new RuntimeException( "Unable to find information about job" +
                                            id + "while constructing partition" );
            }

            //add the job to ADAG
            mPartADAG.addJob(job);

            //build up the files used by the partition
            fileList = job.getUsesList();
            //iterate through the file list
            //populate it in the ADAG object
            Iterator fileIt = fileList.iterator();
            while(fileIt.hasNext()){
                Filename file = (Filename)fileIt.next();
                mPartADAG.addFilename(file.getFilename(),
                                      (file.getLink() == LFN.INPUT)?true:false,
                                      file.getTemporary(),
                                      file.getDontRegister(),file.getDontTransfer());
            }
        }

        //put in the relations amongst
        //jobs in the partition
        //add the relations between the jobs in the partition to the ADAG
        it = mRelationsMap.keySet().iterator();
        while(it.hasNext()){
            String childID = (String)it.next();
            parentIDs =  (List)mRelationsMap.get(childID);

            //get all the parents of the children and populate them in the
            //ADAG object
            Iterator it1 = parentIDs.iterator();
            while(it1.hasNext()){
                mPartADAG.addChild(childID,(String)it1.next());
            }

        }
        mLogger.log("Writing out the DAX File for partition " + partition.getID(),
                    LogManager.DEBUG_MESSAGE_LEVEL);
        //do the actual writing to the file
        this.initializeWriteHandle(index);
        try{
            mPartADAG.toXML(mWriteHandle, new String());
        }
        catch(IOException e){
            mLogger.log("Error while writing out a partition dax :" +
                        e.getMessage(),LogManager.ERROR_MESSAGE_LEVEL);
            return false;
        }
        this.close();
        mLogger.logCompletion("Writing out the DAX File for partition " + partition.getID(),
                              LogManager.DEBUG_MESSAGE_LEVEL);


        //generation was successful
        return true;
    }




    /**
     * The internal callback handler for the DAXParser in Euryale. It stores
     * all the jobs making up the dax in an internal map, which is then referred
     * to get the job information for the jobs making up the partition.
     */
    private class MyCallBackHandler implements Callback {

        /**
         * The empty constructor.
         */
        public MyCallBackHandler(){

        }

        /**
         * Callback when the opening tag was parsed. The attribute maps each
         * attribute to its raw value. The callback initializes the DAG
         * writer.
         *
         * @param attributes is a map of attribute key to attribute value
         */
        public void cb_document(Map attributes) {
            //do nothing at the moment
        }


        /**
         * Callback for the filename from section 1 filenames.
         * Does nothing as the filenames for the partitioned dax are
         * constructed from the jobs.
         */
        public void cb_filename(Filename filename) {
            //an empty implementation
        }

        /**
         * Callback for the job from section 2 jobs. This ends up storing all
         * the jobs in the memory to be used for writing out the partition dax.
         *
         * @param job  the object containing the job information.
         */
        public void cb_job(Job job) {

            String id = job.getID();
            //put it in hashmap and also check for duplicate
            if(mJobMap.put(id,job) != null){
                //warn for the duplicate entry
                mLogger.log("Entry for the job already in ",
                            LogManager.WARNING_MESSAGE_LEVEL);
            }


            if(mCurrentNum == mNumOfJobs){
                //exit or stop the parser.
                cb_done();
            }
        }

        /**
         * Callback for child and parent relationships from section 3.
         * This is an empty implementation, as the Partition object
         * contains the relations amongst the jobs making up the partition.
         *
         * @param child is the IDREF of the child element.
         * @param parents is a list of IDREFs of the included parents.
         */
        public void cb_parents(String child, List parents) {
            //an empty implementation
        }


        /**
         * Callback when the parsing of the document is done. While this state
         * could also be determined from the return of the invocation of the
         * parser, that return may be hidden in another place of the code.
         * This callback can be used to free callback-specific resources.
         */
        public void cb_done(){
            //an empty implementation
        }

    }
}
