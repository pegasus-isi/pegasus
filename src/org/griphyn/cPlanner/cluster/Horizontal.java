/**
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

package org.griphyn.cPlanner.cluster;

import org.griphyn.cPlanner.classes.ADag;
import org.griphyn.cPlanner.classes.SubInfo;
import org.griphyn.cPlanner.classes.PCRelation;

import org.griphyn.cPlanner.common.PegasusProperties;
import org.griphyn.cPlanner.common.LogManager;

import org.griphyn.cPlanner.cluster.aggregator.JobAggregatorInstanceFactory;

import org.griphyn.cPlanner.namespace.VDS;

import org.griphyn.cPlanner.partitioner.Partition;

import java.util.Collections;
import java.util.List;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Comparator;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.HashSet;

/**
 *
 * @author Karan Vahi
 * @version $Revision$
 */

public class Horizontal implements Clusterer {

    /**
     * The default collapse factor for collapsing jobs with same logical name
     * scheduled onto the same execution pool.
     */
    public static final int DEFAULT_COLLAPSE_FACTOR = 3;

    /**
     * A short description about the partitioner.
     */
    public static final String DESCRIPTION = "Horizontal Clustering";

    /**
     * A singleton access to the job comparator.
     */
    private static Comparator mJobComparator = null;

    /**
     * The handle to the logger object.
     */
    protected LogManager mLogger;

    /**
     * The handle to the properties object holding all the properties.
     */
    protected PegasusProperties mProps;


    /**
     * The handle to the job aggregator factory.
     */
    protected JobAggregatorInstanceFactory mJobAggregatorFactory;

    /**
     * ADag object containing the jobs that have been scheduled by the site
     * selector.
     */
    private ADag mScheduledDAG;

    /**
     * Map to hold the jobs sorted by the label of jobs in dax.
     * The key is the logical job name and value is the list of jobs with that
     * logical name.
     *
     * This no longer used, and would be removed later.
     */
    private Map mJobMap;

    /**
     * A Map to store all the job(SubInfo) objects indexed by their logical ID found in
     * the dax. This should actually be in the ADag structure.
     */
    private Map mSubInfoMap;

    /**
     * Map to hold the collapse values for the various execution pools. The
     * values are gotten from the properties file or can be gotten from the
     * resource information catalog a.k.a MDS.
     */
    private Map mCollapseMap;


    /**
     * Replacement table, that identifies the corresponding fat job for a job.
     */
    private Map mReplacementTable;



    /**
     * Singleton access to the job comparator.
     *
     * @return the job comparator.
     */
    private Comparator jobComparator(){
        return (mJobComparator == null)?
                new JobComparator():
                mJobComparator;
    }


    /**
     * The default constructor.
     */
    public Horizontal(){
        mLogger = LogManager.getInstance();
        mJobAggregatorFactory = new JobAggregatorInstanceFactory();
    }

    /**
     *Initializes the Clusterer impelementation
     *
     * @param dag         the workflow that is being clustered.
     * @param properties  the properties passed to the planner.
     * @param submitDir   the base submit directory for the workflow.
     *
     * @throws ClustererException in case of error.
     */
    public void initialize( ADag dag , PegasusProperties properties, String submitDir )
                                                              throws ClustererException{
        mScheduledDAG = dag;
        mProps = properties;
        mJobAggregatorFactory.initialize( properties, dag, submitDir);

        mJobMap = new HashMap();
        mCollapseMap = this.constructMap(mProps.getCollapseFactors());
        mReplacementTable = new HashMap();
        mSubInfoMap = new HashMap();

        for(Iterator it = mScheduledDAG.vJobSubInfos.iterator();it.hasNext();){
            //pass the jobs to the callback
            SubInfo job = (SubInfo)it.next();
            mSubInfoMap.put(job.getLogicalID(), job );
        }


    }

    /**
     * Determine the clusters for a partition. The partition is assumed to
     * contain independant jobs, and multiple clusters maybe created for the
     * partition. Internally the jobs are grouped according to transformation name
     * and then according to the execution site. Each group
     * (having same transformation name and scheduled on same site), is then
     * clustered.
     * The number of clustered jobs created for each group is dependant on the
     * following VDS profiles that can be associated with the jobs.
     * <pre>
     *       1) bundle   (dictates the number of clustered jobs that are created)
     *       2) collapse (the number of jobs that make a single clustered job)
     * </pre>
     *
     * In case of both parameters being associated with the jobs in a group, the
     * bundle parameter overrides collapse parameter.
     *
     * @param partition   the partition for which the clusters need to be
     *                    determined.
     *
     * @throws ClustererException in case of error.
     *
     * @see VDS#BUNDLE_KEY
     * @see VDS#COLLAPSE_KEY
     */
    public void determineClusters( Partition partition ) throws ClustererException {
        Set s = partition.getNodeIDs();
        List l = new ArrayList(s.size());
        mLogger.log("Collapsing jobs in partition " + partition.getID() +
                    " " +  s,
                    LogManager.DEBUG_MESSAGE_LEVEL);

       for(Iterator it = s.iterator();it.hasNext();){
           SubInfo job = (SubInfo)mSubInfoMap.get(it.next());
           l.add(job);
       }
       //group the jobs by their transformation names
       Collections.sort( l, jobComparator() );
       //traverse through the list and collapse jobs
       //referring to same logical transformation
       SubInfo previous = null;
       List clusterList = new LinkedList();
       SubInfo job = null;
       for(Iterator it = l.iterator();it.hasNext();){
           job = (SubInfo)it.next();
           if(previous == null ||
              job.getCompleteTCName().equals(previous.getCompleteTCName())){
               clusterList.add(job);
           }
           else{
               //at boundary collapse jobs
               collapseJobs(previous.getStagedExecutableBaseName(),clusterList,partition.getID());
               clusterList = new LinkedList();
               clusterList.add(job);
           }
           previous = job;
       }
       //cluster the last clusterList
       if(previous != null){
           collapseJobs(previous.getStagedExecutableBaseName(), clusterList, partition.getID());
       }

    }


    /**
     * Am empty implementation of the callout, as state is maintained
     * internally to determine the relations between the jobs.
     *
     * @param partitionID   the id of a partition.
     * @param parents       the list of <code>String</code> objects that contain
     *                      the id's of the parents of the partition.
     *
     * @throws ClustererException in case of error.
     */
    public void parents( String partitionID, List parents ) throws ClustererException{

    }


    /**
     * Collapses the jobs having the same logical name according to the sites
     * where they are scheduled.
     *
     * @param name         the logical name of the jobs in the list passed to
     *                     this function.
     * @param jobs         the list <code>SubInfo</code> objects corresponding
     *                     to the jobs that have the same logical name.
     * @param partitionID  the ID of the partition to which the jobs belong.
     */
    private void collapseJobs( String name, List jobs, String partitionID ){
        String key  = null;
        SubInfo job = null;
        List l      = null;
        //internal map that keeps the jobs according to the execution pool
        Map tempMap    = new java.util.HashMap();
        int[] cFactor  = new int[2]; //the collapse factor for collapsing the jobs
        cFactor[0]     = 0;
        cFactor[1]     = 0;
        SubInfo fatJob = null;

        mLogger.log("Collapsing jobs of type " + name,
                    LogManager.DEBUG_MESSAGE_LEVEL);

        //traverse through all the jobs and order them by the
        //pool on which they are scheduled
        for(Iterator it = jobs.iterator();it.hasNext();){

            job = (SubInfo)it.next();
            key = job.executionPool;
            //check if the job logical name is already in the map
            if(tempMap.containsKey(key)){
                //add the job to the corresponding list.
                l = (List)tempMap.get(key);
                l.add(job);
            }
            else{
                //first instance of this logical name
                l = new java.util.LinkedList();
                l.add(job);
                tempMap.put(key,l);
            }
        }

        //iterate through the built up temp map to get jobs per execution pool
        String factor = null;
        int size = -1;
        //the id for the fatjobs. we want ids
        //unique across the execution pools for a
        //particular type of job being merged.
        int id = 1;

        for(Iterator it = tempMap.entrySet().iterator();it.hasNext();){
            Map.Entry entry = (Map.Entry)it.next();
            l   = (List)entry.getValue();
            size= l.size();
            //the pool name on which the job is to run is the key
            key = (String)entry.getKey();

            if(size <= 1){
                //no need to collapse one job. go to the next iteration
                mLogger.log("\t No collapsing for execution pool " + key,
                            LogManager.DEBUG_MESSAGE_LEVEL);
                continue;
            }

            JobAggregator aggregator = mJobAggregatorFactory.loadInstance( (SubInfo)l.get(0) );
            if(aggregator.entryNotInTC(key)){
                //no need to collapse one job. go to the next iteration
                mLogger.log("\t No collapsing for execution pool " + key,
                            LogManager.DEBUG_MESSAGE_LEVEL);
                continue;
            }

            //checks made ensure that l is not empty at this point
            cFactor = getCollapseFactor(key,(SubInfo)l.get(0),size);
            if(cFactor[0] == 1 && cFactor[1] == 0){
                mLogger.log("\t Collapse factor of " + cFactor +
                            " determined for pool. " + key +
                            "Skipping collapsing",LogManager.DEBUG_MESSAGE_LEVEL);
                continue;
            }

            mLogger.log("\t Collapsing jobs at execution pool " +
                        key + " with collapse factor " +
                        cFactor[0] + "," + cFactor[1],LogManager.DEBUG_MESSAGE_LEVEL);



            //we do collapsing in chunks of 3 instead of picking up
            //from the properties file. ceiling is (x + y -1)/y
            //cFactor = (size + 2)/3;

            if(cFactor[0] >= size){
                //means collapse all the jobs in the list as a fat node
                //Note: Passing a link to iterator might be more efficient, as
                //this would only require a single traversal through the list
                fatJob = aggregator.construct(l.subList(0,size),name,
                                              constructID(partitionID,id));
                updateReplacementTable(l.subList(0,size),fatJob);

                //increment the id
                id++;
                //add the fat job to the dag
                //use the method to add, else add explicitly to DagInfo
                mScheduledDAG.add(fatJob);
            }
            else{
                //do collapsing in chunks of cFactor
                int increment = 0;
                for(int i = 0; i < size; i = i + increment){
                    //compute the increment and decrement cFactor[1]
                    increment = (cFactor[1] > 0) ? cFactor[0] + 1:cFactor[0];
                    cFactor[1]--;

                    if(increment == 1){
                        //we can exit out of the loop as we do not want
                        //any merging for single jobs
                        break;
                    }
                    else if( (i + increment) < size){
                        fatJob = aggregator.construct(l.subList(i, i + increment),
                                                      name,
                                                      constructID(partitionID,id));

                        updateReplacementTable(l.subList(i, i + increment), fatJob);
                    }
                    else{
                        fatJob = aggregator.construct(l.subList(i,size),
                                                    name,
                                                    constructID(partitionID,id));
                        updateReplacementTable(l.subList(i, size),fatJob);
                    }

                    //increment the id
                    id++;

                    //add the fat job to the dag
                    //use the method to add, else add explicitly to DagInfo
                    mScheduledDAG.add(fatJob);
                }
            }

        }

        //explicity free the map
        tempMap = null;
    }



    /**
     * Returns the clustered workflow.
     *
     * @return  the <code>ADag</code> object corresponding to the clustered workflow.
     *
     * @throws ClustererException in case of error.
     */
    public ADag getClusteredDAG() throws ClustererException{
        //do all the replacement of jobs in the main data structure
        //that needs to be returned
        replaceJobs();


        return mScheduledDAG;
    }

    /**
     * Returns a textual description of the transfer implementation.
     *
     * @return a short textual description
     */
    public String description(){
        return this.DESCRIPTION;
    }



    /**
     * A callback that triggers the collapsing of a partition/level of a graph.
     *
     * @param partition the partition that needs to be collapsed.
     *
     */
    private void collapseJobs(Partition partition){
        Set s = partition.getNodeIDs();
        List l = new ArrayList(s.size());
        mLogger.log("Collapsing jobs in partition " + partition.getID() +
                    " " +  s,
                    LogManager.DEBUG_MESSAGE_LEVEL);

       for(Iterator it = s.iterator();it.hasNext();){
           SubInfo job = (SubInfo)mSubInfoMap.get(it.next());
           l.add(job);
       }
       //group the jobs by their transformation names
       Collections.sort(l,jobComparator());
       //traverse through the list and collapse jobs
       //referring to same logical transformation
       SubInfo previous = null;
       List clusterList = new LinkedList();
       SubInfo job = null;
       for(Iterator it = l.iterator();it.hasNext();){
           job = (SubInfo)it.next();
           if(previous == null ||
              job.getCompleteTCName().equals(previous.getCompleteTCName())){
               clusterList.add(job);
           }
           else{
               //at boundary collapse jobs
               collapseJobs(previous.getStagedExecutableBaseName(),clusterList,partition.getID());
               clusterList = new LinkedList();
               clusterList.add(job);
           }
           previous = job;
       }
       //cluster the last clusterList
       if(previous != null){
           collapseJobs(previous.getStagedExecutableBaseName(), clusterList, partition.getID());
       }

       //collapse the jobs in list l
//       collapseJobs(job.logicalName,l,partition.getID());
    }




    /**
     * Returns the collapse factor, that is used to chunk up the jobs of a
     * particular type on a pool. The collapse factor is determined by
     * getting the collapse key in the VDS namespace/profile associated with the
     * job in the transformation catalog. Right now tc overrides the property
     * from the one in the properties file that specifies per pool.
     * There are two orthogonal notions of bundling and collapsing. In case the
     * bundle key is specified, it ends up overriding the collapse key, and
     * the bundle value is used to generate the collapse values.
     *
     * @param pool  the pool where the chunking up is occuring
     * @param job   the <code>SubInfo</code> object containing the job that
     *              is to be chunked up together.
     * @param size  the number of jobs that refer to the same logical
     *              transformation and are scheduled on the same execution pool.
     *
     * @return int array of size 2 where int[0] is the the collapse factor
     *         int[1] is the number of jobs for whom collapsing is int[0] + 1.
     */
    public int[] getCollapseFactor(String pool, SubInfo job,int size){
        String factor = null;
        int result[]  = new int[2];
        result[1]     = 0;

        //the job should have the collapse key from the TC if
        //by the user specified
        factor = (String)job.vdsNS.get(VDS.COLLAPSE_KEY);

        //ceiling is (x + y -1)/y
        String bundle = (String)job.vdsNS.get(VDS.BUNDLE_KEY);
        if(bundle != null){
            int b = Integer.parseInt(bundle);
            result[0] = size/b;
            result[1] = size%b;
            return result;
            //doing no boundary condition checks
            //return (size + b -1)/b;
        }

        //return the appropriate value
        result[0] =(factor == null)?
                   ( (factor = (String)mCollapseMap.get(pool)) == null)?
                         this.DEFAULT_COLLAPSE_FACTOR://the default value
                         Integer.parseInt(factor)//use the value in the prop file
                   :
                   //return the value found in the TC
                   Integer.parseInt(factor);
        return result;

    }


    /**
     * Given an integer id, returns a string id that is used for the fat node
     * construction.
     *
     * @param partitionID  the id of the partition.
     * @param id           the integer id from which the string id has to be
     *                     constructed. The id should be unique for all the
     *                     clustered jobs that are formed for a particular
     *                     partition.
     */
    public String constructID(String partitionID, int id){
        StringBuffer sb = new StringBuffer(8);
        sb.append("P").append(partitionID).append("_");
        sb.append("ID").append(id);

        return sb.toString();
    }

    /**
     * Updates the replacement table.
     *
     * @param jobs       the List of jobs that is being replaced.
     * @param mergedJob  the mergedJob that is replacing the jobs in the list.
     */
    private void updateReplacementTable(List jobs, SubInfo mergedJob){
        if(jobs == null || jobs.isEmpty())
            return;
        String mergedJobName = mergedJob.jobName;
        for(Iterator it = jobs.iterator();it.hasNext();){
            SubInfo job = (SubInfo)it.next();
            //put the entry in the replacement table
            mReplacementTable.put(job.jobName,mergedJobName);
        }

    }




    /**
     * Puts the jobs in the abstract workflow into the job that is index
     * by the logical name of the jobs.
     */
    private void assimilateJobs(){
        Iterator it = mScheduledDAG.vJobSubInfos.iterator();
        SubInfo job = null;
        List l      = null;
        String key  = null;

        while(it.hasNext()){
            job = (SubInfo)it.next();
            key = job.logicalName;
            //check if the job logical name is already in the map
            if(mJobMap.containsKey(key)){
                //add the job to the corresponding list.
                l = (List)mJobMap.get(key);
                l.add(job);
            }
            else{
                //first instance of this logical name
                l = new java.util.LinkedList();
                l.add(job);
                mJobMap.put(key,l);
            }
        }
    }



    /**
     * Constructs a map with the numbers/values for the collapsing factors to
     * collapse the nodes of same type. The user ends up specifying these through
     * the  properties file. The value of the property is of the form
     * poolname1=value,poolname2=value....
     *
     * @param propValue the value of the property got from the properties file.
     *
     * @return the constructed map.
     */
    private Map constructMap(String propValue) {
        Map map = new java.util.TreeMap();

        if (propValue != null) {
            StringTokenizer st = new StringTokenizer(propValue, ",");
            while (st.hasMoreTokens()) {
                String raw = st.nextToken();
                int pos = raw.indexOf('=');
                if (pos > 0) {
                    map.put(raw.substring(0, pos).trim(),
                            raw.substring(pos + 1).trim());
                }
            }
        }

        return map;
    }

    /**
     * The relations/edges are changed in local graph structure.
     */
    private void replaceJobs(){
        boolean val = false;
        List l = null;
        List nl = null;
        SubInfo sub = new SubInfo();
        String msg;

        for( Iterator it = mReplacementTable.entrySet().iterator(); it.hasNext(); ){
            Map.Entry entry = (Map.Entry)it.next();
            String key = (String)entry.getKey();
            mLogger.log("Replacing job " + key +" with " + entry.getValue(),
                        LogManager.DEBUG_MESSAGE_LEVEL);
            //remove the old job
            //remove by just creating a subinfo object with the same key
            sub.jobName = key;
            val = mScheduledDAG.remove(sub);
            if(val == false){
                throw new RuntimeException("Removal of job " + key + " while clustering not successful");
            }
        }
        mLogger.log("All clustered jobs removed from the workflow",
                    LogManager.DEBUG_MESSAGE_LEVEL);

        //Set mergedEdges = new java.util.HashSet();
        //this is temp thing till the hast thing sorted out correctly
        List mergedEdges = new java.util.ArrayList(mScheduledDAG.vJobSubInfos.size());

        //traverse the edges and do appropriate replacements
        String parent = null; String child = null;
        String value = null;
        for( Iterator it = mScheduledDAG.dagInfo.relations.iterator(); it.hasNext(); ){
            PCRelation rel = (PCRelation)it.next();
            //replace the parent and child if there is a need
            parent = rel.parent;
            child  = rel.child;

            msg = ("\n Replacing " + rel);

            value = (String)mReplacementTable.get(parent);
            if(value != null){
                rel.parent = value;
            }
            value = (String)mReplacementTable.get(child);
            if(value != null){
                rel.child = value;
            }
            msg += (" with " + rel);

            //put in the merged edges set
            if(!mergedEdges.contains(rel)){
                val = mergedEdges.add(rel);
                msg += "Add to set : " + val;
            }
           else{
               msg += "\t Duplicate Entry for " + rel;
           }
           mLogger.log( msg, LogManager.DEBUG_MESSAGE_LEVEL );
       }

       //the final edges need to be updated
       mScheduledDAG.dagInfo.relations = null;
       mScheduledDAG.dagInfo.relations = new java.util.Vector(mergedEdges);
   }

   /**
    * A utility method to print short description of jobs in a list.
    *
    * @param l the list of <code>SubInfo</code> objects
    */
   private void printList(List l){
           for(Iterator it = l.iterator();it.hasNext();){
               SubInfo job = (SubInfo)it.next();
               System.out.print( " "+ /*job.getCompleteTCName() +*/
                                 "[" + job.logicalId + "]");
           }

   }


    /**
    * A job comparator, that allows me to compare jobs according to the
    * transformation names. It is applied to group jobs in a particular partition,
    * according to the underlying transformation that is referred.
    * <p>
    * This comparator is not consistent with the SubInfo.equals(Object) method.
    * Hence, should not be used in sorted sets or Maps.
    */
   private class JobComparator implements Comparator{

       /**
         * Compares this object with the specified object for order. Returns a
         * negative integer, zero, or a positive integer if the first argument is
         * less than, equal to, or greater than the specified object. The
         * SubInfo are compared by their transformation name.
         *
         * This implementation is not consistent with the
         * SubInfo.equals(Object) method. Hence, should not be used in sorted
         * Sets or Maps.
         *
         * @param o1 is the first object to be compared.
         * @param o2 is the second object to be compared.
         *
         * @return a negative number, zero, or a positive number, if the
         * object compared against is less than, equals or greater than
         * this object.
         * @exception ClassCastException if the specified object's type
         * prevents it from being compared to this Object.
         */
        public int compare(Object o1, Object o2) {
            if (o1 instanceof SubInfo && o2 instanceof SubInfo) {
                return ( (SubInfo) o1).getCompleteTCName().compareTo( ( (
                    SubInfo) o2).getCompleteTCName());

            }
            else {
                throw new ClassCastException("Objects being compared are not SubInfo");
            }
        }
   }


}