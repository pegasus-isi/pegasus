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


package edu.isi.pegasus.planner.cluster;

import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.AggregatedJob;
import edu.isi.pegasus.planner.classes.PCRelation;

import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.common.logging.LogManager;

import edu.isi.pegasus.planner.cluster.JobAggregator;

import edu.isi.pegasus.planner.cluster.aggregator.JobAggregatorInstanceFactory;

import edu.isi.pegasus.planner.partitioner.Partition;

import java.util.Collection;
import java.util.Vector;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;
import edu.isi.pegasus.planner.classes.PegasusBag;

/**
 * An abstract clusterer that the other clusterers can extend. The abstract
 * implementation treats each partition as a single cluster. It has callouts
 * to determine the ordering of the jobs in the cluster, and the input/output
 * files for the clustered jobs.
 *
 * @author Karan Vahi
 * @version $Revision$
 */

public abstract class Abstract implements Clusterer {

    /**
     * A Map to store all the job(Job) objects indexed by their logical ID found in
     * the dax. This should actually be in the ADag structure.
     */
    protected Map mSubInfoMap;

    /**
     * A Map that indexes the partition ID to the name of clustered job.
     */
    protected Map mPartitionClusterMap;


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
     * The collection of relations, that is constructed for the clustered
     * workflow.
     */
    protected Collection mClusteredRelations;

    /**
     * ADag object containing the jobs that have been scheduled by the site
     * selector.
     */
    protected ADag mScheduledDAG;


    /**
     * The Abstract constructor.
     */
    public Abstract(){
        //mLogger = LogManager.getInstance();
        mJobAggregatorFactory = new JobAggregatorInstanceFactory();
    }

    /**
     * Returns the nodes in the partition as a List in a particular order.
     * The iterator of the list returns the nodes in the order determined by
     * the clusterer.
     *
     * @param p  the partition whose nodes have to be ordered.
     *
     * @return an ordered List of <code>String</code> objects that are the ID's
     *         of the nodes.
     *
     * @throws ClustererException in case of error.
     */
    public abstract List order( Partition p ) throws ClustererException;

    /**
     * Determine the input and output files of the job on the basis of the
     * order of the constituent jobs in the AggregatedJob.
     *
     * @param job  the <code>AggregatedJob</code>
     *
     * @throws ClustererException in case of error.
     */
    public abstract void determineInputOutputFiles( AggregatedJob job );

    /*{
        //by default we do not care about order
        List l = new ArrayList( p.getNodeIDs().size() );
        for( Iterator it = p.getNodeIDs().iterator(); it.hasNext();){
            l.add( it.next() );
        }
        return l;
    }
    */





    /**
     *Initializes the Clusterer impelementation
     *
     * @param dag  the workflow that is being clustered.
     * @param bag   the bag of objects that is useful for initialization.
     *
     * @throws ClustererException in case of error.
     */
    public void initialize( ADag dag , PegasusBag bag  )
        throws ClustererException{

        mLogger = bag.getLogger();

        mScheduledDAG = dag;
        mProps = bag.getPegasusProperties();
        mJobAggregatorFactory.initialize( dag, bag );

        mClusteredRelations = new Vector( dag.dagInfo.relations.size()/2 );

        mSubInfoMap = new HashMap( dag.vJobSubInfos.size() );
        mPartitionClusterMap = new HashMap();

        for(Iterator it = mScheduledDAG.vJobSubInfos.iterator();it.hasNext();){
            Job job = (Job)it.next();
            addJob( job );
        }
    }



    /**
     * It creates  a single clustered job for the partition. If there is only
     * one job in the partition, then no clustering happens.
     *
     * @param partition   the partition for which the clusters need to be
     *                    determined.
     *
     * @throws ClustererException if the clustering executable is not installed
     *         on the remote site or if all the jobs in the partition are not
     *         scheduled on the same site.
     */
    public void determineClusters( Partition partition ) throws ClustererException {
        String pID = partition.getID();

        //do the ordering on the partition as required.
        List nodes  = order( partition );

        List l     = new ArrayList( nodes.size() );

        mLogger.log( "Clustering jobs in partition " + pID +
                     " " +  nodes,
                     LogManager.DEBUG_MESSAGE_LEVEL);

        String prevSite = null;
        String currSite = null;
        for( Iterator it = nodes.iterator(); it.hasNext(); ){
            Job job = ( Job )mSubInfoMap.get( it.next() );
            currSite = job.getSiteHandle();
            l.add( job );

            //sanity check to ensure jobs are scheduled on same site.
            if( prevSite == null || currSite.equals( prevSite) ){
                prevSite = currSite;
                continue;
            }
            else{
                throw new ClustererException("Jobs in the partition " +
                                             partition.getID() +
                                             " not scheduled on the same site!");
            }

        }

        int size = l.size();
        Job firstJob = (Job)l.get(0);

//        System.out.println( " Job to be clustered is " + firstJob);

        if(size == 1){
            //no need to collapse one job. go to the next iteration
            mLogger.log("\t No clustering for partition " + pID,
                        LogManager.DEBUG_MESSAGE_LEVEL);
            associate( partition, firstJob );
            return;
        }

        //do the ordering of the list


        JobAggregator aggregator = mJobAggregatorFactory.loadInstance( firstJob );
        if( aggregator.entryNotInTC( currSite ) ){
            throw new ClustererException ("No installed aggregator executable found for partition " +
                                          pID + " at site " + currSite );
        }

        AggregatedJob clusteredJob = aggregator.construct( l,
                                                           firstJob.getStagedExecutableBaseName(),
                                                           this.constructClusteredJobID( partition ) );



        //replace the jobs in the partition with the clustered job
        //in the original workflow
        for( Iterator it = l.iterator(); it.hasNext(); ){
            Job job = (Job)it.next();
            mLogger.log("Replacing job " + job.getName() +" with " + clusteredJob.getName(),
                        LogManager.DEBUG_MESSAGE_LEVEL);

            //remove the old job
            if( !mScheduledDAG.remove( job ) ){
                String msg = "Removal of job " + job.getName() + " while clustering not successful";
                throw new ClustererException( msg );
            }
        }

        //get the correct input and output files for the job
        this.determineInputOutputFiles( clusteredJob );

        //System.out.println(" Clustered Job is " + clusteredJob );

        mScheduledDAG.add( clusteredJob );

        associate( partition, clusteredJob );
    }

    /**
     * Associates the relations between the partitions with the corresponding
     * relations between the clustered jobs that are created for each Partition.
     *
     * @param partitionID   the id of a partition.
     * @param parents       the list of <code>String</code> objects that contain
     *                      the id's of the parents of the partition.
     *
     * @throws ClustererException in case of clustered job not being found for a partition.
     */
    public void parents( String partitionID, List parents ) throws ClustererException{
        String error = "No cluster job for partition ";
        Job clusteredNode = clusteredJob( partitionID );
        Job parentClusteredNode;

        //throw error if not found
        if( clusteredNode == null){ throw new ClustererException( error + partitionID); }

        for( Iterator it = parents.iterator(); it.hasNext(); ){
            String parent = (String)it.next();
            parentClusteredNode = clusteredJob( parent );

            //throw error if not found
            if( clusteredNode == null){ throw new ClustererException( error + parent); }

            //add a relation between these clustered jobs
            mClusteredRelations.add( new PCRelation( parentClusteredNode.getName(),
                                                     clusteredNode.getName() ));
        }

    }


    /**
     * Returns the clustered workflow.
     *
     * @return  the <code>ADag</code> object corresponding to the clustered workflow.
     *
     * @throws ClustererException in case of error.
     */
    public ADag getClusteredDAG() throws ClustererException{
        //replace the relations of the original DAG and return
        mScheduledDAG.dagInfo.relations = null;
        mScheduledDAG.dagInfo.relations = (Vector)mClusteredRelations;

        return mScheduledDAG;
    }



    /**
     * Returns the ID for the clustered job corresponding to a partition.
     *
     * @param partition  the partition.
     *
     * @return the ID of the clustered job
     */
    protected String constructClusteredJobID( Partition partition ){
        return partition.getID();
    }


    /**
     * Adds jobs to the internal map of jobs that is maintained by the clusterer.
     *
     * @param job  the job being added
     */
    protected void addJob( Job job ){
        mSubInfoMap.put( job.getLogicalID(), job );
    }

    /**
     * Returns the job object corresponding to the id of the job.
     *
     * @param id  the id of the job
     *
     * @return the corresponding job.
     */
    protected Job getJob( String id ){
        return (Job) mSubInfoMap.get( id );
    }


    /**
     * Maps the partition to the corresponding clustered job.
     *
     * @param p   the partition being clustered.
     * @param job the corresponding clustered job.
     */
    protected void associate( Partition p, Job job ){
        mPartitionClusterMap.put( p.getID(), job );
    }

    /**
     * Returns the job corresponding to a partition.
     *
     * @param p  the partition for which the clustered job is reqd.
     *
     * @return the corresponding job, else null in case of job is not found.
     */
    protected Job clusteredJob( Partition p ){
        return this.clusteredJob( p.getID() );
    }

    /**
     * Returns the job corresponding to a partition.
     *
     * @param id   the partition id.
     *
     * @return the corresponding job, else null in case of job is not found.
     */
    protected Job clusteredJob( String id ){
        Object obj = mPartitionClusterMap.get( id );
        return  ( obj == null) ?
               null:
               (Job)obj;
    }

}
