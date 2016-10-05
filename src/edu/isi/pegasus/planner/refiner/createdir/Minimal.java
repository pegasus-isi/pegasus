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

package edu.isi.pegasus.planner.refiner.createdir;


import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.TransferJob;
import edu.isi.pegasus.planner.classes.PegasusBag;

import edu.isi.pegasus.common.logging.LogManager;

import edu.isi.pegasus.planner.catalog.site.classes.FileServer;
import edu.isi.pegasus.planner.classes.DAGJob;
import edu.isi.pegasus.planner.classes.DAXJob;
import edu.isi.pegasus.planner.partitioner.graph.GraphNode;
import java.util.BitSet;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * This strategy for adding create dir jobs to the workflow only adds the minimum 
 * number of edges from the create dir job to the compute jobs in the workflow.
 * 
 * The strategy involves in walking the graph in a BFS order, and updating a bit set
 * associated with each job based on the BitSet of the parent jobs. The BitSet
 * indicates whether an edge exists from the create dir job to an ancestor of the node.
 * 
 * For a node, the bit set is the union of all the parents BitSets. The BFS traversal
 * ensures that the bitsets are of a node are only updated once the parents have
 * been processed
 * 
 * 
 * @author Karan Vahi
 *
 * @version $Revision$
 */
public class Minimal extends AbstractStrategy {


    /**
     * Intializes the class.
     *
     * @param bag    bag of initialization objects
     * @param impl    the implementation instance that creates create dir job
     */
    public void initialize( PegasusBag bag, Implementation impl ){
        super.initialize( bag , impl );
    }
   
    /**
     * Modifies the workflow to add create directory nodes. The workflow passed
     * is a worklow, where the jobs have been mapped to sites.
     * 
     * The strategy involves in walking the graph in a BFS order, and updating a 
     * bit set associated with each job based on the BitSet of the parent jobs. 
     * The BitSet indicates whether an edge exists from the create dir job to an 
     * ancestor of the node.
     * 
     * For a node, the bit set is the union of all the parents BitSets. The BFS 
     * traversal ensures that the bitsets are of a node are only updated once the 
     * parents have been processed.
     * 
     * @param dag   the workflow to which the nodes have to be added.
     * 
     * @return the added workflow
     */
    public ADag addCreateDirectoryNodes( ADag dag ){
        //PM-747 no need for conversion as ADag now implements Graph interface
        return this.addCreateDirectoryNodes( dag , this.getCreateDirSites(dag));

    }
    
    /**
     * Adds create dir nodes to the workflow.
     * 
     * The strategy involves in walking the graph in a BFS order, and updating a 
     * bit set associated with each job based on the BitSet of the parent jobs. 
     * The BitSet indicates whether an edge exists from the create dir job to an 
     * ancestor of the node.
     * 
     * For a node, the bit set is the union of all the parents BitSets. The BFS 
     * traversal ensures that the bitsets are of a node are only updated once the 
     * parents have been processed.
     * 
     * @param workflow  the workflow 
     * @param sites     the staging sites the workflow refers to.
     * 
     * @return 
     */
    public ADag addCreateDirectoryNodes( ADag workflow,  Set<String> sites ) {
        //the number of sites dictates the size of the BitSet associated with each job.
        Map<String, Integer> siteToBitIndexMap = new HashMap();
        int bitSetSize = sites.size();
        int i = 0;
        for( String site: sites ){
            siteToBitIndexMap.put( site, i++ );
        }
        
        
        //create the create dir jobs required but don't add to the workflow
        //till edges are figured out
        //for each execution pool add a create directory node.
        Map<GraphNode,List<GraphNode>> createDirChildrenMap = new HashMap();
        Map<String,GraphNode> createDirMap = new HashMap();//mas site to the associated create dir node
        for (String site: sites ){
            String jobName = getCreateDirJobName( workflow, site );
            Job newJob  = mImpl.makeCreateDirJob( site,
                                              jobName,
                                              mSiteStore.getExternalWorkDirectoryURL( site , FileServer.OPERATION.put )  );
            mLogger.log( "Creating create dir node " + jobName , LogManager.DEBUG_MESSAGE_LEVEL );
            GraphNode node = new GraphNode( newJob.getID() );
            node.setContent(newJob);
            createDirChildrenMap.put(node, new LinkedList<GraphNode>());
            createDirMap.put( site, node );
        }
        
        
        //we use an identity hash map to associate the nodes with the bitmaps 
        Map<GraphNode,BitSet> nodeBitMap = new IdentityHashMap( workflow.size() );
        
        //do a BFS walk over the workflow
        for( Iterator<GraphNode> it = workflow.iterator(); it.hasNext(); ){
            GraphNode node = it.next();
            BitSet set     = new BitSet( bitSetSize );
            Job job        = (Job)node.getContent();
            String site    = getAssociatedCreateDirSite( job );
            
            //check if for stage out jobs there are any parents specified 
            //or not.
            if( job instanceof TransferJob && job.getJobType() == Job.STAGE_OUT_JOB ){
                if( node.getParents().isEmpty() ){
                    //means we have a stage out job only. probably the workflow
                    //was fully reduced in data reuse
                    mLogger.log( "Not considering job for create dir edges " + job.getID() , LogManager.DEBUG_MESSAGE_LEVEL );
                    nodeBitMap.put(node, set);
                    continue;
                }
            }
            
            //the set is a union of all the parents set
            for( GraphNode parent: node.getParents() ){
                BitSet pSet = nodeBitMap.get( parent );
                set.or( pSet );
            }
            
            if( site == null ){
                //only ok for stage worker jobs
                if( job instanceof TransferJob || job.getJobType() == Job.REPLICA_REG_JOB ){
                    mLogger.log( "Not adding edge to create dir job for job " + job.getID(),
                                     LogManager.DEBUG_MESSAGE_LEVEL );
                    nodeBitMap.put(node, set);
                    continue;
                }
                else{
                    throw new RuntimeException( "Job not associated with staging site " + job.getID() );
                }
            }
            
            Object value = siteToBitIndexMap.get( site );
            if( value == null){
                StringBuffer parents = new StringBuffer();
                parents.append( "{");
                for(GraphNode parent : node.getParents()){
                    parents.append( parent.getID() ).append(",");
                }
                parents.append( "}");
                throw new RuntimeException( "Create dir site " + site + " for job " + job.getID() + 
                                            " with parents " + parents + 
                                            " is not present in staging sites for workflow " +  createDirMap.keySet() );
            }
            int index = (Integer)value; 
            if(! set.get( index ) ){
                //none of the parents have an index to the site
                //need to add an edge.
                //String parent = getCreateDirJobName( dag, site );
                GraphNode parent = createDirMap.get( site );
                mLogger.log( "Need to add edge "  + parent.getID() + " -> " + job.getID(),
                             LogManager.DEBUG_MESSAGE_LEVEL );
                createDirChildrenMap.get( parent ).add( node );

                //edge has been added . set the bit to true
                set.set( index );
            }
            
            //set the bitset of createdirs for the node
            nodeBitMap.put(node, set);
        }
        
        
        //for each create dir job add it to the workflow
        //and connect the edges
        for ( Map.Entry<GraphNode, List<GraphNode>> entry : createDirChildrenMap.entrySet()  ){
            GraphNode createDirNode = entry.getKey();
            List<GraphNode> children = entry.getValue();
            mLogger.log(  "Adding node to the worfklow " + createDirNode.getID(),
                          LogManager.DEBUG_MESSAGE_LEVEL );
            for( GraphNode child: children ){
                createDirNode.addChild(child);
                child.addParent( createDirNode );
            }
            workflow.addNode( createDirNode );

        }

        
        return workflow;
    }
    
    
    /**
     * Returns the associated site that job is dependant on.
     * This is site, whose create dir job should be a parent or an ancestor of 
     * the job.
     * 
     * @param job  the job for which we need the associated create dir site.
     * 
     * @return the site 
     */
    private String getAssociatedCreateDirSite( Job job ) {
        String site = null;
        if( job.getJobType() == Job.CHMOD_JOB ){
            site =  job.getStagingSiteHandle();
        }
        else{
            //the parent in case of a transfer job
            //is the non third party site
            site = ( job instanceof TransferJob )?
                               ((TransferJob)job).getNonThirdPartySite():
                               job.getStagingSiteHandle();

            
            if( site == null ){
                //only ok for stage worker jobs
                if( job instanceof TransferJob ){
                    mLogger.log( "Not adding edge to create dir job for job " + job.getID(),
                                     LogManager.DEBUG_MESSAGE_LEVEL );
                    return site;
                    
                }
            }
        }
        return site;
    }

    
    public boolean addDependency(Job job ){
        //put in the dependency only for transfer jobs that stage in data
        //or are jobs running on remote sites
        //or are compute jobs running on local site
        int type = job.getJobType();
        boolean local = job.getSiteHandle().equals("local");
        if( (job instanceof TransferJob &&  type != Job.STAGE_OUT_JOB )
            || (!local
                      || (type == Job.COMPUTE_JOB /*|| type == Job.STAGED_COMPUTE_JOB*/ || job instanceof DAXJob || job instanceof DAGJob ))){


            return true;
        }
        return false;
    }
    
    

}
