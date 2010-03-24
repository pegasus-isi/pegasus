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

package org.griphyn.cPlanner.engine.cleanup;

import org.griphyn.cPlanner.classes.SubInfo;
import org.griphyn.cPlanner.classes.PegasusFile;
import org.griphyn.cPlanner.classes.PlannerOptions;

import org.griphyn.cPlanner.common.PegasusProperties;
import edu.isi.pegasus.common.logging.LogManager;

import org.griphyn.cPlanner.namespace.Condor;

import org.griphyn.cPlanner.partitioner.graph.GraphNode;
import org.griphyn.cPlanner.partitioner.graph.Graph;


import java.util.Map;
import java.util.Iterator;
import java.util.HashMap;
import java.util.List;
import java.util.LinkedList;
import java.util.ArrayList;
import java.util.Set;
import java.util.TreeSet;
import java.util.HashSet;
import java.lang.StringBuffer;
import org.griphyn.cPlanner.classes.PegasusBag;
import org.griphyn.cPlanner.classes.TransferJob;
import org.griphyn.cPlanner.partitioner.graph.MapGraph;


/**
 * This generates  cleanup jobs in the workflow itself.
 *
 *
 * @author Arun ramakrishnan
 * @author Karan Vahi
 *
 * @version $Revision$
 */
public class InPlace implements CleanupStrategy{

    /**
     * The prefix for CLEANUP_JOB ID i.e prefix+the parent compute_job ID becomes
     * ID of the cleanup job.
     */
    public static final String CLEANUP_JOB_PREFIX = "clean_up_";

    /**
     * The mapping to siteHandle to all the jobs that are mapped to it
     * mapping to siteHandle(String) to Set<GraphNodes>
     */
    private HashMap mResMap;

    /**
     * The mapping of siteHandle to all subset of the jobs mapped to it that are
     * leaves in the workflow mapping to siteHandle(String) to Set<GraphNodes>.
     */
    private HashMap mResMapLeaves;

    /**
     * The mapping of siteHandle to all subset of the jobs mapped to it that are
     * leaves in the workflow mapping to siteHandle(String) to Set<GraphNodes>.
     */
    private HashMap mResMapRoots;

    /**
     * The max depth of any job in the workflow useful for a priorityQueue
     * implementation in an array
     */
    private int mMaxDepth;

    /**
     * HashSet of Files that should not be cleaned up
     */
    private HashSet mDoNotClean;

    /**
     * The handle to the CleanupImplementation instance that creates the jobs for us.
     */
    private CleanupImplementation mImpl;

    /**
     * The handle to the properties passed to Pegasus.
     */
    private PegasusProperties mProps;

    /**
     * The handle to the logging object used for logging.
     */
    private LogManager mLogger;

    
    
    /**
     * The default constructor.
     */
    public InPlace(){
        
    }
    
    /**
     * Intializes the class.
     *
     * @param bag    bag of initialization objects
     * @param impl    the implementation instance that creates cleanup job
     */
    public void initialize( PegasusBag bag, CleanupImplementation impl ) {
        mProps = bag.getPegasusProperties();
        mLogger = bag.getLogger();

        mImpl  = impl;

        //intialize the internal structures
        mResMap       = new HashMap();
        mResMapLeaves = new HashMap();
        mResMapRoots  = new HashMap();
        mDoNotClean   = new HashSet();
        mMaxDepth=0;
    }

    /**
     * Adds cleanup jobs to the workflow.
     *
     * @param workflow   the workflow to add cleanup jobs to.
     *
     * @return the workflow with cleanup jobs added to it.
     */
    public Graph addCleanupJobs( Graph workflow ){
        //reset the internal data structures
        reset();

        //add the priorities to all the jobs
        applyJobPriorities( workflow );


        //determine the files that should not be removed from the resource where it is produced
        // i.e file A produced by job J should not be removed if J does not have a stage out job
        // and A has getTransientTransferFlag() set to false
        for( Iterator it = workflow.nodeIterator() ; it.hasNext(); ){
            GraphNode _GN = ( GraphNode ) it.next();
            SubInfo _SI = ( SubInfo ) _GN.getContent();
            //only for compute jobs
            if( ! ( _SI.getJobType() == _SI.COMPUTE_JOB || _SI.getJobType() == _SI.STAGED_COMPUTE_JOB ) ) {
                continue;
            }

            //if the compute job has a stage out job then all the files produced by it can be removed
            // so , skip such cases
            boolean job_has_stageout = false ;
            for( Iterator itjc = _GN.getChildren().iterator() ; itjc.hasNext() ; ){
                SubInfo _SIchild = ( SubInfo ) ( ( GraphNode ) itjc.next() ).getContent() ;
                if( _SIchild.getJobType() == _SIchild.STAGE_OUT_JOB ){
                    job_has_stageout = true;
                    break;
                }
            }
            if( job_has_stageout ) continue;

            //else add files with getTransientTransferFlag() set to false to the do_not_clean List
            Set _ofiles = _SI.getOutputFiles();
            for( Iterator itof = _ofiles.iterator() ; itof.hasNext() ; ){
                PegasusFile of = ( PegasusFile ) itof.next();
                if( of.getTransientTransferFlag() == false ){
                    this.mDoNotClean.add( of );
                }
            }
        }


//        mLogger.log( "The input workflow " + workflow,
//                     LogManager.DEBUG_MESSAGE_LEVEL );

        //set the depth and ResMap values iteratively
        setDepth_ResMap( workflow.getRoots() );

        mLogger.log( "Number of sites " + mResMap.size(),
                LogManager.DEBUG_MESSAGE_LEVEL );

        //output for debug
        StringBuffer message = new StringBuffer();
        for( Iterator it= mResMap.entrySet().iterator(); it.hasNext() ;){
            Map.Entry entry = (Map.Entry)it.next();
            message.append( "Site " ).append( (String)entry.getKey())
            .append(" count jobs = ").append( ( (Set)entry.getValue()).size());
            mLogger.log( message.toString(), LogManager.DEBUG_MESSAGE_LEVEL );

            Set whatever= (Set)entry.getValue() ;
            for( Iterator weit=whatever.iterator(); weit.hasNext() ; ){
                mLogger.log( "* "+ ((GraphNode)weit.next()).getID(),
                        LogManager.DEBUG_MESSAGE_LEVEL );
            }
            message = new StringBuffer();
        }


        //for each site do the process of adding cleanup jobs
        for( Iterator it= mResMap.entrySet().iterator(); it.hasNext() ;){
            Map.Entry entry = (Map.Entry)it.next();
            addCleanUpJobs( (String)entry.getKey() , (Set)entry.getValue() , workflow );
        }

//        mLogger.log( "The resultant workflow with cleanup jobs " + workflow,
//                     LogManager.DEBUG_MESSAGE_LEVEL );


        return workflow;
    }


    /**
     * Resets the internal data structures.
     *
     */
    protected void reset(){
        mResMap.clear();
        mResMapLeaves.clear();
        mResMapRoots.clear();
        mMaxDepth = 0;
    }

    /**
     * A BFS implementation to set depth value (roots have depth 1) and also
     * to populate mResMap ,mResMapLeaves,mResMapRoots which contains all the
     * jobs that are assigned to a particular resource
     *
     * @param roots List of GraphNode objects that are roots
     */
    private void setDepth_ResMap( List roots ){
        LinkedList que = new LinkedList();
        que.addAll( roots );

        for(int i=0; i < que.size(); i++){
            ( (GraphNode)que.get(i) ).setDepth( 1 );
        }

        while( que.size() >= 1 ){
            GraphNode curGN = (GraphNode)que.removeFirst();

            //debug
            /*
            System.out.print(curGN.getDepth() +" "+((SubInfo)curGN.getContent()).getSiteHandle()+" ");
            if( curGN.getChildren() == null )
                System.out.print("0");
            else
                System.out.print( curGN.getChildren().size() );
             */

            //populate mResMap ,mResMapLeaves,mResMapRoots
            SubInfo si = ( SubInfo )curGN.getContent();
            
            //Commented out as for stage out jobs we need non third party
            //site. Karan Jan 8, 2009
//            if( !mResMap.containsKey( si.getSiteHandle() ) ){
//                mResMap.put( si.getSiteHandle(), new HashSet() );
//
//            }
//            ((Set)mResMap.get( si.getSiteHandle() )).add( curGN );
            

            String site =  typeStageOut( si.getJobType() )?
                             ((TransferJob)si).getNonThirdPartySite():   
                             si.getSiteHandle();   
            if( !mResMap.containsKey( site ) ){
                mResMap.put( site, new HashSet() );

            }
            ((Set)mResMap.get( site )).add( curGN );
            
            
            
            //System.out.println( "  site count="+((Set)mResMap.get( si.getSiteHandle() )).size() );


            //now set the depth

            for( Iterator it = curGN.getChildren().iterator() ; it.hasNext() ;){
                GraphNode child = (GraphNode)it.next();
                if(!( child.getDepth() == -1 || child.getDepth() < curGN.getDepth()+1 ) ){
                    continue;
                }

                child.setDepth( curGN.getDepth() + 1 );
                if( child.getDepth() > mMaxDepth ) mMaxDepth=child.getDepth();
                que.addLast( child );
            }

        }

    }



    /**
     * Adds cleanup jobs for the workflow scheduled to a particular site
     * a best first search strategy is implemented based on the depth of the job
     * in the workflow
     *
     * @param site the site ID
     * @param leaves the leaf jobs that are scheduled to site
     * @param workflow the Graph into which new cleanup jobs can be added
     */
    private void addCleanUpJobs( String site, Set leaves, Graph workflow ){

        mLogger.log(  site + " " + leaves.size() , LogManager.DEBUG_MESSAGE_LEVEL );
        //if( !site.equals(new String("k")) )return;
        //file(String) cleaned by GraphNode
        HashMap cleanedBy = new HashMap();

        //the below in case we get rid of the primitive java 1.4
        //PriorityQueue<GraphNode> pQ=new   PriorityQueue<GraphNode>(resMap.get(site).size(),GraphNode_ORDER);

        StringBuffer message = new StringBuffer();
        message.append( "Leaf  jobs scheduled at site ").append( site )
        .append( " are " );
        for( Iterator it = leaves.iterator(); it.hasNext(); ){
            message.append( ((GraphNode)it.next()).getID() );
        }
        mLogger.log( message.toString(), LogManager.DEBUG_MESSAGE_LEVEL );

        //its a Set of GraphNode's
        Set[] pQA = new Set[ mMaxDepth + 1 ];
        for( int i = 0 ; i < pQA.length ; i++ ){
            pQA[i] = new HashSet();
        }

        //populate the priority Array pQA with all the leaf nodes
        for( Iterator it = leaves.iterator() ; it.hasNext() ;){
            GraphNode gN = (GraphNode)it.next();
            pQA[ gN.getDepth() ].add( gN );

        }

        //start the best first cleanup job addition
        for( int curP = mMaxDepth; curP >= 0 ; curP-- ){

            //process all elements in the current priority
            while( pQA[curP].size() >= 1 ){
                GraphNode curGN = (GraphNode) pQA[ curP ].iterator().next();
                pQA[ curP ].remove( curGN );
                SubInfo curGN_SI = (SubInfo) curGN.getContent();

                if( !typeNeedsCleanUp( curGN_SI.getJobType() ) ) { continue; }

                /*if( curGN_SI.getJobType() == SubInfo.STAGE_OUT_JOB ){
                    curGN_SI.getInputFiles().addAll( curGN_SI.getOutputFiles() );
                    curGN_SI.getOutputFiles().clear();
                    
                    System.out.println( curGN_SI.getName() );
                    System.out.println( curGN_SI.getOutputFiles() );
                    System.out.println( curGN_SI.getInputFiles() );
                     
                }*/
                    
                    
//              Leads to corruption of input files for the job.
//                Set fileSet = curGN_SI.getInputFiles();
                Set fileSet = new HashSet( curGN_SI.getInputFiles() );
                fileSet.addAll( curGN_SI.getOutputFiles() );

                //remove the files in fileSet that are in this.mDoNotClean
                Set fileSet2 = new HashSet( fileSet );
                for( Iterator itfs2 = fileSet2.iterator() ; itfs2.hasNext() ; ){
                    Object _dum_pf = itfs2.next() ;
                    if( this.mDoNotClean.contains( _dum_pf ) ){
                        fileSet.remove( _dum_pf );
                    }
                }

                // create new GraphNode with MLogicalID=mLogicalName , mParents
                // mContent ID ,Name , jobtype
                //the files it cleans up are specified in mContent.inputFiles
                //create a dummy GraphNode .first create SubInfo object and then add it to GraphNode
                GraphNode nuGN = new GraphNode( generateCleanupID( curGN_SI ),
                        curGN_SI.getTXName() );
//                                   InPlace.CLEANUP_JOB_PREFIX + curGN.getName() ,
//                                   InPlace.CLEANUP_JOB_PREFIX + curGN.getName() );

                List cleanupFiles = new LinkedList();
                for( Iterator it = fileSet.iterator() ; it.hasNext() ; ){
                    PegasusFile file = (PegasusFile) it.next();

                    //check if its already set up to be cleaned up
                    if( cleanedBy.containsKey( file.getLFN()) ){
                        GraphNode child = (GraphNode) cleanedBy.get( file.getLFN() );
                        if( !child.getParents().contains( curGN ) ){
                            child.addParent( curGN );
                        }
                        if( !curGN.getChildren().contains( child ) ){
                            curGN.addChild( child );
                        }
                    }else{
//                       nuSI.addInputFile( file );
                        cleanupFiles.add( file );
                        cleanedBy.put( file.getLFN(), nuGN );

                        if( !curGN.getChildren().contains( nuGN ) ){
                            curGN.addChild( nuGN );
                        }
                        if( ! nuGN.getParents().contains( curGN ) ){
                            nuGN.addParent( curGN );
                        }
                    }
                }// all the files

                //create a cleanup job if the cleanup node has any parents
                if( nuGN.getParents().size() >= 1 ){
                    mLogger.log( "Adding cleanup job with ID " + nuGN.getID(),
                            LogManager.DEBUG_MESSAGE_LEVEL );
                    
                    // We have always pass the associaated compute job. Since now
                    //a cleanup job can be associated with stageout jobs also, we
                    //need to make sure that for the stageout job the cleanup job
                    //is passed. Karan Jan 9, 2008                    
//                    SubInfo cleanupJob = mImpl.createCleanupJob( nuGN.getID(),
//                            cleanupFiles,
//                            curGN_SI
//                            );
                    SubInfo computeJob;
                    if( typeStageOut( curGN_SI.getJobType() ) ){
                        //find a compute job that is parent of this
                        GraphNode node = (GraphNode)curGN.getParents().get( 0 );
                        computeJob = (SubInfo)node.getContent();
                        message = new StringBuffer();
                        message.append( "For cleanup job " ).append( nuGN.getID() ).
                                append( " the associated compute job is ").append( computeJob.getID() );
                        mLogger.log(  message.toString(), LogManager.DEBUG_MESSAGE_LEVEL );
                    }
                    else{
                        computeJob = curGN_SI;
                    }
                    SubInfo cleanupJob = mImpl.createCleanupJob( nuGN.getID(),
                                                                 cleanupFiles,
                                                                 computeJob
                                                                 );
                    
                    //No longer required as stageout jobs are also cleaned. Karan Jan , 2008
                    //if the corresponding compute job has any transfer or stageout jobs as child add it
                    //as a parent of the cleanup job
                    for( Iterator itc=curGN.getChildren().iterator(); itc.hasNext() ;){
                        GraphNode curGNchild=(GraphNode) itc.next();
                        SubInfo itc_si=(SubInfo) curGNchild.getContent();
                        if( itc_si != null )
                            if( itc_si.getJobType() == SubInfo.STAGE_OUT_JOB ||
                                itc_si.getJobType() == SubInfo.INTER_POOL_JOB ){

                            nuGN.addParent( curGNchild );
                            curGNchild.addChild( nuGN );
                            }
                    }

                    //add the job as a content to the graphnode
                    //and the node itself to the Graph
                    nuGN.setContent( cleanupJob );
                    workflow.addNode(nuGN);
               }
            }
        }

        //output whats file is cleaned by what ?
        mLogger.log( "", LogManager.DEBUG_MESSAGE_LEVEL );
        mLogger.log( "CLEANUP LIST",LogManager.DEBUG_MESSAGE_LEVEL);
        for( Iterator it = cleanedBy.keySet().iterator() ; it.hasNext() ;){
            String lfn = (String)it.next();
            GraphNode cl_GN = (GraphNode)cleanedBy.get(lfn);
            SubInfo cl_si = (SubInfo)cl_GN.getContent();
            //Arun please use a StringBuffer first
            //Karan March 13, 2007
            mLogger.log( "file:" + lfn + "  site:" + cl_si.getSiteHandle() + " " + cl_GN.getID() ,
                    LogManager.DEBUG_MESSAGE_LEVEL );
        }


        //reduce dependencies. for each cleanup job X, look at the parents of
        //the job. For each parent Y see if there is a path to any other parent Z of X.
        //If a path exists, then the edge from Z to cleanup job can
        //be removed.
        int num = 0;
        for( Iterator it = cleanedBy.values().iterator() ; it.hasNext() ;){
            num++;
            mLogger.log(" cleanup job counter = " + num, mLogger.DEBUG_MESSAGE_LEVEL);
            GraphNode cl_GN = (GraphNode)it.next();
            //SubInfo cl_si=(SubInfo)cl_GN.getContent();
            List cl_GNparents = cl_GN.getParents();
            List redundant = new LinkedList();
            HashSet visit = new HashSet();
            for( Iterator itp = cl_GN.getParents().iterator() ; itp.hasNext() ;){
                LinkedList mque = new LinkedList();
                mque.add( itp.next() );

                while( mque.size() > 0 ){
                    GraphNode popGN = (GraphNode) mque.removeFirst();

                    if( visit.contains(popGN) ) { continue; }

                    visit.add(popGN);

                    for( Iterator itp_pop = popGN.getParents().iterator() ; itp_pop.hasNext() ;){
                        GraphNode pop_pGN = (GraphNode) itp_pop.next();
                        //check if its redundant ..if so add it to redundant list
                        if( cl_GNparents.contains( pop_pGN ) ){
                            redundant.add( pop_pGN );
                        }else{
                            //mque.addAll( pop_pGN.getParents() );
                            for( Iterator itgp = pop_pGN.getParents().iterator() ; itgp.hasNext() ;){
                                GraphNode gpGN = (GraphNode) itgp.next();
                                if( ! visit.contains( gpGN ) ){
                                    mque.add( gpGN );
                                }
                            }
                        }
                    }
                }
            }

            //remove all redundant nodes that were found
            for( Iterator itr = redundant.iterator() ; itr.hasNext() ;){
                GraphNode r_GN = (GraphNode) itr.next();
                cl_GN.removeParent( r_GN );
                r_GN.removeChild( cl_GN );
            }
        }


    }


    /**
     * Adds job priorities to the jobs in the workflow on the basis of
     * the levels in the traversal order given by the iterator. Later on
     * this would be a separate refiner.
     *
     * @param workflow   the workflow on which to apply job priorities.
     *
     */
    protected void applyJobPriorities( Graph workflow ){

        for ( Iterator it = workflow.iterator(); it.hasNext(); ){
            GraphNode node = (GraphNode)it.next();
            SubInfo job = ( SubInfo )node.getContent();

            //log to debug
            StringBuffer sb = new StringBuffer();
            sb.append( "Applying priority of " ).append( node.getDepth() ).
                    append( " to " ).append( job.getID() );
            mLogger.log( sb.toString(), LogManager.DEBUG_MESSAGE_LEVEL );

            //apply a priority to the job overwriting any preexisting priority
            job.condorVariables.construct( Condor.PRIORITY_KEY,
                    new Integer( node.getDepth() ).toString());


            //also for compute and staged compute jobs
            //forward to remote job manager also
            //the below hack only works for condor pools
//            if( job.getJobType() == SubInfo.COMPUTE_JOB ||
//                job.getJobType() == SubInfo.STAGED_COMPUTE_JOB ){
//                job.globusRSL.construct( "condorsubmit",
//                                         "(priority " + node.getDepth() + ")");
//            }
        }
        return;
    }

    /**
     * Returns the identifier that is to be assigned to cleanup job.
     *
     * @param job  the job with which the cleanup job is primarily associated.
     *
     * @return the identifier for a cleanup job.
     */
    protected String generateCleanupID( SubInfo job ){
        StringBuffer sb = new StringBuffer();
        sb.append( this.CLEANUP_JOB_PREFIX ).append( job.getID() );
        return sb.toString();
    }

    /**
     * Checks to see which job types are required to be looked at for cleanup.
     * COMPUTE_JOB , STAGE_OUT_JOB , INTER_POOL_JOB  are the ones that need
     * cleanup
     *
     * @param type  the type of the job.
     *
     * @return boolean
     */
    protected boolean typeNeedsCleanUp( int type ){
        return (   type == SubInfo.COMPUTE_JOB
                || type == SubInfo.STAGE_OUT_JOB
                || type == SubInfo.INTER_POOL_JOB
                || type == SubInfo.STAGED_COMPUTE_JOB );
    }

    
    /**
     * Checks to see if job type is a stageout job type.
     *
     * @param type  the type of the job.
     *
     * @return boolean
     */
    protected boolean typeStageOut( int type ){
        return (   type == SubInfo.STAGE_OUT_JOB
                || type == SubInfo.INTER_POOL_JOB
                );
    }
}
