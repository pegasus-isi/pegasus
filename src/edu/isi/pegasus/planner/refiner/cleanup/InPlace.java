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

package edu.isi.pegasus.planner.refiner.cleanup;

import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusFile;

import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.common.logging.LogManager;

import edu.isi.pegasus.planner.namespace.Condor;

import edu.isi.pegasus.planner.partitioner.graph.GraphNode;
import edu.isi.pegasus.planner.partitioner.graph.Graph;

import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.TransferJob;

import edu.isi.pegasus.planner.namespace.Dagman;
import edu.isi.pegasus.planner.partitioner.graph.GraphNodeContent;

import java.util.Map;
import java.util.Iterator;
import java.util.HashMap;
import java.util.List;
import java.util.LinkedList;
import java.util.Set;
import java.util.HashSet;



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
     * The default value for the maxjobs variable for the category of cleanup
     * jobs.
     */
    public static final String DEFAULT_MAX_JOBS_FOR_CLEANUP_CATEGORY = "4";

    /**
     * The default value for the number of clustered cleanup jobs created per
     * level.
     */
    public static final int DEFAULT_CLUSTERED_CLEANUP_JOBS_PER_LEVEL = 2;

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
     * The number of cleanup jobs per level to be created
     */
    private int mCleanupJobsPerLevel;

    
    
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
        
        //set the default value for maxjobs only if not specified
        //in the properties
        String key = this.getDefaultCleanupMaxJobsPropertyKey();
        if( this.mProps.getProperty(key) == null ){
            mLogger.log( "Setting property " + key + " to  " +
                          InPlace.DEFAULT_MAX_JOBS_FOR_CLEANUP_CATEGORY +
                          " to set max jobs for cleanup jobs category",
                          LogManager.CONFIG_MESSAGE_LEVEL );
            mProps.setProperty( key, InPlace.DEFAULT_MAX_JOBS_FOR_CLEANUP_CATEGORY );
        }

        String propValue = mProps.getMaximumCleanupJobsPerLevel();
        int value = -1;
        try{
            value = Integer.parseInt( propValue );
        }
        catch( Exception e ){
            //ignore
        }
        mCleanupJobsPerLevel = ( value > 0 ) ? value: DEFAULT_CLUSTERED_CLEANUP_JOBS_PER_LEVEL;
        mLogger.log( "Maximum number of cleanup jobs to be created per level " + mCleanupJobsPerLevel,
                     LogManager.CONFIG_MESSAGE_LEVEL );
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
        //applyJobPriorities( workflow );


        //determine the files that should not be removed from the resource where it is produced
        // i.e file A produced by job J should not be removed if J does not have a stage out job
        // and A has getTransientTransferFlag() set to false
        for( Iterator it = workflow.nodeIterator() ; it.hasNext(); ){
            GraphNode _GN = ( GraphNode ) it.next();
            Job _SI = ( Job ) _GN.getContent();
            //only for compute jobs
            if( ! ( _SI.getJobType() == _SI.COMPUTE_JOB /*|| _SI.getJobType() == _SI.STAGED_COMPUTE_JOB*/ ) ) {
                continue;
            }

            //if the compute job has a stage out job then all the files produced by it can be removed
            // so , skip such cases
            boolean job_has_stageout = false ;
            for( Iterator itjc = _GN.getChildren().iterator() ; itjc.hasNext() ; ){
                Job _SIchild = ( Job ) ( ( GraphNode ) itjc.next() ).getContent() ;
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
            System.out.print(curGN.getDepth() +" "+((Job)curGN.getContent()).getSiteHandle()+" ");
            if( curGN.getChildren() == null )
                System.out.print("0");
            else
                System.out.print( curGN.getChildren().size() );
             */

            //populate mResMap ,mResMapLeaves,mResMapRoots
            Job si = ( Job )curGN.getContent();
            
            //Commented out as for stage out jobs we need non third party
            //site. Karan Jan 8, 2009
//            if( !mResMap.containsKey( si.getSiteHandle() ) ){
//                mResMap.put( si.getSiteHandle(), new HashSet() );
//
//            }
//            ((Set)mResMap.get( si.getSiteHandle() )).add( curGN );
            

            String site = getSiteForCleanup( si ); 
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
            List<GraphNode> cleanupNodesPerLevel = new LinkedList();

            //process all elements in the current priority
            while( pQA[curP].size() >= 1 ){
                GraphNode curGN = (GraphNode) pQA[ curP ].iterator().next();
                pQA[ curP ].remove( curGN );
                Job curGN_SI = (Job) curGN.getContent();

                if( !typeNeedsCleanUp( curGN_SI.getJobType() ) ) { 
                    continue;
                }

                /*if( curGN_SI.getJobType() == Job.STAGE_OUT_JOB ){
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
                //create a dummy GraphNode .first create Job object and then add it to GraphNode
                GraphNode nuGN = new GraphNode( generateCleanupID( curGN_SI ),
                        curGN_SI.getTXName() );
//                                   InPlace.CLEANUP_JOB_PREFIX + curGN.getName() ,
//                                   InPlace.CLEANUP_JOB_PREFIX + curGN.getName() );

                

                List<PegasusFile> cleanupFiles = new LinkedList();
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

                        cleanupFiles.add( file );
                        
                        //Commented for PM-663
/*
                        cleanedBy.put( file.getLFN(), nuGN );

                        if( !curGN.getChildren().contains( nuGN ) ){
                            curGN.addChild( nuGN );
                        }
                        if( ! nuGN.getParents().contains( curGN ) ){
                            nuGN.addParent( curGN );
                        }
 */
                    }
                }// all the files

                //create a cleanup job if the cleanup cleanupNode has any files to delete
//                if( nuGN.getParents().size() >= 1 ){
                if( !cleanupFiles.isEmpty() ){
                    mLogger.log( "Adding stub cleanup node with ID " + nuGN.getID() + " to the level list for level " + curP,
                            LogManager.DEBUG_MESSAGE_LEVEL );

                    //PM-663, we need to store the compute job
                    //with the cleanupNode but do with a copy
                    CleanupJobContent cleanupContent = new CleanupJobContent( curGN, cleanupFiles ) ;
                    nuGN.setContent( cleanupContent );


                    cleanupNodesPerLevel.add( nuGN );

                     //Commented for PM-663
                    //moved in the outer loop
/*
                    // We have always pass the associaated compute job. Since now
                    //a cleanup job can be associated with stageout jobs also, we
                    //need to make sure that for the stageout job the cleanup job
                    //is passed. Karan Jan 9, 2008                    
//                    Job cleanupJob = mImpl.createCleanupJob( nuGN.getID(),
//                            cleanupFiles,
//                            curGN_SI
//                            );
                    Job computeJob;
                    if( typeStageOut( curGN_SI.getJobType() ) ){
                        //find a compute job that is parent of this
                        GraphNode cleanupNode = (GraphNode)curGN.getParents().get( 0 );
                        computeJob = (Job)cleanupNode.getContent();
                        message = new StringBuffer();
                        message.append( "For cleanup job " ).append( nuGN.getID() ).
                                append( " the associated compute job is ").append( computeJob.getID() );
                        mLogger.log(  message.toString(), LogManager.DEBUG_MESSAGE_LEVEL );
                    }
                    else{
                        computeJob = curGN_SI;
                    }
                    Job cleanupJob = mImpl.createCleanupJob( nuGN.getID(),
                                                                 cleanupFiles,
                                                                 computeJob
                                                                 );
                    
                    //No longer required as stageout jobs are also cleaned. Karan Jan , 2008
                    //if the corresponding compute job has any transfer or stageout jobs as child add it
                    //as a parent of the cleanup job
                    for( Iterator itc=curGN.getChildren().iterator(); itc.hasNext() ;){
                        GraphNode curGNchild=(GraphNode) itc.next();
                        Job itc_si=(Job) curGNchild.getContent();
                        if( itc_si != null )
                            if( itc_si.getJobType() == Job.STAGE_OUT_JOB ||
                                itc_si.getJobType() == Job.INTER_POOL_JOB ){

                            nuGN.addParent( curGNchild );
                            curGNchild.addChild( nuGN );
                            }
                    }

                    //add the job as a content to the graphnode
                    //and the cleanupNode itself to the Graph
                    nuGN.setContent( cleanupJob );
                    workflow.addNode(nuGN);
 */
                }

            }//end of while loop .  //process all elements in the current priority

            //we now have a list of cleanup jobs for this level
            List<GraphNode> clusteredCleanupJobs = clusterCleanupJobs( cleanupNodesPerLevel, cleanedBy , site, curP );
            //for each clustered cleanup cleanupNode , add the associated cleanup job
            for( GraphNode cleanupNode: clusteredCleanupJobs ){
                 // We have always pass the associaated compute job. Since now
                 //a cleanup job can be associated with stageout jobs also, we
                 //need to make sure that for the stageout job the cleanup job
                 //is passed. Karan Jan 9, 2008
                Job computeJob;

                CleanupJobContent cleanupJobContent = (CleanupJobContent)cleanupNode.getContent();
                GraphNode curGN = cleanupJobContent.getNode();
                Job curGN_SI = (Job)curGN.getContent();
                if( typeStageOut( curGN_SI.getJobType() ) ){
                        //find a compute job that is parent of this
                        GraphNode node = (GraphNode)curGN.getParents().get( 0 );
                        computeJob = (Job)node.getContent();
                        message = new StringBuffer();
                        message.append( "For cleanup job " ).append( cleanupNode.getID() ).
                                append( " the associated compute job is ").append( computeJob.getID() );
                        mLogger.log(  message.toString(), LogManager.DEBUG_MESSAGE_LEVEL );
                    }
                    else{
                        computeJob = curGN_SI;
                    }
                    Job cleanupJob = mImpl.createCleanupJob( cleanupNode.getID(),
                                                                 cleanupJobContent.getListOfFilesToDelete(),
                                                                 computeJob
                                                                 );

                    //No longer required as stageout jobs are also cleaned. Karan Jan , 2008
                    //if the corresponding compute job has any transfer or stageout jobs as child add it
                    //as a parent of the cleanup job
                    /*
                    for( Iterator itc=curGN.getChildren().iterator(); itc.hasNext() ;){
                        GraphNode curGNchild=(GraphNode) itc.next();
                        Job itc_si=(Job) curGNchild.getContent();
                        if( itc_si != null )
                            if( itc_si.getJobType() == Job.STAGE_OUT_JOB ||
                                itc_si.getJobType() == Job.INTER_POOL_JOB ){

                            nuGN.addParent( curGNchild );
                            curGNchild.addChild( nuGN );
                            }
                    }*/

                    //add the job as a content to the graphnode
                    //and the cleanupNode itself to the Graph
                    cleanupNode.setContent( cleanupJob );
                    workflow.addNode(cleanupNode);
            }

        }//end of for loop

        //output whats file is cleaned by what ?
        mLogger.log( "", LogManager.DEBUG_MESSAGE_LEVEL );
        mLogger.log( "CLEANUP LIST",LogManager.DEBUG_MESSAGE_LEVEL);
        for( Iterator it = cleanedBy.keySet().iterator() ; it.hasNext() ;){
            String lfn = (String)it.next();
            GraphNode cl_GN = (GraphNode)cleanedBy.get(lfn);
            Job cl_si = (Job)cl_GN.getContent();
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
            //Job cl_si=(Job)cl_GN.getContent();
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
            Job job = ( Job )node.getContent();

            //only apply priority if job is not associated with a priority
            //beforehand
            if( !job.condorVariables.containsKey( Condor.PRIORITY_KEY ) ){

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
//            if( job.getJobType() == Job.COMPUTE_JOB ||
//                job.getJobType() == Job.STAGED_COMPUTE_JOB ){
//                job.globusRSL.construct( "condorsubmit",
//                                         "(priority " + cleanupNode.getDepth() + ")");
//            }
            }
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
    protected String generateCleanupID( Job job ){
        StringBuffer sb = new StringBuffer();
        sb.append( InPlace.CLEANUP_JOB_PREFIX ).append( job.getID() );
        return sb.toString();
    }

    /**
     * Generated an ID for a clustered cleanup job
     *
     *
     * @param site       the site associated with the cleanup jobs
     * @param level    the level of the workflow
     * @param index    the index of the job on that level
     *
     * @return
     */
    public String generateClusteredJobID( String site, int level, int index ){
        StringBuffer sb = new StringBuffer();

        sb.append( InPlace.CLEANUP_JOB_PREFIX ).append( site ).append( "_").
           append( "level" ).append( "_" ).append( level ).
           append( "_" ).append( index );

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
        return (   type == Job.COMPUTE_JOB
                || type == Job.STAGE_OUT_JOB
                || type == Job.INTER_POOL_JOB
                /*|| type == Job.STAGED_COMPUTE_JOB*/ );
    }

    
    /**
     * Checks to see if job type is a stageout job type.
     *
     * @param type  the type of the job.
     *
     * @return boolean
     */
    protected boolean typeStageOut( int type ){
        return (   type == Job.STAGE_OUT_JOB
                || type == Job.INTER_POOL_JOB
                );
    }

    /**
     * Returns site to be used for the cleanup algorithm.
     * For compute jobs the staging site is used, while for stageout jobs 
     *   is used.
     * 
     * For all other jobs the execution site is used.
     * 
     * @param job   the job
     * 
     * @return the site to be used
     */
    protected String getSiteForCleanup( Job job ) {
        /*
        String site =  typeStageOut( job.getJobType() )?
                             ((TransferJob)job).getNonThirdPartySite():
                             job.getStagingSiteHandle();
         */

        String site = null;

        if( typeStageOut( job.getJobType() )){
            //for stage out jobs we prefer the non third party site
            site = ((TransferJob)job).getNonThirdPartySite();
        }
        else if ( job.getJobType() == Job.COMPUTE_JOB ){
            //for compute jobs we refer to the staging site
            site = job.getStagingSiteHandle();
        }
        else{
            //for all other jobs we use the execution site
            site = job.getSiteHandle();
        }
        return site;
    }
    
     /**
     * Returns the property key that can be used to set the max jobs for the
     * default category associated with the registration jobs.
     * 
     * @return the property key
     */
    public String getDefaultCleanupMaxJobsPropertyKey(){
        StringBuffer key = new StringBuffer();
        
        key.append( Dagman.NAMESPACE_NAME ).append( "." ).
            append( CleanupImplementation.DEFAULT_CLEANUP_CATEGORY_KEY ).
            append( "." ).append( Dagman.MAXJOBS_KEY.toLowerCase() );
        
        return key.toString();
    }

    /**
     * Takes in a list of cleanup nodes ,one per cleanupNode(compute/stageout job)
     * whose files need to be deleted) and clusters them into a smaller set
     * of cleanup nodes.
     *
     * @param cleanupNodes  List of  stub cleanup nodes created corresponding to a job
     *                      in the workflow that needs cleanup. the cleanup jobs
     *                      have content as a CleanupJobContent
     *
     * @param cleanedBy  a map that tracks which file was deleted by which cleanup
     *                   job
     * @param site       the site associated with the cleanup jobs
     * @param level      the level of the workflow
     *
     * @return
     */
    private List<GraphNode> clusterCleanupJobs(List<GraphNode> cleanupNodes, HashMap cleanedBy, String site, int level ) {
        List<GraphNode> clusteredCleanupJobs = new LinkedList();

        //sanity check for empty list
        int size = cleanupNodes.size();
        if( size == 0 ){
            return clusteredCleanupJobs;
        }

        //cluster size is how many nodes are clustered into one cleanup cleanupNode
        //it is the ceiling ( x + y -1 )/y
        int clusterSize = ( size + mCleanupJobsPerLevel -1  )/mCleanupJobsPerLevel;


        StringBuffer sb = new StringBuffer();
        sb.append( "Clustering " ).append(  size ).append( " cleanup nodes at level " ).append(  level ).
           append( " with cluster size ").append( clusterSize);
        mLogger.log( sb.toString() , LogManager.DEBUG_MESSAGE_LEVEL );

        //for the time being lets assume one to one mapping
        Iterator<GraphNode> it = cleanupNodes.iterator();
        int counter = 0;
        while( it.hasNext() ){
            List<GraphNode> clusteredConstitutents = new LinkedList();
            for( int i = 1; i <= clusterSize && it.hasNext(); i++ ){
                GraphNode n = it.next();
                clusteredConstitutents.add( n );
            }

            //we have our constituents. create a cleanup job out of this
            GraphNode clusteredCleanupJob = createClusteredCleanupJob( clusteredConstitutents, cleanedBy, site, level, counter );
            clusteredCleanupJobs.add(clusteredCleanupJob);
            counter++;
        }

/*
        for( Iterator<GraphNode> it = cleanupNodes.iterator(); it.hasNext();  ){
            GraphNode cleanupNode = it.next();
            CleanupJobContent content = (CleanupJobContent) cleanupNode.getContent();
            List<PegasusFile> filesToDelete = content.getListOfFilesToDelete();

            GraphNode cleanupNode = cleanupNode; //same for time being
            GraphNode computeNode = content.getNode();
            //files to delete remains the same

            for( PegasusFile file : filesToDelete ){
                cleanedBy.put( file.getLFN(), cleanupNode );
            }

            //add dependencies between the nodes accordingly
                        if( !computeNode.getChildren().contains( cleanupNode ) ){
                            computeNode.addChild( cleanupNode );
                        }
                        if( ! cleanupNode.getParents().contains( computeNode ) ){
                            cleanupNode.addParent( computeNode );
                        }



            clusteredCleanupJobs.add( cleanupNode );
        }
*/
        return clusteredCleanupJobs;

    }


    /**
     * Creates a clustered cleanup graph node that aggregates multiple cleanup nodes
     * into one node
     *
     * @param nodes      list of cleanup nodes that are to be aggregated
     * @param cleanedBy  a map that tracks which file was deleted by which cleanup
     *                   job
     * @param site       the site associated with the cleanup jobs
     * @param level      the level of the workflow
     * @param index      the index of the cleanup job for that level
     * @return
     */
    private GraphNode createClusteredCleanupJob(List<GraphNode> nodes, HashMap cleanedBy, String site, int level, int index ) {
        GraphNode clusteredCleanupNode = new GraphNode( generateClusteredJobID( site, level, index ) );


        //sanity check
        if( nodes.isEmpty() ){
            throw new RuntimeException( "Logic Error in the InPlace Cleanup Algorithm for level " + level + " " + index );
        }

        //add some info
        StringBuffer sb = new StringBuffer();
        sb.append( "\tCreating a clustered cleanup job named " ).append( clusteredCleanupNode.getID() ).
           append( " consisting of " ).append( nodes.size() ).append( " nodes ");
        mLogger.log( sb.toString() , LogManager.DEBUG_MESSAGE_LEVEL );

        //the list of files to be deleted by the clustered cleanup job
        List<PegasusFile> allFilesToDelete = new LinkedList();

        //for each cleanup Node add the files and modify dependencies accordingly
        GraphNode primaryNode = null; //the primary compute node associated with the cleanup job
        for( GraphNode cleanupNode : nodes ){
            CleanupJobContent content = (CleanupJobContent) cleanupNode.getContent();
            List<PegasusFile> filesToDelete = content.getListOfFilesToDelete();
            primaryNode = content.getNode();


            for( PegasusFile file : filesToDelete ){
                cleanedBy.put( file.getLFN(), clusteredCleanupNode );
            }

            allFilesToDelete.addAll( filesToDelete );

            //add dependencies between the compute/stageout node and the clustered cleanup node
            if( !primaryNode.getChildren().contains( clusteredCleanupNode ) ){
                primaryNode.addChild( clusteredCleanupNode );
            }
            if( ! clusteredCleanupNode.getParents().contains( primaryNode ) ){
                clusteredCleanupNode.addParent( primaryNode );
            }
        }

        clusteredCleanupNode.setContent( new CleanupJobContent( primaryNode, allFilesToDelete) );

        return clusteredCleanupNode;
    }





}
/**
 * A container class that is used to hold the contents for a cleanup job
 * 
 * 
 * @author vahi
 */
class CleanupJobContent implements GraphNodeContent{


    /**
     * The graph cleanupNode object for the associated job whose files are being deleted.
     * can be a compute or a stageout job.
     */
    private GraphNode mNode;

    /**
     * The list of files that need to be deleted and are associated with this job.
     */
    private List<PegasusFile> mToBeDeletedFiles;

    /**
     *
     * @param cleanupNode
     * @param files
     */
    public CleanupJobContent( GraphNode node, List<PegasusFile> files ){
        mNode = node;
        mToBeDeletedFiles = files;
    }

    /**
     * Returns the list of files to be deleted for a cleanupNode
     *
     * @return
     */
    public List<PegasusFile> getListOfFilesToDelete() {
        return this.mToBeDeletedFiles;
    }

    /**
     * Returns the associated cleanupNode for which the files are deleted.
     * @return
     */
    public GraphNode getNode() {
        return this.mNode;
    }

}