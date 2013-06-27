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
     * roots in the workflow mapping to siteHandle(String) to Set<GraphNodes>.
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
     * the number of cleanup jobs clustered into a clustered cleanup job
     */
    private int mCleanupJobsSize;

    /**
     * A boolean indicating whether we prefer use the size factor or the num
     * factor
     */
    private boolean mUseSizeFactor;
    
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

        mUseSizeFactor = false;

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

        mCleanupJobsPerLevel = -1;
        String propValue = mProps.getMaximumCleanupJobsPerLevel();
        int value = -1;
        try{
            value = Integer.parseInt( propValue );
        }
        catch( Exception e ){
            //ignore
        }

        if( value > 0 ){
            //user has specified a value for the clustered cleanup
            mCleanupJobsPerLevel = value;
        }
        else{
            //check if a user has
             propValue = mProps.getClusterSizeCleanupJobsPerLevel();
             int clusterSize = -1;
             try{
                clusterSize = Integer.parseInt( propValue );
             }
             catch( Exception e ){
                //ignore
             }
             if( clusterSize > 0 ){
                 //set the algorithm to use it
                 mUseSizeFactor   = true;
                 mCleanupJobsSize = clusterSize;
                 mLogger.log( "Cluster Size of cleanup jobs  " + mCleanupJobsSize,
                              LogManager.CONFIG_MESSAGE_LEVEL );
             }
             else{
                 //we rely on a default value for the clustered cleanup jobs
                 mCleanupJobsPerLevel = DEFAULT_CLUSTERED_CLEANUP_JOBS_PER_LEVEL;
             }
        }
        if( !mUseSizeFactor ){
            //log a config message for the number of cleanup jobs
            mLogger.log( "Maximum number of cleanup jobs to be created per level " + mCleanupJobsPerLevel,
                     LogManager.CONFIG_MESSAGE_LEVEL );
        }

       
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
     * a breadth first search strategy is implemented based on the depth of the job
     * in the workflow
     *
     * @param site the site ID
     * @param leaves the leaf jobs that are scheduled to site
     * @param workflow the Graph into which new cleanup jobs can be added
     */
    private void addCleanUpJobs( String site, Set leaves, Graph workflow ){

        mLogger.log(  site + " " + leaves.size() , LogManager.DEBUG_MESSAGE_LEVEL );
        HashMap cleanedBy = new HashMap();

        //the below in case we get rid of the primitive java 1.4
        //PriorityQueue<GraphNode> pQ=new   PriorityQueue<GraphNode>(resMap.get(site).size(),GraphNode_ORDER);

        StringBuffer message = new StringBuffer();
        message.append( "Leaf  jobs scheduled at site ").append( site )
        .append( " are " );
        for( Iterator it = leaves.iterator(); it.hasNext(); ){
            message.append( ((GraphNode)it.next()).getID() );
            message.append( "," );
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
        
        List<GraphNode> wfCleanupNodes = new LinkedList();//stores all the cleanup nodes added for the workflow
        //start the breadth first cleanup job addition
        for( int curP = mMaxDepth; curP >= 0 ; curP-- ){
            List<GraphNode> cleanupNodesPerLevel = new LinkedList();

            //process all elements in the current priority
            while( pQA[curP].size() >= 1 ){
                GraphNode curGN = (GraphNode) pQA[ curP ].iterator().next();
                pQA[ curP ].remove( curGN );
                Job curGN_SI = (Job) curGN.getContent();

                if( !typeNeedsCleanUp( curGN ) ) { 
                      continue;
                }

                    
//              Leads to corruption of input files for the job.
//                Set fileSet = curGN_SI.getInputFiles();
                Set<PegasusFile> fileSet = new HashSet( curGN_SI.getInputFiles() );
                
                //PM-698 traverse through the input files and unset those
                //that have cleanup flag set to false
                for( Iterator<PegasusFile> it = fileSet.iterator(); it.hasNext(); ){
                    PegasusFile pf = it.next();
                    if( !pf.canBeCleanedup() ){
                        it.remove();
                        mLogger.log( "File " + pf.getLFN() + " will not be cleaned up for job " + curGN_SI.getID() ,
                                     LogManager.DEBUG_MESSAGE_LEVEL );
                    }
                }
                
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
                }

            }//end of while loop .  //process all elements in the current priority

            //we now have a list of cleanup jobs for this level
            List<GraphNode> clusteredCleanupGraphNodes = clusterCleanupGraphNodes( cleanupNodesPerLevel, cleanedBy , site, curP );
            //for each clustered cleanup cleanupNode , add the associated cleanup job
            for( GraphNode cleanupNode: clusteredCleanupGraphNodes ){
                 // We have always pass the associated compute job. Since now
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


                //add the job as a content to the graphnode
                //and the cleanupNode itself to the Graph
                cleanupNode.setContent( cleanupJob );
                workflow.addNode(cleanupNode);
                //add all the cleanup node to wf wide list to 
                //use it for reducing the dependencies
                wfCleanupNodes.add( cleanupNode );
            }

        }//end of for loop

        //output whats file is cleaned by what ?
        mLogger.log( "", LogManager.DEBUG_MESSAGE_LEVEL );
        mLogger.log( "For site: " + site + " number of files cleaned up - " + cleanedBy.keySet().size() ,
                     LogManager.INFO_MESSAGE_LEVEL);
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
        for( GraphNode cleanupNode: wfCleanupNodes ){
            mLogger.log( "Reducing edges for the cleanup node " + cleanupNode.getID(), 
                         mLogger.DEBUG_MESSAGE_LEVEL);
            reduceDependency( cleanupNode );
        }

    }

    /**
     * Reduces the number of edges between the nodes and it's parents.
     * 
     * <pre>
     * For the node look at the parents of the Node. 
     * For each parent Y see if there is a path to any other parent Z of X.
     * If a path exists, then the edge from Z to node can be removed.
     * </pre>
     * 
     * @param node the nodes whose parent edges need to be reduced.
     */
    protected void reduceDependency( GraphNode node ){
        if( node == null ){
            return;
        }
        
        //reduce dependencies. for each cleanup job X, look at the parents of
        //the job. For each parent Y see if there is a path to any other parent Z of X.
        //If a path exists, then the edge from Z to cleanup job can
        //be removed.
        
        List parents = node.getParents();
        List redundant = new LinkedList();
        HashSet visit = new HashSet();
        for( Iterator itp = node.getParents().iterator() ; itp.hasNext() ;){
            LinkedList mque = new LinkedList();
            mque.add( itp.next() );

            while( mque.size() > 0 ){
                GraphNode popGN = (GraphNode) mque.removeFirst();

                if( visit.contains(popGN) ) { continue; }

                visit.add(popGN);

                for( Iterator itp_pop = popGN.getParents().iterator() ; itp_pop.hasNext() ;){
                    GraphNode pop_pGN = (GraphNode) itp_pop.next();
                    //check if its redundant ..if so add it to redundant list
                    if( parents.contains( pop_pGN ) ){
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
            node.removeParent( r_GN );
            r_GN.removeChild( node );
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
     * @param node  the graph node
     *
     * @return boolean
     */
    protected boolean typeNeedsCleanUp( GraphNode node ){
        Job job = (Job) node.getContent();
        int type = job.getJobType();
        boolean cleanup = false;
        
        if( type == Job.COMPUTE_JOB ){
            cleanup = true;
        }
        else if( this.typeStageOut(type) ){
            //for stage-out jobs we need extra checks
            //PM-699 check for stageout jobs with no parents
            if( node.getParents().size() > 0 ){
                cleanup = true;
            }
            else{
                mLogger.log( "Disabling cleanup for stageout job " + node.getID() , 
                              LogManager.INFO_MESSAGE_LEVEL );
            }
        }
        
        return cleanup;
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
     * @return a set of clustered cleanup nodes
     */
    private List<GraphNode> clusterCleanupGraphNodes(List<GraphNode> cleanupNodes, HashMap cleanedBy, String site, int level ) {
        List<GraphNode> clusteredCleanupJobs = new LinkedList();

        //sanity check for empty list
        int size = cleanupNodes.size();
        if( size == 0 ){
            return clusteredCleanupJobs;
        }

        //cluster size is how many nodes are clustered into one cleanup cleanupNode
        int clusterSize = getClusterSize( size );

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

            //we have our constituents. create a cleanup node out of this
            GraphNode clusteredCleanupGraphNode = createClusteredCleanupGraphNode( clusteredConstitutents, cleanedBy, site, level, counter );
            if( clusteredCleanupGraphNode != null ){
                //we only add and increment counter only if the cleanup node
                //is deleting at least one file.
                clusteredCleanupJobs.add(clusteredCleanupGraphNode);
                counter++;
            }
        }

/* if we don't want any clustering to happen
 * then use this and delete the rest of the function
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



            clusteredCleanupGraphNodes.add( cleanupNode );
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
     *
     * @return a clustered cleanup node with the appropriate linkages added to the workflow
     *         else, null if the clustered cleanup node has no files to delete
     */
    private GraphNode createClusteredCleanupGraphNode(List<GraphNode> nodes, HashMap cleanedBy, String site, int level, int index ) {
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
                if( cleanedBy.containsKey( file.getLFN()) ){
                    //somewhere during the clustering of the cleanup nodes at this
                    //level, the file was designated to cleaned up by a
                    //clustered cleanup node
                    GraphNode existingCleanupNode = (GraphNode) cleanedBy.get( file.getLFN() );
                    mLogger.log( "\t\tFile " + file.getLFN() + " already cleaned by clustered cleanup node " + existingCleanupNode.getID(),
                                 LogManager.DEBUG_MESSAGE_LEVEL );

                    if( !existingCleanupNode.getParents().contains( primaryNode ) ){
                        existingCleanupNode.addParent( primaryNode );
                     }
                     if( !primaryNode.getChildren().contains( existingCleanupNode ) ){
                        primaryNode.addChild( existingCleanupNode );
                      }
                }
                else{
                    cleanedBy.put( file.getLFN(), clusteredCleanupNode );
                    allFilesToDelete.add( file );
                }
            }

            //allFilesToDelete.addAll( filesToDelete );

            if( !allFilesToDelete.isEmpty() ){
                //add dependencies between the compute/stageout node and the clustered cleanup node
                //as long as we know that we are creating a clustered cleanup job that is not empty
                if( !primaryNode.getChildren().contains( clusteredCleanupNode ) ){
                    primaryNode.addChild( clusteredCleanupNode );
                }
                if( ! clusteredCleanupNode.getParents().contains( primaryNode ) ){
                    clusteredCleanupNode.addParent( primaryNode );
                }
            }
        }
        
        if( allFilesToDelete.isEmpty() ){
            //the clustered cleanup job we are trying to create has
            //no files to delete
            mLogger.log( "\t\tClustered cleanup node is empty " + clusteredCleanupNode.getID(),
                                 LogManager.DEBUG_MESSAGE_LEVEL );
            return null;
        }

        clusteredCleanupNode.setContent( new CleanupJobContent( primaryNode, allFilesToDelete) );

        return clusteredCleanupNode;
    }

    /**
     * Returns the number of cleanup jobs clustered into one job per level.
     *
     * 
     * @param size   the number of cleanup jobs created by the algorithm before clustering
     *               for the level.
     *
     * @return the number of clustered cleanup jobs to be created for the level
     */
    private int getClusterSize(int size) {

        int result = 1;

        if( this.mUseSizeFactor ){
            return this.mCleanupJobsSize;
            
        }
        else{
            //it is the ceiling ( x + y -1 )/y
            //we use the fixed number of cleanup jobs per level
            result = ( size + mCleanupJobsPerLevel -1  )/mCleanupJobsPerLevel;
        }

        return result ;
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