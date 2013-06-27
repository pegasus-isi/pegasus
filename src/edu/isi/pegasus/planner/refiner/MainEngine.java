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


package edu.isi.pegasus.planner.refiner;

import edu.isi.pegasus.common.logging.LoggingKeys;
import java.util.Iterator;
import java.util.Set;

import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.PegasusBag;

import edu.isi.pegasus.common.logging.LogManager;
import java.io.File;
import java.util.Properties;
import edu.isi.pegasus.planner.catalog.ReplicaCatalog;
import edu.isi.pegasus.planner.catalog.replica.ReplicaFactory;
import edu.isi.pegasus.planner.catalog.replica.impl.SimpleFile;
import edu.isi.pegasus.planner.classes.PlannerCache;


/**
 * The central class that calls out to the various other components of Pegasus.
 *
 * @author Karan Vahi
 * @author Gaurang Mehta
 * @version $Revision$
 */
public class MainEngine
    extends Engine {

    /**
     * The basename of the directory that contains the submit files for the
     * cleanup DAG that for the concrete dag generated for the workflow.
     */
    public static final String CLEANUP_DIR  = "cleanup";

   
    /**
     * The Original Dag object which is constructed by parsing the dag file.
     */
    private ADag mOriginalDag;

    /**
     * The reduced Dag object which is got from the Reduction Engine.
     */
    private ADag mReducedDag;

    /**
     * The cleanup dag for the final concrete dag.
     */
    private ADag mCleanupDag;

    /**
     * The pools on which the Dag should be executed as specified by the user.
     */
    private Set mExecPools;

    

    /**
     * The bridge to the Replica Catalog.
     */
    private ReplicaCatalogBridge mRCBridge;

    /**
     * The handle to the InterPool Engine that calls out to the Site Selector
     * and maps the jobs.
     */
    private InterPoolEngine mIPEng;

    /**
     * The handle to the Reduction Engine that performs reduction on the graph.
     */
    private DataReuseEngine mRedEng;

    /**
     * The handle to the Transfer Engine that adds the transfer nodes in the
     * graph to transfer the files from one site to another.
     */
    private TransferEngine mTransEng;

    /**
     * The engine that ends up creating random directories in the remote
     * execution pools.
     */
    private CreateDirectory mCreateEng;

    /**
     * The engine that ends up creating the cleanup dag for the dag.
     */
    private RemoveDirectory mRemoveEng;

    /**
     * The handle to the Authentication Engine that performs the authentication
     * with the various sites.
     */
    private AuthenticateEngine mAuthEng;

    /**
     * The handle to the node collapser.
     */
    private NodeCollapser mNodeCollapser;

    

    /**
     * This constructor initialises the class variables to the variables
     * passed. The pool names specified should be present in the pool.config file
     *
     * @param orgDag    the dag to be worked on.
     * @param bag       the bag of initialization objects
     */

    public MainEngine( ADag orgDag, PegasusBag bag ) {

        super( bag );
        mOriginalDag = orgDag;
        mExecPools = (Set)mPOptions.getExecutionSites();
        mOutputPool = mPOptions.getOutputSite();

        if (mOutputPool != null && mOutputPool.length() > 0) {
            Engine.mOutputPool = mOutputPool;
        }

    }

    /**
     * The main function which calls the other engines and does the necessary work.
     *
     * @return the planned worflow.
     */
    public ADag runPlanner() {
        String abstractWFName = mOriginalDag.getAbstractWorkflowName();
        //create the main event refinement event
        mLogger.logEventStart( LoggingKeys.EVENT_PEGASUS_REFINEMENT,
                               LoggingKeys.DAX_ID, 
                               abstractWFName );
        
        //refinement process starting
        mOriginalDag.setWorkflowRefinementStarted( true );
        
        //do the authentication against the pools
        if (mPOptions.authenticationSet()) {
            mAuthEng = new AuthenticateEngine( mBag,
                          new java.util.HashSet(mPOptions.getExecutionSites()));

            mLogger.logEventStart( LoggingKeys.EVENT_PEGASUS_AUTHENTICATION, LoggingKeys.DAX_ID, mOriginalDag.getAbstractWorkflowName() );
            Set authenticatedSet = mAuthEng.authenticate();
            if (authenticatedSet.isEmpty()) {
                StringBuffer error = new StringBuffer( );
                error.append( "Unable to authenticate against any site. ").
                      append( "Probably your credentials were not generated" ).
                      append( " or have expired" );
                throw new RuntimeException( error.toString() );
            }
            mLogger.log("Sites authenticated are " +
                        setToString(authenticatedSet, ","),
                        LogManager.DEBUG_MESSAGE_LEVEL);
            mLogger.logEventCompletion();
            mPOptions.setExecutionSites(authenticatedSet);
        }

        String message = null;
        mRCBridge = new ReplicaCatalogBridge( mOriginalDag, mBag );

        //lock down on the workflow task metrics
        //the refinement process will not update them
        mOriginalDag.getWorkflowMetrics().lockTaskMetrics( true );

        mRedEng     = new DataReuseEngine( mOriginalDag, mBag );
        mReducedDag = mRedEng.reduceWorkflow(mOriginalDag, mRCBridge );

        //unmark arg strings
        //unmarkArgs();
        mOriginalDag = null;

        mLogger.logEventStart( LoggingKeys.EVENT_PEGASUS_SITESELECTION, LoggingKeys.DAX_ID, abstractWFName );
        mIPEng = new InterPoolEngine( mReducedDag, mBag );
        mIPEng.determineSites();
        mBag = mIPEng.getPegasusBag();
        mIPEng = null;
        mLogger.logEventCompletion();

        //intialize the deployment engine
        DeployWorkerPackage deploy = DeployWorkerPackage.loadDeployWorkerPackage( mBag );
        deploy.initialize( mReducedDag );

        //do the node cluster
        if( mPOptions.getClusteringTechnique() != null ){
            mLogger.logEventStart( LoggingKeys.EVENT_PEGASUS_CLUSTER, LoggingKeys.DAX_ID, abstractWFName );
            mNodeCollapser = new NodeCollapser( mBag );

            try{
                mReducedDag = mNodeCollapser.cluster( mReducedDag );
            }
            catch ( Exception e ){
                throw new RuntimeException( message, e );
            }

            mNodeCollapser = null;
            mLogger.logEventCompletion();
        }


        message = "Grafting transfer nodes in the workflow";
        PlannerCache plannerCache  = new PlannerCache();
        plannerCache.initialize(mBag, mReducedDag);

        mLogger.log(message,LogManager.INFO_MESSAGE_LEVEL);
        mLogger.logEventStart( LoggingKeys.EVENT_PEGASUS_ADD_TRANSFER_NODES, LoggingKeys.DAX_ID, abstractWFName );
        mTransEng = new TransferEngine( mReducedDag, 
                                        mBag,
                                        mRedEng.getDeletedJobs(),
                                        mRedEng.getDeletedLeafJobs());
        mTransEng.addTransferNodes( mRCBridge , plannerCache );
        mTransEng = null;
        mRedEng = null;
        mLogger.logEventCompletion();
        
        //populate the transient RC into PegasusBag
        mBag.add( PegasusBag.PLANNER_CACHE, plannerCache );
        
        //close the connection to RLI explicitly
        mRCBridge.closeConnection();

        //add the deployment of setup jobs if required
        mReducedDag = deploy.addSetupNodes( mReducedDag );


        if (mPOptions.generateRandomDirectory()) {
            //add the nodes to that create
            //random directories at the remote
            //execution pools.
            message = "Grafting the remote workdirectory creation jobs " +
                        "in the workflow";
            //mLogger.log(message,LogManager.INFO_MESSAGE_LEVEL);
            mLogger.logEventStart( LoggingKeys.EVENT_PEGASUS_GENERATE_WORKDIR, LoggingKeys.DAX_ID, abstractWFName );
            mCreateEng = new CreateDirectory( mBag );
            mCreateEng.addCreateDirectoryNodes( mReducedDag );
            mCreateEng = null;
            mLogger.logEventCompletion();
            
// CLEANUP WORKFLOW GENERATION IS DISABLED FOR 3.2
// JIRA PM-529
//            //create the cleanup dag
//            message = "Generating the cleanup workflow";
//            //mLogger.log(message,LogManager.INFO_MESSAGE_LEVEL);
//            mLogger.logEventStart( LoggingKeys.EVENT_PEGASUS_GENERATE_CLEANUP_WF, LoggingKeys.DAX_ID, mOriginalDag.getAbstractWorkflowName() );
//            //for the cleanup dag the submit directory is the cleanup
//            //subdir
//            File submitDir = new File( this.mPOptions.getSubmitDirectory(), MainEngine.CLEANUP_DIR );
//            mRemoveEng = new RemoveDirectory( mReducedDag, mBag, submitDir.getAbsolutePath() );
//            mCleanupDag = mRemoveEng.generateCleanUPDAG( );
//            mLogger.logEventCompletion();
// END OF COMMENTED OUT CODE
        }

        //add the cleanup nodes in place
        if ( mPOptions.getCleanup() ){ /* should be exposed via command line option */
            message = "Adding cleanup jobs in the workflow";
           // mLogger.log( message, LogManager.INFO_MESSAGE_LEVEL );
            mLogger.logEventStart( LoggingKeys.EVENT_PEGASUS_GENERATE_CLEANUP, LoggingKeys.DAX_ID, abstractWFName );
            CleanupEngine cEngine = new CleanupEngine( mBag );
            mReducedDag = cEngine.addCleanupJobs( mReducedDag );
            mLogger.logEventCompletion();
            
            //for the non pegasus lite case we add the cleanup nodes
            //for the worker package.
            if( !mProps.executeOnWorkerNode() ){
                 //add the cleanup of setup jobs if required
                mReducedDag = deploy.addCleanupNodesForWorkerPackage( mReducedDag );
            }
        }

        mLogger.logEventCompletion();
        return mReducedDag;
    }

    /**
     * Returns the cleanup dag for the concrete dag.
     *
     * @return the cleanup dag if the random dir is given.
     *         null otherwise.
     */
    public ADag getCleanupDAG(){
        return mCleanupDag;
    }

    /**
     * Returns the bag of intialization objects.
     *
     * @return PegasusBag
     */
    public PegasusBag getPegasusBag(){
        return mBag;
    }

  

    
    /**
     * Unmarks the arguments , that are tagged in the DaxParser. At present there are
     * no tagging.
     *
     * @deprecated
     */
    private void unmarkArgs() {
        /*Enumeration e = mReducedDag.vJobSubInfos.elements();
                 while(e.hasMoreElements()){
            SubInfo sub = (SubInfo)e.nextElement();
            sub.strargs = new String(removeMarkups(sub.strargs));
                 }*/
    }

    /**
     * A small helper method that displays the contents of a Set in a String.
     *
     * @param s      the Set whose contents need to be displayed
     * @param delim  The delimited between the members of the set.
     * @return  String
     */
    public String setToString(Set s, String delim) {
        StringBuffer sb = new StringBuffer();
        for( Iterator it = s.iterator(); it.hasNext(); ) {
            sb.append( (String) it.next() ).append( delim );
        }
        String result = sb.toString();
        result = (result.length() > 0) ?
                 result.substring(0, result.lastIndexOf(delim)) :
                 result;
        return result;
    }

}
