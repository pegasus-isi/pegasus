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


package org.griphyn.cPlanner.engine;

import java.util.Iterator;
import java.util.Set;
import java.util.Vector;

import org.griphyn.cPlanner.classes.ADag;
import org.griphyn.cPlanner.classes.PegasusBag;

import org.griphyn.cPlanner.common.LogManager;


/**
 * The central class that calls out to the various other components of Pegasus.
 *
 * @author Karan Vahi
 * @author Gaurang Mehta
 * @version $Revision$
 *
 * @see org.griphyn.cPlanner.classes.ReplicaLocations
 */
public class MainEngine
    extends Engine {

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
     * The pool on which all the output data should be transferred.
     */
    private String mOutputPool;

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
    private ReductionEngine mRedEng;

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
        //do the authentication against the pools
        if (mPOptions.authenticationSet()) {
            mAuthEng = new AuthenticateEngine( mBag,
                          new java.util.HashSet(mPOptions.getExecutionSites()));

            mLogger.log("Authenticating Sites", LogManager.INFO_MESSAGE_LEVEL);
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
            mLogger.logCompletion("Authenticating Sites",
                                  LogManager.INFO_MESSAGE_LEVEL);
            mPOptions.setExecutionSites(authenticatedSet);
        }

        Vector vDelLeafJobs = new Vector();
        String message = null;
        mRCBridge = new ReplicaCatalogBridge( mOriginalDag, mBag );


        mRedEng = new ReductionEngine( mOriginalDag, mBag );
        mReducedDag = mRedEng.reduceDag( mRCBridge );
        vDelLeafJobs = mRedEng.getDeletedLeafJobs();
        mRedEng = null;

        //unmark arg strings
        //unmarkArgs();
        message = "Doing site selection" ;
        mLogger.log(message, LogManager.INFO_MESSAGE_LEVEL);
        mIPEng = new InterPoolEngine( mReducedDag, mBag );
        mIPEng.determineSites();
        mBag = mIPEng.getPegasusBag();
        mIPEng = null;
        mLogger.logCompletion(message,LogManager.INFO_MESSAGE_LEVEL);

        //intialize the deployment engine
        DeployWorkerPackage deploy = DeployWorkerPackage.loadDeployWorkerPackage( mBag );
        deploy.initialize( mReducedDag );

        //do the node cluster
        if( mPOptions.getClusteringTechnique() != null ){
            message = "Clustering the jobs in the workflow";
            mLogger.log(message,LogManager.INFO_MESSAGE_LEVEL);
            mNodeCollapser = new NodeCollapser( mBag );

            try{
                mReducedDag = mNodeCollapser.cluster( mReducedDag );
            }
            catch ( Exception e ){
                throw new RuntimeException( message, e );
            }

            mNodeCollapser = null;
            mLogger.logCompletion(message,LogManager.INFO_MESSAGE_LEVEL);
        }


        message = "Grafting transfer nodes in the workflow";
        mLogger.log(message,LogManager.INFO_MESSAGE_LEVEL);
        mTransEng = new TransferEngine( mReducedDag, vDelLeafJobs, mBag );
        mTransEng.addTransferNodes( mRCBridge );
        mTransEng = null;
        mLogger.logCompletion(message,LogManager.INFO_MESSAGE_LEVEL);

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
            mLogger.log(message,LogManager.INFO_MESSAGE_LEVEL);
            mCreateEng = new CreateDirectory( mBag );
            mCreateEng.addCreateDirectoryNodes( mReducedDag );
            mCreateEng = null;
            mLogger.logCompletion(message,LogManager.INFO_MESSAGE_LEVEL);

            //create the cleanup dag
            message = "Generating the cleanup workflow";
            mLogger.log(message,LogManager.INFO_MESSAGE_LEVEL);
            mRemoveEng = new RemoveDirectory( mReducedDag, mBag );
            mCleanupDag = mRemoveEng.generateCleanUPDAG();
            mLogger.logCompletion(message,LogManager.INFO_MESSAGE_LEVEL);
        }

        //add the cleanup nodes in place
        if ( mPOptions.getCleanup() ){ /* should be exposed via command line option */
            message = "Adding cleanup jobs in the workflow";
            mLogger.log( message, LogManager.INFO_MESSAGE_LEVEL );
            CleanupEngine cEngine = new CleanupEngine( mBag );
            mReducedDag = cEngine.addCleanupJobs( mReducedDag );
            mLogger.logCompletion( message, LogManager.INFO_MESSAGE_LEVEL );
            
            //add the cleanup of setup jobs if required
            mReducedDag = deploy.addCleanupNodesForWorkerPackage( mReducedDag );
        }

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
