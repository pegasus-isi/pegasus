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


package org.griphyn.cPlanner.transfer.refiner;


import org.griphyn.cPlanner.classes.ADag;
import org.griphyn.cPlanner.classes.SubInfo;
import org.griphyn.cPlanner.classes.PegasusFile;
import org.griphyn.cPlanner.classes.FileTransfer;
import org.griphyn.cPlanner.classes.NameValue;
import org.griphyn.cPlanner.classes.PegasusBag;

import edu.isi.pegasus.common.logging.LogManager;

import org.griphyn.cPlanner.engine.ReplicaCatalogBridge;
import org.griphyn.cPlanner.engine.createdir.WindwardImplementation;

import org.griphyn.cPlanner.transfer.MultipleFTPerXFERJobRefiner;

import java.io.File;

import java.util.Collection;
import java.util.Iterator;
import java.util.Properties;

/**
 * A composite transfer refiner.
 * 
 * @author Karan Vahi
 * @version $Revision$
 */

public class Windward extends MultipleFTPerXFERJobRefiner {

    /**
     * A short description of the transfer refinement.
     */
    public static final String DESCRIPTION =
                      "Windward Transfer Refiner";


    /**
     * The bundle refiner.
     */
    private MultipleFTPerXFERJobRefiner mBundleRefiner;
    
    /**
     * The directory path to the kb on the allegro server.
     */
    private String mAllegroKB;
    

    /**
     * The overloaded constructor.
     *
     * @param dag        the workflow to which transfer nodes need to be added.
     * @param bag        the bag of initialization objects
     *
     */
    public Windward( ADag dag, PegasusBag bag ){
        super( dag, bag );
        
        //set the windward specific wf id property
        mProps.setProperty( "pegasus.windward.wf.id", dag.getExecutableWorkflowID() );
        
        mBundleRefiner = (MultipleFTPerXFERJobRefiner)RefinerFactory.loadInstance( "Bundle", bag, dag );
        Properties p = mProps.matchingSubset( WindwardImplementation.ALLEGRO_PROPERTIES_PREFIX, false  );
        mLogger.log( "Allegro Graph properties set are " + p, LogManager.DEBUG_MESSAGE_LEVEL );
        
        String base  = p.getProperty( "basekb" );
        File f = new File( base, mPOptions.getRelativeSubmitDirectory() );
        mAllegroKB = f.getAbsolutePath();
    }

    /**
     * Does nothing to add inter site transfer nodes as the same KB is used
     * for the whole workflow execution
     * 
     * @param job
     * @param files
     */
    public void addInterSiteTXNodes(SubInfo job, Collection files) {
       
    }

    /**
     * 
     * 
     * @param job
     * @param files
     * @param rcb
     */
    public void addStageOutXFERNodes(SubInfo job, Collection files, ReplicaCatalogBridge rcb) {
        this.addStageOutXFERNodes( job, files, rcb, false);
    }

    /**
     * Changes the FileTransfer objects and updates the destination URLs.
     * 
     * 
     * @param job
     * @param files
     * @param rcb
     * @param deletedLeaf
     */
    public void addStageOutXFERNodes( SubInfo job,
                                      Collection files,
                                      ReplicaCatalogBridge rcb, 
                                      boolean deletedLeaf ) {
        NameValue nv;
       
        //update the destination url's to reflect the KB location.
        for( Iterator it = files.iterator(); it.hasNext(); ){
            FileTransfer ft = ( FileTransfer )it.next();
            String lfn = ft.getLFN();
            
            //set the stage out explicitly to false as
            //registration takes care of the stage-out
            //ft.setTransferFlag( PegasusFile.TRANSFER_NOT );
            ft.setRegisterFlag( false );
            
            
            //remove all the destination url;s
            while( ( nv = ft.removeDestURL() ) != null ){
                mLogger.log( "Removing destination URL " + nv + " for LFN " + lfn,
                             LogManager.DEBUG_MESSAGE_LEVEL );
                             
            }
            
            mLogger.log( "Setting destination URL for lfn " + lfn + " to " + mAllegroKB,
                         LogManager.DEBUG_MESSAGE_LEVEL );
            ft.addDestination( "allegro_site" , mAllegroKB );
            
        }
        
        mBundleRefiner.addStageOutXFERNodes( job, files, rcb, deletedLeaf);
    }
    
    /**
     * Adds a job to the workflow .
     * 
     * @param job
     */
    public void addJob(SubInfo job) {
        //redirect to bundle
        mBundleRefiner.addJob( job );
    }

    /**
     * Adds a relation.
     * 
     * @param parent
     * @param child
     */
    public void addRelation(String parent, String child) {
        //redirect to bundle
        mBundleRefiner.addRelation( parent, child );
    }

    /**
     * Adds a relation selectively.
     * 
     * @param parent
     * @param child
     * @param pool
     * @param parentNew
     */
    public void addRelation(String parent, String child, String pool, boolean parentNew) {
        mBundleRefiner.addRelation( parent, child, pool, parentNew );
    }

   

    public void addStageInXFERNodes(SubInfo job, Collection files) {
        //redirect to bundle
        mBundleRefiner.addStageInXFERNodes(job, files);
    }

    public void done() {
         //redirect to bundle
        mBundleRefiner.done( );
    }

    public String getDescription() {
        return Windward.DESCRIPTION;
    }

}
