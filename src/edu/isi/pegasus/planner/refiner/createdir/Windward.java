/*
 * 
 *   Copyright 2007-2008 University Of Southern California
 * 
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 *   Unless required by applicable law or agreed to in writing,
 *   software distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 * 
 */

package edu.isi.pegasus.planner.refiner.createdir;

import edu.isi.pegasus.common.logging.LoggingKeys;

import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.SubInfo;
import edu.isi.pegasus.planner.classes.PlannerOptions;
        
import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.common.PegasusProperties;

import edu.isi.pegasus.common.util.Separator;

import java.util.Iterator;
import java.util.Properties;

import java.io.File;
import java.util.LinkedList;
import java.util.List;

/**
 * A Windward specific strategy for adding the create dir nodes.
 * The create dir nodes are added according the HourGlass Strategy.
 * In addition an extra KB creation job is created that 
 *  - creates a directory  in the Allegro Graph Server
 *  - creates a workflow specific kb in the Allegro Graph Server.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class Windward extends AbstractStrategy{
    
    /**
     * The name concatenating  job that ensures that creates the workflow 
     * specific KB in AllegroGraph instance.
     */
    public static final String CREATE_GU_KB_SUFFIX = "createGUKB";
    
    /**
     * The transformation namespace for the create dir jobs.
     */
    public static final String TRANSFORMATION_NAMESPACE = "pegasus";

    /**
     * The logical name of the transformation that creates directories on the
     * remote execution pools.
     */
    public static final String TRANSFORMATION_NAME = "dirmanager";

    /**
     * The version number for the derivations for create dir  jobs.
     */
    public static final String TRANSFORMATION_VERSION = null;


    /**
     * The complete TC name for dirmanager.
     */
    public static final String COMPLETE_TRANSFORMATION_NAME = Separator.combine(
                                                                 TRANSFORMATION_NAMESPACE,
                                                                 TRANSFORMATION_NAME,
                                                                 TRANSFORMATION_VERSION  );

    /**
     * The derivation namespace for the create dir  jobs.
     */
    public static final String DERIVATION_NAMESPACE = "pegasus";

    /**
     * The logical name of the transformation that creates directories on the
     * remote execution pools.
     */
    public static final String DERIVATION_NAME = "dirmanager";


    /**
     * The version number for the derivations for create dir  jobs.
     */
    public static final String DERIVATION_VERSION = "1.0";
    
    /**
     * The HourGlass implementation.
     */
    protected Strategy mHourGlassStrategy;
    
    /**
     * The Windward implementation that uses seqexec to wrap dirmanager and
     * create-ag-kb into one job.
     */
    protected Implementation mWindwardImplementation;
    
    /**
     * The handle to PegasusProperties.
     */
    protected PegasusProperties mProps;
    
    /**
     * The handle to PlannerOptions.
     */
    protected PlannerOptions mPOptions;
    
    /**
     * Get the site at which the allegro graph server is running.
     */
    //protected String mKbSite;
    
    
    /**
     * Intializes the class.
     *
     * @param bag    bag of initialization objects
     * @param impl    the implementation instance that creates create dir job 
     */
    public void initialize( PegasusBag bag, Implementation impl ){
        super.initialize( bag, impl );
        mHourGlassStrategy = new HourGlass();
        mHourGlassStrategy.initialize( bag, impl );
        
        mWindwardImplementation = new WindwardImplementation();
        mWindwardImplementation.initialize( bag );
        
        mProps = bag.getPegasusProperties();
        mPOptions = bag.getPlannerOptions();
    }
    

    
    /**
     * Uses the HourGlass Strategy to add the directory creation nodes.
     * In addition it adds a root node, to create a KB in the Allegro Graph
     * Server.
     * 
     * @param dag   the workflow to which the nodes have to be added.
     * 
     * @return the added workflow
     */
    public ADag addCreateDirectoryNodes( ADag dag ){
        ADag result = mHourGlassStrategy.addCreateDirectoryNodes( dag );
        
        //the job name for the add kb job
        String kbJobname = getCreateGUKBJobname( dag );
        this.introduceRootDependencies( dag, kbJobname );
        
        //figure out some allegro graph stuff
        Properties p = mProps.matchingSubset( WindwardImplementation.ALLEGRO_PROPERTIES_PREFIX, false  );
        mLogger.log( "Allegro Graph properties set are " + p, LogManager.DEBUG_MESSAGE_LEVEL );
        String site = p.getProperty( "site" );
        String base = p.getProperty( "basekb" );
        String directory = new File( base, 
                                     mPOptions.getRelativeDirectory()  ).getAbsolutePath();
        
        SubInfo kbJob = makeCreateGUKBJob( kbJobname, site, directory );
        result.add( kbJob );
        List l = new LinkedList();
        l.add( kbJob.getID() );
        mLogger.logEntityHierarchyMessage( LoggingKeys.DAX_ID, dag.getAbstractWorkflowID(),
                                           LoggingKeys.JOB_ID, l );
        
        return result;
    }
    
    /**
     * Returns the name of the create GU KB job.
     *
     * @return name
     */
    protected String getCreateGUKBJobname( ADag dag ){
        StringBuffer sb = new StringBuffer();

        sb.append( dag.dagInfo.nameOfADag ).append( "_" ).
           append( dag.dagInfo.index ).append( "_" );

        //append the job prefix if specified in options at runtime
        if ( mJobPrefix != null ) { sb.append( mJobPrefix ); }

        sb.append( Windward.CREATE_GU_KB_SUFFIX  );

        return sb.toString();
    }


    /**
     *
     * @param name   the name to be assigned to the GU KB create job
     * @param site   the site where the job needs to execute
     * @param directory  the directory to be created in the AG server
     * 
     * @return  the dummy concat job.
     */
    public SubInfo makeCreateGUKBJob( String name, String site , String directory) {
        return mWindwardImplementation.makeCreateDirJob( site,
                                                         name, 
                                                         directory );
    }

    /**
     * It traverses through the root jobs of the dag and introduces a new super
     * root node to it.
     *
     * @param dag       the DAG
     * @param newRoot   the name of the job that is the new root of the graph.
     */
    private void introduceRootDependencies( ADag dag, String newRoot) {
        String job = null;

        for( Iterator it = dag.getRootNodes().iterator(); it.hasNext() ; ) {
            job = (String) it.next();
            dag.addNewRelation(newRoot, job);
            mLogger.log( "Adding relation " + newRoot + " -> " + job,LogManager.DEBUG_MESSAGE_LEVEL);

        }
    }

    
}
