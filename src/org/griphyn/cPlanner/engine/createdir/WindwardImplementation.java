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

package org.griphyn.cPlanner.engine.createdir;

import edu.isi.pegasus.planner.catalog.site.classes.GridGateway;
import edu.isi.pegasus.planner.catalog.site.classes.SiteCatalogEntry;
import edu.isi.pegasus.planner.catalog.site.classes.SiteStore;


import org.griphyn.cPlanner.classes.SubInfo;
import org.griphyn.cPlanner.classes.PegasusBag;
import org.griphyn.cPlanner.classes.ADag;
import org.griphyn.cPlanner.classes.PlannerOptions;

import org.griphyn.cPlanner.cluster.JobAggregator;
import org.griphyn.cPlanner.cluster.aggregator.JobAggregatorFactory;

import edu.isi.pegasus.common.logging.LogManager;
import org.griphyn.cPlanner.common.PegasusProperties;

import org.griphyn.cPlanner.namespace.VDS;

import org.griphyn.common.catalog.TransformationCatalog;
import org.griphyn.common.catalog.TransformationCatalogEntry;

import org.griphyn.common.classes.TCType;

import org.griphyn.common.util.Separator;

import java.io.File;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;


/**
 *  A Windward implementation that uses the seqexec client to execute
 *
 * -Pegasus dirmananger to create a remote directory on the remote site.
 * -GU create kb script to create a KB in the Allegro Graph server.
 * 
 * @author Karan Vahi
 * @version $Revision$
 */
public class WindwardImplementation implements Implementation {

    /**
     * The properties prefix to pick up the allegro graph connection 
     * parameters.
     */
    public static final String ALLEGRO_PROPERTIES_PREFIX = "pegasus.windward.allegro.";
    
    /**
     * The name of the GU KB script that is used to create the kb.
     */
    public static final String GU_CREATE_KB_SCRIPT_NAME = "create-kb.sh";
    
    /**
     * The transformation namespace for the create dir jobs.
     */
    public static final String TRANSFORMATION_NAMESPACE = "windward";

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
     * The path to be set for create dir jobs.
     */
    public static final String PATH_VALUE = ".:/bin:/usr/bin:/usr/ucb/bin";

    /**
     * The complete TC name for kickstart.
     */
    public static final String COMPLETE_TRANSFORMATION_NAME = Separator.combine(
                                                                 TRANSFORMATION_NAMESPACE,
                                                                 TRANSFORMATION_NAME,
                                                                 TRANSFORMATION_VERSION  );


    /**
     * The derivation namespace for the create dir  jobs.
     */
    public static final String DERIVATION_NAMESPACE = "windward";

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
     * The handle to the transformation catalog.
     */
    protected TransformationCatalog mTCHandle;
    
    /**
     * The handle to the SiteStore.
     */
    protected SiteStore mSiteStore;
    
    /**
     * The handle to the logging object.
     */
    protected LogManager mLogger;
    
    /**
     * The handle to the pegasus properties.
     */
    protected PegasusProperties mProps;
    
    /**
     * The handle to the PlannerOptions object.
     */
    protected PlannerOptions mPOptions;
    
    /**
     * The handle to the pegasus implementation for creating a directory
     * on the remote site.
     */
    protected Implementation mPegasusImplementation;
    
    /***
     * The hostname for the AllegroGraph KB store.
     */
    private String mAllegroHost;
    
    /***
     * The port for the AllegroGraph KB store.
     */
    private String mAllegroPort;
    
    /***
     * The kb in the allegrograph server.
     */
    private String mAllegroBaseKB;
    
    /**
     * The seqexec job aggregator.
     */
    private JobAggregator mSeqExecAggregator;
    
    private String mWorkflowID;
    
    /**
     * The default constructor.
     */
    public WindwardImplementation(){
        mPegasusImplementation = new DefaultImplementation();
    }
    
    /**
     * Intializes the class.
     *
     * @param bag      bag of initialization objects
     */
    public void initialize( PegasusBag bag ) {
        mLogger    = bag.getLogger();
        mProps     = bag.getPegasusProperties();
        mPOptions  = bag.getPlannerOptions();
        mSiteStore = bag.getHandleToSiteStore();
        mTCHandle  = bag.getHandleToTransformationCatalog();
        mProps     = bag.getPegasusProperties();
        mPegasusImplementation.initialize( bag );     
        
        Properties p = mProps.matchingSubset( WindwardImplementation.ALLEGRO_PROPERTIES_PREFIX, false  );
        mLogger.log( "Allegro Graph properties set are " + p, LogManager.DEBUG_MESSAGE_LEVEL );
        mAllegroHost = p.getProperty( "host" );
        mAllegroPort = p.getProperty( "port" );
        mAllegroBaseKB   = p.getProperty( "basekb" );
        
        //set in the DC interface
        mWorkflowID = mProps.getProperty( "pegasus.windward.wf.id" );
        //just to pass the label have to send an empty ADag.
        //should be fixed
        ADag dag = new ADag();
        dag.dagInfo.setLabel( "windward" );

        mSeqExecAggregator = JobAggregatorFactory.loadInstance( JobAggregatorFactory.SEQ_EXEC_CLASS,
                                                                dag,
                                                                bag  );
        mSeqExecAggregator.setAbortOnFirstJobFailure( true );
    }
    
    /**
     * It creates a seqexec job that creates a make directory job that
     *  - creates a directory on the remote pool using pegasus::dirmanager
     *  - creates a knowledge base using the GU provided script in the remote
     *    Allegro Graph server.
     *
     * @param site  the execution site for which the create dir job is to be
     *                  created.
     * @param name  the name that is to be assigned to the job.
     * @param directory  the directory to be created on the site.
     *
     * @return create dir job.
     */
    public SubInfo makeCreateDirJob( String site, String name, String directory ) {
        SubInfo pegasusJob = mPegasusImplementation.makeCreateDirJob( site, name, directory ); 
        
        //add the arguments to set the mode to 777 always
        pegasusJob.setArguments( pegasusJob.getArguments() + " -m 777" );
        
        SubInfo createGUKBJob = makeCreateGUKBJob( site, directory );
        
        List <SubInfo> l = new LinkedList <SubInfo> ();
        l.add( pegasusJob );
        l.add( createGUKBJob );
        
        
        //add extra logging options to create gu kb job
        StringBuffer extraArgs = new StringBuffer();
        extraArgs.append( " -w " ).append( mWorkflowID );
        extraArgs.append( " -j " ).append( pegasusJob.getID() );
        createGUKBJob.setArguments( createGUKBJob.getArguments() + extraArgs.toString() );
        //System.out.println( createGUKBJob.getArguments() );
        
        
        //now lets merge all these jobs
        SubInfo merged = mSeqExecAggregator.construct( l, "dirmanager", pegasusJob.getName()  );
  
        
        //set the name of the merged job back to the name of
        //pegasus job passed in the function call
        merged.setName( pegasusJob.getName() );
        merged.setJobType( pegasusJob.getJobType() );
        
        return merged;
    }

    /**
     * Constructs  a job that uses GU wrapper to create a KB in the 
     * remote Allegro Graph Server.
     * 
     * @param site
     * @param directory
     * @return
     */
    protected SubInfo makeCreateGUKBJob( String site , String directory ) {
        SiteCatalogEntry s = mSiteStore.lookup( site );
        String guHome   = s.getEnvironmentVariable( "GU_HOME" );
        
        if( guHome == null ){
            throw new RuntimeException( "The environment variable GU_HOME is not set for site " + site );
        }
        
        SubInfo guJob = new SubInfo();
        
        guJob.setTransformation( "windward", "create-ag-kb", null );
        guJob.setDerivation( "windward", "create-ag-kb", null );
        
        //construct path to the remote GU executable
        StringBuffer path = new StringBuffer();
        path.append( guHome ).append( File.separator ).append( GU_CREATE_KB_SCRIPT_NAME );
        guJob.setRemoteExecutable( path.toString() );
        
        GridGateway jobmanager = s.selectGridGateway( GridGateway.JOB_TYPE.transfer );
        guJob.setJobManager( (jobmanager == null) ?
                                  null :
                                  jobmanager.getContact( ) );
        guJob.setStdIn( "" );
        guJob.setStdErr( "" );
        guJob.setStdOut( "" );
        guJob.setSiteHandle( site );
        
        //construct the arguments
        StringBuffer arguments = new StringBuffer();
        arguments.append( " -h " ).append( mAllegroHost );
        arguments.append( " -p " ).append( mAllegroPort );
        arguments.append( " -k " ).append( directory );        
//        arguments.append( File.separator );
//        arguments.append( "gu-kb" );
        
        guJob.setArguments( arguments.toString() );
        
        return guJob;
        
    }
    
    

}
