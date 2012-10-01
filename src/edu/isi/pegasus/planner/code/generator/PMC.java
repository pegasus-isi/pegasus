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


package edu.isi.pegasus.planner.code.generator;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import edu.isi.pegasus.common.credential.CredentialHandler;
import edu.isi.pegasus.common.credential.CredentialHandler.TYPE;
import edu.isi.pegasus.common.credential.CredentialHandlerFactory;
import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.common.util.DefaultStreamGobblerCallback;
import edu.isi.pegasus.common.util.StreamGobbler;
import edu.isi.pegasus.common.util.StreamGobblerCallback;
import edu.isi.pegasus.planner.catalog.site.classes.SiteCatalogEntry;
import edu.isi.pegasus.planner.catalog.site.classes.SiteStore;
import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.Profile;
import edu.isi.pegasus.planner.cluster.JobAggregator;
import edu.isi.pegasus.planner.cluster.aggregator.JobAggregatorFactory;
import edu.isi.pegasus.planner.cluster.aggregator.MPIExec;
import edu.isi.pegasus.planner.code.CodeGeneratorException;
import edu.isi.pegasus.planner.code.GridStart;
import edu.isi.pegasus.planner.code.GridStartFactory;
import edu.isi.pegasus.planner.code.POSTScript;
import edu.isi.pegasus.planner.code.generator.condor.SUBDAXGenerator;
import edu.isi.pegasus.planner.namespace.Dagman;
import edu.isi.pegasus.planner.namespace.Pegasus;
import edu.isi.pegasus.planner.partitioner.graph.Adapter;
import edu.isi.pegasus.planner.partitioner.graph.Graph;
import edu.isi.pegasus.planner.partitioner.graph.GraphNode;

/**
 * This code generator generates a shell script in the submit directory.
 * The shell script can be executed on the submit host to run the workflow
 * locally.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class PMC extends Abstract {

    public static final String PEGASUS_SHELL_RUNNER_FUNCTIONS_BASENAME = "shell-runner-functions.sh ";

    /**
     * The prefix for events associated with job in jobstate.log file
     */
    public static final String JOBSTATE_JOB_PREFIX = "JOB";
    
    
    /**
     * The prefix for events associated with POST_SCRIPT in jobstate.log file
     */
    public static final String JOBSTATE_POST_SCRIPT_PREFIX = "POST_SCRIPT";
    
    
    /**
     * The prefix for events associated with job in jobstate.log file
     */
    public static final String JOBSTATE_PRE_SCRIPT_PREFIX = "PRE_SCRIPT";
    

    /**
     * The handle to the output file that is being written to.
     */
    private PrintWriter mWriteHandle;

    
    /**
     * Handle to the Site Store.
     */
    private SiteStore mSiteStore;

    /**
     * The handle to the GridStart Factory.
     */
    protected GridStartFactory mGridStartFactory;

    /**
     * A boolean indicating whether grid start has been initialized or not.
     */
    protected boolean mInitializeGridStart;

    
    /**
     * The default constructor.
     */
    public PMC( ){
        super();
        mInitializeGridStart = true;
        mGridStartFactory = new GridStartFactory();
    }

    /**
     * Initializes the Code Generator implementation.
     *
     *  @param bag   the bag of initialization objects.
     *
     * @throws CodeGeneratorException in case of any error occuring code generation.
     */
    public void initialize( PegasusBag bag ) throws CodeGeneratorException{
        super.initialize( bag );
        mLogger = bag.getLogger();


        //create the base directory recovery
        File wdir = new File(mSubmitFileDir);
        wdir.mkdirs();

        //get the handle to pool file
        mSiteStore = bag.getHandleToSiteStore();

    }

    /**
     * Generates the code for the concrete workflow in the GRMS input format.
     * The GRMS input format is xml based. One XML file is generated per
     * workflow.
     *
     * @param dag  the concrete workflow.
     *
     * @return handle to the GRMS output file.
     *
     * @throws CodeGeneratorException in case of any error occuring code generation.
     */
    public Collection<File> generateCode( ADag dag ) throws CodeGeneratorException{
        Collection result = new ArrayList( 1 );
        
        


        //we first need to convert internally into graph format
        Graph workflow =    Adapter.convert( dag );

        mGridStartFactory.initialize(mBag, dag);

        //traverse the workflow and enable the jobs with kickstart first
        for( Iterator<GraphNode> it = workflow.nodeIterator(); it.hasNext(); ){
            GraphNode node = it.next();
            Job job = (Job)node.getContent();
            GridStart gridStart = mGridStartFactory.loadGridStart( job , null );

            //trigger the -w option for kickstart always
            job.vdsNS.construct( Pegasus.CHANGE_DIR_KEY , "true" );

            //enable the job
            if( !gridStart.enable( job,false ) ){
                String msg = "Job " +  job.getName() + " cannot be enabled by " +
                             gridStart.shortDescribe() + " to run at " +
                             job.getSiteHandle();
                mLogger.log( msg, LogManager.FATAL_MESSAGE_LEVEL );
                throw new CodeGeneratorException( msg );
            }

        }

        //lets load the PMC cluster implementation
        //and generate the PMC file for it
        JobAggregator aggregator = JobAggregatorFactory.loadInstance( JobAggregatorFactory.MPI_EXEC_CLASS, dag, mBag);
        MPIExec pmcAggregator = (MPIExec)aggregator;
        String name = dag.getLabel() + "-" + dag.dagInfo.index + ".pmc";
        pmcAggregator.generatePMCInputFile(workflow, name );
        
         //the dax replica store
        this.writeOutDAXReplicaStore( dag );

        //write out the braindump file
        this.writeOutBraindump( dag );

        //write out the nelogger file
        this.writeOutStampedeEvents( dag );

        //write out the metrics file
        this.writeOutWorkflowMetrics(dag);

        return result;
    }

    /**
     * Generates the code for a single job in the input format of the workflow
     * executor being used.
     *
     * @param dag    the dag of which the job is a part of.
     * @param job    the <code>Job</code> object holding the information about
     *               that particular job.
     *
     * @throws CodeGeneratorException in case of any error occuring code generation.
     */
    public void generateCode( ADag dag, Job job ) throws CodeGeneratorException{
       throw new CodeGeneratorException( "The code generator only works on the workflow level" );
    }   
    
    /**
     * Returns a Map containing additional braindump entries that are specific
     * to a Code Generator
     * 
     * @param workflow  the executable workflow
     * 
     * @return Map
     */
    public  Map<String, String> getAdditionalBraindumpEntries( ADag workflow ) {
        Map entries = new HashMap();
        entries.put( Braindump.GENERATOR_TYPE_KEY, "pmc" );
        
        return entries;
    }
    

    



}
