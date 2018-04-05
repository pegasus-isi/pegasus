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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

import edu.isi.pegasus.common.logging.LogManager;

import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusBag;

import edu.isi.pegasus.planner.cluster.JobAggregator;
import edu.isi.pegasus.planner.cluster.aggregator.JobAggregatorFactory;
import edu.isi.pegasus.planner.cluster.aggregator.MPIExec;

import edu.isi.pegasus.planner.code.CodeGeneratorException;
import edu.isi.pegasus.planner.code.GridStart;
import edu.isi.pegasus.planner.code.GridStartFactory;

import edu.isi.pegasus.planner.namespace.Pegasus;

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

    /**
     * The handle to the GridStart Factory.
     */
    protected GridStartFactory mGridStartFactory;

    /**
     * A boolean indicating whether grid start has been initialized or not.
     */
    protected boolean mInitializeGridStart;
    
    /**
     * Handle to the PBS Code generator.
     */
    private final PBS mPBS;

    /**
     * The default constructor.
     */
    public PMC( ){
        super();
        mInitializeGridStart = true;
        mGridStartFactory = new GridStartFactory();
        //instantiate the PBS Code generator.
        //we may need a factory later on
        mPBS = new PBS();
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
        mPBS.initialize( bag );
        mLogger = bag.getLogger();


        //create the base directory recovery
        File wdir = new File(mSubmitFileDir);
        wdir.mkdirs();


    }

    /**
     * Generates the code for the concrete workflow in the GRMS input format.
     * The GRMS input format is xml based. One XML file is generated per
     * workflow.
     *
     * @param dag  the concrete workflow.
     *
     * @return handle to the PMC file generated in the submit directory.
     *
     * @throws CodeGeneratorException in case of any error occuring code generation.
     */
    public Collection<File> generateCode( ADag dag ) throws CodeGeneratorException{
        Collection result = new ArrayList( 1 );

        //PM-747 no need for conversion as ADag now implements Graph interface
        Graph workflow = dag;

        mGridStartFactory.initialize( mBag, 
                                      dag,
                                      this.getDAGFilename(dag, POSTSCRIPT_LOG_SUFFIX ) );

        Job prevJob = null;
        //traverse the workflow and enable the jobs with kickstart first
        for( Iterator<GraphNode> it = workflow.nodeIterator(); it.hasNext(); ){
            GraphNode node = it.next();
            Job job = (Job)node.getContent();
            String site = job.getSiteHandle();

            //sanity check
            if( !( prevJob == null || prevJob.getSiteHandle().equalsIgnoreCase( site ) )){
                StringBuffer error = new StringBuffer();
                error.append( "Site Mismatch between jobs " ).append( "(" ).
                      append( job.getID() ).append( ":" ).append( site ).append( "," ).
                      append( prevJob.getID() ).append( ":" ).append( prevJob.getSiteHandle() ).
                      append( ") ").append( "." ).
                      append( "For the PMC Code generator all jobs should be mapped to the same site. ");
                throw new CodeGeneratorException( error.toString() );
            }

            GridStart gridStart = mGridStartFactory.loadGridStart( job , null );

            //trigger the -w option for kickstart always
            job.vdsNS.construct( Pegasus.CHANGE_DIR_KEY , "true" );

            //the stdin of the job is handled outside of
            //kickstart module for time being.
            //assumption is that we are always using kickstart
            //for this scenario
            String stdin = job.getStdIn();
            StringBuffer kickstartPreArgs = new StringBuffer();
            boolean prepend = false;
            if( stdin == null || stdin.length() == 0 ){
                //nothing to do
            }
            else{
                //construct the kickstart arguments for connecting
                // the task stdin
                kickstartPreArgs.append("-i ");
                if( stdin.startsWith( File.separator ) ){
                    kickstartPreArgs.append( stdin );
                }
                else{
                    //prepend the submit dirctory
                    //PM-833 figure out the job submit directory
                    String jobSubmitDirectory = new File( job.getFileFullPath( mSubmitFileDir, ".in" )).getParent();
                    kickstartPreArgs.append( jobSubmitDirectory ).append( File.separator).append( stdin );
                }
                kickstartPreArgs.append(' ');
                //reset stdin as we don't want
                //kickstart module to handle it
                job.setStdIn( "" );
                prepend = true;
            }

            //enable the job
            if( !gridStart.enable( job,false ) ){
                String msg = "Job " +  job.getName() + " cannot be enabled by " +
                             gridStart.shortDescribe() + " to run at " +
                             job.getSiteHandle();
                mLogger.log( msg, LogManager.FATAL_MESSAGE_LEVEL );
                throw new CodeGeneratorException( msg );
            }

            if( prepend ){
                //job has already been kickstarted.
                //prepend the -i option for stdin
                StringBuffer args = new StringBuffer();
                args.append( kickstartPreArgs ).append( job.getArguments() );
                job.setArguments( args.toString() );
            }

            prevJob = job;

        }

        //lets load the PMC cluster implementation
        //and generate the PMC file for it
        JobAggregator aggregator = JobAggregatorFactory.loadInstance( JobAggregatorFactory.MPI_EXEC_CLASS, dag, mBag);
        MPIExec pmcAggregator = (MPIExec)aggregator;
        String name = pmcBasename( dag );
        //PM-660 designate that the graph is for the whole workflow
        pmcAggregator.generatePMCInputFile(workflow, name, ".", false );

        //lets generate the PBS input file
        mPBS.generateCode( dag );

         //the dax replica store
        this.writeOutDAXReplicaStore( dag );

        //write out the braindump file
        this.writeOutBraindump( dag );

        //write out the nelogger file
        this.writeOutStampedeEvents( dag );

        //write out the metrics file
//        this.writeOutWorkflowMetrics(dag);

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
     * to a Code Generator. The entries added for this are from the scheduler
     * specific generator
     * 
     * @param workflow  the executable workflow
     * 
     * @return Map
     */
    public Map<String, String> getAdditionalBraindumpEntries( ADag workflow ) {
        Map<String, String> entries = this.mPBS.getAdditionalBraindumpEntries(workflow);
        entries.put("dag", this.getPathtoPMCFile(workflow));
        return entries;
    }

    /**
     * Returns the basename for the pmc file for the dag
     * 
     * @param dag the workflow
     * 
     * @return the basenmae
     */
    protected String pmcBasename( ADag dag ) {
        StringBuffer name = new StringBuffer();
        name.append(  dag.getLabel() ).append( "-" ).
             append( dag.getIndex() ).append( ".dag" );
        return name.toString();
    }
    

    /**
     * Returns the basename for the pmc file for the dag
     *
     * @param dag the workflow
     *
     * @return the basenmae
     */
    protected String getPathtoPMCFile( ADag dag ) {
        StringBuilder script = new StringBuilder();
        script.append( this.mSubmitFileDir ).append( File.separator ).
               append( this.pmcBasename(dag) );
        return script.toString();
    }





}
