/**
 *  Copyright 2007-2017 University Of Southern California
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

package edu.isi.pegasus.planner.cluster.aggregator;


import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.AggregatedJob;

import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.code.gridstart.PegasusLite;
import edu.isi.pegasus.planner.namespace.Globus;
import edu.isi.pegasus.planner.namespace.Pegasus;
import edu.isi.pegasus.planner.partitioner.graph.Graph;
import edu.isi.pegasus.planner.partitioner.graph.GraphNode;


import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.Iterator;

/**
 * Aggergates jobs together, to be launched by pegasus-aws-batch tool.
 * 
 * @author Karan Vahi vahi@isi.edu
 * @version $Revision$
 */

public class AWSBatch extends Abstract {

    /**
     * The logical name of the transformation that is able to run multiple
     * jobs via pegasus-aws-batch.
     */
    public static final String COLLAPSE_LOGICAL_NAME = "aws-batch";

    /**
     * The basename of the executable that is able to run multiple
     * jobs via mpi.
     */
    public static String EXECUTABLE_BASENAME = "pegasus-aws-batch";

    //the common files required for the pegasus-aws-batch
    /**
     * The basename of the script used to launch jobs in the AWS Batch via
     * the fetch and run example
     */
    public static final String PEGASUS_AWS_BATCH_LAUNCH_BASENAME = "pegasus-aws-batch-launch.sh";
    
    /**
     * The basename of the script used to launch jobs in the AWS Batch via
     * the fetch and run example
     */
    public static final String PEGASUS_LITE_COMMON_FILE_BASENAME = PegasusLite.PEGASUS_LITE_COMMON_FILE_BASENAME;
    
    /**
     * The environment variable that designates the key used by fetch_and_run.sh
     * executable in batch containers 
     */
    public static final String BATCH_FILE_TYPE_KEY = "BATCH_FILE_TYPE";
    
    /**
     * The environment variable that designates the key used by fetch_and_run.sh
     * executable for batch containers to pull the user script from s3
     */
    public static final String BATCH_FILE_S3_URL_KEY = "BATCH_FILE_S3_URL";
    
    /**
     * The  environment variable that designates the key for AWS Batch s3 bucket
     */
    public static String PEGASUS_AWS_BATCH_BUCKET_KEY = "PEGASUS_AWS_BATCH_BUCKET";
    

    /**
     * The default constructor.
     */
    public AWSBatch(){
        super();
    }

    /**
     *Initializes the JobAggregator impelementation
     *
     * @param dag  the workflow that is being clustered.
     * @param bag   the bag of objects that is useful for initialization.
     *
     *
     */
    public void initialize( ADag dag , PegasusBag bag  ){
        super.initialize(dag, bag);
    }


    /**
     * Enables the abstract clustered job for execution and converts it to it's
     * executable form. Also associates the post script that should be invoked
     * for the AggregatedJob
     *
     * @param job          the abstract clustered job
     */
    public void makeAbstractAggregatedJobConcrete( AggregatedJob job ){
        //PM-962 for PMC aggregated values for runtime and memory don't get mapped
        //to the PMC job itself
       
        super.makeAbstractAggregatedJobConcrete(job);

       
        return;
    }

    /**
     * Writes out the input file for the aggregated job
     *
     * @param job   the aggregated job
     *
     * @return shareSHDirectoryPath to the input file
     */
    protected File writeOutInputFileForJobAggregator(AggregatedJob job) {
        File dir = new File( this.mDirectory, job.getRelativeSubmitDirectory() );
        return this.generateAWSBatchInputFile(job,  new File( dir, job.getID() + ".in"), true );
    }


    /**
     * Writes out the input file for the aggregated job
     *
     * @param job   the aggregated job
     * @param stdin  the name of input file for batch to be generated
     * @param isClustered  a boolean indicating whether the graph belongs to a
     *                     clustered job or not.
     *
     * @return shareSHDirectoryPath to the input file
     */
    public File generateAWSBatchInputFile(Graph job, File stdIn , boolean isClustered ) {
        try {
            Writer writer;
            writer = new BufferedWriter(new FileWriter( stdIn ));
            JsonFactory factory = new JsonFactory();
            JsonGenerator generator = factory.createGenerator( writer );
            generator.setPrettyPrinter(new DefaultPrettyPrinter());
            //traverse throught the jobs to determine input/output files
            //and merge the profiles for the jobs
            int taskid = 1;
            
            /*
            "jobDefinition": "XXXX",
         "jobName": "pegasus-test-job-1",
         "jobQueue": "XXXX",
         "executable": "pegasus-aws-batch-launch.sh" ,
         "arguments": "sample_pegasus_lite.sh", 
         "environment": [ 
             { 
                "name": "BATCH_FILE_TYPE",
                "value": "script"
            },
            { 
                "name": "BATCH_FILE_S3_URL",
                "value": "s3://karan-cmd-test-bucket/pegasus-aws-batch-launch.sh"
            },
            {
                "name": "TRANSFER_INPUT_FILES",
                "value": "./sample_pegasus_lite.sh"
            }
        ]
            */
            
            generator.writeStartObject();
            generator.writeArrayFieldStart( "SubmitJob" );
            for( Iterator<GraphNode> it = job.nodeIterator(); it.hasNext(); taskid++ ) {
                GraphNode node = it.next();
                Job constitutentJob = (Job) node.getContent();

                //handle stdin
                if( constitutentJob instanceof AggregatedJob ){
                    //slurp in contents of it's stdin
                    throw new RuntimeException( "Clustering of clustered jobs not supported with " + AWSBatch.COLLAPSE_LOGICAL_NAME );
                }
                generator.writeStartObject();
                generator.writeStringField( "jobName",    constitutentJob.getID() );
                generator.writeStringField( "executable", constitutentJob.getRemoteExecutable());
                generator.writeStringField( "arguments", constitutentJob.getArguments());
                
                if( !constitutentJob.envVariables.isEmpty() ){
                    generator.writeArrayFieldStart( "environment" );
                    for( Iterator<String> envIT = constitutentJob.envVariables.getProfileKeyIterator(); envIT.hasNext();){
                        String key = envIT.next();
                        generator.writeStartObject();
                        generator.writeStringField( "name", key );
                        generator.writeStringField( "value", (String)constitutentJob.envVariables.get(key) );
                        generator.writeEndObject();
                    }
                    generator.writeEndArray();
                }
                
                generator.writeEndObject();
            }
            generator.writeEndArray();
            generator.writeEndObject();
            generator.close();

        }
        catch(IOException e){
            mLogger.log("While writing the stdIn file " + e.getMessage(),
                        LogManager.ERROR_MESSAGE_LEVEL);
            throw new RuntimeException( "While writing the stdIn file " + stdIn, e );
        }

        return stdIn;

    }

    
    /**
     * Returns the logical name of the transformation that is used to
     * collapse the jobs.
     *
     * @return the the logical name of the collapser executable.
     * @see #COLLAPSE_LOGICAL_NAME
     */
    public String getClusterExecutableLFN(){
        return COLLAPSE_LOGICAL_NAME;
    }

    /**
     * Returns the executable basename of the clustering executable used.
     *
     * @return the executable basename.
     * @see #EXECUTABLE_BASENAME
     */
    public String getClusterExecutableBasename(){
        return AWSBatch.EXECUTABLE_BASENAME;
    }

    /**
     * Determines whether there is NOT an entry in the transformation catalog
     * for the job aggregator executable on a particular site.
     *
     * @param site       the site at which existence check is required.
     *
     * @return boolean  true if an entry does not exists, false otherwise.
     */
    public boolean entryNotInTC(String site) {
        //batch always runs in site "local"
        return this.entryNotInTC( AWSBatch.TRANSFORMATION_NAMESPACE,
                                  AWSBatch.COLLAPSE_LOGICAL_NAME,
                                  AWSBatch.TRANSFORMATION_VERSION,
                                  this.getClusterExecutableBasename(),
                                  "local");
    }


    /**
     * Returns the arguments with which the <code>AggregatedJob</code>
     * needs to be invoked with. At present any empty argument string is
     * returned.
     *
     * @param job  the <code>AggregatedJob</code> for which the arguments have
     *             to be constructed.
     *
     * @return argument string
     */
    public String aggregatedJobArguments( AggregatedJob job ){
        //the stdin of the job actually needs to be passed as arguments
        File jobSubmitDir = new File(this.mDirectory, job.getRelativeSubmitDirectory() );
        String stdin  = jobSubmitDir + File.separator + job.getStdIn();
        StringBuffer args = new StringBuffer();
        
        //add --max-wall-time option PM-625
        String walltime = (String) job.globusRSL.get( Globus.MAX_WALLTIME_KEY );
       
        int divisor = 1;
        if( walltime == null ){
            //PM-962 fall back on pegasus profile runtime key which is in seconds
            walltime = job.vdsNS.getStringValue( Pegasus.RUNTIME_KEY );
            if( walltime != null ){
                divisor = 60;
            }
        }
        
        args.append( "--conf" ).append( " " ).
                  append(  mProps.getPropertiesInSubmitDirectory( )  ).
                  append( " " );
        
        
        //debug log level
        args.append( "--log-level" ).append( " " ).append( "debug" ).append( " " );
        
        //we log to a file based on jobname
        args.append( "--log-file" ).append( " " ).append( jobSubmitDir).append( File.separator).append( job.getID() + ".log" ).append( " " );
        
        //the job name is the prefix for the time being
        args.append( "--prefix" ).append( " " ).append( job.getID() ).append( " " );
        
        //the S3 bucket to use is picked up from the environment
        String bucket = (String) job.envVariables.get( AWSBatch.PEGASUS_AWS_BATCH_BUCKET_KEY );
        if( bucket == null ){
            throw new RuntimeException( "Clustered job not associated with S3 bucket for AWS Batch " + job.getID() );
        }
        args.append( "--s3" ).append( " " ).append( bucket ).append( " " );
        
        //add any files to be transferred from submit host
        args.append( "--files" ).append( " " );
        
        //check any credentials have to be transferred
        String files = job.condorVariables.getIPFilesForTransfer();
        job.condorVariables.removeIPFilesForTransfer();
        if( files != null ){
            args.append( files );
            if( !files.endsWith( ",") ){
                args.append( "," );
            }
        }
        //add the --files option to transfer the common shell scripts required
        StringBuilder shareSHDirectoryPath = new StringBuilder();
        File share = mProps.getSharedDir();
        if( share == null ){
            throw new RuntimeException( "Property for Pegasus share directory is not set" );
        }
        shareSHDirectoryPath.append( share.getAbsolutePath() ).append( File.separator ).
             append( "sh" ).append( File.separator );
        args.append( shareSHDirectoryPath ).append( AWSBatch.PEGASUS_LITE_COMMON_FILE_BASENAME ).append( "," ).
             append( shareSHDirectoryPath ).append( AWSBatch.PEGASUS_AWS_BATCH_LAUNCH_BASENAME ).append( " " );

        // Construct any extra arguments specified in profiles or properties
        // This should go last, otherwise we can't override any automatically-
        // generated arguments
        String extraArgs = job.vdsNS.getStringValue(Pegasus.CLUSTER_ARGUMENTS);
        if (extraArgs != null) {
            args.append(extraArgs).append(" ");
        }
        
        args.append( stdin );

        return args.toString();
    }


    /**
     * Setter method to indicate , failure on first consitutent job should
     * result in the abort of the whole aggregated job. Ignores any value
     * passed, as AWSBatch does not handle it for time being.
     *
     * @param fail  indicates whether to abort or not .
     */
    public void setAbortOnFirstJobFailure( boolean fail){

    }

    /**
     * Returns a boolean indicating whether to fail the aggregated job on
     * detecting the first failure during execution of constituent jobs.
     *
     * @return boolean indicating whether to fail or not.
     */
    public boolean abortOnFristJobFailure(){
        return false;
    }
    
    /**
     * A boolean indicating whether ordering is important while traversing 
     * through the aggregated job. 
     * 
     * @return false
     */
    public boolean topologicalOrderingRequired(){
        //ordering is not important, as PMC has a graph representation
        return false;
    }

    /**
     * Looks at the profile keys associated with the job to generate the argument
     * string fragment containing the cpu required for the job.
     * 
     * @param job   the Job for which memory requirements has to be determined.
     * 
     * @return  the arguments fragment else empty string
     */
    public String getCPURequirementsArgument( Job job ){
        StringBuffer result = new StringBuffer();
        String value = job.vdsNS.getStringValue( Pegasus.PMC_REQUEST_CPUS_KEY );
        
        if( value == null ){
            value = job.vdsNS.getStringValue( Pegasus.CORES_KEY );
        }

        if( value != null ){
        
            int cpus = -1;
            
            //sanity check on the value
            try{
                cpus = Integer.parseInt( value );
            }
            catch( Exception e ){
                            /* ignore */
            }

            if ( cpus < 0 ){
                //throw an error for negative value
                complain( "Invalid Value specified for cpu count ", job.getID(), value );
            }

            //add only if we have a +ve memory value
            if( cpus > 0 ){
                result.append( "-c ").append( cpus ).append( " " );
                        
            }

            
        }
        
        return result.toString();
    }

    /**
     * Looks at the profile keys associated with the job to generate the argument
     * string fragment containing the memory required for the job.
     *
     * @param job   the Job for which memory requirements has to be determined.
     *
     * @return  the arguments fragment else empty string
     */
    public String getMemoryRequirementsArgument( Job job ){
        StringBuffer result = new StringBuffer();
        String value = job.vdsNS.getStringValue( Pegasus.PMC_REQUEST_MEMORY_KEY );
        
        //default to memory parameter. both profiles are in MB
        if( value == null ){
            value = job.vdsNS.getStringValue( Pegasus.MEMORY_KEY );
        }

        if( value != null ){

            double memory = -1;

            //sanity check on the value
            try{
                memory = Double.parseDouble( value );
            }
            catch( Exception e ){
                            /* ignore */
            }

            if ( memory < 0 ){
                //throw an error for negative value
                complain( "Invalid Value specified for memory ", job.getID(), value );
            }

            //add only if we have a +ve memory value
            if( memory > 0 ){
                result.append( "-m ").append( (long)memory ).append( " " );

            }


        }

        return result.toString();
    }

    /**
     * Looks at the profile keys associated with the job to generate the argument
     * string fragment containing the priority to be associated for the job.
     * Negative values are allowed
     *
     * @param job   the Job for which memory requirements has to be determined.
     *
     * @return  the arguments fragment else empty string
     */
    public String getPriorityArgument( Job job ){
        StringBuffer result = new StringBuffer();
        String value = job.vdsNS.getStringValue( Pegasus.PMC_PRIORITY_KEY );

        if( value != null ){

            int priority = 0;

            //sanity check on the value
            try{
                priority = Integer.parseInt( value );
            }
            catch( Exception e ){
                 //throw an error for invalid value
                complain( "Invalid Value specified for job priority ", job.getID(), value );
            }


            //=ve values are allowed priorities
            result.append( "-p ").append( priority ).append( " " );

        }

        return result.toString();
    }
    
    /**
     * Looks at the profile key for pegasus::pmc_task_arguments to determine if extra
     * arguments are required for the task.
     * 
     * @param job  the constitunt job for which extra arguments need to be determined
     * @return     the arguments if they are present, or an empty string
     */
    public String getExtraArguments( Job job ) {
        String extra = job.vdsNS.getStringValue(Pegasus.PMC_TASK_ARGUMENTS);
        if (extra == null) {
            return "";
        }
        return extra + " ";
    }

    /**
     * Complains for invalid values passed in profiles
     *
     * @param message   the string describing the error message
     * @param id        id of the job
     * @param value     value of the CPU passed
     */
    private void complain( String message, String id, String value) {
        StringBuffer sb = new StringBuffer();
        sb.append( message ).append( value ).append( " for job " )
          .append( id );

        throw new RuntimeException( sb.toString() );
    }

    

}
