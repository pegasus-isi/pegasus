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

package edu.isi.pegasus.planner.cluster.aggregator;


import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.AggregatedJob;

import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.namespace.Globus;
import edu.isi.pegasus.planner.namespace.Pegasus;
import edu.isi.pegasus.planner.partitioner.graph.Graph;
import edu.isi.pegasus.planner.partitioner.graph.GraphNode;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Iterator;

/**
 * This class aggregates the smaller jobs in a manner such that
 * they are launched at remote end, by mpiexec on n nodes where n is the nodecount
 * associated with the aggregated job that is being lauched by mpiexec.
 * The executable mpiexec is a Pegasus tool distributed in the Pegasus worker package, and
 * can be usually found at $PEGASUS_HOME/bin/mpiexec.
 *
 * @author Karan Vahi vahi@isi.edu
 * @version $Revision$
 */

public class MPIExec extends Abstract {

    /**
     * The logical name of the transformation that is able to run multiple
     * jobs via mpi.
     */
    public static final String COLLAPSE_LOGICAL_NAME = "mpiexec";

    /**
     * The basename of the executable that is able to run multiple
     * jobs via mpi.
     */
    public static String EXECUTABLE_BASENAME = "pegasus-mpi-cluster";


    /**
     * The default constructor.
     */
    public MPIExec(){
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
        String computedRuntime = (String)job.vdsNS.removeKey( Pegasus.RUNTIME_KEY );
        if( computedRuntime == null ){
            //remove the globus maxwalltime if set
            computedRuntime = (String)job.globusRSL.removeKey( Globus.MAX_WALLTIME_KEY );
        }
        
        String computedMemory  = (String)job.vdsNS.removeKey( Pegasus.MEMORY_KEY );
        if( computedMemory == null ){
            //remove the globus maxmemory if set
            //memory for the PMC job should be set with pmc executable
            computedMemory = (String)job.globusRSL.removeKey( Globus.MAX_MEMORY_KEY );
        }
        
        super.makeAbstractAggregatedJobConcrete(job);

        //only do something for the runtime if runtime is not 
        //picked up from other profile sources for PMC jobs
        if( computedRuntime != null &&
                !( job.globusRSL.containsKey( Globus.MAX_WALLTIME_KEY ) ||
                        ( job.vdsNS.containsKey( Pegasus.RUNTIME_KEY) ) )) {
            
            //do some estimation here for the runtime?
            
        }
        
        //also put in jobType as mpi only if a user has not specified
        //any other jobtype before hand
        if( !job.globusRSL.containsKey( "jobtype" ) ){
            job.globusRSL.checkKeyInNS("jobtype","mpi");
        }

        //reset the stdin as we use condor file io to transfer
        //the input file
        String stdin = job.getStdIn();
        job.setStdIn( "" );
        
        //slight cheating here.
        File stdinFile = new File( mDirectory, stdin );

        job.condorVariables.addIPFileForTransfer( stdinFile.getAbsolutePath() );
        return;
    }

    /**
     * Writes out the input file for the aggregated job
     *
     * @param job   the aggregated job
     *
     * @return path to the input file
     */
    protected File writeOutInputFileForJobAggregator(AggregatedJob job) {
        return this.generatePMCInputFile(job, job.getID() + ".in", job.getRelativeSubmitDirectory(), true );
    }


    /**
     * Writes out the input file for the aggregated job
     *
     * @param job   the aggregated job
     * @param name  the name of PMC file to be generated
     * @param relativeDir relative submit directory for the job 
     * @param isClustered  a boolean indicating whetehre the graph belongs to a
     *                     clustered job or not.
     *
     * @return path to the input file
     */
    public File generatePMCInputFile(Graph job, String name , String relativeDir, boolean isClustered ) {
        File stdIn = null;
        try {
            BufferedWriter writer;
            //PM-1261 the .in file should be in the same directory where all job submit files go
            File directory = new File( this.mDirectory, relativeDir );
            stdIn = new File( directory ,name);
            writer = new BufferedWriter(new FileWriter( stdIn ));

            //traverse throught the jobs to determine input/output files
            //and merge the profiles for the jobs
            int taskid = 1;
            for( Iterator<GraphNode> it = job.nodeIterator(); it.hasNext(); taskid++ ) {
                GraphNode node = it.next();
                Job constitutentJob = (Job) node.getContent();



                //handle stdin
                if( constitutentJob instanceof AggregatedJob ){
                    //slurp in contents of it's stdin
                    File file = new File ( mDirectory, constitutentJob.getStdIn() );
                    BufferedReader reader = new BufferedReader(
                                                             new FileReader( file )
                                                               );
                    String line;
                    while( (line = reader.readLine()) != null ){
                        //ignore comment out lines
                        if( line.startsWith( "#" ) ){
                            continue;
                        }
                        writer.write( line );
                        writer.write( "\n" );
                        taskid++;
                    }
                    reader.close();
                    //delete the previous stdin file
                    file.delete();
                }
                else{
                    //write out the argument string to the
                    //stdin file for the fat job

                    //genereate the comment string that has the
                    //taskid transformation derivation
                    writer.write( getCommentString( constitutentJob, taskid ) + "\n" );

                    //the id associated with the task is dependant on whether
                    //the input file is generated for the whole workflow or
                    //a clustered job. PM-660
                    StringBuffer task = new StringBuffer();
                    task.append( "TASK" ).append( " " ).
                         append( isClustered?
                                constitutentJob.getLogicalID(): //for file generation for a clustered job we want the ID in the DAX
                                constitutentJob.getID() //for file generation as part of PMC code generator we want the pegasus assigned job id
                                ).append( " " );

                    //check and add if a job has requested any memory or cpus
                    //JIRA PM-601, PM-620 and PM-621
                    task.append( getMemoryRequirementsArgument( constitutentJob ) );
                    task.append( getCPURequirementsArgument( constitutentJob ) );
                    task.append( getPriorityArgument( constitutentJob ) );

                    //PM-654 post add the arguments if any specified by pmc_arguments
                    // This needs to go after all other arguments
                    task.append( getExtraArguments( constitutentJob ) );
                    
                    task.append( constitutentJob.getRemoteExecutable() ).append( " " ).
                         append(  constitutentJob.getArguments() ).append( "\n" );
                    writer.write( task.toString() );
                }
            }

            writer.write( "\n" );

            //lets write out the edges
            for( Iterator<GraphNode> it = job.nodeIterator(); it.hasNext() ; ){
                GraphNode gn = (GraphNode) it.next();

                //get a list of parents of the node
                for( GraphNode child : gn.getChildren() ){
                    StringBuffer edge = new StringBuffer();
                    edge.append(  "EDGE" ).append( " " ).append( gn.getID() ).
                         append( " " ).append( child.getID() ).append( "\n" );
                    writer.write( edge.toString() );
                }
            }

            //closing the handle to the writer
            writer.close();
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
        return MPIExec.EXECUTABLE_BASENAME;
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
        return this.entryNotInTC( MPIExec.TRANSFORMATION_NAMESPACE,
                                  MPIExec.COLLAPSE_LOGICAL_NAME,
                                  MPIExec.TRANSFORMATION_VERSION,
                                  this.getClusterExecutableBasename(),
                                  site);
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
        String stdin  = job.getStdIn();
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
        
        if( walltime != null ){
            long value = -1;

            try{
                value = Long.parseLong(walltime );
            }
            catch( Exception e ){
                //ignore
            }

            //walltime is specified in minutes
            if( value > 1 ){
                value = value / divisor;
                if( value > 10 ){
                    //subtract 5 minutes to give PMC a chance to return all stdouts
                    //do this only if walltime is at least more than 10 minutes
                    value = ( value - 5);
                }
                args.append( "--max-wall-time " ).append( value ).append(" ");
            }
        }
        
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
     * passed, as MPIExec does not handle it for time being.
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
