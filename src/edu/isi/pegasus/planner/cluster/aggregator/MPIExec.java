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
import edu.isi.pegasus.planner.namespace.Pegasus;
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
        super.makeAbstractAggregatedJobConcrete(job);

        //also put in jobType as mpi only if a user has not specified
        //any other jobtype before hand
        if( !job.globusRSL.containsKey( "jobtype" ) ){
            job.globusRSL.checkKeyInNS("jobtype","mpi");
        }

        
        job.setArguments( this.aggregatedJobArguments( job ) );
        
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
        File stdIn = null;
        try {
            BufferedWriter writer;
            String name = job.getID() + ".in";
            stdIn = new File(mDirectory,name);
            writer = new BufferedWriter(new FileWriter( stdIn ));

            //traverse throught the jobs to determine input/output files
            //and merge the profiles for the jobs
            int taskid = 1;
            for( Iterator it = job.constituentJobsIterator(); it.hasNext(); taskid++ ) {
                Job constitutentJob = (Job) it.next();



                //handle stdin
                if( constitutentJob instanceof AggregatedJob ){
                    //slurp in contents of it's stdin
                    File file = new File ( mDirectory, job.getStdIn() );
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

                    StringBuffer task = new StringBuffer();
                    task.append( "TASK" ).append( " " ).append( constitutentJob.getLogicalID() ).append( " " );

                    //check and add if a job has requested any memory
                    //JIRA PM-601
                    String memory = constitutentJob.vdsNS.getStringValue( Pegasus.REQUEST_MEMORY_KEY );
                    if( memory != null ){
                        task.append( "-m ").
                             append( memory ).append( " " );
                    }

                    task.append( constitutentJob.getRemoteExecutable() ).append( " " ).
                         append(  constitutentJob.getArguments() ).append( "\n" );
                    writer.write( task.toString() );
                }
            }

            writer.write( "\n" );

            //lets write out the edges
            for( Iterator it = job.nodeIterator(); it.hasNext() ; ){
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
        
        //construct any extra arguments specified in profiles
        //or properties
        String extraArgs = job.vdsNS.getStringValue( Pegasus.CLUSTER_ARGUMENTS );
        
        if( extraArgs != null ){
            args.append( extraArgs ).append( " " );
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

}
