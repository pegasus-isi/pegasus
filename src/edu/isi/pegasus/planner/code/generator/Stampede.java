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

import edu.isi.ikcap.workflows.util.logging.LoggingKeys;
import edu.isi.pegasus.common.logging.LogFormatter;
import edu.isi.pegasus.common.logging.LogFormatterFactory;
import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.code.CodeGeneratorException;


import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.AggregatedJob;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.Job;

import edu.isi.pegasus.planner.classes.PCRelation;
import edu.isi.pegasus.planner.classes.PlannerOptions;
import edu.isi.pegasus.planner.code.CodeGenerator;
import edu.isi.pegasus.planner.common.PegasusProperties;

import edu.isi.pegasus.planner.namespace.Dagman;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;

/**
 * A Stampede Events Code Generator that generates events in netlogger format
 * for the exectuable workflow.  This generators generates the events about
 * 
 * <pre>
 *   the tasks int he abstract workflow
 *   the edges in the abstract workflow
 *   jobs in the executable workflow
 *   the edges in the executable workflow
 *   relationship about how the tasks in the abstract workflow map to jobs in the
 *   executable workflow.
 * </pre>
 *
 * @author Karan Vahi
 * @version $Revision: 3409 $
 */
public class Stampede implements CodeGenerator {


    /**
     * The suffix to use while constructing the name of the metrics file
     */
    public static final String NETLOGGER_BP_FILE_SUFFIX = ".static.bp";

    public static final String NETLOGGER_LOG_FORMATTER_IMPLEMENTOR = "Netlogger";

    /**
     * The handle to the netlogger log formatter.
     */
    private LogFormatter mLogFormatter;

    /**
     * The bag of initialization objects.
     */
    protected PegasusBag mBag;


    /**
     * The directory where all the submit files are to be generated.
     */
    protected String mSubmitFileDir;

    /**
     * The object holding all the properties pertaining to Pegasus.
     */
    protected PegasusProperties mProps;

    /**
     * The object containing the command line options specified to the planner
     * at runtime.
     */
    protected PlannerOptions mPOptions;

    /**
     * The handle to the logging object.
     */
    protected LogManager mLogger;

    /**
     * Initializes the Code Generator implementation.
     *
     * @param bag   the bag of initialization objects.
     *
     * @throws CodeGeneratorException in case of any error occuring code generation.
     */
    public void initialize( PegasusBag bag ) throws CodeGeneratorException{
        mBag           = bag;
        mProps         = bag.getPegasusProperties();
        mPOptions      = bag.getPlannerOptions();
        mSubmitFileDir = mPOptions.getSubmitDirectory();
        mLogger        = bag.getLogger();
        mLogFormatter = LogFormatterFactory.loadInstance( NETLOGGER_LOG_FORMATTER_IMPLEMENTOR );
    }

  

    
    /**
     * Generates the code for the executable workflow in terms of a braindump
     * file that contains workflow metadata useful for monitoring daemons etc.
     *
     * @param dag  the concrete workflow.
     *
     * @return the Collection of <code>File</code> objects for the files written
     *         out.
     *
     * @throws CodeGeneratorException in case of any error occuring code generation.
     */
    public Collection<File> generateCode(ADag dag) throws CodeGeneratorException {

        PrintWriter writer = null;
        File f = new File( mSubmitFileDir , Abstract.getDAGFilename( this.mPOptions,
                                                                     dag.dagInfo.nameOfADag,
                                                                     dag.dagInfo.index,
                                                                     Stampede.NETLOGGER_BP_FILE_SUFFIX ) );

        boolean generateCodeForExecutableWorkflow = dag.hasWorkflowRefinementStarted();
        
        String uuid = dag.getWorkflowUUID();
        try {
            writer = new PrintWriter(new BufferedWriter(new FileWriter(f, true) ));
        } catch ( IOException ioe ) {
            throw new CodeGeneratorException( "Unable to intialize writer to netlogger file " , ioe );
        }

        
        if( generateCodeForExecutableWorkflow ){
            //events generation for executable workflow
            for( Iterator<Job> it = dag.jobIterator(); it.hasNext(); ){
                Job job = it.next();
                generateEventsForExecutableJob( writer, dag, job );
            }

            //monte wants the task map events generated separately
            //en mass. Lets iterate again
            for( Iterator<Job> it = dag.jobIterator(); it.hasNext(); ){
                Job job = it.next();
                generateTaskMapEvents( writer, dag, job );
            }


            //write out the edge informatiom for the workflow
            for ( Iterator<PCRelation> it =  dag.dagInfo.relations.iterator(); it.hasNext(); ){
                PCRelation relation = it.next();
                mLogFormatter.addEvent( "job.edge", "wf.id", uuid );

                mLogFormatter.add( "parent_exec_job.id", relation.getParent() );
                mLogFormatter.add( "child_exec_job.id", relation.getChild() );

                writer.println( mLogFormatter.createLogMessage() );
                mLogFormatter.popEvent();
            }


        }
        else{
            //events generation for abstract workflow
            for( Iterator<Job> it = dag.jobIterator(); it.hasNext(); ){
                Job job = it.next();
                generateEventsForDAXTask( writer, dag, job );
            }
            
            //write out the edge informatiom for the workflow
            for ( Iterator<PCRelation> it =  dag.dagInfo.relations.iterator(); it.hasNext(); ){
                PCRelation relation = it.next();
                mLogFormatter.addEvent( "task.edge", "wf.id", uuid );

                mLogFormatter.add( "parent_abs_job.id", relation.getParent() );
                mLogFormatter.add( "child_abs_job.id", relation.getChild() );

                writer.println( mLogFormatter.createLogMessage() );
                mLogFormatter.popEvent();
            }

        }


        writer.close();
        
        Collection<File> result = new LinkedList();
        result.add(f);
        return result;
    }
    
    /**
     * Generates stampede events corresponding to jobs/tasks in the DAX
     * 
     * @param writer  the writer stream to write the events too
     * @param workflow  the  workflow.
     * @param job     the job for which to generate the events.
     */
    protected void generateEventsForDAXTask(PrintWriter writer, ADag workflow, Job job) 
            throws CodeGeneratorException {
            
        String wfuuid = workflow.getWorkflowUUID();
        //sanity check
        if ( !( job.getJobType() == Job.COMPUTE_JOB ||
            job.getJobType() == Job.DAG_JOB ||
            job.getJobType() == Job.DAX_JOB ) ){
            
            //jobs/tasks in the dax can only be of the above types
            throw new CodeGeneratorException( 
                    "Invalid Job Type for a DAX Task while generating Stampede Events of type  " + job.getJobTypeDescription() +
                    " for workflow " + workflow.getAbstractWorkflowName() );
            
        }

        
        mLogFormatter.addEvent( "task", "abs_task.id", job.getLogicalID() );

        mLogFormatter.add( "wf.id" , wfuuid );

        //disconnect??
        mLogFormatter.add( "tasktype", job.getJobTypeDescription() );

        mLogFormatter.add( "executable", job.getCompleteTCName() );
        mLogFormatter.add( "arguments", job.getArguments() );
            
        writer.println( mLogFormatter.createLogMessage() );
        mLogFormatter.popEvent();

    }
    
    
    /**
     * Generates stampede events corresponding to an executable job
     * 
     * @param writer  the writer stream to write the events too
     * @param workflow  the  workflow.
     * @param job     the job for which to generate the events.
     */
    protected void generateEventsForExecutableJob(PrintWriter writer, ADag dag, Job job) 
            throws CodeGeneratorException{
            
        String wfuuid = dag.getWorkflowUUID();
        mLogFormatter.addEvent( "job", "exec_job.id", job.getID() );

        mLogFormatter.add( "wf.id" , wfuuid );

        //disconnect??
        mLogFormatter.add( "submit_file", job.getID() + ".sub" );
        mLogFormatter.add( "jobtype", job.getJobTypeDescription() );

        mLogFormatter.add( "clustered", Boolean.toString( job instanceof AggregatedJob ) );
        mLogFormatter.add( "max_retries",
                           job.dagmanVariables.containsKey( Dagman.RETRY_KEY ) ?
                                            (String)job.dagmanVariables.get( Dagman.RETRY_KEY ):
                                            "0" );

           
        mLogFormatter.add( "executable" , job.getRemoteExecutable() );
        mLogFormatter.add( "arguments" , job.getArguments() );
    
        //determine count of jobs
        int taskCount = getTaskCount( job );

        mLogFormatter.add( "task_count", Integer.toString( taskCount ) );
        writer.println( mLogFormatter.createLogMessage() );
        mLogFormatter.popEvent();

    }
    
    /**
     * Generates the task.map events that link the jobs in the DAX with the
     * jobs in the executable workflow 
     * 
     * 
     * @param writer  the writer stream to write the events too
     * @param workflow  the  workflow.
     * @param job     the job for which to generate the events.
     */
    protected void generateTaskMapEvents(PrintWriter writer, ADag dag, Job job) {

        String wfuuid = dag.getWorkflowUUID();
        //add task map events
        //only compute jobs/ dax and dag jobs have task events associated
        if( job.getJobType() == Job.COMPUTE_JOB ||
            job.getJobType() == Job.DAG_JOB ||
            job.getJobType() == Job.DAX_JOB ){


            if( job instanceof AggregatedJob ){
                AggregatedJob j = (AggregatedJob)job;

                //go through the job constituents and task.map events
                for( Iterator<Job> cit = j.constituentJobsIterator(); cit.hasNext(); ){
                    Job constituentJob = cit.next();
                    if( constituentJob.getJobType() == Job.COMPUTE_JOB ){
                        //create task.map event
                        //to the job in the DAX
                        mLogFormatter.addEvent( "task.map", LoggingKeys.JOB_ID, job.getID() );

                        //to be retrieved
                        mLogFormatter.add( "wf.id" , wfuuid );
                        mLogFormatter.add( "exec_job.id", job.getID() );
                        mLogFormatter.add( "abs_task.id", constituentJob.getLogicalID() );
                        writer.println( mLogFormatter.createLogMessage() );

                        //writer.write( "\n" );
                        mLogFormatter.popEvent();

                    }
                    else{
                        //for time being lets warn
                        mLogger.log( "Constituent Job " + constituentJob.getName() + " not of type compute for clustered job " + j.getName(),
                                      LogManager.WARNING_MESSAGE_LEVEL );

                    }

                }

            }
            else{
                //create a single task.map event that maps compute job
                //to the job in the DAX
                mLogFormatter.addEvent( "task.map", "exec_job.id", job.getID() );

                //to be retrieved
                mLogFormatter.add( "wf.id" , wfuuid );
                mLogFormatter.add( "abs_task.id", job.getLogicalID() );

                writer.println( mLogFormatter.createLogMessage() );
                mLogFormatter.popEvent();
            }
        }
    }


    /**
     * Method not implemented. Throws an exception.
     * 
     * @param dag  the workflow
     * @param job  the job for which the code is to be generated.
     * 
     * @throws edu.isi.pegasus.planner.code.CodeGeneratorException
     */
    public void generateCode( ADag dag, Job job ) throws CodeGeneratorException {
        throw new CodeGeneratorException( "Stampede generator only generates code for the whole workflow" );
    }



    /**
     * Returns the task count for a job. The task count is the number of tasks/jobs
     * in the DAX that map to this job. jobs inserted by Pegasus, which do not
     * have a mapped task from the DAX, will have its task_count set to 0.
     *
     * @param job  the executable job.
     *
     * @return task count
     */
    private int getTaskCount( Job job ) {
        int count = 0;
        int type = job.getJobType();

        if ( job instanceof AggregatedJob && type == Job.COMPUTE_JOB ){
            //a clustered job the number of constituent is count
            count = ((AggregatedJob)job).numberOfConsitutentJobs();
        }
        else if ( type == Job.COMPUTE_JOB ){
            //non clustered job check whether compute or not
            //and make sure there is dax job associated with it
            if( job.getLogicalID().length() == 0 ){
                //takes care of the untar job that is tagged as compute
                mLogger.log( "Not creating event pegasus.task.count for job " + job.getID(),
                             LogManager.DEBUG_MESSAGE_LEVEL );
                count = 0;
            }
            else{
                count = 1;
            }
        }
        return count;
    }

    public boolean startMonitoring() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void reset() throws CodeGeneratorException {
        throw new UnsupportedOperationException("Not supported yet.");
    }


    
}
