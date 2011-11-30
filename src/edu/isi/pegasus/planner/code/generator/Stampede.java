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

import edu.isi.pegasus.planner.refiner.DeployWorkerPackage;
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
 * @version $Revision$
 */
public class Stampede implements CodeGenerator {


    /**
     * The suffix to use while constructing the name of the metrics file
     */
    public static final String NETLOGGER_BP_FILE_SUFFIX = ".static.bp";

    public static final String NETLOGGER_LOG_FORMATTER_IMPLEMENTOR = "Netlogger";

    
    /**
     * The attribute key for workflow id.
     */
    public static final String WORKFLOW_ID_KEY = "xwf.id";
    
    /**
     * The event name for task info
     */
    public static final String TASK_EVENT_NAME = "task.info";
    
    /**
     * The attribute key for task id
     */
    public static final String TASK_ID_KEY = "task.id";
    
    /**
     * The attribute key for task type
     */
    public static final String TYPE_KEY = "type";
    
    /**
     * The attribute key for type description
     */
    public static final String TYPE_DESCRIPTION_KEY = "type_desc";
    
    /**
     * The attribute key for transformation
     */
    public static final String TASK_TRANSFORMATION_KEY = "transformation";
    
    /**
     * The attribute key for task arguments.
     */
    public static final String ARGUMENTS_KEY = "argv";
    
    /**
     * The event name for task edge
     */
    public static final String TASK_EDGE_EVENT_NAME = "task.edge";
    
    /**
     * The atrribute key for parent task id.
     */
    public static final String PARENT_TASK_ID_KEY = "parent.task.id";
    
    
    /**
     * The atrribute key for child task id.
     */
    public static final String CHILD_TASK_ID_KEY = "child.task.id";
    
    /**
     * The event name for a job
     */
    public static final String JOB_EVENT_NAME = "job.info";
    
    /**
     * The attribute key for job id
     */
    public static final String JOB_ID_KEY = "job.id";
    
    
    /**
     * Teh attribute key for the submit file
     */
    public static final String JOB_SUBMIT_FILE_KEY = "submit_file";
    
    /**
     * The attribute key for whether a job is clustered or not
     */
    public static final String JOB_CLUSTERED_KEY = "clustered";
    
    /**
     * The attribute key for how many times a job is retried
     */
    public static final String JOB_MAX_RETRIES_KEY = "max_retries";
    
    /**
     * The attribute key for the number of tasks in the job
     */
    public static final String JOB_TASK_COUNT_KEY = "task_count";
    
    
    /**
     * The attribute key for the executable
     */
    public static final String JOB_EXECUTABLE_KEY = "executable";
    
     /**
     * The event name for job edge
     */
    public static final String JOB_EDGE_EVENT_NAME = "job.edge";
    
    /**
     * The atrribute key for parent job id.
     */
    public static final String PARENT_JOB_ID_KEY = "parent.job.id";
    
    /**
     * The atrribute key for child job id.
     */
    public static final String CHILD_JOB_ID_KEY = "child.job.id";
    
    /**
     * The event name for task map event
     */
    public static final String TASK_MAP_EVENT_NAME = "wf.map.task_job";
    
    
    
    
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
                mLogFormatter.addEvent( Stampede.JOB_EDGE_EVENT_NAME, Stampede.WORKFLOW_ID_KEY, uuid );

                mLogFormatter.add( Stampede.PARENT_JOB_ID_KEY, relation.getParent() );
                mLogFormatter.add( Stampede.CHILD_JOB_ID_KEY, relation.getChild() );

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
                mLogFormatter.addEvent( Stampede.TASK_EDGE_EVENT_NAME, Stampede.WORKFLOW_ID_KEY, uuid );

                mLogFormatter.add( Stampede.PARENT_TASK_ID_KEY, relation.getAbstractParentID() );
                mLogFormatter.add( Stampede.CHILD_TASK_ID_KEY, relation.getAbstractChildID() );

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

        
        mLogFormatter.addEvent( Stampede.TASK_EVENT_NAME, Stampede.WORKFLOW_ID_KEY , wfuuid );

        mLogFormatter.add( Stampede.TASK_ID_KEY, job.getLogicalID() );

        mLogFormatter.add( Stampede.TYPE_KEY, Integer.toString( job.getJobType() ));
        mLogFormatter.add( Stampede.TYPE_DESCRIPTION_KEY, job.getJobTypeDescription() );

        mLogFormatter.add( Stampede.TASK_TRANSFORMATION_KEY, job.getCompleteTCName() );
        
        //only add arguments attribute if arguments are not 
        //null and length > 0 . Job constructor initializes arguments to ""
        if( job.getArguments() != null && job.getArguments().length() > 0 ){
            mLogFormatter.add( Stampede.ARGUMENTS_KEY, job.getArguments() );
        }
        
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
        mLogFormatter.addEvent( Stampede.JOB_EVENT_NAME, Stampede.WORKFLOW_ID_KEY , wfuuid  );

        mLogFormatter.add( Stampede.JOB_ID_KEY, job.getID() );

        mLogFormatter.add( Stampede.JOB_SUBMIT_FILE_KEY, job.getID() + ".sub" );
        mLogFormatter.add( Stampede.TYPE_KEY, Integer.toString( job.getJobType() ));
        mLogFormatter.add( Stampede.TYPE_DESCRIPTION_KEY, job.getJobTypeDescription() );


        mLogFormatter.add( Stampede.JOB_CLUSTERED_KEY, booleanToInt( job instanceof AggregatedJob ) );
        mLogFormatter.add( Stampede.JOB_MAX_RETRIES_KEY,
                           job.dagmanVariables.containsKey( Dagman.RETRY_KEY ) ?
                                            (String)job.dagmanVariables.get( Dagman.RETRY_KEY ):
                                            "0" );

           
        mLogFormatter.add( Stampede.JOB_EXECUTABLE_KEY , job.getRemoteExecutable() );
        
        //only add arguments attribute if arguments are not 
        //null and length > 0 . Job constructor initializes arguments to ""
        if( job.getArguments() != null && job.getArguments().length() > 0 ){
            mLogFormatter.add( Stampede.ARGUMENTS_KEY , job.getArguments() );
        }
    
        //determine count of jobs
        int taskCount = getTaskCount( job );

        mLogFormatter.add( Stampede.JOB_TASK_COUNT_KEY, Integer.toString( taskCount ) );
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

            // untar jobs created as part of worker package staging
            //are of type compute but we don't want
            if( job.getLogicalID() == null || job.getLogicalID().isEmpty() ){
                //dont warn if a job is compute and transformation name is untar
                if( job.getJobType() == Job.COMPUTE_JOB &&
                    job.getCompleteTCName().equals( DeployWorkerPackage.COMPLETE_UNTAR_TRANSFORMATION_NAME ) ){
                    //dont do anything
                    return;
                }
                else{
                    //warn and return
                    mLogger.log( "No corresponding DAX task for compute job " + job.getName() ,
                                 LogManager.WARNING_MESSAGE_LEVEL );
                    return;
                }
            }


            if( job instanceof AggregatedJob ){
                AggregatedJob j = (AggregatedJob)job;

                //go through the job constituents and task.map events
                for( Iterator<Job> cit = j.constituentJobsIterator(); cit.hasNext(); ){
                    Job constituentJob = cit.next();
                    if( constituentJob.getJobType() == Job.COMPUTE_JOB ){
                        //create task.map event
                        //to the job in the DAX
                        mLogFormatter.addEvent( Stampede.TASK_MAP_EVENT_NAME, Stampede.WORKFLOW_ID_KEY , wfuuid );

                        //to be retrieved
                        mLogFormatter.add( Stampede.JOB_ID_KEY, job.getID() );
                        //mLogFormatter.add( "exec_job.id", job.getID() );
                        mLogFormatter.add( Stampede.TASK_ID_KEY, constituentJob.getLogicalID() );
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
                mLogFormatter.addEvent( Stampede.TASK_MAP_EVENT_NAME, Stampede.WORKFLOW_ID_KEY , wfuuid  );

                //to be retrieved
                mLogFormatter.add( Stampede.JOB_ID_KEY, job.getID() );
                mLogFormatter.add( Stampede.TASK_ID_KEY, job.getLogicalID() );

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

    /**
     * Returns boolean as an integer
     * 
     * @param value the boolean value
     * 
     * @return 0 for false and 1 for true
     */
    public String booleanToInt( boolean value ){
        return   value ? "1" : "0";
    }
    
    public boolean startMonitoring() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void reset() throws CodeGeneratorException {
        throw new UnsupportedOperationException("Not supported yet.");
    }


    
}
