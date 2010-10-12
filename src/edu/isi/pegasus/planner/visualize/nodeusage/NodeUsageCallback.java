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


package edu.isi.pegasus.planner.visualize.nodeusage;

import edu.isi.pegasus.common.logging.LogManagerFactory;
import edu.isi.pegasus.common.logging.LogManager;

import edu.isi.pegasus.common.util.Currently;

import edu.isi.pegasus.planner.visualize.Callback;
import edu.isi.pegasus.planner.visualize.JobMeasurements;
import edu.isi.pegasus.planner.visualize.WorkflowMeasurements;

import org.griphyn.vdl.invocation.StatInfo;

import java.util.List;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.Map;
import java.util.HashMap;
import java.util.Date;

import java.io.File;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.IOException;
import org.griphyn.vdl.invocation.Machine;


/**
 * Implements callback interface to calculate node usage or number of jobs
 * over time.
 *
 * @author Karan Vahi
 * @version $Revision$
 */

public class NodeUsageCallback implements Callback {

    /**
     * The name of the tailstatd file.
     */
    public static final String JOBSTATE_LOG = "jobstate.log";

    /**
     * The state in the jobstate that is taken to designate the GRIDSTART_PREJOB
     * time.
     */
    public static final String GRIDSTART_PREJOB_STATE = "EXECUTE";

    /**
     * The state in the jobstate that is taken to designate the GRIDSTART_MAINJOB
     * time.
     */
    public static final String GRIDSTART_MAINJOB_STATE = "EXECUTE";

    /**
     * The state in the jobstate that is taken to designate the GRIDSTART_POSTJOB
     * time.
     */
    public static final String GRIDSTART_POSTJOB_STATE = "JOB_TERMINATED";


    /**
     * The logical site where the job was run.
     */
    protected String mSite;

    /**
     * The WorkflowMeasurements object created during the callback construction.
     */
    protected WorkflowMeasurements mWFMeasurements;

    /**
     * The main job whose record is being parsed.
     */
    protected String mMainJob;

    /**
     * The handle to the logger.
     */
    protected LogManager mLogger;

    /**
     * Stores all the space readings for the current invocation record.
     */
    protected JobMeasurements mJobMeasurements;

    /**
     * A Map store that stores JobMeasurements objects indexed by the name of the jobs.
     */
    private Map mJMStore;


    /**
     * The directory where all the files reside.
     */
    private String mDirectory ;

    /**
     * The number of jobs executing at any given time per site.
     */
    private Map mNumJobsStore;

    /**
     * The default constructor.
     */
    public NodeUsageCallback() {
        mWFMeasurements = new WorkflowMeasurements();
        mJMStore        = new HashMap();
        mNumJobsStore   = new HashMap();
        mLogger =  LogManagerFactory.loadSingletonInstance();
    }

    /**
     * Initializes the callback.
     *
     * @param directory   the directory where all the files reside.
     * @param useStatInfo  boolean indicating whether to use stat info or not.
     */
    public void initialize( String directory , boolean useStatInfo){
        mDirectory = directory;
        File jobstate = new File( directory, this.JOBSTATE_LOG );

        //some sanity checks on file
        if ( jobstate.exists() ){
            if ( !jobstate.canRead() ){
                throw new RuntimeException( "The jobstate file does not exist " + jobstate );
            }
        }
        else{
            throw new RuntimeException( "Unable to read the jobstate file " + jobstate );
        }

        BufferedReader reader ;

        try{
            reader = new BufferedReader( new FileReader( jobstate ) );

            String line, time = null, job = null, state = null, token;
            int count = 0;
            StringTokenizer st;

            while ( (line = reader.readLine()) != null) {
                String site = null;
                //parse the line contents
                st = new StringTokenizer( line );
                count = 1;
                while( st.hasMoreTokens() ){
                    token = ( String )st.nextToken();
                    switch ( count ){
                        case 1:
                            time = token;
                            break;

                        case 2:
                            job = token;
                            break;

                        case 3:
                            state = token;
                            break;

                        case 5:
                            site = token;
                            break;

                        default:
                            break;
                    }
                    count ++;
                }

                if ( !validState( state ) ){
                    //ignore and move to next line
                    continue;
                }

                JobMeasurements js = ( mJMStore.containsKey( job ) )?
                                (JobMeasurements)mJMStore.get( job ):
                                new JobMeasurements( job );
                Date d = new Date( Long.parseLong( time ) * 1000 );
                //add date for that particular event
                js.setTime( d, this.getEventType( state ) );

                if( state.equals( this.GRIDSTART_MAINJOB_STATE ) ){
                    //increment value
                    int num = this.getCurrentNumOfJobs( site );
                    mNumJobsStore.put( site, new Integer( ++num ) );
                    mWFMeasurements.addMeasurement( site,
                        new NumJobsMeasurement(d, new Integer(num), job ) );
                }
                else if( state.equals( this.GRIDSTART_POSTJOB_STATE ) ){
                    //decrement value
                    int num = this.getCurrentNumOfJobs( site );
                    mNumJobsStore.put( site, new Integer( --num ) );
                    mWFMeasurements.addMeasurement( site,
                        new NumJobsMeasurement(d, new Integer(num), job ) );
                }



 //               Space s = new Space( d );
 //               s.setAssociatedJob( job );
 //               js.addSpaceReading( s, this.getEventType( state ));

                //specific quirk because i am using same trigger for pre job and main job
 //               if( state.equals( this.GRIDSTART_PREJOB_STATE ) ){
 //                   //add the same event reading for the main job
 //                   js.addSpaceReading( (Space)s.clone(), JobMeasurements.GRIDSTART_MAINJOB_EVENT_TYPE );
 //               }

                //add the js back
                mJMStore.put( job, js );
            }
        }
        catch( IOException ioe ){
            throw new RuntimeException( "While reading jobstate file " + jobstate, ioe );
        }

        //System.out.println( "Job space store is " + mJMStore );

    }


    public void cbInvocationStart( String job, String resource) {
        mMainJob = job;
        mSite    = resource;
        mJobMeasurements = new JobMeasurements( job );
    }


    public void cbStdIN(List jobs, String data) {

    }


    public void cbStdOut(List jobs, String data) {
    }

    public void cbStdERR(List jobs, String data) {

    }

    /**
     * Returns the number of jobs that are executing for a particular site
     *
     * @param site  the name of the site.
     *
     * @return number of jobs
     */
    private int getCurrentNumOfJobs( String site ){
        if( site == null ){ throw new RuntimeException( "Null site specified");}

        int value = 0;

        if( mNumJobsStore.containsKey( site) ){
            value = ((Integer)mNumJobsStore.get( site )).intValue();
        }
        else{
            mNumJobsStore.put( site, new Integer(value) );
        }

        return value;
    }

    /**
     * Callback function for when stat information for an input file is
     * encountered. Empty for time being.
     *
     * @param filename  the name of the file.
     * @param info      the <code>StatInfo</code> about the file.
     *
     */
    public void cbInputFile( String filename, StatInfo info ){
        //do nothing
    }

    /**
     * Callback function for when stat information for an output file is
     * encountered. The size of the file is computed and stored.
     *
     * @param filename  the name of the file.
     * @param info      the <code>StatInfo</code> about the file.
     *
     */
    public void cbOutputFile( String filename, StatInfo info ){
        //do nothing
    }


    /**
     * Callback signalling that an invocation record has been parsed.
     * Stores the total compute size, somewhere in the space structure
     * for the jobs.
     *
     *
     */
    public void cbInvocationEnd() {

    }

    /**
     * Returns the SpaceUsage store built.
     *
     * @return SpaceUsage
     */
    public Object getConstructedObject() {
        return mWFMeasurements;
    }




   /**
    * Callback signalling that we are done with the parsing of the files.
    */
   public void done(){
       mWFMeasurements.sort();
   }

   /**
     * Returns a boolean indicating whether the state is valid or not.
     *
     * @param state  the state
     *
     * @return boolean
     */
    protected boolean validState( String state ){
        return ( state.equals( this.GRIDSTART_MAINJOB_STATE ) ||
                 state.equals( this.GRIDSTART_POSTJOB_STATE ) ||
                 state.equals( this.GRIDSTART_PREJOB_STATE )
                 );
    }

    /**
     * Returns the event type matching a particular job type
     *
     * @param state the state of the job
     *
     * @return the corresponding event type
     */
    private int getEventType( String state ){
        int event = -1;
        if ( state.equals( this.GRIDSTART_PREJOB_STATE ) ){
            event = JobMeasurements.GRIDSTART_PREJOB_EVENT_TYPE;
        }
        else if ( state.equals( this.GRIDSTART_MAINJOB_STATE ) ){
            event = JobMeasurements.GRIDSTART_MAINJOB_EVENT_TYPE;
        }
        else if ( state.equals( this.GRIDSTART_POSTJOB_STATE ) ){
            event = JobMeasurements.GRIDSTART_POSTJOB_EVENT_TYPE;
        }
        return event;
    }



    /**
     * Returns boolean indicating whether the job is a cleanup job or not.
     * Does it on the basis of the name of the job.
     *
     * @param name  the name  of the job.
     *
     * @return boolean
     */
    public boolean cleanupJob( String name ){
        return name.startsWith( "clean_up_" );
    }

    /**
     * Callback for the metadata retrieved from the kickstart record.
     * 
     * @param metadata
     */
    public void cbMetadata( Map metadata ){
        
    }

    /**
     * Callback to pass the machine information on which the job is executed.
     * 
     * @param machine
     */
    public void cbMachine( Machine machine ){
        
    }
}
