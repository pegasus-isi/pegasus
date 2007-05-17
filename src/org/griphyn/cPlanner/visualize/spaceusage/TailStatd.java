/*
 * This file or a portion of this file is licensed under the terms of
 * the Globus Toolkit Public License, found in file GTPL, or at
 * http://www.globus.org/toolkit/download/license.html. This notice must
 * appear in redistributions of this file, with or without modification.
 *
 * Redistributions of this Software, with or without modification, must
 * reproduce the GTPL in: (1) the Software, or (2) the Documentation or
 * some other similar material which is provided with the Software (if
 * any).
 *
 * Copyright 1999-2004 University of Chicago and The University of
 * Southern California. All rights reserved.
 */
package org.griphyn.cPlanner.visualize.spaceusage;

import org.griphyn.cPlanner.visualize.Callback;

import org.griphyn.cPlanner.common.LogManager;

import java.io.File;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.IOException;

import java.util.StringTokenizer;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Date;

/**
 * The callback parses the jobstate.log file to order the events of how
 * the jobs executed.
 *
 * @author Karan Vahi vahi@isi.edu
 *
 * @version $Revision: 1.3 $
 */

public class TailStatd extends SpaceUsageCallback{

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
     * The directory where all the files reside.
     */
    private String mDirectory ;

    /**
     * A Map store that stores JobSpace objects indexed by the name of the jobs.
     */
    private Map mJobSpaceStore;

    /**
     * The handle to the logging object
     */
    private LogManager mLogger;

    /**
     * The default constructor.
     */
    public TailStatd(){
        super();
        mDirectory = ".";
        mJobSpaceStore = new HashMap();
    }

    /**
     * Initializes the callback.
     *
     * @param directory   the directory where all the files reside.
     * @param useStatInfo  boolean indicating whether to use stat info or not.
     */
    public void initialize( String directory, boolean useStatInfo ){
        super.initialize( directory, useStatInfo );
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
            reader = new BufferedReader(new FileReader(jobstate));

            String line, time = null, job = null, state = null, token;
            int count = 0;
            StringTokenizer st;
            while ( (line = reader.readLine()) != null) {

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

                        default:
                    }
                    count ++;
                }

                if ( !validState( state ) ){
                    //ignore and move to next line
                    continue;
                }

                JobSpace js = ( mJobSpaceStore.containsKey( job ) )?
                                (JobSpace)mJobSpaceStore.get( job ):
                                new JobSpace( job );
                Date d = new Date( Long.parseLong( time ) * 1000 );
                Space s = new Space( d );
                s.setAssociatedJob( job );
                js.addSpaceReading( s, this.getEventType( state ));

                //specific quirk because i am using same trigger for pre job and main job
                if( state.equals( this.GRIDSTART_PREJOB_STATE ) ){
                    //add the same event reading for the main job
                    js.addSpaceReading( (Space)s.clone(), JobSpace.GRIDSTART_MAINJOB_EVENT_TYPE );
                }

                //add the js back
                mJobSpaceStore.put( job, js );
            }
        }
        catch( IOException ioe ){
            throw new RuntimeException( "While reading jobstate file " + jobstate, ioe );
        }

        //System.out.println( "Job space store is " + mJobSpaceStore );
    }

    /**
     * Callback for the starting of an invocation record.
     *
     * @param job      the job/file being parsed.
     * @param resource  the site id where the job was executed.
     */
    public void cbInvocationStart( String job, String resource) {
        mMainJob = job;
        mSite    = resource;

        mJobOutSize = 0;
        //get the one from store!
        mJobSpace = (JobSpace)mJobSpaceStore.get( job );
    }


    /**
     * Parses the content and stores it in a SpaceUsage object.
     * The date is taken from the jobstate.log instead of the date in the record.
     *
     * @param header   the header from which the content was collected.
     * @param content  the Content.
     *
     * @return Space
     */
    protected Space parseContent( String header, String content ){
        String date = null;
        String size = null;

        for ( StringTokenizer st = new StringTokenizer( content ); st.hasMoreTokens(); ){
            if ( date == null ) { date = st.nextToken(); }
            else{
                size = st.nextToken();
                break;
            }
        }

        JobSpace js = (JobSpace)mJobSpaceStore.get( mMainJob );
        Space s = js.getSpaceReading( this.getEventTypeForHeader( header ) );

        if( s ==  null ){
            throw new RuntimeException( "JobState " + js + " did not contain information about header " +
                                          header );
        }

        s.setSize( size );
        return s;
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
     * @param type the state of the job
     *
     * @return the corresponding event type
     */
    private int getEventType( String state ){
        int event = -1;
        if ( state.equals( this.GRIDSTART_PREJOB_STATE ) ){
            event = JobSpace.GRIDSTART_PREJOB_EVENT_TYPE;
        }
        else if ( state.equals( this.GRIDSTART_MAINJOB_STATE ) ){
            event = JobSpace.GRIDSTART_MAINJOB_EVENT_TYPE;
        }
        else if ( state.equals( this.GRIDSTART_POSTJOB_STATE ) ){
            event = JobSpace.GRIDSTART_POSTJOB_EVENT_TYPE;
        }
        return event;
    }
}