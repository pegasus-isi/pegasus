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


package org.griphyn.cPlanner.visualize.spaceusage;

import org.griphyn.cPlanner.common.LogManager;

import org.griphyn.common.util.Currently;

import org.griphyn.cPlanner.visualize.Callback;

import org.griphyn.vdl.invocation.StatInfo;

import java.util.List;
import java.util.Iterator;
import java.util.Map;
import java.util.StringTokenizer;

/**
 * Implements callback interface to calculate space usage.
 *
 * @author not attributable
 * @version 1.0
 */

public class SpaceUsageCallback implements Callback {

    /**
     * The marker for the PREJOB. The stdout corresponding to the
     * PREJOB is enclosed within this marker.
     */
    public static final String PREJOB_MARKER = "@@@PREJOB@@@";

    /**
     * The marker for the MAINJOB. The stdout corresponding to the
     * MAINJOB is enclosed within this marker.
     */
    public static final String MAINJOB_MARKER = "@@@MAINJOB@@@";


    /**
     * The marker for the POSTJOB. The stdout corresponding to the
     * POSTJOB is enclosed within this marker.
     */
    public static final String POSTJOB_MARKER = "@@@POSTJOB@@@";


    /**
     * The logical site where the job was run.
     */
    protected String mSite;

    /**
     * The SpaceUsage object created during the callback construction.
     */
    protected SpaceUsage mSpaceStore;

    /**
     * The main job whose record is being parsed.
     */
    protected String mMainJob;

    /**
     * The handle to the logger.
     */
    protected LogManager mLogger;

    /**
     * Boolean indicating whether to use stat data or not for computing directory
     * sizes.
     */
    protected boolean mUseStatInfo;

    /**
     * Stores in bytes the size of all the output files for a job.
     */
    protected long mJobOutSize;

    /**
     * Stores in bytes the size of all the input files for a job.
     */
    protected long mJobInSize;


    /**
     * Stores all the space readings for the current invocation record.
     */
    protected JobSpace mJobSpace;

    /**
     * The default constructor.
     */
    public SpaceUsageCallback() {
        mSpaceStore = new SpaceUsage();
        mLogger = LogManager.getInstance();
        mUseStatInfo = false;
        mJobOutSize = 0;
        mJobInSize = 0;
    }

    /**
     * Initializes the callback.
     *
     * @param directory   the directory where all the files reside.
     * @param useStatInfo  boolean indicating whether to use stat info or not.
     */
    public void initialize( String directory , boolean useStatInfo){
        mUseStatInfo = useStatInfo;
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
        mJobSpace = new JobSpace( job );
    }


    public void cbStdIN(List jobs, String data) {

    }


    public void cbStdOut(List jobs, String data) {
        this.parseData( data );
    }

    public void cbStdERR(List jobs, String data) {

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
        if( mUseStatInfo ){
            //sanity check
            if( info == null){
                //log a warning
                mLogger.log( "No stat info for input file " + filename,
                             LogManager.WARNING_MESSAGE_LEVEL );
                return;
            }

            //increment the size to the already stored size.
            mJobInSize += info.getSize();
            mLogger.log( "\tInput file is " + filename + " of size " + info.getSize() ,
                         LogManager.DEBUG_MESSAGE_LEVEL);
        }

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
        if( mUseStatInfo ){
            //sanity check
            if( info == null){
                //log a warning
                mLogger.log( "No stat info for output file " + filename,
                             LogManager.WARNING_MESSAGE_LEVEL );
                return;
            }

            //increment the size to the already stored size.
            mJobOutSize += info.getSize();
            mLogger.log( "\tOutput file is " + filename + " of size " + info.getSize() ,
                         LogManager.DEBUG_MESSAGE_LEVEL);
        }
    }


    /**
     * Callback signalling that an invocation record has been parsed.
     * Stores the total compute size, somewhere in the space structure
     * for the jobs.
     *
     *
     */
    public void cbInvocationEnd() {
        //if we are using statinfo
        if( mUseStatInfo ){
            //we get just the post job record, and put the mJobOut size in
            //there
            Space s = mJobSpace.getSpaceReading( JobSpace.GRIDSTART_POSTJOB_EVENT_TYPE );


            if( s == null ){
                mLogger.log( "No space reading for job " + mMainJob,
                            LogManager.WARNING_MESSAGE_LEVEL );
                return;
            }

            if( cleanupJob( mMainJob ) ){
                float size = (float)mJobInSize;
                size = size/1024;
                //for cleanup jobs we take input size
                s.setCleanupFlag( true );
                s.setSize( size , 'K' );
                mLogger.log( "For job " + mMainJob + " total input file size in K " +
                             size, LogManager.DEBUG_MESSAGE_LEVEL );
                mSpaceStore.addRecord( mSite, s ) ;
                //we add a duplicate space reading that gets populated
                //later with the reading of directory after the cleanup job
                Space s1 = (Space)s.clone();
                s1.setCleanupFlag( false );
                s1.setSize( "0" );
                mSpaceStore.addRecord( mSite, s1 );
            }
            else{
                //for all other jobs we take output sizes
                float size = (float)mJobOutSize;
                size = size/1024;
                s.setSize( size , 'K' );
                mSpaceStore.addRecord( mSite, s ) ;
                mLogger.log( "For job " + mMainJob + " total output file size in K " +
                             size, LogManager.DEBUG_MESSAGE_LEVEL );
            }

        }
        else{
            //we put all the valid records into the space store
            for( Iterator it = mJobSpace.spaceReadingsIterator(); it.hasNext(); ){
                Object obj = it.next();
                if( obj == null ){
                    //go to next reading
                    continue;
                }
                mSpaceStore.addRecord( mSite, (Space)obj );
            }
        }

        //reset per site data
        mSite = "";
        mMainJob = "";
        mJobOutSize = 0;
        mJobInSize  = 0;
        mJobSpace = null;
    }

    /**
     * Returns the SpaceUsage store built.
     *
     * @return SpaceUsage
     */
    public Object getConstructedObject() {
        return mSpaceStore;
    }


   /**
    * Parses the data in the data section.
    *
    * @param data String
    */
   private void parseData ( String data ) {
       //sanity check
       if ( data == null ){ return; }

       //System.out.println( "DATA is " + data );

       //parse through
       String token;
       String header = null;
       StringBuffer content = new StringBuffer();

       boolean start = false;
       for( StringTokenizer st = new StringTokenizer(data); st.hasMoreTokens(); ){
           token = st.nextToken();
           if ( validHeader( token ) ){

               //if start is true we have one job data
               if( start ){

                   //token needs to match the previous header
                   if( token.equals( header ) ){

                       mLogger.log( "Content before parsing " + content,
                                    LogManager.DEBUG_MESSAGE_LEVEL );

                       Space s = parseContent( header, content.toString() );

                       //if the content was set for the MAINJOB
                       //set the marker to be true
                       if ( token.equals( this.MAINJOB_MARKER ) ){
                           s.setCleanupFlag( true );
                       }

//                       mSpaceStore.addRecord( mSite, s ) ;
                       mJobSpace.addSpaceReading( s, this.getEventTypeForHeader( header ));

                       mLogger.log( "Content after parsing is " + s,
                                    LogManager.DEBUG_MESSAGE_LEVEL );
                       start = !start;
                       content = new StringBuffer();
                   }
                   else{
                       /* error */
                       throw new RuntimeException(
                            "Incorrect placement of markers in stdout (" +
                           header + " , " + token + " )" );

                   }


                   continue;

               }

               //token is a valid header
               header = token;

               //header is matched.
               start = !start;
           }
           else if ( start ){
               //we have already matched a header.
               content.append( token ).append( " " );
           }
       }
   }

   /**
    * Callback signalling that we are done with the parsing of the files.
    */
   public void done(){
       mSpaceStore.sort();

       //we have all the records.
       //need to do some mischief in case of using statinfo
       if( mUseStatInfo ){
           //go thru space usage for each site.
           for( Iterator it = mSpaceStore.siteIterator(); it.hasNext(); ){
               String site = (String) it.next();

               float dir_size = 0;

               //go through space usage for a particular site
               for (Iterator sizeIT = mSpaceStore.getSizes( site ).iterator();  sizeIT.hasNext(); ) {
                   Space s = (Space) sizeIT.next();

                   if( s.getCleanupFlag() ){
                       //subtract directory size
                       dir_size -= s.getSize( 'K' );

                   }
                   else{
                       dir_size += s.getSize( 'K' );

                       //set the size back
                       s.setSize( dir_size, 'K' );
                   }
                   mLogger.log( "Directory size after job " + s.getAssociatedJob() + " in K is " + dir_size,
                                LogManager.DEBUG_MESSAGE_LEVEL );


               }

           }

       }
   }

    /**
     * Parses the data in the data section.
     *
     * @param data String
     */
     /*
    private void parseData ( String data ) {
        int length = ( data == null ) ? 0 : data.length();

        System.out.println( "Data is " + data );

        String header = PREJOB_MARKER;
        StringBuffer content = new StringBuffer();
        boolean start = false; boolean end = true;
        for ( int i = 0; i < length; i++){
            char c = data.charAt( i );

            if ( c == '@' ){
                //see if look ahead matches
                if ( i + header.length() < length &&
                     ( data.substring( i, i + header.length()).equals( header ) )
                     ){

                        //if start is true we have one job data
                        if ( start ) {

                            //we are capturing date for post jobs only
                            if ( header.equalsIgnoreCase( POSTJOB_MARKER ) ){
                                System.out.println("Content before parsing " +
                                                   content);
                                Space s = parseContent(content.toString());
                                mSpaceStore.addRecord(mSite, s);
                                System.out.println("CONTENT IS " + s);
                            }
                            content = new StringBuffer();
                            start = false; end = true;

                            //skip to the character after the header
                            i = i + header.length() - 1;
                            header = POSTJOB_MARKER;
                            continue;
                        }

                        //header is matched.
                        start = !start;
                        end   = !end;

                        //skip to the character after the header
                        i = i + header.length() - 1;
                    }
                else if ( start ) { content.append( c ); }
            }
            else if ( start ){
                //add to content
                content.append( c );
            }
        }
    }
    */


    /**
     * Returns a boolean indicating whether the token passed matches
     * a header or not. In the specific case of using statinfo, for calculating
     * directory sizes, we only mainjob and postjob markers are valid.
     *
     * @param token  the token to be matched.
     * @param state  the current header being processed
     *
     * @return boolean
     */
    protected boolean validHeader( String token ){
        return ( this.mUseStatInfo ) ?
               //only two headers are valid.
               (token.equals( this.MAINJOB_MARKER ) || token.equals( this.POSTJOB_MARKER )):
               //all three headers are valid.
               (token.equals( this.MAINJOB_MARKER ) || token.equals( this.PREJOB_MARKER )
                || token.equals( this.POSTJOB_MARKER ));

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
        return name.startsWith( "cln_" );
    }

    /**
     * Parses the content and stores it in a Space object.
     *
     * @param header   the header from which the content was collected.
     * @param content  the Content.
     *
     * @return Space
     */
    protected Space parseContent( String header, String content ){
        String date = null;
        String size = null;
        Space s ;
        for ( StringTokenizer st = new StringTokenizer( content ); st.hasMoreTokens(); ){
            if ( date == null ) { date = st.nextToken(); }
            else{
                size = st.nextToken();
                break;
            }
        }
        s = new Space( Currently.parse(date), size );
        s.setAssociatedJob( mMainJob );
        return s;
    }

    /**
     * Returns the event type matching a header.
     *
     * @param header
     *
     * @return the corresponding event type
     */
    protected int getEventTypeForHeader( String marker ){
        int event = -1;
        if ( marker.equals( this.PREJOB_MARKER ) ){
            event = JobSpace.GRIDSTART_PREJOB_EVENT_TYPE;
        }
        if ( marker.equals( this.MAINJOB_MARKER ) ){
            event = JobSpace.GRIDSTART_MAINJOB_EVENT_TYPE;
        }
        if ( marker.equals( this.POSTJOB_MARKER ) ){
            event = JobSpace.GRIDSTART_POSTJOB_EVENT_TYPE;
        }
        return event;
    }

	public Map cbGetMetadata() {
		// TODO Auto-generated method stub
		return null;
	}

}
