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
package org.griphyn.cPlanner.common;

import org.griphyn.cPlanner.common.LogManager;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.BufferedReader;
import java.io.IOException;

/**
 * A Stream gobbler class to take care of reading from a stream and optionally
 * write out to another stream. Allows for non blocking reads on both stdout
 * and stderr, when invoking a process through Runtime.exec().
 * Also, the user can specify a callback that is called whenever anything
 * is read from the stream.
 *
 * @author Karan Vahi
 */
public class StreamGobbler extends Thread {

    /**
     * The input stream that is to be read from.
     */
    private InputStream mIPStream;

    /**
     * The output stream to which the contents have to be redirected to.
     */
    private OutputStream mOPStream;

    /**
     * The callback to be used.
     */
    private StreamGobblerCallback mCallback;

    /**
     * The prompt that is to be written to the output stream.
     */
    private String mPrompt;

    /**
     * A boolean indicating whether the thread has started or not.
     */
    private boolean mStarted;

    /**
     * The handle to the logging object.
     */
    private LogManager mLogger;

    /**
     * The overloaded constructor.
     *
     * @param is        the input stream from which to read from.
     * @param callback  the callback to call when a line is read.
     */
    public StreamGobbler(InputStream is, StreamGobblerCallback callback) {
        this.mIPStream = is;
        mCallback      = callback;
        mLogger        = LogManager.getInstance();

        //set the prompt to nothing
        mPrompt = "";
    }

    /**
     * Sets the output stream to which to redirect the contents of the input
     * stream.
     *
     * @param ops     the output stream.
     * @param prompt  the prompt for the output stream.
     */
    public void redirect ( OutputStream ops , String prompt ){

        if( mStarted ){
            //should throw a specific VTor exception
            throw new RuntimeException("The thread has already started execution");
        }

        mOPStream = ops;
        mPrompt   = ( prompt == null ) ? "" : prompt;
    }


    /**
     * The main method of the gobbler, that does all the work.
     */
    public void run() {
        try{
            mStarted = true;
            PrintWriter pw = ( mOPStream == null) ? null : new PrintWriter(mOPStream);

            boolean redirect = !(pw == null);

            InputStreamReader isr = new InputStreamReader(mIPStream);
            BufferedReader br     = new BufferedReader(isr);
            String line = null;
            while ( (line = br.readLine()) != null) {
                //redirect to output stream
                if (redirect) pw.println(mPrompt + line);

                //callout to the callback
                mCallback.work( line );

                //be nice and sleep
                this.sleep(5);

            }
        }
        catch( IOException e){
            mLogger.log(" While reading in StreamGobbler ",e,
                        LogManager.ERROR_MESSAGE_LEVEL);
        }
        catch ( InterruptedException e ){
            //ignore
        }
        finally{
            mStarted = false;
        }

    }

    /**
     * Closes the open connections to the streams whenever this object
     * is destroyed.
     */
    protected void finalize(){
        close();
    }


    /**
     * Closes the underneath input and output stream that were opened.
     */
    public void close(){
        //close the input stream
        try{
            if( mIPStream != null)  mIPStream.close();

        }catch(IOException e){
            //ignore
        }finally{
           mIPStream = null;
        }

        //close the output stream
        try{
            if( mOPStream != null)  mOPStream.close();

        }catch(IOException e){
            //ignore
        }finally{
           mOPStream = null;
        }

    }
}
