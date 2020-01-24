/**
 * Copyright 2007-2008 University Of Southern California
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.isi.pegasus.common.util;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.common.logging.LogManagerFactory;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;

/**
 * A Stream gobbler class to take care of reading from a stream and optionally write out to another
 * stream. Allows for non blocking reads on both stdout and stderr, when invoking a process through
 * Runtime.exec(). Also, the user can specify a callback that is called whenever anything is read
 * from the stream.
 *
 * @author Karan Vahi
 */
public class StreamGobbler extends Thread {

    /** The input stream that is to be read from. */
    private InputStream mIPStream;

    /** The output stream to which the contents have to be redirected to. */
    private OutputStream mOPStream;

    /** The callback to be used. */
    private StreamGobblerCallback mCallback;

    /** The prompt that is to be written to the output stream. */
    private String mPrompt;

    /** A boolean indicating whether the thread has started or not. */
    private boolean mStarted;

    /** The handle to the logging object. */
    private LogManager mLogger;

    /**
     * The overloaded constructor.
     *
     * @param is the input stream from which to read from.
     * @param callback the callback to call when a line is read.
     */
    public StreamGobbler(InputStream is, StreamGobblerCallback callback) {
        this.mIPStream = is;
        mCallback = callback;
        mLogger = LogManagerFactory.loadSingletonInstance();

        // set the prompt to nothing
        mPrompt = "";
    }

    /**
     * Sets the output stream to which to redirect the contents of the input stream.
     *
     * @param ops the output stream.
     * @param prompt the prompt for the output stream.
     */
    public void redirect(OutputStream ops, String prompt) {

        if (mStarted) {
            // should throw a specific VTor exception
            throw new RuntimeException("The thread has already started execution");
        }

        mOPStream = ops;
        mPrompt = (prompt == null) ? "" : prompt;
    }

    /** The main method of the gobbler, that does all the work. */
    public void run() {
        try {
            mStarted = true;
            PrintWriter pw = (mOPStream == null) ? null : new PrintWriter(mOPStream);

            boolean redirect = !(pw == null);

            InputStreamReader isr = new InputStreamReader(mIPStream);
            BufferedReader br = new BufferedReader(isr);
            String line = null;
            while ((line = br.readLine()) != null) {
                // redirect to output stream
                if (redirect) pw.println(mPrompt + line);

                // callout to the callback
                mCallback.work(line);

                // be nice and sleep
                this.sleep(5);
            }
        } catch (IOException e) {
            mLogger.log(" While reading in StreamGobbler ", e, LogManager.ERROR_MESSAGE_LEVEL);
        } catch (InterruptedException e) {
            // ignore
        } finally {
            mStarted = false;
        }
    }

    /** Closes the open connections to the streams whenever this object is destroyed. */
    protected void finalize() {
        close();
    }

    /** Closes the underneath input and output stream that were opened. */
    public void close() {
        // close the input stream
        try {
            if (mIPStream != null) mIPStream.close();

        } catch (IOException e) {
            // ignore
        } finally {
            mIPStream = null;
        }

        // close the output stream
        try {
            if (mOPStream != null) mOPStream.close();

        } catch (IOException e) {
            // ignore
        } finally {
            mOPStream = null;
        }
    }
}
