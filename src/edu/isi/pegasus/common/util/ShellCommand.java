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
import java.io.IOException;
import org.apache.log4j.Level;

/**
 * A utility class that has convenience methods to execute a command and retrive the stdout and
 * stderr
 *
 * @author Karan Vahi
 */
public class ShellCommand {

    /** The default logger. */
    private LogManager mLogger;

    /** Buffer of the output of the command */
    private String mOutputBuffer;

    /** Buffer of the output of the command */
    private String mErrorBuffer;

    /**
     * Factory method to instantiate the class.
     *
     * @return instance to the class
     */
    public static ShellCommand getInstance() {
        return getInstance(null);
    }

    /**
     * Factory method to instantiate the class.
     *
     * @param logger the logger object
     * @return instance to the class.
     */
    public static ShellCommand getInstance(LogManager logger) {
        if (logger == null) {
            logger = LogManagerFactory.loadSingletonInstance();
        }
        return new ShellCommand(logger);
    }

    /**
     * The default constructor.
     *
     * @param logger the logger object
     */
    private ShellCommand(LogManager logger) {
        mLogger = logger;
    }

    /**
     * Executes a command with the arguments passed
     *
     * @return the exitcode
     */
    public int execute(String command, String args) {
        int exitcode = -1;
        try {
            // set the callback and run the grep command
            ShellCommandCallback stdoutCallback = new ShellCommandCallback();
            ShellCommandCallback stderrCallback = new ShellCommandCallback();
            Runtime r = Runtime.getRuntime();
            String execCommand = command;
            if (args != null) {
                execCommand = execCommand + " " + args;
            }
            Process p = r.exec(execCommand);

            // spawn off the gobblers
            StreamGobbler ips = new StreamGobbler(p.getInputStream(), stdoutCallback);
            StreamGobbler eps = new StreamGobbler(p.getErrorStream(), stderrCallback);
            ips.start();
            eps.start();

            // wait for the threads to finish off
            ips.join();
            mOutputBuffer = stdoutCallback.getOutput();
            eps.join();
            mErrorBuffer = stderrCallback.getOutput();

            // get the status
            exitcode = p.waitFor();
            if (exitcode != 0) {
                mLogger.log(
                        "Command " + execCommand + " exited with status " + exitcode,
                        LogManager.WARNING_MESSAGE_LEVEL);
                mLogger.log(mErrorBuffer, LogManager.DEBUG_MESSAGE_LEVEL);
            }

        } catch (IOException ioe) {
            mLogger.log("IOException while executng command ", ioe, LogManager.ERROR_MESSAGE_LEVEL);
        } catch (InterruptedException ie) {
            // ignore
        }

        return exitcode;
    }

    /**
     * Returns the stdout of the last command executed.
     *
     * @return
     */
    public String getSTDOut() {
        return mOutputBuffer;
    }

    /**
     * Returns the stderr of the last command executed.
     *
     * @return
     */
    public String getSTDErr() {
        return mErrorBuffer;
    }

    /** An inner class, that implements the StreamGobblerCallback to store the output returned */
    private static class ShellCommandCallback implements StreamGobblerCallback {
        /** Buffer of the output of the command */
        private StringBuilder mOutputBuffer;

        private int mLine = 0;

        /** The Default Constructor */
        public ShellCommandCallback() {
            mOutputBuffer = new StringBuilder();
        }

        /**
         * Callback whenever a mLine is read from the stream by the StreamGobbler.
         *
         * @param line the mLine that is read.
         */
        public void work(String line) {
            if (mLine++ > 0) {
                mOutputBuffer.append("\n");
            }
            mOutputBuffer.append(line);
        }

        /**
         * Returns the condor version detected.
         *
         * @return the condor version else null
         */
        public String getOutput() {
            return mOutputBuffer.toString();
        }
    }

    /**
     * The main program to test.
     *
     * @param args
     */
    public static void main(String[] args) {
        LogManager logger = LogManagerFactory.loadSingletonInstance();
        logger.setLevel(Level.DEBUG);
        ShellCommand cv = ShellCommand.getInstance();

        logger.logEventStart("ShellCommand", "ShellCommand", "Version");

        cv.execute("/opt/condor/default/bin/condor_config_val", "MOUNT_UNDER_SCRATCH");
        System.out.println(cv.getSTDOut());

        logger.logEventCompletion();
    }
}
