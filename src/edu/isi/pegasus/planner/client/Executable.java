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
package edu.isi.pegasus.planner.client;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.common.logging.LogManagerFactory;
import edu.isi.pegasus.common.util.FactoryException;
import edu.isi.pegasus.common.util.Version;
import edu.isi.pegasus.planner.common.PegasusProperties;
import gnu.getopt.Getopt;
import gnu.getopt.LongOpt;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.MissingResourceException;

/**
 * The interface which defines all the methods , any executable should implement.
 *
 * @author GAURANG MEHTA
 * @author KARAN VAHI
 * @version $Revision$
 */
public abstract class Executable {

    /** The default properties file to be picked up for the conf option. */
    public static String DEFAULT_PROPERTIES_FILE = "pegasus.properties";

    /** The LogManager object which is used to log all the messages. */
    protected LogManager mLogger;

    /** The object holding all the properties pertaining to Pegasus. */
    protected PegasusProperties mProps;

    /** It stores the verison of the Griphyn Virtual Data System software. */
    protected String mVersion;

    /** The error message to be logged. */
    protected String mLogMsg;

    /** The command line options passed to the executable */
    private String[] commandLineOpts;

    /** The default constructor. */
    public Executable() {
        this(null);
    }

    /**
     * The constructor which ends up initialising the PegasusProperties object.
     *
     * @param logger the logger to use. Can be null.
     */
    public Executable(LogManager logger) {
        mLogger = logger;
    }

    /**
     * Looks up for the conf property in the arguments passed to the executable
     *
     * @param opts command line arguments passed to the executable
     * @param confChar the short option corresponding to the conf property
     * @return
     */
    protected String lookupConfProperty(String[] opts, char confChar) {
        LongOpt[] longOptions = new LongOpt[1];
        longOptions[0] = new LongOpt("conf", LongOpt.REQUIRED_ARGUMENT, null, confChar);
        Getopt g = new Getopt("Executable", opts, confChar + ":", longOptions, false);
        g.setOpterr(false);
        String propertyFilePath = null;
        int option = 0;
        while ((option = g.getopt()) != -1) {
            if (option == confChar) {
                propertyFilePath = g.getOptarg();
                break;
            }
        }

        if (propertyFilePath == null) {
            // PM-1018 if no --conf provided fall back to pegasus.properties
            // in the current working directory from where command is called
            propertyFilePath = "." + File.separatorChar + Executable.DEFAULT_PROPERTIES_FILE;
        }

        return propertyFilePath;
    }

    /**
     * Initialize the executable object
     *
     * @param opts the command line argument passed by the user
     * @param confChar the short option corresponding the conf property.
     */
    protected void initialize(String[] opts, char confChar) {
        this.commandLineOpts = opts;
        String propertyFile = lookupConfProperty(getCommandLineOptions(), confChar);
        mProps = PegasusProperties.getInstance(propertyFile);
        mVersion = Version.instance().toString();
        // setup logging before doing anything with properties
        try {
            setupLogging(mLogger, mProps);
        } catch (IOException ioe) {
            throw new RuntimeException("Unable to initialize the logger ", ioe);
        }
        mLogMsg = new String();

        sanityCheckOnProperties();
        loadProperties();
    }

    /**
     * Initialize the executable object
     *
     * @param opts the command line argument passed to the executable
     */
    protected void initialize(String[] opts) {
        initialize(opts, 'c');
    }

    /**
     * Returns an error message that chains all the lower order error messages that might have been
     * thrown.
     *
     * @param e the Exception for which the error message has to be composed.
     * @return the error message.
     */
    public static String convertException(Exception e) {
        return Executable.convertException(e, LogManager.TRACE_MESSAGE_LEVEL);
    }
    /**
     * Returns an error message that chains all the lower order error messages that might have been
     * thrown.
     *
     * @param e the Exception for which the error message has to be composed.
     * @param logLevel the user specified level for the logger
     * @return the error message.
     */
    public static String convertException(Exception e, int logLevel) {
        StringBuffer message = new StringBuffer();
        int i = 0;

        // check if we want to throw the whole stack trace
        if (logLevel >= LogManager.INFO_MESSAGE_LEVEL) {
            // we want the stack trace to a String Writer.
            StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));

            return sw.toString();
        }

        // append all the causes
        for (Throwable cause = e; cause != null; cause = cause.getCause()) {
            if (cause instanceof FactoryException) {
                // do the specialized convert for Factory Exceptions
                message.append(((FactoryException) cause).convertException(i));
                break;
            }
            message.append("\n [")
                    .append(Integer.toString(++i))
                    .append("] ")
                    .append(cause.getClass().getName())
                    .append(": ")
                    .append(cause.getMessage());

            // append just one elment of stack trace for each exception
            message.append(" at ").append(cause.getStackTrace()[0]);
        }
        return message.toString();
    }

    /**
     * Sets up the logging options for this class. Looking at the properties file, sets up the
     * appropriate writers for output and stderr.
     *
     * @param logger the logger to use. Can be null.
     * @param properties reference of pegasus properties object.
     */
    protected void setupLogging(LogManager logger, PegasusProperties properties)
            throws IOException {
        if (logger != null) {
            mLogger = logger;
            return;
        }

        // setup the logger for the default streams.
        mLogger = LogManagerFactory.loadSingletonInstance(properties);
        mLogger.logEventStart("event.pegasus.planner", "planner.version", mVersion);

        // get the logging value set in properties
        String value = properties.getProperty("pegasus.log.*");

        // use defaults if nothing is set.
        if (value == null) {
            mLogger.log("Logging to default streams", LogManager.DEBUG_MESSAGE_LEVEL);
            return;
        } else {
            // take a backup of the log if required.
            File f = new File(value);
            File dir = f.getParentFile();
            String basename = f.getName();

            NumberFormat formatter = new DecimalFormat("000");
            File backupFile = null;
            // start from 000 onwards and check for existence
            for (int i = 0; i < 999; i++) {
                StringBuffer backup = new StringBuffer();
                backup.append(basename).append(".").append(formatter.format(i));

                // check if backup file exists.
                backupFile = new File(dir, backup.toString());
                if (!backupFile.exists()) {
                    break;
                }
            }

            // log both output and error messages to value specified
            mLogger.setWriters(backupFile.getAbsolutePath());
        }
    }

    /** Loads all the properties that would be needed by the Toolkit classes. */
    public abstract void loadProperties();

    /** This method is used to print the long version of the command. */
    public abstract void printLongVersion();

    /** This is used to print the short version of the command. */
    public abstract void printShortVersion();

    /**
     * This function is passed command line arguments. In this function you generate the valid
     * options and parse the options specified at run time.
     */
    // public abstract void executeCommand(String[] args);

    /**
     * Generates an array of valid <code>LongOpt</code> objects which contain all the valid options
     * to the Executable.
     */
    public abstract LongOpt[] generateValidOptions();

    /** Returns the version of the Griphyn Virtual Data System. */
    public String getGVDSVersion() {
        StringBuffer sb = new StringBuffer();
        sb.append("Pegasus Release Version ").append(mVersion);
        return sb.toString();
    }

    /**
     * Logs messages to the singleton logger.
     *
     * @param msg is the message itself.
     * @param level is the level to generate the log message for.
     */
    public void log(String msg, int level) {
        mLogger.log(msg, level);
    }

    /**
     * Get the value of the environment variable.
     *
     * @param envVariable the environment variable whose value you want.
     * @return String corresponding to the value of the environment variable if it is set. null if
     *     the environment variable is not set
     */
    public String getEnvValue(String envVariable) {
        String value = null;
        value = System.getProperty(envVariable);
        return value;
    }

    /**
     * Returns the command line arguments passed to the executable
     *
     * @return command line arguments passed to the executable
     */
    protected String[] getCommandLineOptions() {
        String[] optsClone = new String[commandLineOpts.length];
        for (int i = 0; i < commandLineOpts.length; i++) {
            optsClone[i] = commandLineOpts[i];
        }
        return optsClone;
    }

    /**
     * Does a sanity check on the properties to make sure that all the required properties are
     * loaded.
     */
    protected void sanityCheckOnProperties() {
        // check required properties
        if (mProps.getProperty("pegasus.home.bindir") == null) {
            throw new MissingResourceException(
                    "The pegasus.home.bindir property was not set ",
                    "java.util.Properties",
                    "pegasus.home.bindir");
        }

        if (mProps.getProperty("pegasus.home.schemadir") == null) {
            throw new MissingResourceException(
                    "The pegasus.home.schemadir property was not set ",
                    "java.util.Properties",
                    "pegasus.home.schemadir");
        }

        if (mProps.getProperty("pegasus.home.sharedstatedir") == null) {
            throw new MissingResourceException(
                    "The pegasus.home.sharedstatedir property was not set ",
                    "java.util.Properties",
                    "pegasus.home.sharedstatedir");
        }

        if (mProps.getProperty("pegasus.home.sysconfdir") == null) {
            throw new MissingResourceException(
                    "The pegasus.home.sysconfdir property was not set ",
                    "java.util.Properties",
                    "pegasus.home.sysconfdir");
        }
    }
}
