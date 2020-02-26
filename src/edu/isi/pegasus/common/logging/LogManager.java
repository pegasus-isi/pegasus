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
package edu.isi.pegasus.common.logging;

import edu.isi.pegasus.planner.common.PegasusProperties;
import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import org.apache.log4j.Level;

/**
 * The logging class that to log messages at different levels. Currently the following levels are
 * supported.
 *
 * <p>Eventually, each of the level can have a different writer stream underneath.
 *
 * <p>The messages can be logged at various levels. The various levels of logging with increasing
 * levels of verbosity are displayed in the following table.
 *
 * <p>
 *
 * <table border="1">
 * <tr align="left"><th>Logging Level</th><th>Description</th></tr>
 * <tr align="left"><th>FATAL</th>
 *  <td>all fatal error messages are logged in this level.</td>
 * </tr>
 * <tr align="left"><th>ERROR</th>
 *  <td>all non fatal error messages are logged in this level.</td>
 * </tr>
 * <tr align="left"><th>WARNING</th>
 *  <td>all warning messages are logged in this level.</td>
 * </tr>
 * <tr align="left"><th>INFO</th>
 *  <td>all information logging messages are logged in this level.</td>
 * </tr>
 * <tr align="left"><th>CONFIG</th>
 *  <td>all configuration messages are logged in this level.</td>
 * </tr>
 * <tr align="left"><th>DEBUG</th>
 *  <td>all debug messages are logged in this level.</td>
 * </tr>
 * </table>
 *
 * @author Karan Vahi
 * @author Gaurang Mehta
 * @version $Revision$
 */
public abstract class LogManager {

    /** The version of the Logging API */
    public static final String VERSION = "2.1";

    /** Prefix for the property subset to use with the LogManager */
    public static final String PROPERTIES_PREFIX = "pegasus.log.manager";

    /** Suffx for an event completion message. */
    public static final String MESSAGE_DONE_PREFIX = " -DONE";

    // level  constants that loosely match Log4J and are used
    // to generate the appropriate mask values.

    /** The level value, to indicate a FATAL error message. */
    public static final int FATAL_MESSAGE_LEVEL = 0;

    /** The level value, to indicate an ERROR message. */
    public static final int ERROR_MESSAGE_LEVEL = 1;

    /** The level value, to indicate a CONSOLE error message. */
    public static final int CONSOLE_MESSAGE_LEVEL = 2;

    /** The level value, to indicate a WARNING error message. */
    public static final int WARNING_MESSAGE_LEVEL = 3;

    /** The level value, to indicate a INFO message. */
    public static final int INFO_MESSAGE_LEVEL = 4;

    /** The level value, to indicate a CONFIG message. */
    public static final int CONFIG_MESSAGE_LEVEL = 5;

    /** The level value, to indicate a DEBUG message. */
    public static final int DEBUG_MESSAGE_LEVEL = 6;

    /** The level value, to indicate a DEBUG message. */
    public static final int TRACE_MESSAGE_LEVEL = 7;

    /** Ensures only one object is created always. Implements the Singleton. */
    private static LogManager mLogger;

    /** The default Logger */
    public static final String DEFAULT_LOGGER = "Default";

    /** The Log4j logger. */
    public static final String LOG4J_LOGGER = "Log4j";

    /**
     * The debug level. Higher the level the more the detail is logged. At present can be 0 or 1.
     * This is set according to the option given by the user, whether verbose or not.
     */
    protected int mDebugLevel;

    /** The LogFormatter to use to format the message. */
    protected LogFormatter mLogFormatter;

    /** The constructor. */
    public LogManager() {
        mDebugLevel = 0;
    }

    /**
     * To get a reference to the the object.
     *
     * @param logger the logger to use for logging
     * @param formatter the log formatter to use for formatting messages
     * @return a singleton access to the object.
     */
    public static LogManager getInstance(String logger, String formatter) {
        if (mLogger == null) {
            mLogger =
                    LogManagerFactory.loadSingletonInstance(
                            PegasusProperties.nonSingletonInstance());
            /*if( logger == null || logger.equals( DEFAULT_LOGGER ) ){
                mLogger = new Default();
            }
            else if( logger.equals( LOG4J_LOGGER )){
                mLogger = new Log4j();
            }
            else{
                throw new RuntimeException( "Unknown Logger Implementation Specified" + logger );
            }
            */
        }
        return mLogger;
    }

    /**
     * Sets the log formatter to use for formatting the messages.
     *
     * @param formatter the formatter to use.
     * @param properties properties that the underlying implementations understand
     */
    public abstract void initialize(LogFormatter formatter, Properties properties);

    /**
     * Checks the destination location for existence, if it can be created, if it is writable etc.
     *
     * @param file is the file to write out to.
     * @throws IOException in case of error while writing out files.
     */
    protected static void sanityCheckOnFile(File file) throws IOException {
        if (file.exists()) {
            // location exists
            if (file.isFile()) {
                // ok, is a file
                if (file.canWrite()) {
                    // can write, all is well
                    return;
                } else {
                    // all is there, but I cannot write to file
                    throw new IOException(
                            "Cannot write to existing file " + file.getAbsolutePath());
                }
            } else {
                // exists but not a file
                throw new IOException(
                        "File "
                                + file.getAbsolutePath()
                                + " already "
                                + "exists, but is not a file.");
            }
        } else {
            // check to see if you can write to the parent directory
            // could have tried to do just a make dir on parent directory.
            sanityCheckOnDirectory(file.getParentFile());
        }
    }

    /**
     * Checks the destination location for existence, if it can be created, if it is writable etc.
     *
     * @param dir is the new base directory to optionally create.
     * @throws IOException in case of error while writing out files.
     */
    protected static void sanityCheckOnDirectory(File dir) throws IOException {
        if (dir.exists()) {
            // location exists
            if (dir.isDirectory()) {
                // ok, isa directory
                if (dir.canWrite()) {
                    // can write, all is well
                    return;
                } else {
                    // all is there, but I cannot write to dir
                    throw new IOException("Cannot write to existing directory " + dir.getPath());
                }
            } else {
                // exists but not a directory
                throw new IOException(
                        "Destination "
                                + dir.getPath()
                                + " already "
                                + "exists, but is not a directory.");
            }
        } else {
            // does not exist, try to make it
            if (!dir.mkdirs()) {
                throw new IOException("Unable to create directory destination " + dir.getPath());
            }
        }
    }

    /**
     * Sets the debug level. All those messages are logged which have a level less than equal to the
     * debug level.
     *
     * @param level the level to which the debug level needs to be set to.
     */
    public void setLevel(Level level) {
        int value = level.toInt();
        switch (value) {
            case Level.DEBUG_INT:
                value = LogManager.DEBUG_MESSAGE_LEVEL;
                break;

            case Level.INFO_INT:
                value = LogManager.INFO_MESSAGE_LEVEL;
                break;

            case Level.WARN_INT:
                value = LogManager.WARNING_MESSAGE_LEVEL;
                break;

            case Level.ERROR_INT:
                value = LogManager.ERROR_MESSAGE_LEVEL;
                break;

            default:
                value = LogManager.FATAL_MESSAGE_LEVEL;
                break;
        }
        setLevel(value, false);
    }

    /**
     * Sets the debug level. All those messages are logged which have a level less than equal to the
     * debug level. In addition the console messages are always logged.
     *
     * @param level the level to which the debug level needs to be set to.
     */
    public void setLevel(int level) {
        setLevel(level, true);
    }

    /**
     * Sets the debug level. All those messages are logged which have a level less than equal to the
     * debug level. In case the boolean info is set, all the info messages are also logged.
     *
     * @param level the level to which the debug level needs to be set to.
     * @param info boolean denoting whether the CONSOLE messages need to be logged or not.
     */
    protected abstract void setLevel(int level, boolean info);

    /**
     * Returns the debug level.
     *
     * @return the level to which the debug level has been set to.
     */
    public abstract int getLevel();

    /**
     * Sets both the output writer and the error writer to the same underlying writer.
     *
     * @param out is the name of a file to append to. Special names are <code>stdout</code> and
     *     <code>stderr</code>, which map to the system's respective streams.
     */
    public abstract void setWriters(String out);

    /**
     * Log the message represented by the internal log buffer. The log buffer is populated via the
     * add methods.
     *
     * @param level the level on which the message has to be logged.
     */
    public void log(int level) {
        this.log(mLogFormatter.createLogMessage(), level);
    }

    /**
     * Creates a log message with the contents of the internal log buffer. The log buffer is
     * populated via the add methods. It then resets the buffer before logging the log message
     *
     * @param level the level on which the message has to be logged.
     */
    public void logAndReset(int level) {
        this.logAlreadyFormattedMessage(mLogFormatter.createLogMessageAndReset(), level);
    }

    /**
     * Logs the exception on the appropriate queue if the level of the message is less than or equal
     * to the level set for the Logger. For INFO level message, the boolean indicating that a
     * completion message is to follow is set to true always.
     *
     * @param message the message to be logged.
     * @param e the exception to be logged
     * @param level the level on which the message has to be logged.
     * @see #setLevel(int)
     * @see #log(String,int)
     */
    public abstract void log(String message, Exception e, int level);

    /**
     * A stop gap function .
     *
     * @param message already formatted message
     * @param level the level on which to log.
     */
    protected abstract void logAlreadyFormattedMessage(String message, int level);

    /**
     * Logs the message on the appropriate queue if the level of the message is less than or equal
     * to the level set for the Logger. For INFO level message, the boolean indicating that a
     * completion message is to follow is set to true always.
     *
     * @param message the message to be logged.
     * @param level the level on which the message has to be logged.
     * @see #setLevel(int)
     */
    public void log(String message, int level) {
        mLogFormatter.add(message);
        this.logAlreadyFormattedMessage(mLogFormatter.createLogMessageAndReset(), level);
    }

    /**
     * Log an event start message to INFO level
     *
     * @param name the name of the event to be associated
     * @param entityName the primary entity that is associated with the event e.g. workflow
     * @param entityID the id of that entity.
     */
    public void logEventStart(String name, String entityName, String entityID) {
        logEventStart(name, entityName, entityID, LogManager.INFO_MESSAGE_LEVEL);
    }

    /**
     * Log an event start message.
     *
     * @param name the name of the event to be associated
     * @param entityName the primary entity that is associated with the event e.g. workflow
     * @param entityID the id of that entity.
     * @param level the level at which event needs to be logged.
     */
    public void logEventStart(String name, String entityName, String entityID, int level) {
        mLogFormatter.addEvent(name, entityName, entityID);
        this.logAlreadyFormattedMessage(mLogFormatter.getStartEventMessage(), level);
    }

    /**
     * Log an event start message to the INFO Level
     *
     * @param name the name of the event to be associated
     * @param map Map indexed by entity name . The values is corresponding EntityID
     */
    public void logEventStart(String name, Map<String, String> map) {
        this.logEventStart(name, map, LogManager.INFO_MESSAGE_LEVEL);
    }

    /**
     * Log an event start message.
     *
     * @param name the name of the event to be associated
     * @param map Map indexed by entity name . The values is corresponding EntityID
     * @param level the level to log to
     */
    public void logEventStart(String name, Map<String, String> map, int level) {
        mLogFormatter.addEvent(name, map);
        this.logAlreadyFormattedMessage(mLogFormatter.getStartEventMessage(), level);
    }

    /** Logs the completion message on the basis of the debug level. */
    public void logEventCompletion() {
        // this.log( LogManager.INFO_MESSAGE_LEVEL );
        this.logEventCompletion(LogManager.INFO_MESSAGE_LEVEL);
    }

    /**
     * Logs the completion message on the basis of the debug level.
     *
     * @param level the debug level of the start message for whose completion you want.
     */
    public abstract void logEventCompletion(int level);

    /**
     * Log a message that connects the parent entities with the children. For e.g. can we use to
     * create the log messages connecting the jobs with the workflow they are part of.
     *
     * @param parentType the type of parent entity
     * @param parentID the id of the parent entity
     * @param childIDType the type of children entities
     * @param childIDs Collection of children id's
     */
    public void logEntityHierarchyMessage(
            String parentType, String parentID, String childIDType, Collection<String> childIDs) {
        this.logEntityHierarchyMessage(
                parentType, parentID, childIDType, childIDs, LogManager.DEBUG_MESSAGE_LEVEL);
    }

    /**
     * Log a message that connects the parent entities with the children. For e.g. can we use to
     * create the log messages connecting the jobs with the workflow they are part of.
     *
     * @param parentType the type of parent entity
     * @param parentID the id of the parent entity
     * @param childIDType the type of children entities
     * @param childIDs Collection of children id's
     * @param level the logging level.
     */
    public void logEntityHierarchyMessage(
            String parentType,
            String parentID,
            String childIDType,
            Collection<String> childIDs,
            int level) {

        this.logAlreadyFormattedMessage(
                mLogFormatter.createEntityHierarchyMessage(
                        parentType, parentID, childIDType, childIDs),
                level);
    }

    /**
     * Add to the internal log buffer message a value with the default key. The buffer is logged
     * later when the log() method is called.
     *
     * @param value
     * @return self-reference
     */
    public LogManager add(String value) {
        return add("msg", value);
    }

    /**
     * Add to the internal log buffer message a value with the key oassed The buffer is logged later
     * when the log() method is called.
     *
     * @param key
     * @param value
     * @return Self-reference, so calls can be chained
     */
    public LogManager add(String key, String value) {
        mLogFormatter.add(key, value);
        return this;
    }
}
