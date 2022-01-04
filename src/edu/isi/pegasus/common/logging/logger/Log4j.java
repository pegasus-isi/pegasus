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
package edu.isi.pegasus.common.logging.logger;

import edu.isi.pegasus.common.logging.LogFormatter;
import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.common.logging.LogManagerFactory;
import java.io.PrintStream;
import java.util.Collection;
import java.util.Properties;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.config.builder.api.AppenderComponentBuilder;
import org.apache.logging.log4j.core.config.builder.api.ConfigurationBuilder;
import org.apache.logging.log4j.core.config.builder.api.ConfigurationBuilderFactory;
import org.apache.logging.log4j.core.config.builder.api.LayoutComponentBuilder;
import org.apache.logging.log4j.core.config.builder.api.RootLoggerComponentBuilder;
import org.apache.logging.log4j.core.config.builder.impl.BuiltConfiguration;

/**
 * A Log4j implementation of the LogManager interface. Using this allows us us log messages using
 * Log4j
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class Log4j extends LogManager {

    /** The property that specifies the path to the log4j properties file. */
    private static final String LOG4J2_CONF_PROPERTY = "log4j2.properties";

    // level  constants that loosely match Log4J and are used
    // to generate the appropriate mask values.

    /** The handle to a log4j logger object. */
    private Logger mLogger;

    /** Keeps track of log4j's root logger as singleton. */
    private static Logger mRoot;

    /** Initializes the root logger when this class is loaded. */
    static {
        /*
        if ((mRoot = Logger.getRootLogger()) != null) {

            // get hold of all appenders and override the console appender
            for (Enumeration e = mRoot.getAllAppenders(); e.hasMoreElements(); ) {
                Appender a = (Appender) e.nextElement();
                if (a instanceof ConsoleAppender) {
                    // set the layout of the console appender
                    // this can be overriden by the log4j.properties file
                    a.setLayout(new PatternLayout("%d{yyyy-MM-dd HH:mm:ss.SSS} %-5p [%c{1}] %m%n"));
                }
            }
            mRoot.setLevel(Level.INFO);
            mRoot.debug("starting");
        }
        */
        if ((mRoot = org.apache.logging.log4j.LogManager.getRootLogger()) != null) {
            // picked up from this example https://www.baeldung.com/log4j2-programmatic-config
            ConfigurationBuilder<BuiltConfiguration> builder =
                    ConfigurationBuilderFactory.newConfigurationBuilder();
            AppenderComponentBuilder console = builder.newAppender("stdout", "Console");
            builder.add(console);
            LayoutComponentBuilder standard = builder.newLayout("PatternLayout");
            standard.addAttribute("pattern", "%d{yyyy-MM-dd HH:mm:ss.SSS} %-5p [%c{1}] %m%n");
            console.add(standard);
            RootLoggerComponentBuilder rootLogger = builder.newRootLogger(Level.INFO);
            rootLogger.add(builder.newAppenderRef("stdout"));
            builder.add(rootLogger);
            Configurator.initialize(builder.build());
        }
    }

    /** The properties passed at runtime */
    private Properties mProperties;

    /** The constructor. */
    public Log4j() {
        // configure properties through log4j.properties file
        mLogger = org.apache.logging.log4j.LogManager.getLogger("pegasus");
    }

    /**
     * Sets the log formatter to use for formatting the messages.
     *
     * @param formatter the formatter to use.
     * @param properties properties that the underlying implementations understand
     */
    public void initialize(LogFormatter formatter, Properties properties) {
        mLogFormatter = formatter;
        mProperties = properties;

        // set formatter to pegasus always for time being
        mLogFormatter.setProgramName("pegasus");

        // specify the path to the log4j properties file if specified.
        String conf = properties.getProperty(Log4j.LOG4J2_CONF_PROPERTY);
        if (conf != null) {
            // PropertyConfigurator.configure(conf);
            Configurator.initialize(null, conf);
        }
    }

    /**
     * Boolean indicating whether a timestamp should be logged when logging a message
     *
     * @param logTimestamp boolean
     */
    public void configure(boolean logTimestamp) {
        // empty implementation for timebeing.
    }

    /**
     * Log a message that connects the parent entities with the children. For e.g. can we use to
     * create the log messages connecting the jobs with the workflow they are part of. They are by
     * default logged to INFO level
     *
     * @param parentType the type of parent entity
     * @param parentID the id of the parent entity
     * @param childIDType the type of children entities
     * @param childIDs Collection of children id's
     */
    public void logEntityHierarchyMessage(
            String parentType, String parentID, String childIDType, Collection<String> childIDs) {
        this.logEntityHierarchyMessage(
                parentType, parentID, childIDType, childIDs, LogManager.INFO_MESSAGE_LEVEL);
    }

    /**
     * Sets the debug level. All those messages are logged which have a level less than equal to the
     * debug level.
     *
     * @param level the level to which the debug level needs to be set to.
     */
    public void setLevel(Level level) {
        setLevel(level, true);
    }

    /**
     * Sets the debug level. All those messages are logged which have a level less than equal to the
     * debug level. In addition the info messages are always logged.
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
     * @param info boolean denoting whether the INFO messages need to be logged or not.
     */
    protected void setLevel(int level, boolean info) {
        Level l = Level.ALL;
        switch (level) {
            case LogManager.FATAL_MESSAGE_LEVEL:
                l = Level.FATAL;
                break;

            case LogManager.ERROR_MESSAGE_LEVEL:
                l = Level.ERROR;
                break;

            case LogManager.WARNING_MESSAGE_LEVEL:
                l = Level.WARN;
                break;

            case LogManager.CONFIG_MESSAGE_LEVEL:
                l = Level.INFO;
                break;

            case LogManager.INFO_MESSAGE_LEVEL:
                l = Level.INFO;
                break;

            case LogManager.DEBUG_MESSAGE_LEVEL:
                l = Level.DEBUG;
                break;

            case LogManager.TRACE_MESSAGE_LEVEL:
                l = Level.TRACE;
                break;
        }
        // lets set only the pegasus logger
        Configurator.setLevel("pegasus", l);
        // mLogger.setLevel(l);
    }

    /**
     * Sets the debug level. All those messages are logged which have a level less than equal to the
     * debug level. In case the boolean info is set, all the info messages are also logged.
     *
     * @param level the level to which the debug level needs to be set to.
     * @param info boolean denoting whether the INFO messages need to be logged or not.
     */
    protected void setLevel(Level level, boolean info) {
        /*
        mDebugLevel = level.toInt();
        mLogger.setLevel(level);
        */

        // lets set only the pegasus logger
        Configurator.setLevel("pegasus", level);
        mDebugLevel = LogManager.log4jLevelsToIntValue().get(level);
    }

    /**
     * Returns the debug level.
     *
     * @return the level to which the debug level has been set to.
     */
    public int getLevel() {
        return mDebugLevel;
    }

    /**
     * Sets both the output writer and the error writer to the same underlying writer.
     *
     * @param out is the name of a file to append to. Special names are <code>stdout</code> and
     *     <code>stderr</code>, which map to the system's respective streams.
     */
    public void setWriters(String out) {
        throw new UnsupportedOperationException("Log4jLogger does not support setWriters(out)");
    }

    /**
     * Sets the passed printstream for a particular stream type. Not Supported for Log4j
     *
     * @param type the stream type to which the print stream should be set
     * @param ps the print stream
     */
    public void setWriter(STREAM_TYPE type, PrintStream ps) {
        throw new UnsupportedOperationException(
                "Not supported yet."); // To change body of generated methods, choose Tools |
        // Templates.
    }

    /**
     * Return the print stream corresponding to a particular type Not Supported for Log4j
     *
     * @param type the stream type
     * @return the print stream
     */
    @Override
    public PrintStream getWriter(STREAM_TYPE type) {
        throw new UnsupportedOperationException(
                "Not supported yet."); // To change body of generated methods, choose Tools |
        // Templates.
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
    public void log(String message, Exception e, int level) {
        switch (level) {
            case LogManager.FATAL_MESSAGE_LEVEL:
                mLogger.fatal(message, e);
                break;

            case LogManager.ERROR_MESSAGE_LEVEL:
                mLogger.error(message, e);
                break;

            case LogManager.WARNING_MESSAGE_LEVEL:
                mLogger.warn(message, e);
                break;

            case LogManager.CONFIG_MESSAGE_LEVEL:
                mLogger.info(message, e);
                break;

            case LogManager.INFO_MESSAGE_LEVEL:
                mLogger.info(message, e);
                break;

            case LogManager.DEBUG_MESSAGE_LEVEL:
                mLogger.debug(message, e);
                break;

            case LogManager.TRACE_MESSAGE_LEVEL:
                mLogger.trace(message, e);
                break;
        }
    }

    /**
     * Logs the message on the appropriate queue if the level of the message is less than or equal
     * to the level set for the Logger. For INFO level message, the boolean indicating that a
     * completion message is to follow is set to true always.
     *
     * @param message the message to be logged.
     * @param level the level on which the message has to be logged.
     * @see #setLevel(int)
     */
    protected void logAlreadyFormattedMessage(String message, int level) {

        switch (level) {
            case LogManager.FATAL_MESSAGE_LEVEL:
                mLogger.fatal(message);
                break;

            case LogManager.ERROR_MESSAGE_LEVEL:
                mLogger.error(message);
                break;

            case LogManager.WARNING_MESSAGE_LEVEL:
                mLogger.warn(message);
                break;

            case LogManager.CONFIG_MESSAGE_LEVEL:
                mLogger.info(message);
                break;

            case LogManager.INFO_MESSAGE_LEVEL:
                mLogger.info(message);
                break;

            case LogManager.DEBUG_MESSAGE_LEVEL:
                mLogger.debug(message);
                break;

            case LogManager.TRACE_MESSAGE_LEVEL:
                mLogger.trace(message);
                break;
        }
    }

    /**
     * Logs the completion message on the basis of the debug level.
     *
     * @param level the debug level of the start message for whose completion you want.
     */
    public void logEventCompletion(int level) {
        String message = mLogFormatter.getEndEventMessage();
        logAlreadyFormattedMessage(message, level);
        mLogFormatter.popEvent();
    }

    public static void main(String args[]) {
        LogManager manager = LogManagerFactory.loadInstance("Log4j", "Simple", new Properties());
        manager.setLevel(Level.DEBUG);
        manager.logEventStart("test.transfer.generator.stagein", "setup", "0");
        manager.log("ERROR message level should print", ERROR_MESSAGE_LEVEL);
        manager.log("DEBUG message level should print", DEBUG_MESSAGE_LEVEL);
        manager.log("TRACE message level SHOULD NOT print", TRACE_MESSAGE_LEVEL);
        manager.logEventCompletion();
    }
}
