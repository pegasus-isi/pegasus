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
import edu.isi.pegasus.common.util.Currently;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
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
public class Default extends LogManager {

    // level  constants that loosely match Log4J and are used
    // to generate the appropriate mask values.

    /** The type value to indicate a FATAL error message. */
    private static final int FATAL_MESSAGE_TYPE = 0x1;

    /** The type value to indicate an ERROR message. */
    private static final int ERROR_MESSAGE_TYPE = 0x2;

    /** The type value to indicate a CONSOLE message. */
    private static final int CONSOLE_MESSAGE_TYPE = 0x4;

    /** The type value to indicate a WARNING message. */
    private static final int WARNING_MESSAGE_TYPE = 0x8;

    /** The type value to indicate an INFORMATIVE message. */
    private static final int INFO_MESSAGE_TYPE = 0x10;

    /** The type value to indicate a CONFIG message. */
    private static final int CONFIG_MESSAGE_TYPE = 0x20;

    /** The type value to indicate a DEBUG message. */
    private static final int DEBUG_MESSAGE_TYPE = 0x40;

    /** The type value to indicate a DEBUG message. */
    private static final int TRACE_MESSAGE_TYPE = 0x80;

    /** Ensures only one object is created always. Implements the Singleton. */
    private static Default logger;

    /**
     * The stream to which one writes. It is System.out by default for the current release. One can
     * set it using setOutputWriter.
     *
     * @see #setOutputWriter
     */
    private PrintStream mOutStream;

    /** The stream to which all the error messages are logged.By default it is System.err */
    private PrintStream mErrStream;

    /** The mask that needs to be deployed to determine what messages are to be logged. */
    private int mMask;

    /** This is used to format the time stamp. */
    private static Currently mFormatter;

    /** The constructor. */
    public Default() {
        mDebugLevel = 0;
        mOutStream = new PrintStream(System.out, true);
        mErrStream = new PrintStream(System.err, true);
        Default.mFormatter = new Currently("yyyy.MM.dd HH:mm:ss.SSS zzz: ");
        // by default we are logging only CONSOLE
        // and all message less than WARN
        mMask = generateMask(WARNING_MESSAGE_LEVEL, false);
    }

    /**
     * Sets the log formatter to use for formatting the messages.
     *
     * @param formatter the formatter to use.
     * @param properties properties that the underlying implementations understand
     */
    public void initialize(LogFormatter formatter, Properties properties) {
        mLogFormatter = formatter;
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
                value = Default.DEBUG_MESSAGE_LEVEL;
                break;

            case Level.INFO_INT:
                value = Default.INFO_MESSAGE_LEVEL;
                break;

            case Level.WARN_INT:
                value = Default.WARNING_MESSAGE_LEVEL;
                break;

            case Level.ERROR_INT:
                value = Default.ERROR_MESSAGE_LEVEL;
                break;

            default:
                value = Default.FATAL_MESSAGE_LEVEL;
                break;
        }
        setLevel(value, false);
    }

    /**
     * Sets the debug level. All those messages are logged which have a level less than equal to the
     * debug level.
     *
     * @param level the level to which the debug level needs to be set to.
     */
    public void setLevel(int level) {
        setLevel(level, false);
    }

    /**
     * Sets the debug level. All those messages are logged which have a level less than equal to the
     * debug level. In case the boolean info is set, all the info messages are also logged.
     *
     * @param level the level to which the debug level needs to be set to.
     * @param info boolean denoting whether the INFO messages need to be logged or not.
     */
    protected void setLevel(int level, boolean info) {
        mDebugLevel = level;
        mMask = generateMask(level, info);
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
     * @see #setWriters(OutputStream)
     */
    public void setWriters(String out) {
        try {
            //            mOutStream    = (PrintStream)getPrintStream(out);
            //            mErrStream = mOutStream;
            PrintStream ps = (PrintStream) getPrintStream(out);
            System.setOut(ps);
            System.setErr(ps);
            mOutStream = System.out;
            mErrStream = System.err;
        } catch (IOException e) {
            // log on the existing streams !!!
            log("Unable to set streams for logging ", e, this.WARNING_MESSAGE_LEVEL);
        }
    }

    /**
     * Sets both the output writer and the error writer to the same underlying writer.
     *
     * <p>Note: The previous stream is not closed automatically.
     *
     * @param err the stream to which error messages are to be logged.
     */
    /*
    public void setWriters(OutputStream err){
        mOutStream = new PrintWriter( err, true );
        mErrStream = mOutStream;
    }
    */

    /**
     * Sets the writer associated with the class to the one specified for all type of messages other
     * than error messages.
     *
     * @param out is the name of a file to append to. Special names are <code>stdout</code> and
     *     <code>stderr</code>, which map to the system's respective streams.
     * @see #setOutputWriter(OutputStream)
     */
    public void setOutputWriter(String out) {
        try {
            mOutStream = (PrintStream) getPrintStream(out);
        } catch (IOException e) {
            // log on the existing streams !!!
            log("Unable to set streams for logging ", e, this.WARNING_MESSAGE_LEVEL);
        }
    }

    /**
     * Sets the writer associated with the class to the one specified for all type of messages other
     * than error messages. By default it is System.out.
     *
     * @param out the stream to which the messages are logged.
     * @see #setErrorWriter(OutputStream)
     */
    public void setOutputWriter(OutputStream out) {
        mOutStream = new PrintStream(out, true);
    }

    /**
     * Certains levels like FATAL, ERROR and WARN can be set to log to a different stream, than the
     * default stream used for writing other messages. By default, these messages are logged to
     * stderr. Note: The previous stream is not closed automatically.
     *
     * @param out is the name of a file to append to. Special names are <code>stdout</code> and
     *     <code>stderr</code>, which map to the system's respective streams.
     * @see #setErrorWriter(OutputStream)
     */
    public void setErrorWriter(String out) {
        try {
            mErrStream = (PrintStream) getPrintStream(out);
        } catch (IOException e) {
            // log on the existing streams !!!
            log("Unable to set streams for logging ", e, this.WARNING_MESSAGE_LEVEL);
        }
    }

    /**
     * Certains levels like FATAL, ERROR and WARN can be set to log to a different stream, than the
     * default stream used for writing other messages. By default, these messages are logged to
     * stderr. Note: The previous stream is not closed automatically.
     *
     * @param err the stream to which error messages are to be logged.
     */
    public void setErrorWriter(OutputStream err) {
        mErrStream = new PrintStream(err, true);
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
        StringBuffer msg = new StringBuffer();
        msg.append(message).append(" ").append(e.getClass()).append(": ").append(e.getMessage());
        log(msg.toString(), level);
    }

    /**
     * Logs the message on the appropriate queue if the level of the message is less than or equal
     * to the level set for the Logger. For INFO level message, the boolean indicating that a
     * completion message is to follow is set to true always.
     *
     * @param message the message to be logged.
     * @param level the level on which the message has to be logged.
     * @see #setLevel(int)
     * @see #log(String,int,boolean)
     */
    public void logAlreadyFormattedMessage(String message, int level) {
        log(message, level, (level == this.INFO_MESSAGE_LEVEL) ? true : false);
    }

    /**
     * Logs the message on the appropriate queue if the level of the message is less than or equal
     * to the level set for the Logger.
     *
     * @param message the message to be logged.
     * @param level the level on which the message has to be logged.
     * @param comp boolean indicating whether a completion message follows or not.
     * @see #setLevel(int)
     */
    private void log(String message, int level, boolean comp) {
        int type = (int) Math.pow(2, level);
        if ((type & mMask) != 0x0) {
            // we need to log the message
            // get hold of the writer to be used to logging the message.
            PrintStream writer = getPrintStream(level);
            writer.print(Default.mFormatter.now());
            String prefix = getPrefix(type);
            message = prefix + " " + message;
            /*
                          *uncomment if we want commpetion message for INFO
                          *on same line
                         if(comp){
                             if((mMask & INFO_MESSAGE_TYPE) == INFO_MESSAGE_TYPE){
                                 //we need to just print the message
                                 writer.print(message);
                             }
                             else{
                                 //write out on a new line and
                                 //push the message to the stack
                                 writer.println(message);
            //                     mMsgStack.push(message);
                             }
                         }
                         else{
                             writer.println(message);
                         }
                         */
            writer.println(message);
            writer.flush();
        }
    }

    /**
     * Gets the timestamp nicely formatted. It generates the date-timestamp in extended ISO 8601
     * format. It generates the timestamp using the local timezone not the UTC. An example of the
     * date-timestamp generated would be 2003-06-06T14:31:27-07:00 where -07:00 denotes the timezone
     * offset of the local timezone from UTC.
     *
     * @return the formattted timestamp;
     */
    public String getTimeStamp() {
        String st = Default.mFormatter.now();

        st = Currently.iso8601(false);

        return st;
    }

    /**
     * Logs the completion message on the basis of the debug level.
     *
     * @param level the debug level of the start message for whose completion you want.
     */
    public void logEventCompletion(int level) {
        String message = mLogFormatter.getEndEventMessage();
        mLogFormatter.popEvent();

        int type = (int) Math.pow(2, level);
        if ((type & mMask) != 0x0) {
            PrintStream writer = getPrintStream(level);
            /*uncomment if we want commpetion message for INFO
              on same line
            if ( (mMask & INFO_MESSAGE_TYPE) == INFO_MESSAGE_TYPE) {
                writer.println(" (completed)");
            }
            else {
                writer.print(LogManager.mFormatter.now());
                writer.println(message + " (completed)");
            }
            */
            String prefix = getPrefix(type);
            message = prefix + " " + message;
            writer.print(Default.mFormatter.now());
            writer.println(message);
            // writer.println(message + " (completed)");
        }
    }

    /**
     * Generates the appropriate mask value, corresponding to the level passed.
     *
     * @param level the level to which the debug level needs to be set to.
     * @param info boolean denoting whether the CONSOLE messages need to be logged or not.
     * @return mask corresponding to the debug level passed.
     */
    private int generateMask(int level, boolean info) {

        // construct the appropriate mask
        int mask = 0x0;
        for (int i = 0; i <= level; i++) {
            mask |= (int) Math.pow(2, i);
        }
        if (info) {
            mask |= CONSOLE_MESSAGE_TYPE;
        }
        return mask;
    }

    /**
     * Returns the prefix that needs to be logged corresponding to a particular message type, when a
     * message is being logged. Should be returning an enumerated data type.
     *
     * @param type the type for which prefix is required.
     * @return the message type
     */
    private String getPrefix(int type) {
        String result = null;
        switch (type) {
            case FATAL_MESSAGE_TYPE:
                result = "[FATAL ERROR]";
                break;

            case ERROR_MESSAGE_TYPE:
                result = "[ERROR]";
                break;

            case CONSOLE_MESSAGE_TYPE:
                result = "";
                break;

            case WARNING_MESSAGE_TYPE:
                result = "[WARNING]";
                break;

            case INFO_MESSAGE_TYPE:
                result = "[INFO]";
                break;

            case CONFIG_MESSAGE_TYPE:
                result = "[CONFIG]";
                break;

            case DEBUG_MESSAGE_TYPE:
                result = "[DEBUG]";
                break;

            case TRACE_MESSAGE_TYPE:
                result = "[TRACE]";
                break;

            default:
                result = "[UNKNOWN]";
        }
        return result;
    }

    /**
     * Sets an internal writer to point to a particular stream.
     *
     * @param out is the name of a file to append to. Special names are <code>stdout</code> and
     *     <code>stderr</code>, which map to the system's respective streams.
     * @return the corresponding PrintStream.
     * @throws IOException in case of being unable to open a stream.
     */
    private PrintStream getPrintStream(String out) throws IOException {
        // check if value refers to any of the predefined streams
        OutputStream stream;
        if (out.equalsIgnoreCase("stdout")) {
            stream = System.out;
        } else if (out.equalsIgnoreCase("stderr")) {
            stream = System.err;
        } else {
            // try to create an output stream to file specified
            File f = new File(out);

            // do some sanity checks on file
            sanityCheckOnFile(f);
            stream = new FileOutputStream(f);
        }
        return new PrintStream(stream);
    }

    /**
     * Returns a PrintWriter stream on which to log the message. Later on this, function would
     * return the appropriate LOG4J queue on which the message needs to be logged.
     *
     * @param level the level
     * @return PrintWriter for logging the message.
     */
    private PrintStream getPrintStream(int level) {
        return ((level >= FATAL_MESSAGE_LEVEL && level < CONSOLE_MESSAGE_LEVEL)
                        || level == WARNING_MESSAGE_LEVEL)
                ? mErrStream
                : mOutStream;
    }
}
