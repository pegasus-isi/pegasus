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

package org.griphyn.cPlanner.common;


import org.apache.log4j.Level;

import java.io.File;
import java.io.IOException;


/**
 * The logging class that to log messages at different levels.
 * Currently the following levels are supported.<p>
 *
 * Eventually, each of the level can have a different writer stream underneath.
 *
 * <p>
 * The messages can be logged at various levels. The various levels of logging
 * with increasing levels of verbosity are displayed in the following table.
 *
 * <p>
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

    //level  constants that loosely match Log4J and are used
    //to generate the appropriate mask values.

    /**
     * The level value, to indicate a FATAL error message.
     */
    public static final int FATAL_MESSAGE_LEVEL = 0;

    /**
     * The level value, to indicate an ERROR message.
     */
    public static final int ERROR_MESSAGE_LEVEL = 1;

    /**
     * The level value, to indicate a WARNING error message.
     */
    public static final int WARNING_MESSAGE_LEVEL = 2;

    /**
     * The level value, to indicate a INFO message.
     */
    public static final int INFO_MESSAGE_LEVEL = 3;

    /**
     * The level value, to indicate a CONFIG message.
     */
    public static final int CONFIG_MESSAGE_LEVEL = 4;

    /**
     * The level value, to indicate a DEBUG message.
     */
    public static final int DEBUG_MESSAGE_LEVEL = 5;

    

    /**
     * Ensures only one object is created always. Implements the Singleton.
     */
    private static LogManager logger;

    /**
     * The debug level. Higher the level the more the detail is logged. At present
     * can be 0 or 1. This is set according to the option given by the user, whether
     * verbose or not.
     */
    protected int mDebugLevel;

 

    /**
     * The constructor.
     */
    protected LogManager(){
        mDebugLevel    = 0;
        
    }

    /**
     * To get a reference to the the object.
     *
     * @return a singleton access to the object.
     */
    public static LogManager getInstance(){
        if(logger == null){
            logger = new DefaultLogger();
        }
        return logger;
    }

    /**
     * Checks the destination location for existence, if it can
     * be created, if it is writable etc.
     *
     * @param file is the file to write out to.
     *
     * @throws IOException in case of error while writing out files.
     */
    protected static void sanityCheckOnFile( File file ) throws IOException{
        if (file.exists()) {
            // location exists
            if (file.isFile()) {
                // ok, is a file
                if (file.canWrite()) {
                    // can write, all is well
                    return;
                }
                else {
                    // all is there, but I cannot write to file
                    throw new IOException("Cannot write to existing file " +
                                          file.getAbsolutePath());
                }
            }
            else {
                // exists but not a file
                throw new IOException("File " + file.getAbsolutePath() +
                                      " already " +
                                      "exists, but is not a file.");
            }
        }
        else {
            // check to see if you can write to the parent directory
            //could have tried to do just a make dir on parent directory.
            sanityCheckOnDirectory( file.getParentFile());
        }
    }

    /**
     * Checks the destination location for existence, if it can
     * be created, if it is writable etc.
     *
     * @param dir is the new base directory to optionally create.
     *
     * @throws IOException in case of error while writing out files.
     */
    protected static void sanityCheckOnDirectory( File dir ) throws IOException{
        if ( dir.exists() ) {
            // location exists
            if ( dir.isDirectory() ) {
                // ok, isa directory
                if ( dir.canWrite() ) {
                    // can write, all is well
                    return;
                } else {
                    // all is there, but I cannot write to dir
                    throw new IOException( "Cannot write to existing directory " +
                                           dir.getPath() );
                }
            } else {
                // exists but not a directory
                throw new IOException( "Destination " + dir.getPath() + " already " +
                                       "exists, but is not a directory." );
            }
        } else {
            // does not exist, try to make it
            if ( ! dir.mkdirs() ) {
                throw new IOException( "Unable to create directory destination " +
                                       dir.getPath() );
            }
        }
    }



    /**
     * Sets the debug level. All those messages are logged which have a
     * level less than equal to the debug level.
     *
     * @param level   the level to which the debug level needs to be set to.
     */
    public void setLevel(Level level){
        int value = level.toInt();
        switch(value){
            case Level.DEBUG_INT:
                value = this.DEBUG_MESSAGE_LEVEL;
                break;

            case Level.INFO_INT:
                value = this.INFO_MESSAGE_LEVEL;
                break;

            case Level.WARN_INT:
                value = this.WARNING_MESSAGE_LEVEL;
                break;

            case Level.ERROR_INT:
                value = this.ERROR_MESSAGE_LEVEL;
                break;

            default:
                value = this.FATAL_MESSAGE_LEVEL;
                break;
        }
        setLevel(value,false);
    }


    /**
     * Sets the debug level. All those messages are logged which have a
     * level less than equal to the debug level. In addition the info messages
     * are always logged.
     *
     * @param level   the level to which the debug level needs to be set to.
     */
    public void setLevel(int level){
        setLevel(level,true);
    }


    /**
     * Sets the debug level. All those messages are logged which have a
     * level less than equal to the debug level. In case the boolean info
     * is set, all the info messages are also logged.
     *
     * @param level the level to which the debug level needs to be set to.
     * @param info  boolean denoting whether the INFO messages need to be
     *              logged or not.
     */
    protected abstract void setLevel(int level, boolean info);


    /**
     * Returns the debug level.
     *
     * @return  the level to which the debug level has been set to.
     */
    public abstract int getLevel();

    /**
     * Sets both the output writer and the error writer to the same
     * underlying writer.
     *
     * @param out is the name of a file to append to. Special names are
     * <code>stdout</code> and <code>stderr</code>, which map to the
     * system's respective streams.
     *
     * @see #setWriters(OutputStream)
     */
    public abstract void setWriters( String out );

    /**
     * Sets both the output writer and the error writer to the same
     * underlying writer.
     *
     * Note: The previous stream is not closed automatically.
     *
     * @param err  the stream to which error messages are to be logged.
     */
    //public abstract void setWriters(OutputStream err);


    /**
     * Logs the exception on the appropriate queue if the level of the message
     * is less than or equal to the level set for the Logger. For INFO level
     * message, the boolean indicating that a completion message is to follow
     * is set to true always.
     *
     * @param message  the message to be logged.
     * @param e        the exception to be logged
     * @param level    the level on which the message has to be logged.
     *
     * @see #setLevel(int)
     * @see #log(String,int)
     */
    public abstract void log(String message, Exception e,int level);


    /**
     * Logs the message on the appropriate queue if the level of the message
     * is less than or equal to the level set for the Logger. For INFO level
     * message, the boolean indicating that a completion message is to follow
     * is set to true always.
     *
     * @param message  the message to be logged.
     * @param level    the level on which the message has to be logged.
     *
     * @see #setLevel(int)
     * @see #log(String,int,boolean)
     */
    public abstract void log ( String message, int level);

    


     /**
      * Logs the completion message on the basis of the debug level.
      *
      * @param message the message to be logged.
      * @param level  the debug level of the start message for whose completion
      *                    you want.
      */
     public abstract void logCompletion(String message,int level);


   
}
