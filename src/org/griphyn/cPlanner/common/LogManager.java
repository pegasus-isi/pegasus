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

import org.griphyn.common.util.Currently;

import org.apache.log4j.Level;

import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Writer;

import java.util.Stack;

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
 * @version $Revision: 1.12 $
 */
public class LogManager {

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
     * The type value to indicate a FATAL error message.
     */
    private static final int FATAL_MESSAGE_TYPE = 0x1;

    /**
     * The type value to indicate an ERROR message.
     */
    private static final int ERROR_MESSAGE_TYPE = 0x2;

    /**
     * The type value to indicate a WARNING message.
     */
    private static final int WARNING_MESSAGE_TYPE = 0x4;

    /**
     * The type value to indicate an INFORMATIVE message.
     */
    private static final int INFO_MESSAGE_TYPE = 0x8;

    /**
     * The type value to indicate a CONFIG message.
     */
    private static final int CONFIG_MESSAGE_TYPE = 0x10;

    /**
     * The type value to indicate a DEBUG message.
     */
    private static final int DEBUG_MESSAGE_TYPE = 0x20;

    /**
     * Ensures only one object is created always. Implements the Singleton.
     */
    private static LogManager logger;

    /**
     * The debug level. Higher the level the more the detail is logged. At present
     * can be 0 or 1. This is set according to the option given by the user, whether
     * verbose or not.
     */
    private int mDebugLevel;

    /**
     * The stream to which one writes. It is System.out by default for the
     * current release. One can set it using setOutputWriter.
     *
     * @see #setOutputWriter
     */
    private PrintWriter mWriter;

    /**
     * The stream to which all the error messages are logged.By default it is
     * System.err
     */
    private PrintWriter mErrWriter;

    /**
     * The mask that needs to be deployed to determine what messages are to be
     * logged.
     */
    private int mMask;

    /**
     * This is used to format the time stamp.
     */
    private static Currently mFormatter ;

    /**
     * The constructor.
     */
    private LogManager(){
        mDebugLevel    = 0;
        mWriter        = new PrintWriter(System.out,true);
        mErrWriter     = new PrintWriter(System.err,true);
        LogManager.mFormatter = new Currently( "yyyy.MM.dd HH:mm:ss.SSS zzz: " );
        //by default we are logging only INFO
        //and all message less than WARN
        mMask = generateMask(WARNING_MESSAGE_LEVEL,true);
    }

    /**
     * To get a reference to the the object.
     *
     * @return a singleton access to the object.
     */
    public static LogManager getInstance(){
        if(logger == null){
            logger = new LogManager();
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
    private static void sanityCheckOnFile( File file ) throws IOException{
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
    private static void sanityCheckOnDirectory( File dir ) throws IOException{
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
    public void setLevel(int level, boolean info){
        mDebugLevel = level;
        mMask = generateMask(level,info);
    }


    /**
     * Returns the debug level.
     *
     * @return  the level to which the debug level has been set to.
     */
    public int getLevel(){
        return mDebugLevel;
    }

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
    public void setWriters(String out){
        try{
            mWriter    = (PrintWriter)getWriter(out);
            mErrWriter = mWriter;
        }
        catch(IOException e){
            //log on the existing streams !!!
            log("Unable to set streams for logging ",e,
                this.WARNING_MESSAGE_LEVEL);
        }

    }


    /**
     * Sets both the output writer and the error writer to the same
     * underlying writer.
     *
     * Note: The previous stream is not closed automatically.
     *
     * @param err  the stream to which error messages are to be logged.
     */
    public void setWriters(OutputStream err){
        mWriter = new PrintWriter( err, true );
        mErrWriter = mWriter;
    }


    /**
     * Sets the writer associated with the class to the one specified for all
     * type of messages other than error messages.
     *
     * @param out is the name of a file to append to. Special names are
     * <code>stdout</code> and <code>stderr</code>, which map to the
     * system's respective streams.
     *
     * @see #setOutputWriter(OutputStream)
     */
    public void setOutputWriter(String out){
        try{
            mWriter = (PrintWriter)getWriter(out);
        }
        catch(IOException e){
            //log on the existing streams !!!
            log("Unable to set streams for logging ",e,
                this.WARNING_MESSAGE_LEVEL);
        }
    }

    /**
     * Sets the writer associated with the class to the one specified for all
     * type of messages other than error messages.
     * By default it is System.out.
     *
     * @param out  the stream to which the messages are logged.
     *
     * @see #setErrorWriter(OutputStream)
     */
    public void setOutputWriter(OutputStream out){
        mWriter = new PrintWriter( out, true );
    }

    /**
     * Certains levels like FATAL, ERROR and WARN can be set to log to a
     * different stream, than the default stream used for writing other messages.
     * By default, these messages are logged to stderr.
     * Note: The previous stream is not closed automatically.
     *
     * @param out is the name of a file to append to. Special names are
     * <code>stdout</code> and <code>stderr</code>, which map to the
     * system's respective streams.
     *
     * @see #setErrorWriter(OutputStream)
     */
    public void setErrorWriter(String out){
        try{
            mErrWriter = (PrintWriter)getWriter(out);
        }
        catch(IOException e){
            //log on the existing streams !!!
            log("Unable to set streams for logging ",e,
                this.WARNING_MESSAGE_LEVEL);
        }

    }


    /**
     * Certains levels like FATAL, ERROR and WARN can be set to log to a
     * different stream, than the default stream used for writing other messages.
     * By default, these messages are logged to stderr.
     * Note: The previous stream is not closed automatically.
     *
     * @param err  the stream to which error messages are to be logged.
     */
    public void setErrorWriter(OutputStream err){
        mErrWriter = new PrintWriter( err, true );
    }



    /**
     * Logs the exception on the appropriate queue if the level of the message
     * is less than or equal to the level set for the Logger. For INFO level
     * message, the boolean indicating that a completion message is to follow
     * is set to true always.
     *
     * @param message  the message to be logged.
     * @param level    the level on which the message has to be logged.
     *
     * @see #setLevel(int)
     * @see #log(String,int)
     */
    public void log(String message, Exception e,int level){
        StringBuffer msg = new StringBuffer();
        msg.append(message).append(": ").append(e.getMessage());
        log(msg.toString(),level);
    }


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
    public void log(String message, int level){
        log(message,level,(level == this.INFO_MESSAGE_LEVEL) ?
                            true : false);
    }

    /**
     * Logs the message on the appropriate queue if the level of the message
     * is less than or equal to the level set for the Logger.
     *
     * @param message  the message to be logged.
     * @param level    the level on which the message has to be logged.
     * @param comp     boolean indicating whether a completion message
     *                 follows or not.
     *
     * @see #setLevel(int)
     */
     private void log(String message, int level, boolean comp){
         int type = (int)Math.pow(2, level);
         if( (type & mMask) != 0x0 ){
             //we need to log the message
             //get hold of the writer to be used to logging the message.
             PrintWriter writer = getWriter(level);
             writer.print(LogManager.mFormatter.now());
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
      * Gets the timestamp nicely formatted. It generates the date-timestamp
      * in extended ISO 8601 format. It generates the timestamp using
      * the local timezone not the UTC. An example of the date-timestamp
      * generated would be 2003-06-06T14:31:27-07:00 where -07:00 denotes the timezone
      * offset of the local timezone from UTC.
      *
      * @return the formattted timestamp;
      */
     public String getTimeStamp(){
         String st =  LogManager.mFormatter.now();

         st = Currently.iso8601(false);

         return st;
     }

     /**
      * Logs the completion message on the basis of the debug level.
      *
      * @param message the message to be logged.
      * @param level  the debug level of the start message for whose completion
      *                    you want.
      */
     public void logCompletion(String message,int level){
         int type = (int)Math.pow(2, level);
         if( (type & mMask) != 0x0 ){
             PrintWriter writer = getWriter(level);
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
            writer.print(LogManager.mFormatter.now());
            writer.println(message + " (completed)");
         }
     }


    /**
     * Generates the appropriate mask value, corresponding to the level
     * passed.
     *
     * @param level the level to which the debug level needs to be set to.
     * @param info  boolean denoting whether the INFO messages need to be
     *              logged or not.
     *
     * @return mask corresponding to the debug level passed.
     */
    private int generateMask(int level,boolean info){

        //construct the appropriate mask
        int mask = 0x0;
        for(int i = 0; i <= level; i++){
            mask |= (int)Math.pow(2,i);
        }
        if(info){
            mask |= INFO_MESSAGE_TYPE;
        }
        return mask;
    }

    /**
     * Returns the prefix that needs to be logged corresponding to a particular
     * message type, when a message is being logged.
     * Should be returning an enumerated data type.
     *
     * @return the message type
     */
    private String getPrefix(int type){
        String result = null;
        switch(type){
            case FATAL_MESSAGE_TYPE:
                result = "[FATAL ERROR]";
                break;

            case ERROR_MESSAGE_TYPE:
                result = "[ERROR]";
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

            default:
                result = "[UNKNOWN]";
        }
        return result;
    }

    /**
     * Sets an internal writer to point to a particular stream.
     *
     * @param out is the name of a file to append to. Special names are
     * <code>stdout</code> and <code>stderr</code>, which map to the
     * system's respective streams.
     *
     * @return the corresponding writer.
     *
     * @throws IOException in case of being unable to open a stream.
     */
    private Writer getWriter( String out ) throws IOException{
        //check if value refers to any of the predefined streams
        OutputStream stream;
        if( out.equalsIgnoreCase("stdout")){ stream = System.out; }
        else if( out.equalsIgnoreCase("stderr")){ stream = System.err; }
        else{
            //try to create an output stream to file specified
            File f = new File( out );

            //do some sanity checks on file
            sanityCheckOnFile( f );
            stream = new FileOutputStream( f);
        }
        return new PrintWriter(stream);
    }

    /**
     * Returns a PrintWriter stream on which to log the message. Later on
     * this, function would return the appropriate LOG4J queue on which
     * the message needs to be logged.
     *
     * @return PrintWriter for logging the message.
     */
    private PrintWriter getWriter(int level){
        return (level >= FATAL_MESSAGE_LEVEL && level <= WARNING_MESSAGE_LEVEL)?
               mErrWriter:
               mWriter;
    }
}
