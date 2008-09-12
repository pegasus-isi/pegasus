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


import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import org.apache.log4j.Appender;
import org.apache.log4j.PropertyConfigurator;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.PatternLayout;

import java.util.Enumeration;

/**
 *
 * 
 *
 * @author Karan Vahi
 * @author Gaurang Mehta
 * @version $Revision$
 */
public class Log4jLogger extends LogManager{

    //level  constants that loosely match Log4J and are used
    //to generate the appropriate mask values.

    /**
     * The handle to a log4j logger object.
     */
    private Logger mLogger;
    
    /**
     * Keeps track of log4j's root logger as singleton.
     */
    private static Logger mRoot;
    
    /**
     * Initializes the root logger when this class is loaded.
     */
    static {
        if ( (mRoot = Logger.getRootLogger()) != null ) {
      
            //get hold of all appenders and override the console appender
            for ( Enumeration e = mRoot.getAllAppenders(); e.hasMoreElements(); ){
                Appender a = ( Appender )e.nextElement();
                if( a instanceof ConsoleAppender ){
                    //set the layout of the console appender 
                    //this can be overriden by the log4j.properties file
                    a.setLayout( new PatternLayout("%d{yyyy-MM-dd HH:mm:ss.SSS} %-5p [%c{1}] %m%n") );
                }
            }
            mRoot.setLevel( Level.INFO );
            mRoot.debug( "starting" );
        }
    }

    /**
     * The constructor.
     */
    protected Log4jLogger(){
        //configure properties through log4j.properties file
        //PropertyConfigurator.configure( "/Users/Vahi/Pegasus/work/log4j.properties" );
        mLogger = Logger.getLogger( "pegasus" );
    }

    


    /**
     * Sets the debug level. All those messages are logged which have a
     * level less than equal to the debug level.
     *
     * @param level   the level to which the debug level needs to be set to.
     */
    public void setLevel(Level level){
        setLevel( level, true );
    }


    /**
     * Sets the debug level. All those messages are logged which have a
     * level less than equal to the debug level. In addition the info messages
     * are always logged.
     *
     * @param level   the level to which the debug level needs to be set to.
     */
    public void setLevel(int level){
        setLevel( level, true );
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
    protected void setLevel( int  level, boolean info){
        Level l = Level.ALL;
        switch( level ){
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
                
        }
        mLogger.setLevel( l );
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
    protected void setLevel(Level level, boolean info){
        mDebugLevel = level.toInt();
        mLogger.setLevel( level );
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
        throw new UnsupportedOperationException( "Log4jLogger does not support setWriters(out)" );
    }




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
    public void log( String message, Exception e, int level ){
        switch( level ){
            case LogManager.FATAL_MESSAGE_LEVEL:
                mLogger.fatal( message, e );
                break;
                
            case LogManager.ERROR_MESSAGE_LEVEL:
                mLogger.error( message, e );
                break;
                
            case LogManager.WARNING_MESSAGE_LEVEL:
                mLogger.warn( message, e );
                break;
                
            case LogManager.CONFIG_MESSAGE_LEVEL:
                mLogger.info( message, e );
                break;
                
            case LogManager.INFO_MESSAGE_LEVEL:
                mLogger.info( message, e );
                break;
                
            case LogManager.DEBUG_MESSAGE_LEVEL:
                mLogger.debug( message, e );
                break;
                
        }
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
        
        switch( level ){
            case LogManager.FATAL_MESSAGE_LEVEL:
                mLogger.fatal( message );
                break;
                
            case LogManager.ERROR_MESSAGE_LEVEL:
                mLogger.error( message );
                break;
                
            case LogManager.WARNING_MESSAGE_LEVEL:
                mLogger.warn( message );
                break;
                
            case LogManager.CONFIG_MESSAGE_LEVEL:
                mLogger.info( message );
                break;
                
            case LogManager.INFO_MESSAGE_LEVEL:
                mLogger.info( message );
                break;
                
            case LogManager.DEBUG_MESSAGE_LEVEL:
                mLogger.debug( message );
                break;
                
        }
        
    }

   
     /**
      * Logs the completion message on the basis of the debug level.
      *
      * @param message the message to be logged.
      * @param level  the debug level of the start message for whose completion
      *                    you want.
      */
     public void logCompletion( String message, int level ){
         log( message + " COMPLETED", level );
     }

}
