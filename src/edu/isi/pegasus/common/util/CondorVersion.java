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

package edu.isi.pegasus.common.util;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.common.logging.LogManagerFactory;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.griphyn.cPlanner.common.StreamGobbler;
import org.griphyn.cPlanner.common.StreamGobblerCallback;

/**
 * A utility class that allows us to determine condor version.
 * 
 * @author Karan Vahi
 */
public class CondorVersion {

    /**
     * The condor version command to be executed.
     */
    public static final String CONDOR_VERSION_COMMAND = "condor_version";
    
    
     /**
      * Store the regular expressions necessary to parse the output of
      * condor_version
      * e.g. $CondorVersion: 7.1.0 Apr  1 2008 BuildID: 80895 
      */
    private static final String mRegexExpression =
                                     "\\$CondorVersion:\\s*([0-9][.][0-9][.][0-9])[a-zA-Z:0-9\\s]*\\$";


    /**
     * Stores compiled patterns at first use, quasi-Singleton.
     */
    private static Pattern mPattern = null;
   
    /**
     * Converts a string into the corresponding integer value.
     * 
     * @param version
     * 
     * @return int value of the version, else -1 in case of null version 
     *         or incorrect formatted string
     */
    public static int intValue( String version ){
        int result = 0;
        if( version == null ){
            return -1;
        }
        
        //split on .
        try{
            String[] subs = version.split( "\\." );
            int index = subs.length;
            for( int i = 0, y = subs.length - 1; y >= 0; y--,i++){
                result += (int) (Math.pow(10, y) * (Integer.parseInt(subs[i])));
            }
        }
        catch( NumberFormatException nfe ){
            result = -1;
        }
        
        return result;
    }
    
    
    /**
     * The default logger.
     */
    private LogManager mLogger;
    
    /**
     * Factory method to instantiate the class.
     */
    public static CondorVersion getInstance( ){
        return getInstance( null );
    }
    
    /**
     * Factory method to instantiate the class.
     * 
     * 
     * @param logger   the logger object
     */
    public static CondorVersion getInstance( LogManager logger ){
        if( logger == null ){
            logger = LogManagerFactory.loadSingletonInstance();
        }
        return new CondorVersion( logger );
    }
    
    /**
     * The default constructor.
     * 
     * @param logger   the logger object
     */
    private CondorVersion( LogManager logger ){
        mLogger = logger;
        if( mPattern == null ){
             mPattern = Pattern.compile( mRegexExpression );
         }
    }
    
    /**
     * Returns the condor version parsed by executing the condor_version 
     * command. 
     * 
     * @return the version number as int else -1 if unable to determine.
     */
    public int versionAsInt(){
        return CondorVersion.intValue( version() );
    }
    
    /**
     * Returns the condor version parsed by executing the condor_version 
     * command. 
     * 
     * @return the version number as String else null if unable to determine.
     */
    public String version(){
        String version = null;
        
        try{
            //set the callback and run the grep command
            CondorVersionCallback c = new CondorVersionCallback( );
            Runtime r = Runtime.getRuntime();
            Process p = r.exec( CONDOR_VERSION_COMMAND );
            
            //Process p = r.exec( CONDOR_VERSION_COMMAND );

            //spawn off the gobblers
            StreamGobbler ips = new StreamGobbler(p.getInputStream(), c);
            StreamGobbler eps = new StreamGobbler(p.getErrorStream(),
                                                  new StreamGobblerCallback(){
                                                      //we cannot log to any of the default stream
                                                      LogManager mLogger = this.mLogger;
                                                      public void work(String s){
                                                          mLogger.log("Output on stream gobller error stream " +
                                                                      s,LogManager.DEBUG_MESSAGE_LEVEL);
                                                      }
                                                     });
            ips.start();
            eps.start();

            //wait for the threads to finish off
            ips.join();
            version = c.getVersion();
            eps.join();

            //get the status
            int status = p.waitFor();
            if( status != 0){
                mLogger.log("Command " + CONDOR_VERSION_COMMAND + " exited with status " + status,
                            LogManager.WARNING_MESSAGE_LEVEL);
            }

        }
        catch(IOException ioe){
            mLogger.log("IOException while determining condor_version ", ioe,
                        LogManager.ERROR_MESSAGE_LEVEL);
            ioe.printStackTrace();
        }
        catch( InterruptedException ie){
            //ignore
        }
        
        return version;
    }
    
    /**
     * An inner class, that implements the StreamGobblerCallback to determine
     * the version of Condor being used.
     *
     */
    private class CondorVersionCallback implements StreamGobblerCallback{

        
       

        /**
         * The version detected.
         */
        private String mVersion;

        /**
         * The Default Constructor
         */
        public CondorVersionCallback(  ){
            mVersion = null;
        }

        /**
         * Callback whenever a line is read from the stream by the StreamGobbler.
         * Counts the occurences of the word that are in the line, and
         * increments to the global counter.
         *
         * @param line   the line that is read.
         */
        public void work( String line ){
            Matcher matcher = mPattern.matcher( line );
            if( matcher.matches( ) ){
                mVersion = matcher.group( 1 );
            }
        }

        /**
         * Returns the condor version detected.
         *
         * @return the condor version else null
         */
        public String getVersion(){
            return mVersion;
        }

        

    }
    
    public static void main( String[] args ){
        LogManager logger =  LogManagerFactory.loadSingletonInstance();
        CondorVersion cv = CondorVersion.getInstance();
        
        logger.logEventStart( "CondorVersion", "CondorVersion", "Version");
        System.out.println( "Condor Version is " + cv.version() );
        
        System.out.println( CondorVersion.intValue( "7.1.2") ); 
        System.out.println( CondorVersion.intValue( "7.1.5s" ) ); 
        logger.logEventCompletion();
    }
}
