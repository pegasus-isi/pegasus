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

/**
 * A utility class that allows us to determine condor version.
 * 
 * @author Karan Vahi
 */
public class CondorVersion {

    /**
     * Predefined Constant for condor version 7.1.0
     */
    public static final long v_7_1_0 = CondorVersion.numericValue( "7.1.0" );
            
    
    /**
     * Predefined Constant for condor version 7.1.2
     */
    public static final long v_7_1_2 = CondorVersion.numericValue( "7.1.2" );
            
    
    /**
     * Predefined Constant for condor version 7.1.3
     */
    public static final long v_7_1_3 = CondorVersion.numericValue( "7.1.3" );
    
    /**
     * Predefined Constant for condor version 8.3.6
     */
    public static final long v_8_3_6 = CondorVersion.numericValue( "8.3.6" );
    
    /**
     * Predefined Constant for condor version 8.5.6
     */
    public static final long v_8_5_6 = CondorVersion.numericValue( "8.5.6" );
    
    
    /**
     * The maximum number of components version can have. MAJOR, MINOR, PATCH
     */
    private static final  int MAX_NUMBER_OF_VERSION_COMPONENTS = 3;
    
    /**
     * The maximum number of digits each component of version can have.
     */
    private static final  int MAX_VERSION_PRECISION = 2;
    
    
    /**
     * The condor version command to be executed.
     */
    public static final String CONDOR_VERSION_COMMAND = "condor_version";
    
    
     /**
      * Store the regular expressions necessary to parse the output of
      * condor_version. The rule for the format is
      *
      * $CondorVersion: 7.4.1 Dec 17 2009 <ANY_ARBITRARY_STRING> $
      * where <ANY_ARBITRARY_STRING> may or may not be there, and can include spaces but is
      * really completely arbitrary.
      *
      * e.g. $CondorVersion: 7.1.0 Apr  1 2008 BuildID: 80895$
      */
    private static final String mRegexExpression =
//                                     "\\$CondorVersion:\\s*([0-9][\\.][0-9][\\.][0-9])[a-zA-Z:0-9\\s]*\\$";
                                         //"\\$CondorVersion:\\s*([0-9][\\.][0-9][\\.][0-9])[\\w\\W\\s]*\\$";
                                         "\\$CondorVersion:\\s*([0-9][\\.][0-9][\\.][0-9])[\\p{ASCII}\\s]*\\$";


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
     * 
     * @deprecated
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
     * Converts a string into the corresponding numeric value.
     * 
     * @param version in form of major.minor.patch. You can opt to omit the 
     *                minor and patch versions if you want
     * 
     * @return float value of the version, else -1 in case of null version 
     *         or incorrect formatted string
     */
    public static long numericValue( String version ){
        long result = 0;
        if( version == null ){
            return -1;
        }
        
        //we are converting to XX.XX.XX 
        //add extra padding that is leading zero if only one digit
        char[] arr = new char[6];
        
        //split on .
        try{
            String[] subs = version.split( "\\." );
            int y = subs.length;
            if ( y > CondorVersion.MAX_NUMBER_OF_VERSION_COMPONENTS ){
                throw new IllegalArgumentException( 
                        "Only version numbers with max two dots are accepted i.e ( MAJOR.MINOR.PATCH ) " + version );
            }
            
            int i = 0;
           
                //for each sub convert to a two digit form
                for( int z = 0; z < y; z++ ){
                    
                    //compute the sub length
                    int len = subs[z].length();
                    if( len > CondorVersion.MAX_VERSION_PRECISION ){
                        throw new IllegalArgumentException( "Only two digit precision is allowed in version numbers "  + version);
                    }
                    
                    //add leading zeros if required
                    for ( int d = CondorVersion.MAX_VERSION_PRECISION - len; d > 0; d--){
                        arr[i++] = '0';
                    }
                    
                    //copy into arr the sub[z]
                    for( int d = 0; d < len; d++){
                        char ch = subs[z].charAt( d );
                        if( !Character.isDigit(ch) ){
                            throw new IllegalArgumentException( "Non digit specified in version "  + version);
                        }
                        arr[i++] = ch;
                    }
                }
            
            //add trailing zeroes if required
            while( i < 6 ){  
                arr[i++] = '0';
            }
            
        }
        catch( NumberFormatException nfe ){
            result = -1;
        }
        
        return Long.parseLong( new String(arr) );
    }
    
    /**
     * The default logger.
     */
    private LogManager mLogger;
    
    /**
     * Factory method to instantiate the class.
     *
     * @return  instance to the class
     */
    public static CondorVersion getInstance( ){
        return getInstance( null );
    }
    
    /**
     * Factory method to instantiate the class.
     * 
     * 
     * @param logger   the logger object
     *
     * @return  instance to the class.
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
    public long numericValue(){
        long result = -1;
        try{
            result = CondorVersion.numericValue( version() );
        }
        catch( Exception e ){
            mLogger.log("Exception while parsing condor_version ", e,
                        LogManager.ERROR_MESSAGE_LEVEL);
        }
        return result;
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
        }
        catch( InterruptedException ie){
            //ignore
        }
        
        mLogger.log( "Condor Version as string " + version, LogManager.DEBUG_MESSAGE_LEVEL );
        return version;
    }
    
    /**
     * An inner class, that implements the StreamGobblerCallback to determine
     * the version of Condor being used.
     *
     */
    private static class CondorVersionCallback implements StreamGobblerCallback{

        
       

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

    /**
     * The main program to test.
     *
     * @param args
     */
    public static void main( String[] args ){
        LogManager logger =  LogManagerFactory.loadSingletonInstance();
        CondorVersion cv = CondorVersion.getInstance();
        
        logger.logEventStart( "CondorVersion", "CondorVersion", "Version");
        System.out.println( "Condor Version is " + cv.version() );
        
        System.out.println( "10.0.0 is " + CondorVersion.numericValue( "10.0.0") );
        System.out.println( "7.1.2 is " + CondorVersion.numericValue( "7.1.2") );
        System.out.println( "7.1.18 is " + CondorVersion.numericValue( "7.1.18" ) ); 
        System.out.println( "7.1.19 is " + CondorVersion.numericValue( "7.1.19" ) ); 
        System.out.println( "6.99.9 is " + CondorVersion.numericValue( "6.99.9" ) ); 
        System.out.println( "7 is " + CondorVersion.numericValue( "7.2.2" ) );
        logger.logEventCompletion();

        //some sanity checks on the Regex
        String version = "$CondorVersion: 7.4.1 Dec 17 2009 UWCS-PRE $";
        Matcher matcher = cv.mPattern.matcher( version );
        if( matcher.matches() ){
            System.out.println( "Version for " + version + " is " + matcher.group( 1 ));
        }


        version = "$CondorVersion: 7.4.1 Dec 17 2009 BuildID: 204351 $";
        matcher = cv.mPattern.matcher( version );
        if( matcher.matches() ){
            System.out.println( "Version for " + version + " is " + matcher.group( 1 ));
        }
    }
}
