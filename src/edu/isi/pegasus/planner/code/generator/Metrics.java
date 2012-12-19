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
package edu.isi.pegasus.planner.code.generator;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.common.logging.LogManagerFactory;

import edu.isi.pegasus.planner.classes.PlannerMetrics;

import edu.isi.pegasus.common.util.Boolean;

import edu.isi.pegasus.planner.classes.PegasusBag;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;

import java.net.HttpURLConnection;
import java.net.URL;


/**
 * A Metrics file generator that generates a metrics file in the submit directory
 *
 * The following metrics are logged in the metrics file
 *
 * <pre>
 * </pre>
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class Metrics  {


    /**
     * The suffix to use while constructing the name of the metrics file
     */
    public static final String METRICS_FILE_SUFFIX = ".metrics";


    /**
     * The default URL for the metrics server to use
     */
    public static final String METRICS_SERVER_DEFAULT_URL = "http://metrics.pegasus.isi.edu/metrics";

    /**
     * The name of the environment variable that sets whether to collect metrics or not
     */
    public static final String COLLECT_METRICS_ENV_VARIABLE = "PEGASUS_METRICS";
    
    /**
     * The name of the environment variable that overrides the default server url
     */
    public static final String METRICS_SERVER_URL_ENV_VARIABLE = "PEGASUS_METRICS_SERVER";
    
    
    /**
     * boolean indicating whether to log metrics or not
     */
    private boolean mSendMetricsToServer;

    /**
     * The url to which to log to.
     */
    private String mMetricsServerURL;

    /**
     * The logger object
     */
    private  LogManager mLogger;


    public Metrics(){
        mSendMetricsToServer = true;
        mMetricsServerURL    = METRICS_SERVER_DEFAULT_URL;
    }

    /**
     * Initializes the object
     *
     * @param bag   bag of pegasus objects
     */
    public void initialize( PegasusBag bag ){
        String value = System.getenv( COLLECT_METRICS_ENV_VARIABLE );
        mSendMetricsToServer =  Boolean.parse(  value, true );

        value = System.getenv( METRICS_SERVER_URL_ENV_VARIABLE );
        if( value != null ){
            mMetricsServerURL = value;
        }

        //intialize the logger defensively
        if( bag != null ){
            mLogger = bag.getLogger();
        }
        if( mLogger == null ){
            mLogger = LogManagerFactory.loadSingletonInstance();
        }
    }

    /**
     * Logs the metrics to the metrics server and to the submit directory
     *
     * @param metrics
     *
     * @throws IOException
     */
    public void logMetrics( PlannerMetrics metrics ) throws IOException{
        //lets write out to the local file
        this.writeOutMetricsFile( metrics );

        if( this.mSendMetricsToServer ){
            sendMetricsToServer( metrics , mMetricsServerURL );
        }
    }


    
    /**
     * Writes out the workflow metrics file in the submit directory
     *
     * @param metrics  the metrics to be written out.
     *
     * @return the path to metrics file in the submit directory
     *
     * @throws IOException in case of error while writing out file.
     */
    private File writeOutMetricsFile( PlannerMetrics metrics ) throws IOException{
        
        if( metrics == null ){
            throw new IOException( "NULL Metrics passed" );
        }
        
        //create a writer to the braindump.txt in the directory.
        File f =  metrics.getMetricsFileLocationInSubmitDirectory();

        if( f == null ){
            throw new IOException( "The metrics file location is not yet initialized" );
        }

        PrintWriter writer =
                  new PrintWriter(new BufferedWriter(new FileWriter(f)));
        
 
        writer.println( metrics.toPrettyJson() );
        writer.write(  "\n" );

        writer.close();
                
        return f;
    }

    /**
     * Sends the planner metrics to the metrics server
     *
     * @param metrics   the metrics to log
     * @param url       the url to send the metrics to
     */
    private void sendMetricsToServer(PlannerMetrics metrics, String url ) throws IOException{
        mLogger.log( "Planner Metrics will be sent to " + url,
                     LogManager.DEBUG_MESSAGE_LEVEL );

        URL u = new URL( url );
        HttpURLConnection connection = (HttpURLConnection) u.openConnection();
        connection.setDoOutput( true );
        //connection.setDoInput( true );
        connection.setRequestMethod( "POST" );

        connection.setRequestProperty( "Content-Type", "application/json");

        try{
            OutputStreamWriter out = new OutputStreamWriter(
                                         connection.getOutputStream());

            try{
                //String payload = URLEncoder.encode( metrics.toJson(), "UTF-8") ;
                String payload = metrics.toJson();
                out.write(  payload );
            }
            finally{
                out.close();
            }

            int responseCode = connection.getResponseCode();

            if( responseCode == 202 ){
                mLogger.log( "Metrics succesfully sent to the server", LogManager.DEBUG_MESSAGE_LEVEL );
            }
            else{
                mLogger.log( "Unable to send metrics to the server " + responseCode + " " + connection.getResponseMessage(),
                             LogManager.DEBUG_MESSAGE_LEVEL );
            }

            BufferedReader in = new BufferedReader(
                                    new InputStreamReader(
                                    connection.getInputStream()));
            String result;
            while (( result = in.readLine()) != null) {
                System.out.println( result );
            }
        }
        finally{
            connection.disconnect();
        }
    }

    /**
     * Writes out the planner metrics to the global log.
     *
     * @param pm  the metrics to be written out.
     *
     * @return boolean
     */
    /*
    protected boolean writeOutMetrics( PlannerMetrics pm  ){
        boolean result = false;
        System.out.print( pm.toPrettyJson() );
        if ( mProps.writeOutMetrics() ) {
            File log = new File( mProps.getMetricsLogFile() );


            //do a sanity check on the directory
            try{
                sanityCheck( log.getParentFile() );
                //open the log file in append mode
                FileOutputStream fos = new FileOutputStream( log ,true );

                //get an exclusive lock
                FileLock fl = fos.getChannel().lock();
                try{
                    mLogger.log( "Logging Planner Metrics to " + log,
                                 LogManager.DEBUG_MESSAGE_LEVEL );
                    //write out to the planner metrics to fos
                    fos.write( pm.toPrettyJson().getBytes() );
                }
                finally{
                    fl.release();
                    fos.close();
                }

            }
            catch( IOException ioe ){
                mLogger.log( "Unable to write out planner metrics ", ioe,
                             LogManager.DEBUG_MESSAGE_LEVEL );
                return false;
            }

            result = true;
        }
        return result;
    }
    */
   
    
    
}
