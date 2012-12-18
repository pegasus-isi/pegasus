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

import edu.isi.pegasus.planner.code.CodeGeneratorException;


import edu.isi.pegasus.planner.classes.PlannerMetrics;




import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;


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
     * Logs the metrics to the metrics server and to the submit directory
     *
     * @param metrics
     *
     * @throws IOException
     */
    public void logMetrics( PlannerMetrics metrics ) throws IOException{
        //lets write out to the local file
        this.writeOutMetricsFile( metrics );
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
   
    /**
     * Resets the Code Generator implementation.
     *
     * @throws CodeGeneratorException in case of any error occuring code generation.
     */
    public void reset( )throws CodeGeneratorException{
        
        
    }

    public boolean startMonitoring() {
        throw new UnsupportedOperationException("Not supported yet.");
    }
    
}
