/**
 *  Copyright 2007-2015 University Of Southern California
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
package edu.isi.pegasus.planner.common;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.common.logging.LogManagerFactory;
import edu.isi.pegasus.common.util.DefaultStreamGobblerCallback;
import edu.isi.pegasus.common.util.FindExecutable;
import edu.isi.pegasus.common.util.StreamGobbler;
import edu.isi.pegasus.common.util.StreamGobblerCallback;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.PlannerOptions;
import java.io.File;
import java.io.IOException;

/**
 * Helper class to call out to pegasus-worker-create to create a pegasus 
 * worker package on the submit host
 * 
 * @author Karan Vahi
 */
public class CreateWorkerPackage {
    private LogManager mLogger;

    private PegasusBag mBag;
    
    public CreateWorkerPackage(PegasusBag bag ){
        mBag    = bag;
        mLogger = bag.getLogger();
    }
   
    
    /**
     * Creates the pegasus worker package and returns a File object pointing
     * to the worker package created
     * 
     * @param directory
     * 
     * @return file object to created worker package
     * @throws RuntimeException in case of errors
     */
    public File create(  ){
        PlannerOptions options = mBag.getPlannerOptions();
        if( options == null ){
            throw new RuntimeException( "No planner options specified " + options );
        }
        return this.create( new File( options.getSubmitDirectory()) );
    }
   
    /**
     * Creates the pegasus worker package and returns a File object pointing
     * to the worker package created
     * 
     * @param directory
     * 
     * @return file object to created worker package
     * @throws RuntimeException in case of errors
     */
    public File create( File directory ){
        String basename = "pegasus-worker-create";
        File pegasusWorkerCreate = FindExecutable.findExec( basename );
        //pegasusWorkerCreate = new File( "/lfs1/software/install/pegasus/pegasus-4.6.0dev/bin/pegasus-worker-create");
        if( pegasusWorkerCreate == null ){
            throw new RuntimeException( "Unable to find path to " + basename );
        }
        
        //construct arguments for pegasus-db-admin
        StringBuffer args = new StringBuffer();
        args.append( directory.getAbsolutePath() );
        String command = pegasusWorkerCreate.getAbsolutePath() + " " + args;
        mLogger.log("Executing  " + command,
                         LogManager.DEBUG_MESSAGE_LEVEL );
            
        File result = null;
        try{
            //set the callback and run the pegasus-run command
            Runtime r = Runtime.getRuntime();
            Process p = r.exec(command );
            WorkerPackageCallback c = new WorkerPackageCallback( mLogger );

            //spawn off the gobblers with the already initialized default callback
            StreamGobbler ips =
                new StreamGobbler( p.getInputStream(), c );
            StreamGobbler eps =
                new StreamGobbler( p.getErrorStream(), new DefaultStreamGobblerCallback(
                                                             LogManager.ERROR_MESSAGE_LEVEL));

            ips.start();
            eps.start();

            //wait for the threads to finish off
            ips.join();
            eps.join();

            //get the status
            int status = p.waitFor();

            mLogger.log( basename + " exited with status " + status,
                         LogManager.DEBUG_MESSAGE_LEVEL );

            if( status != 0 ){
                throw new RuntimeException( basename + " failed with non zero exit status " + command );
            }
            String workerPackage = c.getWorkerPackage();
            result = new File( directory, workerPackage );
            if( !result.exists() ){
                throw new RuntimeException( "Worker package created does not exist " + result.getAbsolutePath() );
            }
        }
        catch(IOException ioe){
            mLogger.log("IOException while executing " + basename, ioe,
                        LogManager.ERROR_MESSAGE_LEVEL);
            throw new RuntimeException( "IOException while executing " + command , ioe );
        }
        catch( InterruptedException ie){
            //ignore
        }
        return result;
    } 
    
     
}

/**
 * An inner class, that implements the StreamGobblerCallback to go through the
 * output of pegasus-worker-create
 *
 */
class WorkerPackageCallback implements StreamGobblerCallback {

    /**
     * The version detected.
     */
    private String mWorkerPackage;

    /**
     * echo "# PEGASUS_WORKER_PACKAGE=${WORKER_PACKAGE_NAME}"
     */
    public static final String VARIABLE_NAME= "PEGASUS_WORKER_PACKAGE";
    private final LogManager mLogger;
    
    /**
     * The Default Constructor
     */
    public WorkerPackageCallback(LogManager logger ) {
        mLogger = logger;
        mWorkerPackage = null;
    }

    /**
     * Callback whenever a line is read from the stream by the StreamGobbler.
     * Counts the occurences of the word that are in the line, and increments to
     * the global counter.
     *
     * @param line the line that is read.
     */
    public void work(String line) {
               
        if( line == null ){
            return;
        }
        
        mLogger.log( line , LogManager.DEBUG_MESSAGE_LEVEL );
        if( line.startsWith( "# " ) ){
            line = line.substring( 2 );
            String[] arr = line.split( "=", 2 );
            String key = arr[0];
            if( key == null ){
                return;
            }
            
            if(  key.equals( VARIABLE_NAME ) ){
                if( arr.length != 2 ){
                    throw new RuntimeException( "Output of pegasus-woker-create malformed " + line );
                }
                mWorkerPackage=arr[1];
            }
        }
    }

    /**
     * Returns the worker package that was created
     *
     * @return the created worker package
     */
    public String getWorkerPackage() {
        return mWorkerPackage;
    }

    public static void main( String[] args ){
        LogManager logger = LogManagerFactory.loadSingletonInstance();
        logger.setLevel( LogManager.DEBUG_MESSAGE_LEVEL);
        PegasusBag bag = new PegasusBag();
        bag.add( PegasusBag.PEGASUS_LOGMANAGER, logger );
        logger.logEventStart( "Main function", "test", "l", LogManager.DEBUG_MESSAGE_LEVEL);
    
        CreateWorkerPackage cw = new CreateWorkerPackage( bag);
        
        File wp = cw.create( new File( "/tmp/") );
        System.out.println( "Created worker package " + wp );
    }
}


