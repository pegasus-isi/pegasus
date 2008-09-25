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


package edu.isi.pegasus.planner.client;


import org.griphyn.cPlanner.toolkit.Executable;

import edu.isi.pegasus.common.logging.LogManager;

import edu.isi.pegasus.common.logging.LogManagerFactory;
import edu.isi.pegasus.common.logging.LoggingKeys;
import org.griphyn.cPlanner.visualize.Callback;
import org.griphyn.cPlanner.visualize.KickstartParser;

import org.griphyn.cPlanner.provenance.NetloggerCallback;

import org.griphyn.common.util.Version;
import org.griphyn.common.util.FactoryException;

import org.griphyn.vdl.toolkit.FriendlyNudge;

import gnu.getopt.Getopt;
import gnu.getopt.LongOpt;

import java.io.IOException;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.griphyn.common.util.VDSProperties;

/**
 * This parses the kickstart records and logs via Log4j the kickstart record
 * in the Netlogger Format.
 *
 *
 * @author Karan Vahi
 *
 * @version $Revision$
 */

public class NetloggerExitcode extends Executable{

  

   
    /**
     * The logging level to be used.
     */
    private int mLoggingLevel;

    /**
     * The kickstart file being parsed.
     */
    private String mFilename;
    
    /**
     * The id of the job.
     */
    private String mJobID;
    
    /**
     * The workflow id.
     */
    private String mWorkflowID;

    /**
     * Default constructor.
     */
    public NetloggerExitcode(){
        super();
        mLogMsg = new String();
        mVersion = Version.instance().toString();
        mLoggingLevel = 0;
    }

    /**
     * The main program.
     *
     *
     * @param args the main arguments passed to the plotter.
     */
    public static void main(String[] args) {

        NetloggerExitcode me = new NetloggerExitcode();
        int result = 0;
        double starttime = new Date().getTime();
        double execTime  = -1;

        try{
            result = me.executeCommand( args );
        }
        catch ( FactoryException fe){
            me.log( fe.convertException() , LogManager.FATAL_MESSAGE_LEVEL);
            result = 2;
        }
        catch ( Exception e ) {
            //unaccounted for exceptions
            me.log(e.getMessage(),
                         LogManager.FATAL_MESSAGE_LEVEL );
            e.printStackTrace();
            result = 7;
        } finally {
            double endtime = new Date().getTime();
            execTime = (endtime - starttime)/1000;
        }

        // warn about non zero exit code
        if ( result != 0 ) {
            me.log("Non-zero exit-code " + result,
                         LogManager.WARNING_MESSAGE_LEVEL );
        }
        else{
            //log the time taken to execute
            me.log("Time taken to execute is " + execTime + " seconds",
                         LogManager.INFO_MESSAGE_LEVEL);
        }
        
        me.log( "Exiting with exitcode " + result, LogManager.DEBUG_MESSAGE_LEVEL );
        me.mLogger.logEventCompletion();
        System.exit(result);
    }

    /**
     * Sets up the logging options for this class. Looking at the properties
     * file, sets up the appropriate writers for output and stderr.
     */
    protected void setupLogging(){
        //setup the logger for the default streams.
        mLogger = LogManagerFactory.loadSingletonInstance( mProps );
        mLogger.logEventStart( "netlogger-exitcode", "postscript", "netlogger-exitcode-" + mVersion );

    }


    /**
     * Executes the command on the basis of the options specified.
     *
     * @param args the command line options.
     * 
     * @return  the exitcode to exit with
     */
    public int executeCommand(String[] args) {
        int result = 0;
        parseCommandLineArguments(args);

        //set logging level only if explicitly set by user
        if( mLoggingLevel > 0 ) { mLogger.setLevel( mLoggingLevel ); }


        //do sanity check on input directory
        if( mFilename == null ){
            throw new RuntimeException(
                "You need to specify the file containing kickstart records");
        }

        KickstartParser su = new KickstartParser();

        Callback c = new NetloggerCallback();

        c.initialize( null, true );
        su.setCallback( c );

        try{
           Map eventIDMap = new HashMap();
           eventIDMap.put( LoggingKeys.DAG_ID, mWorkflowID );
           eventIDMap.put( LoggingKeys.JOB_ID , mJobID );
           mLogger.logEventStart( LoggingKeys.EVENT_WORKFLOW_JOB_STATUS, eventIDMap ); 
           log( "Parsing file " + mFilename , LogManager.DEBUG_MESSAGE_LEVEL );
           su.parseKickstartFile( mFilename );
           
           //grab the list of map objects
           List<Map<String,String>> records = (List<Map<String, String>>) c.getConstructedObject();
           //iterate through all the records and log them.
           for( Map<String,String> m : records ){
               for( String key : m.keySet() ){
                   mLogger.add( key, m.get(key) );
               }
               int exitcode = Integer.parseInt( m.get( "job.exitcode" ) );
               if( exitcode != 0 ){
                   result = 8;
               }
               mLogger.logAndReset( LogManager.INFO_MESSAGE_LEVEL );
           }
                   
        }
        catch (IOException ioe) {
            log( "Unable to parse kickstart file " + mFilename + convertException( ioe ),
            LogManager.DEBUG_MESSAGE_LEVEL);
            result = 5;
        }
        catch( FriendlyNudge fn ){
            log( "Problem parsing file " + mFilename + convertException( fn ),
                    LogManager.WARNING_MESSAGE_LEVEL );
        }
        finally{
            //we are done with parsing
            c.done();
            mLogger.logEventCompletion();
        }
        return result;
    }


    /**
     * Parses the command line arguments using GetOpt and returns a
     * <code>PlannerOptions</code> contains all the options passed by the
     * user at the command line.
     *
     * @param args  the arguments passed by the user at command line.
     */
    public void parseCommandLineArguments(String[] args){
        LongOpt[] longOptions = generateValidOptions();

        Getopt g = new Getopt( "plot-node-usage", args,
                              "f:w:j:hvV",
                              longOptions, false);
        g.setOpterr(false);

        int option = 0;

        while( (option = g.getopt()) != -1){
            //System.out.println("Option tag " + (char)option);
            switch (option) {

                case 'f':
                    mFilename   = g.getOptarg();
                    break;
                
                case 'j':
                    mJobID      = g.getOptarg();
                    break;
                    
                case 'w':
                    mWorkflowID = g.getOptarg();
                    break;
                    
                case 'h'://help
                    printLongVersion();
                    System.exit( 0 );
                    return;

                
                case 'v'://verbose
                    mLoggingLevel++;
                    break;

                case 'V'://version
                    mLogger.log(getGVDSVersion(),LogManager.INFO_MESSAGE_LEVEL);
                    System.exit(0);


                default: //same as help
                    printShortVersion();
                    throw new RuntimeException("Incorrect option or option usage " +
                                               (char)option);

            }
        }

    }

    /**
     * Logs messages to the  logger. Adds the workflow id.
     *
     * @param msg is the message itself.
     * @param level is the level to generate the log message for.
     */
    public void log( String msg, int level ){
        mLogger.add( msg );
        mLogger.logAndReset(level);
    }

    /**
     * Tt generates the LongOpt which contain the valid options that the command
     * will accept.
     *
     * @return array of <code>LongOpt</code> objects , corresponding to the valid
     * options
     */
    public LongOpt[] generateValidOptions(){
        LongOpt[] longopts = new LongOpt[6];

        longopts[0]   = new LongOpt( "file", LongOpt.REQUIRED_ARGUMENT, null, 'f' );
        longopts[1]   = new LongOpt( "wf-id", LongOpt.REQUIRED_ARGUMENT, null, 'w' );
        longopts[2]   = new LongOpt( "verbose", LongOpt.NO_ARGUMENT, null, 'v' );
        longopts[3]   = new LongOpt( "help", LongOpt.NO_ARGUMENT, null, 'h' );
        longopts[4]   = new LongOpt( "version", LongOpt.NO_ARGUMENT, null, 'V' );
        longopts[5]   = new LongOpt( "job-id", LongOpt.REQUIRED_ARGUMENT, null, 'j' );
        return longopts;
    }


    /**
     * Prints out a short description of what the command does.
     */
    public void printShortVersion(){
        String text =
          "\n $Id$ " +
          "\n " + getGVDSVersion() +
          "\n Usage : netlogger-exitcode [-Dprop  [..]] -f <kickstart output file>  " +
          " -w <workflow-id>  -j <job id>  [-v] [-V] [-h]";

        System.out.println(text);
    }

    /**
     * Prints the long description, displaying in detail what the various options
     * to the command stand for.
     */
    public void printLongVersion(){

        String text =
           "\n $Id$ " +
           "\n " + getGVDSVersion() +
           "\n netlogger-exitcode - Parses the kickstart output and logs relevant information using pegasus logger."  +
           "\n The Pegasus Logger can be configured by specifying the following properties " + 
           "\n                          pegasus.log.manager " + 
           "\n                          pegasus.log.formatter ." +
           "\n Usage: netlogger-exitcode [-Dprop  [..]] --file  -f <kickstart output file>  " +
           "\n        --wf-id <workflow-id>  --job-id <job id>  [--version] [--verbose] [--help]" +
           "\n" +
           "\n Mandatory Options " +
           "\n --file              the kickstart output file to be parsed. May contain multiple invocation records." +
           "\n Other Options  " +
           "\n -w |--wf-id         the workflow id to use while logging." +
           "\n -j |--job-id        the job id to use while logging." +
           "\n -v |--verbose       increases the verbosity of messages about what is going on" +
           "\n -V |--version       displays the version of the Pegasus Workflow Planner" +
           "\n -h |--help          generates this help." +
           "\n " +
           "\n   0  remote application ran to conclusion with exit code zero." + 
           "\n   2  an error occured while loading a specific module implementation at runtime" +
           "\n   1  remote application concluded with a non-zero exit code." +
           "\n   5  invocation record has an invalid state, unable to parse." + 
           "\n   7  illegal state, stumbled over an exception, try --verbose for details. "  +
           "\n   8  multiple 0..5 failures during parsing of multiple records" +
           "\n";

        System.out.println(text);
        //mLogger.log(text,LogManager.INFO_MESSAGE_LEVEL);
    }

    /**
     * Loads all the properties that would be needed by the Toolkit classes.
     */
    public void loadProperties(){
        //empty for time being
    }

}




