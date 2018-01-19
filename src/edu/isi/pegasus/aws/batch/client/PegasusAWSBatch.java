/**
 *  Copyright 2007-2017 University Of Southern California
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

package edu.isi.pegasus.aws.batch.client;

/**
 *
 * @author Karan Vahi
 */


import edu.isi.pegasus.aws.batch.builder.Job;
import edu.isi.pegasus.aws.batch.classes.AWSJob;
import edu.isi.pegasus.aws.batch.impl.Synch;
import edu.isi.pegasus.planner.common.PegasusProperties;

import java.io.File;
import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Arrays;

import static java.util.Arrays.asList;
import java.util.Date;
import java.util.EnumMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.ValueConverter;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

/**
 *
 * @author Karan Vahi
 */
public class PegasusAWSBatch {

    private static Logger mLogger;
    
    private final OptionParser mOptionParser;
    
    /**
     * Initializes the root logger when this class is loaded.
     */
    static {
	if ((mLogger = Logger.getRootLogger()) != null) {
	    mLogger.removeAllAppenders(); // clean house only once
	    mLogger.addAppender(new ConsoleAppender(new PatternLayout(
		    "%d{yyyy-MM-dd HH:mm:ss.SSS} %-5p [%c{1}] %m%n")));
	    mLogger.setLevel(Level.INFO);
            //reset logger to class specific
            mLogger = Logger.getLogger( PegasusAWSBatch.class.getName() );
	}
    }
    
    public PegasusAWSBatch(){
        //mLogger = org.apache.log4j.Logger.getLogger( PegasusAWSBatch.class.getName() );
        mOptionParser = new OptionParser();
    }
    
    public OptionSet parseCommandLineOptions( String[] args ) throws IOException{
        
        mOptionParser.acceptsAll(asList( "a", "account"), "the AWS account to use for running jobs ").
                withRequiredArg().ofType( String.class );
        mOptionParser.acceptsAll(asList( "C", "conf"), "the properties file containing to use ").
                withRequiredArg().ofType( String.class );
        mOptionParser.acceptsAll(asList( "ce", "compute-environment"), "the json file containing compute environment description to create or the ARN of existing compute environment or basename of an existing compute environment").
                withRequiredArg().ofType( String.class );
        mOptionParser.acceptsAll(asList( "c", "create"), "does not run any jobs. Only creates the job definition, compute environment and the job queue");
        mOptionParser.acceptsAll(asList( "d", "delete"), "does not run any jobs. Only deletes the job definition, compute environment and the job queue");
        mOptionParser.acceptsAll(asList( "f", "files"), "comma separated list of files that need to be copied to the associated s3 bucket before any task starts").
                withRequiredArg().ofType( String.class );
        mOptionParser.acceptsAll(asList( "j", "job-definition"), "the json file containing job definition to register for executing jobs or the ARN of existing job definition or basename of an existing job definition").
                withRequiredArg().ofType( String.class );
         mOptionParser.acceptsAll(asList( "m", "merge-logs"), "prefix to use for merging all the tasks' stdout to a single file.").
                withRequiredArg().ofType( String.class );
        mOptionParser.acceptsAll(asList( "p", "prefix"), "prefix to use for creating compute environment, job definition, job queue and s3 bucket").
               withRequiredArg().ofType( String.class ).required();
        mOptionParser.acceptsAll(asList( "q", "job-queue"), "the json file containing the job queue description to create or the ARN of existing job queue or basename of an existing job queue").
                withRequiredArg().ofType( String.class );
        mOptionParser.acceptsAll(asList( "r", "region"), "the AWS region to run the jobs in ").
                withRequiredArg().ofType( String.class );
        mOptionParser.acceptsAll(asList( "s", "s3"), "the S3 bucket to use for lifecycle of the client. If not specifed then a bucket is created based on the prefix passed").
                withRequiredArg().ofType( String.class );
        mOptionParser.acceptsAll(asList( "l", "log-file"), "log to a file identified by the argument value").
                withRequiredArg().ofType( String.class );
        mOptionParser.acceptsAll(asList( "L", "log-level"), "sets the logging level").
                withRequiredArg().withValuesConvertedBy( new ValueConverter(){
            @Override
            public Object convert(String string) {
                return Level.toLevel(string);
            }

            @Override
            public Class valueType() {
                return Level.class;
                
            }

            @Override
            public String valuePattern() {
                return "error, warn, info, debug, trace";
            }
 } );
        mOptionParser.acceptsAll(asList( "h", "help"),   "generates help for the tool").forHelp();
        OptionSet options = null;
        try{
            options = mOptionParser.parse(args); 
        }
        catch( OptionException e){
            mLogger.error( e );
            mLogger.info( "Provide valid options");
            mOptionParser.printHelpOn( System.err );
            System.exit( -1 );
        }
        return options;
    }
    
    
    
     /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        
        PegasusAWSBatch me = new PegasusAWSBatch();
        int result = 0;
        double starttime = new Date().getTime();
        double execTime  = -1;
        
        try{	
            OptionSet options = me.parseCommandLineOptions(args);
            result = me.executeCommand( options );
        }
        catch ( Exception e){
            result = 3;
            me.mLogger.error(e);
        }
        finally {
            double endtime = new Date().getTime();
            execTime = (endtime - starttime)/1000;
        }

        // warn about non zero exit code
        if ( result != 0 ) {
            me.mLogger.warn( "Non-zero exit-code " + result);
        }
        else{
            //log the time taken to execute
            me.mLogger.info("Time taken to execute is " + execTime + " seconds");
        }
        
        me.mLogger.debug( "Exiting with exitcode " + result  );
        System.exit(result);
      
               
    }

    /**
     * Executes the client with the options passed on command line
     * @param options
     * 
     * @return exit code with which to exit
     */
    protected int executeCommand( OptionSet options ) {
        Level logLevel = Level.INFO;
        int exitcode = 0;
        if( options.has( "log-file") ){
            File f = new File( (String)options.valueOf( "log-file") );
            setupFileLogging( f, true );
        }
        
        if( options.has( "log-level")){
            logLevel = (Level) options.valueOf( "log-level");
        }
        mLogger.setLevel(logLevel);
        mLogger.debug( "Executing command");
        
        if( options.has( "help") ){
            try {
                mOptionParser.printHelpOn( System.out );
                System.exit(0);
            } catch (IOException ex) {
                mLogger.error( ex);
            }
        }
        
        //check for jobs to submit option
        List<String> submitJobFiles = (List<String>) options.nonOptionArguments();
        mLogger.info( "Job submit files are " + submitJobFiles );
        
        Properties props = new Properties();
        if( options.has( "conf") ){
            String confFile = (String) options.valueOf( "conf" );
            PegasusProperties p = PegasusProperties.getInstance(confFile);
            //strip out pegasus prefix in parsed properties file
            props = p.matchingSubset( "pegasus", false);
        }
        
        //sanity checks
        boolean allEntitiesRequired = true;
        if( options.has( "create" ) || options.has( "delete") ){
            allEntitiesRequired = false;
        }
        
        if( submitJobFiles.isEmpty() ){
            if( !(options.has( "create" ) || options.has( "delete"))  ){
                throw new RuntimeException( "specify the job submit file");
            }
        }
        else if( options.has( "create" )  ){
            throw new RuntimeException( "-s|--setup option cannot be specified along with jobs to run");
        }
        
        List<String> files = options.has( "files" ) ?
                Arrays.asList(  ((String) options.valueOf( "files" )).split(",") ):
                new LinkedList();
        
        String key = Synch.AWS_BATCH_PROPERTY_PREFIX + ".prefix";
        String awsBatchPrefix = getAWSOptionValue(options, "prefix", props, key );
        props.setProperty( key , awsBatchPrefix);
        
        key = Synch.AWS_PROPERTY_PREFIX + ".region";
        String awsRegion      = getAWSOptionValue(options, "region", props, key );
        props.setProperty( key , awsRegion);
        
        key = Synch.AWS_PROPERTY_PREFIX + ".account";
        String awsAccount      = getAWSOptionValue(options, "account", props, key );
        props.setProperty( key , awsAccount);   
        
        EnumMap<Synch.BATCH_ENTITY_TYPE,String> jsonMap = new EnumMap<>( Synch.BATCH_ENTITY_TYPE.class);
        
        key = Synch.AWS_BATCH_PROPERTY_PREFIX + ".job_definition";
        String jobDefinition = getAWSOptionValue(options, "job-definition", props, key, allEntitiesRequired );
        jsonMap.put(Synch.BATCH_ENTITY_TYPE.job_definition, jobDefinition );
        
        key = Synch.AWS_BATCH_PROPERTY_PREFIX + ".compute_environment";
        String computeEnvironment = getAWSOptionValue(options, "compute-environment", props, key , allEntitiesRequired);
        jsonMap.put(Synch.BATCH_ENTITY_TYPE.compute_environment, computeEnvironment );
        
        key = Synch.AWS_BATCH_PROPERTY_PREFIX + ".job_queue";
        try{
            String jobQueue = getAWSOptionValue(options, "job-queue", props, key , false);
            // when running jobs we need to create job queue even if they dont specify
            jobQueue = (jobQueue == null && allEntitiesRequired) ? Synch.NULL_VALUE : jobQueue;
            jsonMap.put(Synch.BATCH_ENTITY_TYPE.job_queue, jobQueue );
        }
        catch( Exception e ){
            mLogger.debug( "Ignoring e as job queue can be created based on compute environemnt ", e);
        }
        
        key = Synch.AWS_BATCH_PROPERTY_PREFIX + ".s3_bucket";
        String s3Bucket = getAWSOptionValue(options, "s3", props, key , false);
        // when running jobs we need to create S3 bucketeven if they dont specify
        s3Bucket = (s3Bucket == null && allEntitiesRequired) ? Synch.NULL_VALUE : s3Bucket;
        jsonMap.put(Synch.BATCH_ENTITY_TYPE.s3_bucket, s3Bucket );
        
        mLogger.info( "Going to connect with properties " + props + " and json map " + jsonMap );
        
        Synch sc = new Synch();
        try {
            sc.initialze( props, logLevel, jsonMap );
            
            if( options.has( "delete" ) ){
                sc.deleteSetup( jsonMap );
                return exitcode ;
            }
            else{
                //we do setup both in case of running jobs or just doing setup
                sc.setup(jsonMap, allEntitiesRequired);
            }
            
            if( options.has( "create" ) ) {
                return exitcode;
            }
        } catch (IOException ex) {
            mLogger.error(ex, ex);
        }
        
        //transfer any common files required for all tasks
        sc.transferCommonInputFiles( files );
        
        sc.monitor();
        Job jobBuilder = new Job();
        long sequenceID = 1;
        for( String f : submitJobFiles ){
            mLogger.info( "Submitting jobs from file " + f );
            for( AWSJob j : jobBuilder.createJob( new File( f ) )){
                j.setSequenceID(sequenceID++);
                sc.submit(j);
            }
        }
        sc.signalToExitAfterJobsComplete();
        exitcode = sc.awaitTermination();
        
        //merge logs if required
        if( options.has( "merge-logs") ){
            String prefix = (String) options.valueOf( "merge-logs" );
            File stdout =  new File( prefix + ".out") ;
            File stderr =  new File( prefix + ".err" );
            mLogger.info( "Merging Tasks stdout to  " + stdout  + " and stderr to " + stderr );
            sc.mergeLogs( stdout, stderr );
        }
        
        return exitcode;
    }
    
    /**
     * Returns the value of a particular option
     * 
     * @param options
     * @param option
     * @param props
     * @param key
     * @param required
     * @return 
     */
    private String getAWSOptionValue( OptionSet options, String option, Properties props, String key ){
        return this.getAWSOptionValue(options, option, props, key, true);
    }
    
    /**
     * Returns the value of a particular option
     * 
     * @param options
     * @param option
     * @param props
     * @param key
     * @param required
     * @return 
     */
    private String getAWSOptionValue( OptionSet options, String option, Properties props, String key, boolean required ){
        String value = (String) options.valueOf( option );
        
        value =  ( value == null ) ? props.getProperty( key ) : value;
        if( value == null && required ){
            throw new RuntimeException( "Unable to determine value of pegasus." + key + 
                                        " Either specify in properties or set command line option " + option );
        }
        return value;
    }

    /**
     * Sets up logging to a file instead of stdout and stderr
     * 
     * @param f 
     * @param rotateLogs whether to rotate logs following a numbering scheme .00X
     */
    private void setupFileLogging( File f , boolean rotateLogs ) {
        File log = f;
        if( rotateLogs ){
            //setup logging to 00X files
            NumberFormat nf = new DecimalFormat( "000" );
           
            for( int i = 0; i < 1000; i++ ){
                String name = f.getAbsolutePath() + "." +  nf.format( i );
                log  = new File( name );
                if( !log.exists() ){
                    break;
                }
            }

        }
        
        if ((mLogger = Logger.getRootLogger()) != null) {
            mLogger.removeAllAppenders(); // clean house only once
            try {
                mLogger.addAppender(new FileAppender(new PatternLayout(
                        "%d{yyyy-MM-dd HH:mm:ss.SSS} %-5p [%c{1}] %m%n"), log.getAbsolutePath() ));
            } catch (IOException ex) {
                System.err.println( "Unable to set logging to file " + log );
            }
            mLogger.setLevel(Level.INFO);
            //reset logger to class specific
            mLogger = Logger.getLogger( PegasusAWSBatch.class.getName() );
        }
    }
    
}