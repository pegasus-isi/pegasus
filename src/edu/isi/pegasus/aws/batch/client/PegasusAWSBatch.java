/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
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
import static java.util.Arrays.asList;
import java.util.Date;
import java.util.EnumMap;
import java.util.Properties;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.ValueConverter;
import org.apache.log4j.ConsoleAppender;
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
        mOptionParser.acceptsAll(asList( "co", "compute-environment"), "the json file containing compute environment description to create ").
                withRequiredArg().ofType( String.class );
        mOptionParser.acceptsAll(asList( "j", "job-definition"), "the json file containing job definition to register for executing jobs ").
                withRequiredArg().ofType( String.class );
        mOptionParser.acceptsAll(asList( "p", "prefix"), "prefix to use for creating compute environment, job definition, job queue").
               withRequiredArg().ofType( String.class ).required();
        mOptionParser.acceptsAll(asList( "q", "job-queue"), "the json file containing the job queue description to create ").
                withRequiredArg().ofType( String.class );
        mOptionParser.acceptsAll(asList( "r", "region"), "the AWS region to run the jobs in ").
                withRequiredArg().ofType( String.class );
        mOptionParser.acceptsAll(asList( "l", "log-level"), "sets the logging level").
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
            me.executeCommand( options );
        }
        catch ( Exception e){
            result = 1;
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
     */
    protected void executeCommand( OptionSet options ) {
        
        if( options.has( "log-level")){
            mLogger.setLevel((Level) options.valueOf( "log-level"));
        }
        mLogger.debug( "Executing command");
        
        if( options.has( "help") ){
            try {
                mOptionParser.printHelpOn( System.out );
                System.exit(0);
            } catch (IOException ex) {
                mLogger.error( ex);
            }
        }
        
        
        Properties props = new Properties();
        if( options.has( "conf") ){
            String confFile = (String) options.valueOf( "conf" );
            PegasusProperties p = PegasusProperties.getInstance(confFile);
            //strip out pegasus prefix in parsed properties file
            props = p.matchingSubset( "pegasus", false);
            System.out.println( "Properties with pegasus prefix remove " + props );
        }
        
        String key = Synch.AWS_BATCH_PROPERTY_PREFIX + ".prefix";
        String awsBatchPrefix = getAWSOptionValue(options, "prefix", props, key );
        props.setProperty( key , awsBatchPrefix);
        
        key = Synch.AWS_PROPERTY_PREFIX + ".region";
        String awsRegion      = getAWSOptionValue(options, "region", props, key );
        props.setProperty( key , awsRegion);
        
        key = Synch.AWS_PROPERTY_PREFIX + ".account";
        String awsAccount      = getAWSOptionValue(options, "account", props, key );
        props.setProperty( key , awsAccount);   
        
        EnumMap<Synch.JSON_FILE_TYPE,File> jsonMap = new EnumMap<>( Synch.JSON_FILE_TYPE.class);
        
        key = Synch.AWS_BATCH_PROPERTY_PREFIX + ".job_definition";
        String jobDefinition = getAWSOptionValue(options, "job-definition", props, key );
        jsonMap.put(Synch.JSON_FILE_TYPE.job_defintion, new File(jobDefinition) );
        
        key = Synch.AWS_BATCH_PROPERTY_PREFIX + ".compute_environment";
        String computeEnvironment = getAWSOptionValue(options, "compute-environment", props, key );
        jsonMap.put(Synch.JSON_FILE_TYPE.compute_environment, new File(computeEnvironment) );
        
        key = Synch.AWS_BATCH_PROPERTY_PREFIX + ".job_queue";
        try{
            String jobQueue = getAWSOptionValue(options, "job-queue", props, key );
            jsonMap.put(Synch.JSON_FILE_TYPE.job_queue, new File(jobQueue) );
        }
        catch( Exception e ){
            mLogger.debug( "Ignoring e as job queue can be created based on compute environemnt ", e);
        }
        
        mLogger.info( "Going to connect with properties " + props + " and json map " + jsonMap );
        
        Synch sc = new Synch();
        try {
            sc.initialze( props, jsonMap );
        } catch (IOException ex) {
            mLogger.error(ex, ex);
        }
        sc.monitor();
        Job jobBuilder = new Job();
        for( AWSJob j : jobBuilder.createJob( new File("sample-job-submit.json") )){
            sc.submit(j);
        }
        sc.signalToExitAfterJobsComplete();
        sc.awaitTermination();

    }
    
    private String getAWSOptionValue( OptionSet options, String option, Properties props, String key ){
        String value = (String) options.valueOf( option );
        
        value =  ( value == null ) ? props.getProperty( key ) : value;
        if( value == null ){
            throw new RuntimeException( "Unable to determine value of pegasus." + key + 
                                        " Either specify in properties or set command line option " + option );
        }
        return value;
    }
    
}