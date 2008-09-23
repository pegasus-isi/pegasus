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


package org.griphyn.cPlanner.toolkit;


import edu.isi.pegasus.common.logging.LoggingKeys;
import edu.isi.pegasus.planner.catalog.SiteCatalog;

import edu.isi.pegasus.planner.catalog.site.SiteCatalogException;
import edu.isi.pegasus.planner.catalog.site.SiteFactory;

import edu.isi.pegasus.planner.catalog.site.classes.SiteStore;
import edu.isi.pegasus.planner.catalog.site.classes.SiteCatalogEntry;

import org.griphyn.cPlanner.code.CodeGenerator;
import org.griphyn.cPlanner.code.generator.CodeGeneratorFactory;

import org.griphyn.cPlanner.classes.ADag;
import org.griphyn.cPlanner.classes.DagInfo;
import org.griphyn.cPlanner.classes.NameValue;
import org.griphyn.cPlanner.classes.PlannerMetrics;
import org.griphyn.cPlanner.classes.PlannerOptions;
import org.griphyn.cPlanner.classes.PegasusBag;

import org.griphyn.cPlanner.common.PegasusProperties;
import edu.isi.pegasus.common.logging.LogManager;
import org.griphyn.cPlanner.common.StreamGobbler;
import org.griphyn.cPlanner.common.DefaultStreamGobblerCallback;
import org.griphyn.cPlanner.common.RunDirectoryFilenameFilter;

import org.griphyn.cPlanner.engine.MainEngine;

import org.griphyn.cPlanner.engine.createdir.WindwardImplementation;


import org.griphyn.cPlanner.parser.dax.Callback;
import org.griphyn.cPlanner.parser.dax.DAXCallbackFactory;

import org.griphyn.cPlanner.parser.pdax.PDAXCallbackFactory;

import org.griphyn.cPlanner.parser.DaxParser;
import org.griphyn.cPlanner.parser.PDAXParser;

import org.griphyn.common.catalog.work.WorkFactory;
import org.griphyn.common.catalog.WorkCatalog;

import org.griphyn.common.catalog.transformation.TransformationFactory;

import org.griphyn.common.util.Version;
import org.griphyn.common.util.Currently;
import org.griphyn.common.util.FactoryException;

import gnu.getopt.Getopt;
import gnu.getopt.LongOpt;

import java.io.File;
import java.io.IOException;
import java.io.FileOutputStream;

import java.nio.channels.FileLock;

import java.util.Collection;
import java.util.List;
import java.util.Date;
import java.util.Map;
import java.util.Iterator;


import java.text.NumberFormat;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.LinkedList;

import java.util.Properties;
import java.util.Set;
import org.griphyn.cPlanner.classes.SubInfo;


/**
 * This is the main program for the Pegasus. It parses the options specified
 * by the user and calls out to the appropriate components to parse the abstract
 * plan, concretize it and then write the submit files.
 *
 * @author Gaurang Mehta
 * @author Karan Vahi
 * @version $Revision$
 */

public class CPlanner extends Executable{

    /**
     * The default megadag mode that is used for generation of megadags in
     * deferred planning.
     */
    public static final String DEFAULT_MEGADAG_MODE = "dag";


    /**
     * The basename of the directory that contains the submit files for the
     * cleanup DAG that for the concrete dag generated for the workflow.
     */
    public static final String CLEANUP_DIR  = "cleanup";


    /**
     * The final successful message that is to be logged.
     */
    private static final String EMPTY_FINAL_WORKFLOW_MESSAGE =
        "\n\n\n" +
        "The executable workflow generated contains no nodes.\n" +
        "It seems that the output files are already at the output site. \n"+
        "To regenerate the output data from scratch specify --force option." +
        "\n\n\n";

   /**
    * The message to be logged in case of empty executable workflow.
    */
   private static final String SUCCESS_MESSAGE =
       "\n\n\n" +
       "I have concretized your abstract workflow. The workflow has been entered \n" +
       "into the workflow database with a state of \"planned\". The next step is \n" +
       "to start or execute your workflow. The invocation required is" +
       "\n\n\n";



    /**
     * The object containing all the options passed to the Concrete Planner.
     */
    private PlannerOptions mPOptions;

    /**
     * The object containing the bag of pegasus objects
     */
    private PegasusBag mBag;

    /**
     * The PlannerMetrics object storing the metrics about this planning instance.
     */
    private PlannerMetrics mPMetrics;

    /**
     * The number formatter to format the run submit dir entries.
     */
    private NumberFormat mNumFormatter;

    /**
     * The user name of the user running Pegasus.
     */
    private String mUser;

    /**
     * Default constructor.
     */
    public CPlanner(){
        super();
        mLogMsg = new String();
        mVersion = Version.instance().toString();
        mNumFormatter = new DecimalFormat( "0000" );

        this.mPOptions        = new PlannerOptions();
        mPOptions.setSubmitDirectory( ".", null );
        mPOptions.setExecutionSites(new java.util.HashSet());
        mPOptions.setOutputSite("");

        mUser = mProps.getProperty( "user.name" ) ;
        if ( mUser == null ){ mUser = "user"; }

        mPMetrics = new PlannerMetrics();
        mPMetrics.setUser( mUser );

        mBag = new PegasusBag();
    }

    /**
     * The main program for the CPlanner.
     *
     *
     * @param args the main arguments passed to the planner.
     */
    public static void main(String[] args) {

        CPlanner cPlanner = new CPlanner();
        int result = 0;
        double starttime = new Date().getTime();
        double execTime  = -1;

        try{
            cPlanner.executeCommand( args );
        }
        catch ( FactoryException fe){
            cPlanner.log( fe.convertException() , LogManager.FATAL_MESSAGE_LEVEL);
            result = 2;
        }
        catch ( RuntimeException rte ) {
            //catch all runtime exceptions including our own that
            //are thrown that may have chained causes
            cPlanner.log( convertException(rte),
                         LogManager.FATAL_MESSAGE_LEVEL );
            rte.printStackTrace();
            result = 1;
        }
        catch ( Exception e ) {
            //unaccounted for exceptions
            cPlanner.log(e.getMessage(),
                         LogManager.FATAL_MESSAGE_LEVEL );
            result = 3;
        } finally {
            double endtime = new Date().getTime();
            execTime = (endtime - starttime)/1000;
        }

        // warn about non zero exit code
        if ( result != 0 ) {
            cPlanner.log("Non-zero exit-code " + result,
                         LogManager.WARNING_MESSAGE_LEVEL );
        }
        else{
            //log the time taken to execute
            cPlanner.log("Time taken to execute is " + execTime + " seconds",
                         LogManager.INFO_MESSAGE_LEVEL);
        }

        System.exit(result);
    }


    /**
     * Loads all the properties that are needed by this class.
     */
    public void loadProperties(){


    }



    /**
     * Executes the command on the basis of the options specified.
     *
     * @param args the command line options.
     */
    public void executeCommand( String[] args ) {
        executeCommand( parseCommandLineArguments( args ) );
    }

    /**
     * Executes the command on the basis of the options specified.
     *
     * @param options the command line options.
     *
     *
     * @return the Collection of <code>File</code> objects for the files written
     *         out.
     */
    public Collection<File> executeCommand( PlannerOptions options ) {
        String message = new String();
        mPOptions = options;

        mBag.add( PegasusBag.PEGASUS_PROPERTIES, mProps );
        mBag.add( PegasusBag.PLANNER_OPTIONS, mPOptions );
        mBag.add( PegasusBag.PEGASUS_LOGMANAGER, mLogger );



        Collection result = null;

        //print help if asked for
        if( mPOptions.getHelp() ) { printLongVersion(); return result; }

        //set the logging level only if -v was specified
        //else bank upon the the default logging level
        if(mPOptions.getLoggingLevel() > 0){
            mLogger.setLevel(mPOptions.getLoggingLevel());
        }


        //do sanity check on dax file
        String dax         = mPOptions.getDAX();
        String pdax        = mPOptions.getPDAX();
        String baseDir     = mPOptions.getBaseSubmitDirectory();

        if( dax == null && pdax == null ){
            mLogger.log( "\nNeed to specify either a dax file ( using --dax )  or a pdax file (using --pdax) to plan",
                         LogManager.INFO_MESSAGE_LEVEL);
            this.printShortVersion();
            return result;
        }

        if( mPOptions.getPartitioningType() != null ){
            // partition and plan the workflow
            doPartitionAndPlan( mProps, options );
            return result;
        }

        //check if sites set by user. If user has not specified any sites then
        //load all sites from site catalog.
        Collection eSites  = mPOptions.getExecutionSites();
        Set<String> toLoad = new HashSet<String>( mPOptions.getExecutionSites() );
        //add the output site if specified
        if( mPOptions.getOutputSite() != null ){
            toLoad.add( mPOptions.getOutputSite() );
        }
        mLogger.log( "Sites to load in site store " + toLoad, LogManager.DEBUG_MESSAGE_LEVEL );        
        mLogger.log( "Execution sites are         " + eSites, LogManager.DEBUG_MESSAGE_LEVEL );
        
        //load the site catalog and transformation catalog accordingly
        SiteStore s = loadSiteStore( toLoad );
        s.setForPlannerUse( mProps, mPOptions);
        
        mBag.add( PegasusBag.SITE_STORE, s );
        mBag.add( PegasusBag.TRANSFORMATION_CATALOG, 
                  TransformationFactory.loadInstance( mBag )  );
        

        //populate planner metrics
        mPMetrics.setStartTime( new Date() );
        mPMetrics.setVOGroup( mPOptions.getVOGroup() );
        mPMetrics.setBaseSubmitDirectory( mPOptions.getSubmitDirectory() );
        mPMetrics.setDAX( mPOptions.getDAX() );

        
            //Commented for new Site catalog
//        UserOptions opts = UserOptions.getInstance(mPOptions);
        

        //try to get hold of the vds properties
        //set in the jvm that user specifed at command line
        mPOptions.setVDSProperties(mProps.getMatchingProperties("pegasus.",false));

        List allVDSProps = mProps.getMatchingProperties("pegasus.",false);
        mLogger.log("Pegasus Properties set by the user",LogManager.CONFIG_MESSAGE_LEVEL );
        for(java.util.Iterator it = allVDSProps.iterator(); it.hasNext();){
            NameValue nv = (NameValue)it.next();
            mLogger.log(nv.toString(),LogManager.CONFIG_MESSAGE_LEVEL);

        }

        
        if(dax == null && pdax != null
           && !eSites.isEmpty()){
            //do the deferreed planning by parsing
            //the partition graph in the pdax file.
            result = doDeferredPlanning();
        }
        else if(pdax == null && dax != null
             && !eSites.isEmpty()){

            Callback cb =  DAXCallbackFactory.loadInstance( mProps, dax, "DAX2CDAG" );

            DaxParser daxParser = new DaxParser( dax, mBag, cb );

            ADag orgDag = (ADag)cb.getConstructedObject();

            //generate the flow ids for the classads information
            orgDag.dagInfo.generateFlowName();
            orgDag.dagInfo.setFlowTimestamp( mPOptions.getDateTime( mProps.useExtendedTimeStamp() ));
            orgDag.dagInfo.setDAXMTime( dax );
            orgDag.dagInfo.generateFlowID();
            orgDag.dagInfo.setReleaseVersion();

            //log id hiearchy message
            //that connects dax with the jobs
            logIDHierarchyMessage( orgDag , LoggingKeys.DAX_ID, orgDag.getAbstractWorkflowID() );

            //write out a the relevant properties to submit directory
            int state = 0;
            String relativeDir; //the submit directory relative to the base specified
            try{
                //create the base directory if required
                relativeDir = /*( mPOptions.partOfDeferredRun() )?
                                        null:*/
                                        ( mPOptions.getRelativeSubmitDirectory() == null )?
                                                //create our own relative dir
                                                createSubmitDirectory( orgDag,
                                                                       baseDir,
                                                                       mUser,
                                                                       mPOptions.getVOGroup(),
                                                                       mProps.useTimestampForDirectoryStructure() ):
                                                mPOptions.getRelativeSubmitDirectory();
                mPOptions.setSubmitDirectory( baseDir, relativeDir  );
                state++;
                mProps.writeOutProperties( mPOptions.getSubmitDirectory() );

                mPMetrics.setRelativeSubmitDirectory( mPOptions.getRelativeSubmitDirectory() );

                //also log in the planner metrics where the properties are
                mPMetrics.setProperties( mProps.getPropertiesInSubmitDirectory() );
            }
            catch( IOException ioe ){
                String error = ( state == 0 ) ?
                               "Unable to write  to directory" :
                               "Unable to write out properties to directory";
                throw new RuntimeException( error + mPOptions.getSubmitDirectory() , ioe );

            }
            
            boolean rescue = handleRescueDAG( orgDag, mPOptions );
            if( rescue ){
                mLogger.log( "No planning attempted. Rescue dag will be submitted", 
                             LogManager.INFO_MESSAGE_LEVEL );
                result = new LinkedList(  );
                result.add(  new File ( mPOptions.getSubmitDirectory(), 
                                        this.getDAGFilename( orgDag, mPOptions)));
                return result;
            }
            

            //check if a random directory is specified by the user
            if(mPOptions.generateRandomDirectory() && mPOptions.getRandomDir() == null){
                //user has specified the random dir name but wants
                //to go with default name which is the flow id
                //for the workflow unless a basename is specified.
                mPOptions.setRandomDir(getRandomDirectory(orgDag));
            }
            else if( mPOptions.getRandomDir() != null ){
                //keep the name that the user passed
            }
            else if( relativeDir != null ){
                //the relative directory constructed on the submit host
                //is the one required for remote sites
                mPOptions.setRandomDir( relativeDir );
            }

            //populate the singleton instance for user options
            //UserOptions opts = UserOptions.getInstance(mPOptions);
            MainEngine cwmain = new MainEngine( orgDag, mBag );

            ADag finalDag = cwmain.runPlanner();
            DagInfo ndi = finalDag.dagInfo;

            //store the workflow metrics from the final dag into
            //the planner metrics
            mPMetrics.setWorkflowMetrics( finalDag.getWorkflowMetrics() );

            //we only need the script writer for daglite megadag generator mode
            CodeGenerator codeGenerator = null;
            codeGenerator = CodeGeneratorFactory.
                                     loadInstance( cwmain.getPegasusBag() );


            //before generating the codes for the workflow check
            //for emtpy workflows
            if( finalDag.isEmpty() ){

               mLogger.log( EMPTY_FINAL_WORKFLOW_MESSAGE, LogManager.INFO_MESSAGE_LEVEL );
               return result;
            }

            message = "Generating codes for the concrete workflow";
            log( message, LogManager.INFO_MESSAGE_LEVEL );
            try{
                result = codeGenerator.generateCode(finalDag);
                
                //connect the jobs and the DAG via the hierarchy message
                this.logIDHierarchyMessage( finalDag, LoggingKeys.DAG_ID, finalDag.getExecutableWorkflowID() );

                //generate only the braindump file that is required.
                //no spawning off the tailstatd for time being
                codeGenerator.startMonitoring();

                /*
                if (mPOptions.monitorWorkflow()) {
                    //submit files successfully generated.
                    //spawn off the monitoring daemon
                    codeGenerator.startMonitoring();
                }
               */
            }
            catch ( Exception e ){
                throw new RuntimeException( "Unable to generate code", e );
            }
            mLogger.log( message + " -DONE", LogManager.INFO_MESSAGE_LEVEL );

            //create the submit files for cleanup dag if
            //random dir option specified
            if(mPOptions.generateRandomDirectory()){
                ADag cleanupDAG = cwmain.getCleanupDAG();
                PlannerOptions cleanupOptions = (PlannerOptions)mPOptions.clone();

                //submit files are generated in a subdirectory
                //of the submit directory
                message = "Generating code for the cleanup workflow";
                mLogger.log( message, LogManager.INFO_MESSAGE_LEVEL );
                //set the submit directory in the planner options for cleanup wf
                cleanupOptions.setSubmitDirectory( cleanupOptions.getSubmitDirectory(), this.CLEANUP_DIR );
                PegasusBag bag = cwmain.getPegasusBag();
                bag.add( PegasusBag.PLANNER_OPTIONS, cleanupOptions );
                codeGenerator = CodeGeneratorFactory.
                              loadInstance( cwmain.getPegasusBag() );

                try{
                    codeGenerator.generateCode(cleanupDAG);
                }
                catch ( Exception e ){
                    throw new RuntimeException( "Unable to generate code", e );
                }

                mLogger.log(message + " -DONE",LogManager.INFO_MESSAGE_LEVEL);
            }

            //log entry in to the work catalog
            boolean nodatabase = logEntryInWorkCatalog( finalDag, baseDir, relativeDir );

            //write out  the planner metrics  to global log
            mPMetrics.setEndTime( new Date() );
            writeOutMetrics( mPMetrics );

            if( mPOptions.submitToScheduler() ){//submit the jobs
                StringBuffer invocation = new StringBuffer();
                //construct the path to the bin directory
                invocation.append( mProps.getPegasusHome() ).append( File.separator ).
                           append( "bin" ).append( File.separator ).append( getPegasusRunInvocation ( nodatabase ) );

                boolean submit = submitWorkflow( invocation.toString() );
                if ( !submit ){
                    throw new RuntimeException(
                        "Unable to submit the workflow using pegasus-run" );
                }
            }
            else{
                //log the success message
                this.logSuccessfulCompletion(nodatabase);
            }
        }
        else{
            printShortVersion();
            throw new RuntimeException("Invalid combination of arguments passed");
        }

        return result;
    }

    /**
     * Parses the command line arguments using GetOpt and returns a
     * <code>PlannerOptions</code> contains all the options passed by the
     * user at the command line.
     *
     * @param args  the arguments passed by the user at command line.
     *
     * @return the  options.
     */
    public PlannerOptions parseCommandLineArguments( String[] args ){
        return parseCommandLineArguments( args, true );
    }


    /**
     * Parses the command line arguments using GetOpt and returns a
     * <code>PlannerOptions</code> contains all the options passed by the
     * user at the command line.
     *
     * @param args  the arguments passed by the user at command line.
     * @param sanitizePath  whether to sanitize path during construction of options
     *
     *
     * @return the  options.
     */
    public PlannerOptions parseCommandLineArguments( String[] args, boolean sanitizePath ){
        LongOpt[] longOptions = generateValidOptions();

        Getopt g = new Getopt("pegasus-plan",args,
                              "vhfSnzpVr::aD:d:s:o:P:c:C:b:g:2:j:3:",
                              longOptions,false);
        g.setOpterr(false);

        int option = 0;
        PlannerOptions options = new PlannerOptions();
        options.setSanitizePath( sanitizePath );

        while( (option = g.getopt()) != -1){
            //System.out.println("Option tag " + (char)option);
            switch (option) {

                case 1://monitor
                    options.setMonitoring( true );
                    break;

                case 'z'://deferred
                    options.setPartOfDeferredRun( true );
                    break;

                case 'a'://authenticate
                    options.setAuthentication(true);
                    break;

                case 'b'://optional basename
                    options.setBasenamePrefix(g.getOptarg());
                    break;

                case 'c'://cache
                    options.setCacheFiles( g.getOptarg() );
                    break;

                case 'C'://cluster
                    options.setClusteringTechnique( g.getOptarg() );
                    break;

                case 'd'://dax
                    options.setDAX(g.getOptarg());
                    break;

                case 'D': //dir
                    options.setSubmitDirectory( g.getOptarg(), null );
                    break;

                case '2'://relative-dir
                    options.setRelativeSubmitDirectory( g.getOptarg() );
                    break;

                case '3'://rescue
                    options.setNumberOfRescueTries( g.getOptarg() );
                    break;
                    
                case 'f'://force
                    options.setForce(true);
                    break;

                case 'g': //group
                    options.setVOGroup( g.getOptarg() );
                    break;

                case 'h'://help
                    options.setHelp(true);
                    break;

                case 'j'://job-prefix
                    options.setJobnamePrefix( g.getOptarg() );
                    break;

                case 'm'://megadag option
                    options.setMegaDAGMode(g.getOptarg());
                    break;

                case 'n'://nocleanup option
                    options.setCleanup( false );
                    break;

                case 'o'://output
                    options.setOutputSite(g.getOptarg());
                    break;

                case 'p'://partition and plan
                    options.setPartitioningType( "Whole" );
                    break;

                case 'P'://pdax file
                    options.setPDAX(g.getOptarg());
                    break;

                case 'r'://randomdir
                    options.setRandomDir(g.getOptarg());
                    break;

                case 'S'://submit option
                    options.setSubmitToScheduler( true );
                    break;

                case 's'://sites
                    options.setExecutionSites( g.getOptarg() );
                    break;


                case 'v'://verbose
                    options.incrementLogging();
                    break;

                case 'V'://version
                    mLogger.log(getGVDSVersion(),LogManager.INFO_MESSAGE_LEVEL);
                    System.exit(0);


                default: //same as help
                    printShortVersion();
                    throw new RuntimeException("Incorrect option or option usage " +
                                               option);

            }
        }
        return options;

    }

    /**
     * Logs an entry in the work catalog about the workflow being planned.
     *
     * @param dag ADag
     * @param baseDir String
     * @param relativeDir String
     *
     * @return boolean
     */
    protected boolean logEntryInWorkCatalog( ADag dag, String baseDir, String relativeDir ){
        //connect to the work catalog and populate
        //an entry in it for the current workflow
        WorkCatalog wc = null;
        boolean nodatabase = false;
        try{
            wc = WorkFactory.loadInstance( mProps );
        }
        catch( Exception e ) {
            //just log and proceed
            mLogger.log( "Ignoring: " + convertException( e ),  LogManager.DEBUG_MESSAGE_LEVEL );
            nodatabase = true;
        }
        if ( wc != null ) {
            wc.insert( baseDir, mPOptions.getVOGroup(), dag.getLabel(),
                       new File( relativeDir == null ? "." : relativeDir  ).getName(),
                       mUser,
                       Currently.parse( dag.getMTime() ),
                       Currently.parse( dag.dagInfo.getFlowTimestamp() ),
                       -2  );
            try{
                wc.close();
            } catch( Exception e ) {
                /*ignore */
                nodatabase = true;
            }
        }
        return nodatabase;
    }


    /**
     * Submits the workflow for execution using pegasus-run, a wrapper around
     * pegasus-submit-dag.
     *
     * @param invocation    the pegasus run invocation
     *
     * @return boolean indicating whether could successfully submit the workflow or not.
     */
    public boolean submitWorkflow( String invocation ){
        boolean result = false;
        try{
            //set the callback and run the pegasus-run command
            Runtime r = Runtime.getRuntime();

            mLogger.log( "Executing  " + invocation,
                         LogManager.DEBUG_MESSAGE_LEVEL );
            Process p = r.exec( invocation );

            //spawn off the gobblers with the already initialized default callback
            StreamGobbler ips =
                new StreamGobbler( p.getInputStream(), new DefaultStreamGobblerCallback(
                                                                   LogManager.INFO_MESSAGE_LEVEL ));
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

            mLogger.log( "Submission of workflow exited with status " + status,
                         LogManager.DEBUG_MESSAGE_LEVEL );

            result = (status == 0) ?true : false;
        }
        catch(IOException ioe){
            mLogger.log("IOException while running tailstatd ", ioe,
                        LogManager.ERROR_MESSAGE_LEVEL);
        }
        catch( InterruptedException ie){
            //ignore
        }
        return result;

    }

    /**
     * Partitions and plans the workflow. First step of merging DAGMan and
     * Condor
     *
     * @param properties   the properties passed to the planner.
     * @param options      the options passed to the planner.
     */
    protected void doPartitionAndPlan( PegasusProperties properties, PlannerOptions options ){
        //we first need to get the label of DAX
        Callback cb =  DAXCallbackFactory.loadInstance( properties, options.getDAX(), "DAX2Metadata" );
        try{
            DaxParser daxParser = new DaxParser( options.getDAX(), mBag, cb );
        }catch( Exception e ){
            //ignore
        }
        Map metadata = ( Map ) cb.getConstructedObject();
        String label = (String) metadata.get( "name" );

        String baseDir = options.getBaseSubmitDirectory();
        String relativeDir = null;
        //construct the submit directory structure
        try{
            relativeDir = (options.getRelativeSubmitDirectory() == null) ?
                                 //create our own relative dir
                                 createSubmitDirectory(label,
                                                       baseDir,
                                                       mUser,
                                                       options.getVOGroup(),
                                                       properties.useTimestampForDirectoryStructure()) :
                                 options.getRelativeSubmitDirectory();
        }
        catch( IOException ioe ){
            String error = "Unable to write  to directory" ;
            throw new RuntimeException( error + options.getSubmitDirectory() , ioe );

        }

        options.setSubmitDirectory( baseDir, relativeDir  );
        mLogger.log( "Submit Directory for workflow is " + options.getSubmitDirectory() , LogManager.DEBUG_MESSAGE_LEVEL );

        //now let us run partitiondax
        //mLogger.log( "Partitioning Workflow" , LogManager.INFO_MESSAGE_LEVEL );
        mLogger.logEventStart( LoggingKeys.EVENT_PEGASUS_PARTITION, LoggingKeys.DAX_ID, options.getDAX() );
        PartitionDAX partitionDAX = new PartitionDAX();
        File dir = new File( options.getSubmitDirectory(), "dax" );
        String pdax = partitionDAX.partitionDAX(
                                                  properties,
                                                  options.getDAX(),
                                                  dir.getAbsolutePath(),
                                                  options.getPartitioningType() );

        mLogger.log( "PDAX file generated is " + pdax , LogManager.DEBUG_MESSAGE_LEVEL );
        mLogger.logEventCompletion();
                
        //now run pegasus-plan with pdax option
        CPlanner pegasusPlan = new CPlanner();
        options.setDAX( null );
        options.setPDAX( pdax );
        options.setPartitioningType( null );

        pegasusPlan.executeCommand( options );

    }

    /**
     * Sets the basename of the random directory that is created on the remote
     * sites per workflow. The name is generated by default from teh flow ID,
     * unless a basename prefix is specifed at runtime in the planner options.
     *
     * @param dag  the DAG containing the abstract workflow.
     *
     * @return  the basename of the random directory.
     */
    protected String getRandomDirectory(ADag dag){

        //constructing the name of the dagfile
        StringBuffer sb = new StringBuffer();
        String bprefix = mPOptions.getBasenamePrefix();
        if( bprefix != null){
            //the prefix is not null using it
            sb.append(bprefix);
            sb.append("-");
            //append timestamp to generate some uniqueness
            sb.append(dag.dagInfo.getFlowTimestamp());
        }
        else{
            //use the flow ID that contains the timestamp and the name both.
            sb.append(dag.dagInfo.flowID);
        }
        return sb.toString();
    }


    /**
     * Tt generates the LongOpt which contain the valid options that the command
     * will accept.
     *
     * @return array of <code>LongOpt</code> objects , corresponding to the valid
     * options
     */
    public LongOpt[] generateValidOptions(){
        LongOpt[] longopts = new LongOpt[24];

        longopts[0]   = new LongOpt( "dir", LongOpt.REQUIRED_ARGUMENT, null, 'D' );
        longopts[1]   = new LongOpt( "dax", LongOpt.REQUIRED_ARGUMENT, null, 'd' );
        longopts[2]   = new LongOpt( "sites", LongOpt.REQUIRED_ARGUMENT, null, 's' );
        longopts[3]   = new LongOpt( "output", LongOpt.REQUIRED_ARGUMENT, null, 'o' );
        longopts[4]   = new LongOpt( "verbose", LongOpt.NO_ARGUMENT, null, 'v' );
        longopts[5]   = new LongOpt( "help", LongOpt.NO_ARGUMENT, null, 'h' );
        longopts[6]   = new LongOpt( "force", LongOpt.NO_ARGUMENT, null, 'f' );
        longopts[7]   = new LongOpt( "submit", LongOpt.NO_ARGUMENT, null, 'S' );
        longopts[8]   = new LongOpt( "version", LongOpt.NO_ARGUMENT, null, 'V' );
        longopts[9]   = new LongOpt( "randomdir", LongOpt.OPTIONAL_ARGUMENT, null, 'r' );
        longopts[10]  = new LongOpt( "authenticate", LongOpt.NO_ARGUMENT, null, 'a' );
        //deferred planning options
        longopts[11]  = new LongOpt( "pdax", LongOpt.REQUIRED_ARGUMENT, null, 'P' );
        longopts[12]  = new LongOpt( "cache", LongOpt.REQUIRED_ARGUMENT, null, 'c' );
        longopts[13]  = new LongOpt( "megadag", LongOpt.REQUIRED_ARGUMENT, null, 'm' );
        //collapsing for mpi
        longopts[14]  = new LongOpt( "cluster", LongOpt.REQUIRED_ARGUMENT, null, 'C' );
        //more deferred planning stuff
        longopts[15]  = new LongOpt( "basename", LongOpt.REQUIRED_ARGUMENT, null, 'b' );
        longopts[16]  = new LongOpt( "monitor", LongOpt.NO_ARGUMENT, null , 1 );
        longopts[17]  = new LongOpt( "nocleanup", LongOpt.NO_ARGUMENT, null, 'n' );
        longopts[18]  = new LongOpt( "group",   LongOpt.REQUIRED_ARGUMENT, null, 'g' );
        longopts[19]  = new LongOpt( "deferred", LongOpt.NO_ARGUMENT, null, 'z');
        longopts[20]  = new LongOpt( "relative-dir", LongOpt.REQUIRED_ARGUMENT, null, '2' );
        longopts[21]  = new LongOpt( "pap", LongOpt.NO_ARGUMENT, null, 'p' );
        longopts[22]  = new LongOpt( "job-prefix", LongOpt.REQUIRED_ARGUMENT, null, 'j' );
        longopts[23]  = new LongOpt( "rescue", LongOpt.REQUIRED_ARGUMENT, null, '3');
        return longopts;
    }


    /**
     * Prints out a short description of what the command does.
     */
    public void printShortVersion(){
        String text =
          "\n $Id$ " +
          "\n " + getGVDSVersion() +
          "\n Usage : pegasus-plan [-Dprop  [..]] -d|-P <dax file|pdax file> " +
          " [-s site[,site[..]]] [-b prefix] [-c f1[,f2[..]]] [-f] [-m style] " /*<dag|noop|daglite>]*/ +
          "\n [-a] [-b basename] [-C t1[,t2[..]]  [-D  <base dir  for o/p files>] [-j <job-prefix>] " +
          " [ --relative-dir <relative directory to base directory> ][-g <vogroup>] [-o <output site>] " +
          "\n [-r[dir name]] [--monitor] [-S] [-n]  [-v] [-V] [-h]";

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
           "\n pegasus-plan - The main class which is used to run  Pegasus. "  +
           "\n Usage: pegasus-plan [-Dprop  [..]] --dax|--pdax <file> [--sites <execution sites>] " +
           "\n [--authenticate] [--basename prefix] [--cache f1[,f2[..]] [--cluster t1[,t2[..]] " +
           "\n [--dir <dir for o/p files>] [--force] [--group vogroup] [--megadag style] [--monitor] [--nocleanup] " +
           "\n [--output output site] [--randomdir=[dir name]] [--verbose] [--version][--help] " +
           "\n" +
           "\n Mandatory Options " +
           "\n -d |-P fn "+
           "\n --dax|--pdax       the path to the dax file containing the abstract workflow " +
           "\n                    or the path to the pdax file containing the partition graph " +
           "\n                    generated by the partitioner." +
           "\n Other Options  " +
           "\n -a |--authenticate turn on authentication against remote sites ." +
           "\n -b |--basename     the basename prefix while constructing the per workflow files like .dag etc." +
           "\n -c |--cache        comma separated list of replica cache files." +
           "\n -C |--cluster      comma separated list of clustering techniques to be applied to the workflow to " +
           "\n                    to cluster jobs in to larger jobs, to avoid scheduling overheads." +
           "\n -D |--dir          the directory where to generate the concrete workflow." +
           "\n --relative-dir     the relative directory to the base directory where to generate the concrete workflow." +
           "\n -f |--force        skip reduction of the workflow, resulting in build style dag." +
           "\n -g |--group        the VO Group to which the user belongs " +
           "\n -j |--job-prefix   the prefix to be applied while construction job submit filenames " +
           "\n -m |--megadag      type of style to use while generating the megadag in deferred planning." +
           "\n -o |--output       the output site where the data products during workflow execution are transferred to." +
           "\n -s |--sites        comma separated list of executions sites on which to map the workflow." +
           "\n -r |--randomdir    create random directories on remote execution sites in which jobs are executed" +
           "\n                    can optionally specify the basename of the remote directories" +
           "\n     --monitor      monitor the execution of the workflow, using workflow monitor daemon like tailstatd." +
           "\n -n |--nocleanup    generates only the separate cleanup workflow. Does not add cleanup nodes to the concrete workflow." +
           "\n -S |--submit       submit the executable workflow generated" +
           "\n -v |--verbose      increases the verbosity of messages about what is going on" +
           "\n -V |--version      displays the version of the Pegasus Workflow Management System" +
           "\n -h |--help         generates this help." +
           "\n The following exitcodes are produced" +
           "\n 0 concrete planner planner was able to generate a concretized workflow" +
           "\n 1 an error occured. In most cases, the error message logged should give a" +
           "\n   clear indication as to where  things went wrong." +
           "\n 2 an error occured while loading a specific module implementation at runtime" +
           "\n ";

        System.out.println(text);
        //mLogger.log(text,LogManager.INFO_MESSAGE_LEVEL);
    }


    /**
     * This ends up invoking the deferred planning code, that generates
     * the MegaDAG that is used to submit the partitioned daxes in layers.
     *
     * @return the Collection of <code>File</code> objects for the files written
     *         out.
     */
    private Collection<File> doDeferredPlanning(){
        String mode = mPOptions.getMegaDAGMode();
        mode  = (mode == null)?
                   DEFAULT_MEGADAG_MODE:
                   mode;

        String file = mPOptions.getPDAX();

        //get the name of the directory from the file
        String directory = new File(file).getParent();
        //System.out.println("Directory in which partitioned daxes are " + directory);

        int errorStatus = 1;
        ADag megaDAG = null;
        Collection result = null;
        try{
            //load the correct callback handler
            org.griphyn.cPlanner.parser.pdax.Callback c =
                PDAXCallbackFactory.loadInstance(mProps, mPOptions, directory);
            errorStatus = 2;

            //this is a bug. Should not be called. To be corrected by Karan
            //Commented for new Site catalog
//            UserOptions y = UserOptions.getInstance(mPOptions);


            //intialize the bag of objects and load the site selector
            PegasusBag bag = new PegasusBag();
            bag.add( PegasusBag.PEGASUS_LOGMANAGER, mLogger );
            bag.add( PegasusBag.PEGASUS_PROPERTIES, mProps );
            bag.add( PegasusBag.PLANNER_OPTIONS, mPOptions );
//            bag.add( PegasusBag.TRANSFORMATION_CATALOG, TCMode.loadInstance() );
          //bag.add( PegasusBag.TRANSFORMATION_MAPPER, mTCMapper );
            bag.add( PegasusBag.PEGASUS_LOGMANAGER, mLogger );
            bag.add( PegasusBag.SITE_STORE, mBag.getHandleToSiteStore() );
            bag.add( PegasusBag.TRANSFORMATION_CATALOG, mBag.getHandleToTransformationCatalog() );

            //start the parsing and let the fun begin
            PDAXParser p = new PDAXParser( file , mProps );
            p.setCallback(c);
            p.startParser(file);

            megaDAG = (ADag)c.getConstructedObject();

            CodeGenerator codeGenerator = null;
            //load the Condor Writer that understands HashedFile Factories.
            codeGenerator = CodeGeneratorFactory.loadInstance( bag );
            errorStatus = 3;
            result = codeGenerator.generateCode( megaDAG );

            //generate only the braindump file that is required.
            //no spawning off the tailstatd for time being
            codeGenerator.startMonitoring();

        }
        catch(FactoryException fe){
            //just rethrow for time being. we need error status as 2
            throw fe;
        }
        catch(Exception e){
            String message;
            switch(errorStatus){
                case 1:
                    message = "Unable to load the PDAX Callback ";
                    break;

                case 2:
                    message = "Unable to parse the PDAX file ";
                    break;

                case 3:
                    message = "Unable to generate the code for the MegaDAG";
                    break;

                default:
                    //unreachable code
                    message = "Unknown Error " ;
                    break;
            }
            throw new RuntimeException(message, e);
        }



        this.logSuccessfulCompletion( this.logEntryInWorkCatalog( megaDAG,
                                                                  mPOptions.getBaseSubmitDirectory(),
                                                                  mPOptions.getRelativeSubmitDirectory() ));

        return result;
    }

    /**
     * Creates the submit directory for the workflow. This is not thread safe.
     *
     * @param dag     the workflow being worked upon.
     * @param dir     the base directory specified by the user.
     * @param user    the username of the user.
     * @param vogroup the vogroup to which the user belongs to.
     * @param timestampBased boolean indicating whether to have a timestamp based dir or not
     *
     * @return  the directory name created relative to the base directory passed
     *          as input.
     *
     * @throws IOException in case of unable to create submit directory.
     */
    protected String createSubmitDirectory( ADag dag,
                                            String dir,
                                            String user,
                                            String vogroup,
                                            boolean timestampBased ) throws IOException {

        return createSubmitDirectory( dag.getLabel(), dir, user, vogroup, timestampBased );
    }

    /**
     * Creates the submit directory for the workflow. This is not thread safe.
     *
     * @param label   the label of the workflow
     * @param dir     the base directory specified by the user.
     * @param user    the username of the user.
     * @param vogroup the vogroup to which the user belongs to.
     * @param timestampBased boolean indicating whether to have a timestamp based dir or not
     *
     * @return  the directory name created relative to the base directory passed
     *          as input.
     *
     * @throws IOException in case of unable to create submit directory.
     */
    protected String createSubmitDirectory( String label,
                                            String dir,
                                            String user,
                                            String vogroup,
                                            boolean timestampBased ) throws IOException {
        File base = new File( dir );
        StringBuffer result = new StringBuffer();

        //do a sanity check on the base
        sanityCheck( base );

        //add the user name if possible
        base = new File( base, user );
        result.append( user ).append( File.separator );

        //add the vogroup
        base = new File( base, vogroup );
        sanityCheck( base );
        result.append( vogroup ).append( File.separator );

        //add the label of the DAX
        base = new File( base, label );
        sanityCheck( base );
        result.append( label ).append( File.separator );

        //create the directory name
        StringBuffer leaf = new StringBuffer();
        if( timestampBased ){
            leaf.append( mPOptions.getDateTime( mProps.useExtendedTimeStamp() ) );
        }
        else{
            //get all the files in this directory
            String[] files = base.list( new RunDirectoryFilenameFilter() );
            //find the maximum run directory
            int num, max = 1;
            for( int i = 0; i < files.length ; i++ ){
                num = Integer.parseInt( files[i].substring( RunDirectoryFilenameFilter.SUBMIT_DIRECTORY_PREFIX.length() ) );
                if ( num + 1 > max ){ max = num + 1; }
            }

            //create the directory name
            leaf.append( RunDirectoryFilenameFilter.SUBMIT_DIRECTORY_PREFIX ).append( mNumFormatter.format( max ) );
        }
        result.append( leaf.toString() );
        base = new File( base, leaf.toString() );
        mLogger.log( "Directory to be created is " + base.getAbsolutePath(),
                     LogManager.DEBUG_MESSAGE_LEVEL );
        sanityCheck( base );

        return result.toString();
    }


    /**
     * Checks the destination location for existence, if it can
     * be created, if it is writable etc.
     *
     * @param dir is the new base directory to optionally create.
     *
     * @throws IOException in case of error while writing out files.
     */
    protected static void sanityCheck( File dir ) throws IOException{
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
                throw new IOException( "Unable to create  directory " +
                                       dir.getPath() );
            }
        }
    }


    /**
     * Writes out the planner metrics to the global log.
     *
     * @param pm  the metrics to be written out.
     *
     * @return boolean
     */
    protected boolean writeOutMetrics( PlannerMetrics pm  ){
        boolean result = false;
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
                    fos.write( pm.toString().getBytes() );
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

    /**
     * Returns the basename of the dag file
     * 
     * @param dag       the dag that was parsed.
     * @param options   the planner options
     * 
     * @return boolean  true means submit the rescue
     *                  false do the planning operation
     */
    protected String getDAGFilename( ADag dag, PlannerOptions options ){
        //determine the name of the .dag file that will be written out.
        //constructing the name of the dagfile
        StringBuffer sb = new StringBuffer();
        String bprefix = options.getBasenamePrefix();
        if( bprefix != null){
            //the prefix is not null using it
            sb.append(bprefix);
        }
        else{
            //generate the prefix from the name of the dag
            sb.append(dag.dagInfo.nameOfADag).append("-").
                append(dag.dagInfo.index);
        }
        //append the suffix
        sb.append( ".dag" );
        return sb.toString();
    }
    
    /**
     * Checks for rescue dags, and determines whether to plan or not.
     * 
     * 
     * @param dag       the dag that was parsed.
     * @param options   the planner options
     * 
     * @return boolean  true means submit the rescue
     *                  false do the planning operation
     */
    protected boolean handleRescueDAG( ADag dag, PlannerOptions options ) {
        
        

        return this.handleRescueDAG(  getDAGFilename( dag, options ),
                                      options.getSubmitDirectory(),
                                      options.getNumberOfRescueTries() );

    }
    
    /**
     * Checks for rescue dags, and determines whether to plan or not.
     * 
     * 
     * @param dag           the dag file for the dax
     * @param dir           the submit directory.  
     * @param numOfRescues  the number of rescues to handle.
     * 
     * 
     * @return true means submit the rescue
     *                  false do the planning operation
     */
    protected boolean handleRescueDAG( String dag, String dir, int numOfRescues ) {
        boolean result = false;
        //sanity check
        if( numOfRescues < 1 ){
            return result;
        }
        
        //check for existence of dag file
        
        if( numOfRescues < 1 ){
            return result;
        }
        
        //check for existence of dag file
        //if it does not exists means we need to plan
        File f  = new File( dir, dag );
        mLogger.log( "Determining existence of dag file " + f.getAbsolutePath(),
                     LogManager.DEBUG_MESSAGE_LEVEL );        
        if ( !f.exists() ){
            return result;
        }
        
        
        
        //check for existence of latest rescue file.
        NumberFormat nf = new DecimalFormat( "000" );
        
        String rescue = dag + ".rescue" +  nf.format( numOfRescues );
        f  = new File( dir, rescue );
        mLogger.log( "Determining existence of rescue file " + f.getAbsolutePath(),
                     LogManager.DEBUG_MESSAGE_LEVEL );
         
        
         return  f.exists() ?
                 result  ://means we need to start planning now
                 !result;
          
      }

    /**
     * 
     * @param sites
     * @return SiteStore object containing the information about the sites.
     */
    private SiteStore loadSiteStore( Collection<String> sites ) {
        SiteStore result = new SiteStore();
        if( sites.isEmpty() ) {
            mLogger.log("No sites given by user. Will use sites from the site catalog",
                        LogManager.DEBUG_MESSAGE_LEVEL);
            sites.add( "*" );
        }
        SiteCatalog catalog = null;
        
        /* load the catalog using the factory */
        catalog = SiteFactory.loadInstance( mProps );
        
        /* always load local site */
        List<String> toLoad = new ArrayList<String>( sites );
        toLoad.add( "local" );

        /* add the windward allegro graph site if reqd
         * a kludge for time being 
         */
        //figure out some allegro graph stuff
        Properties p = mProps.matchingSubset( WindwardImplementation.ALLEGRO_PROPERTIES_PREFIX, false  );
        String site = p.getProperty( "site" );
        if( site != null ){
            toLoad.add( site );
        }
        
        /* load the sites in site catalog */
        try{
            catalog.load( toLoad );
            
            //load into SiteStore from the catalog.
            for( Iterator<String> it = toLoad.iterator(); it.hasNext(); ){
                SiteCatalogEntry s = catalog.lookup( it.next() );
                if( s != null ){
                    result.addEntry( s );
                }
            }
            
            /* query for the sites, and print them out */
            mLogger.log( "Sites loaded are "  + result.list( ) ,
                         LogManager.DEBUG_MESSAGE_LEVEL );
            
        }
        catch ( SiteCatalogException e ){
            throw new RuntimeException( "Unable to load from site catalog " , e );
        }
        finally{
            /* close the connection */
            try{
                catalog.close();
            }catch( Exception e ){}
        }

        return result;
    }

    /**
     * Logs a message that connects the jobs with DAX/DAG
     * 
     * 
     * @param dag           the DAG object
     * @param parentType    the parent type
     * @param parentID      the parent id
     */
    private void logIDHierarchyMessage(ADag dag, String parentType, String parentID ) {
        //log the create id hieararchy message that 
        //ties the DAX with the jobs in it.
        //in bunches of 1000
        Enumeration e = dag.vJobSubInfos.elements();
        while( e.hasMoreElements() ){
            List<String> l = new LinkedList<String>();
            for( int i = 0; e.hasMoreElements() && i++ < 1000; ){
                SubInfo job = (SubInfo)e.nextElement();
                l.add( job.getID() );
            }
            mLogger.logEntityHierarchyMessage( parentType, parentID, LoggingKeys.JOB_ID, l );
        }
    }


    /**
     * Logs the successful completion message.
     *
     * @param nodatabase boolean indicating whether to add a nodatabase option or not.
     */
    private void logSuccessfulCompletion( boolean nodatabase ){
        StringBuffer message = new StringBuffer();
        message.append( this.SUCCESS_MESSAGE ).
                append( "" ).append( getPegasusRunInvocation( nodatabase ) ).
                append( "\n\n" );
        mLogger.log( message.toString(), LogManager.INFO_MESSAGE_LEVEL );

    }

    /**
     * Returns the pegasus-run invocation on the workflow planned.
     *
     * @param nodatabase boolean indicating whether to add a nodatabase option or not.
     *
     * @return  the pegasus-run invocation
     */
    private String getPegasusRunInvocation( boolean nodatabase ){
        StringBuffer result = new StringBuffer();

        result.append( "pegasus-run ").
               append( "-Dpegasus.user.properties=" ).append( mProps.getPropertiesInSubmitDirectory() ).
               append( nodatabase ? " --nodatabase"  : "" ).
               append( " " ).append( mPOptions.getSubmitDirectory() );

        return result.toString();

    }

}





