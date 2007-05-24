/*
 * This file or a portion of this file is licensed under the terms of
 * the Globus Toolkit Public License, found in file GTPL, or at
 * http://www.globus.org/toolkit/download/license.html. This notice must
 * appear in redistributions of this file, with or without modification.
 *
 * Redistributions of this Software, with or without modification, must
 * reproduce the GTPL in: (1) the Software, or (2) the Documentation or
 * some other similar material which is provided with the Software (if
 * any).
 *
 * Copyright 1999-2004 University of Chicago and The University of
 * Southern California. All rights reserved.
 */

package org.griphyn.cPlanner.toolkit;


import org.griphyn.cPlanner.code.CodeGenerator;
import org.griphyn.cPlanner.code.CodeGeneratorException;
import org.griphyn.cPlanner.code.generator.CodeGeneratorFactory;

import org.griphyn.cPlanner.classes.ADag;
import org.griphyn.cPlanner.classes.DagInfo;
import org.griphyn.cPlanner.classes.NameValue;
import org.griphyn.cPlanner.classes.PlannerOptions;

import org.griphyn.cPlanner.common.PegasusProperties;
import org.griphyn.cPlanner.common.UserOptions;
import org.griphyn.cPlanner.common.LogManager;

import org.griphyn.cPlanner.engine.MainEngine;

import org.griphyn.cPlanner.poolinfo.PoolMode;

import org.griphyn.cPlanner.parser.dax.Callback;
import org.griphyn.cPlanner.parser.dax.DAXCallbackFactory;

import org.griphyn.cPlanner.parser.pdax.PDAXCallbackFactory;

import org.griphyn.cPlanner.parser.DaxParser;
import org.griphyn.cPlanner.parser.PDAXParser;

import org.griphyn.common.catalog.work.WorkFactory;
import org.griphyn.common.catalog.WorkCatalog;

import org.griphyn.common.util.Version;
import org.griphyn.common.util.Currently;
import org.griphyn.common.util.FactoryException;

import gnu.getopt.Getopt;
import gnu.getopt.LongOpt;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.FilenameFilter;

import java.util.Collection;
import java.util.List;
import java.util.Date;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.Vector;
import java.util.HashSet;
import java.util.Iterator;

import java.util.regex.Pattern;

import java.text.NumberFormat;
import java.text.DecimalFormat;

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
     * The prefix for the submit directory.
     */
    public static final String SUBMIT_DIRECTORY_PREFIX = "run";

    /**
     * The basename of the directory that contains the submit files for the
     * cleanup DAG that for the concrete dag generated for the workflow.
     */
    public static final String CLEANUP_DIR  = "cleanup";

    /**
     * The object containing all the options passed to the Concrete Planner.
     */
    private PlannerOptions mPOptions;


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
        mPOptions.setSubmitDirectory(".");
        mPOptions.setExecutionSites(new java.util.HashSet());
        mPOptions.setOutputSite("");

        mUser = mProps.getProperty( "user.name" ) ;
        if ( mUser == null ){ mUser = "user"; }

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
            result = 1;
        }
        catch ( Exception e ) {
            //unaccounted for exceptions
            cPlanner.log(e.getMessage(),
                         LogManager.FATAL_MESSAGE_LEVEL );
            e.printStackTrace();
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
    public void executeCommand(String[] args) {
        String message = new String();
        boolean singleTransfer  = false;
        mPOptions = parseCommandLineArguments(args);

        //print help if asked for
        if( mPOptions.getHelp() ) { printLongVersion(); return; }

        //set the logging level only if -v was specified
        //else bank upon the the default logging level
        if(mPOptions.getLoggingLevel() > 0){
            mLogger.setLevel(mPOptions.getLoggingLevel());
        }

        UserOptions opts = UserOptions.getInstance(mPOptions);

        //try to get hold of the vds properties
        //set in the jvm that user specifed at command line
        mPOptions.setVDSProperties(mProps.getMatchingProperties("pegasus.",false));

        List allVDSProps = mProps.getMatchingProperties("pegasus.",false);
        mLogger.log("Pegasus Properties set by the user",LogManager.CONFIG_MESSAGE_LEVEL );
        for(java.util.Iterator it = allVDSProps.iterator(); it.hasNext();){
            NameValue nv = (NameValue)it.next();
            mLogger.log(nv.toString(),LogManager.CONFIG_MESSAGE_LEVEL);

        }



        String dax         = mPOptions.getDAX();
        String pdax        = mPOptions.getPDAX();
        String submitDir   = mPOptions.getSubmitDirectory();

        //check if sites set by user. If user has not specified any sites then
        //load all sites from site catalog.
        Collection eSites  = mPOptions.getExecutionSites();
        if(eSites.isEmpty()) {
            mLogger.log("No sites given by user. Will use sites from the site catalog",
                        LogManager.DEBUG_MESSAGE_LEVEL);
            List sitelist=(
                PoolMode.loadPoolInstance(
                              PoolMode.getImplementingClass(mProps.getPoolMode()),
                              mProps.getPoolFile(),PoolMode.SINGLETON_LOAD)).getPools();

            if(sitelist != null){
                if ( sitelist.contains( "local" ) ) {
                    sitelist.remove( "local" );
                }
                if( sitelist.size() >= 1 ) {
                    Set siteset = new HashSet( sitelist );
                    mPOptions.setExecutionSites( siteset );
                    eSites = mPOptions.getExecutionSites();
                } else {
                    throw new RuntimeException("Only local site is available. " +
                                               " Make sure your site catalog contains more sites");
                }
            } else {
                throw new RuntimeException("No sites present in the site catalog. " +
                                           "Please make sure your site catalog is correctly populated");
            }
        }

        if(dax == null && pdax != null
           && !eSites.isEmpty()){
            //do the deferreed planning by parsing
            //the partition graph in the pdax file.
            doDeferredPlanning();
        }
        else if(pdax == null && dax != null
             && !eSites.isEmpty()){

            Callback cb =  DAXCallbackFactory.loadInstance( mProps, dax, "DAX2CDAG" );

            DaxParser daxParser = new DaxParser( dax, mProps, cb );

            ADag orgDag = (ADag)cb.getConstructedObject();


            //write out a the relevant properties to submit directory
            int state = 0;
            String relativeDir; //the submit directory relative to the base specified
            try{
                //create the base directory if required
                relativeDir = createSubmitDirectory( submitDir, mUser, "ligo", orgDag.getLabel() );
                mPOptions.setSubmitDirectory( new File ( submitDir, relativeDir ) );
                state++;
                mProps.writeOutProperties( mPOptions.getSubmitDirectory() );
            }
            catch( IOException ioe ){
                String error = ( state == 0 ) ?
                               "Unable to write  to directory" :
                               "Unable to write out properties to directory";
                throw new RuntimeException( error + submitDir , ioe );

            }

            //check if a random directory is specified by the user
            if(mPOptions.generateRandomDirectory() && mPOptions.getRandomDir() == null){
                //user has specified the random dir name but wants
                //to go with default name which is the flow id
                //for the workflow unless a basename is specified.
                mPOptions.setRandomDir(getRandomDirectory(orgDag));
            }
            else{
                //the relative directory constructed on the submit host
                //is the one required for remote sites
                mPOptions.setRandomDir( relativeDir );
            }

            //populate the singleton instance for user options
            //UserOptions opts = UserOptions.getInstance(mPOptions);
            MainEngine cwmain = new MainEngine( orgDag, mProps, mPOptions);

            ADag finalDag = cwmain.runPlanner();
            DagInfo ndi = finalDag.dagInfo;

            //we only need the script writer for daglite megadag generator mode
            CodeGenerator codeGenerator = null;
            codeGenerator = CodeGeneratorFactory.
                                     loadInstance( mProps, mPOptions, mPOptions.getSubmitDirectory());


            message = "Generating codes for the concrete workflow";
            log( message, LogManager.INFO_MESSAGE_LEVEL );
            try{
                codeGenerator.generateCode(finalDag);

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
            mLogger.logCompletion( message, LogManager.INFO_MESSAGE_LEVEL );

            //create the submit files for cleanup dag if
            //random dir option specified
            if(mPOptions.generateRandomDirectory()){
                ADag cleanupDAG = cwmain.getCleanupDAG();

                //submit files are generated in a subdirectory
                //of the submit directory
                File f = new File( mPOptions.getSubmitDirectory(), this.CLEANUP_DIR );
                message = "Generating code for the cleanup workflow";
                mLogger.log(message,LogManager.INFO_MESSAGE_LEVEL);
                //set the submit directory in the planner options for cleanup wf
                mPOptions.setSubmitDirectory(f.getAbsolutePath());
                codeGenerator = CodeGeneratorFactory.
                              loadInstance( mProps, mPOptions, mPOptions.getSubmitDirectory() );

                try{
                    codeGenerator.generateCode(cleanupDAG);
                }
                catch ( Exception e ){
                    throw new RuntimeException( "Unable to generate code", e );
                }

                mLogger.logCompletion(message,LogManager.INFO_MESSAGE_LEVEL);
            }

            //connect to the work catalog and populate
            //an entry in it for the current workflow
            WorkCatalog wc = null;
            try{
                wc = WorkFactory.loadInstance( mProps );
            }
            catch( Exception e ) {
                //just log and proceed
                mLogger.log( "Ignoring: " + convertException( e ),  LogManager.DEBUG_MESSAGE_LEVEL );
            }
            if ( wc != null ) {
                wc.insert( submitDir, mPOptions.getVOGroup(), finalDag.getLabel(),
                           new File( relativeDir ).getName(),
                           mUser,
                           Currently.parse( finalDag.getMTime() ),
                           Currently.parse( finalDag.dagInfo.getFlowTimestamp() ),
                          -2  );
            }
            wc.close();

            if(mPOptions.submitToScheduler()){//submit the jobs
                throw new RuntimeException( "Direct submission is not supported at present" );
            }
        }
        else{
            printShortVersion();
            throw new RuntimeException("Invalid combination of arguments passed");
        }
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
    public PlannerOptions parseCommandLineArguments(String[] args){
        LongOpt[] longOptions = generateValidOptions();

        Getopt g = new Getopt("pegasus-plan",args,
                              "vhfRnVr::aD:d:s:o:P:c:C:b:g:",
                              longOptions,false);
        g.setOpterr(false);

        int option = 0;
        PlannerOptions options = new PlannerOptions();

        while( (option = g.getopt()) != -1){
            //System.out.println("Option tag " + (char)option);
            switch (option) {

                case 1://monitor
                    options.setMonitoring( true );
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
                    options.setSubmitDirectory(g.getOptarg());
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

                case 'm'://megadag option
                    options.setMegaDAGMode(g.getOptarg());
                    break;

                case 'n'://nocleanup option
                    options.setCleanup( false );
                    break;

                case 'o'://output
                    options.setOutputSite(g.getOptarg());
                    break;


                case 'P'://pdax file
                    options.setPDAX(g.getOptarg());
                    break;

                case 'r'://randomdir
                    options.setRandomDir(g.getOptarg());
                    break;

//                case 'R'://submit option
//                    options.setSubmitToScheduler( true);
//                    break;

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
                                               (char)option);

            }
        }
        return options;

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
        LongOpt[] longopts = new LongOpt[19];

        longopts[0]   = new LongOpt( "dir", LongOpt.REQUIRED_ARGUMENT, null, 'D' );
        longopts[1]   = new LongOpt( "dax", LongOpt.REQUIRED_ARGUMENT, null, 'd' );
        longopts[2]   = new LongOpt( "sites", LongOpt.REQUIRED_ARGUMENT, null, 's' );
        longopts[3]   = new LongOpt( "output", LongOpt.REQUIRED_ARGUMENT, null, 'o' );
        longopts[4]   = new LongOpt( "verbose", LongOpt.NO_ARGUMENT, null, 'v' );
        longopts[5]   = new LongOpt( "help", LongOpt.NO_ARGUMENT, null, 'h' );
        longopts[6]   = new LongOpt( "force", LongOpt.NO_ARGUMENT, null, 'f' );
        longopts[7]   = new LongOpt( "run", LongOpt.NO_ARGUMENT, null, 'R' );
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
          "\n [-a] [-b basename] [-C t1[,t2[..]]  [-D  <dir  for o/p files>] [-g <vogroup>] [-o <output site>] " +
          "[-r[dir name]] [--monitor] [-n]  [-v] [-V] [-h]";

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
           "\n -f |--force        skip reduction of the workflow, resulting in build style dag." +
           "\n -g |--group        the VO Group to which the user belongs " +
           "\n -m |--megadag      type of style to use while generating the megadag in deferred planning." +
           "\n -o |--output       the output site where the data products during workflow execution are transferred to." +
           "\n -s |--sites        comma separated list of executions sites on which to map the workflow." +
           "\n -r |--randomdir    create random directories on remote execution sites in which jobs are executed" +
           "\n                    can optionally specify the basename of the remote directories" +
           "\n     --monitor      monitor the execution of the workflow, using workflow monitor daemon like tailstatd." +
           "\n -n |--nocleanup    generates only the separate cleanup workflow. Does not add cleanup nodes to the concrete workflow." +
// "\n -R |--run          submit the concrete workflow generated" +
           "\n -v |--verbose      increases the verbosity of messages about what is going on" +
           "\n -V |--version      displays the version of the Griphyn Virtual Data System" +
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
     */
    private void doDeferredPlanning(){
        String mode = mPOptions.getMegaDAGMode();
        mode  = (mode == null)?
                   DEFAULT_MEGADAG_MODE:
                   mode;

        String file = mPOptions.getPDAX();

        //get the name of the directory from the file
        String directory = new File(file).getParent();
        //System.out.println("Directory in which partitioned daxes are " + directory);

        int errorStatus = 1;
        try{
            //load the correct callback handler
            org.griphyn.cPlanner.parser.pdax.Callback c =
                PDAXCallbackFactory.loadInstance(mProps, mPOptions, directory);
            errorStatus = 2;

            //this is a bug. Should not be called. To be corrected by Karan
            UserOptions y = UserOptions.getInstance(mPOptions);

            //start the parsing and let the fun begin
            PDAXParser p = new PDAXParser( file , mProps );
            p.setCallback(c);
            p.startParser(file);
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

                default:
                    //unreachable code
                    message = "Unknown Error " ;
                    break;
            }
            throw new RuntimeException(message, e);
        }

    }


    /**
     * Creates the submit directory for the workflow. This is not thread safe.
     *
     * @param dir     the base directory specified by the user.
     * @param user    the username of the user.
     * @param vogroup the vogroup to which the user belongs to.
     * @param label   the label in the DAX.
     *
     * @return  the directory name created relative to the base directory passed
     *          as input.
     *
     * @throws IOException in case of unable to create submit directory.
     */
    protected String createSubmitDirectory( String dir, String user, String vogroup, String label ) throws IOException {
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


        //get all the files in this directory
        String[] files = base.list( new RunDirectoryFilenameFilter() );
        //find the maximum run directory
        int num, max = 1;
        for( int i = 0; i < files.length ; i++ ){
            num = Integer.parseInt( files[i].substring( SUBMIT_DIRECTORY_PREFIX.length() ) );
            if ( num + 1 > max ){ max = num + 1; }
        }

        //create the directory name
        StringBuffer leaf = new StringBuffer();
        leaf.append( SUBMIT_DIRECTORY_PREFIX ).append( mNumFormatter.format( max ) );

        result.append( leaf.toString() );
        base = new File( base, leaf.toString() );
        mLogger.log( "Directory to be created is " + base.getAbsolutePath(),
                     LogManager.DEBUG_MESSAGE_LEVEL );
        sanityCheck( base );

        return result.toString();
    }


    /**
     * This generates a Vector from a string with the constituents being the
     * words making up the string.The String is comma separated.
     *
     * @param tokString   the string which is tokenized in to the Vector
     *
     * @return Vector
     */
    private Vector generateVector(String tokString){
        Vector vFiles = new Vector();
        int noOfFiles = 0;
        StringTokenizer st = new StringTokenizer(tokString,",");
        while(st.hasMoreElements()){
            noOfFiles++;
            vFiles.addElement(st.nextToken().trim());
        }

        return vFiles;
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
                throw new IOException( "Unable to create base directory " +
                                       dir.getPath() );
            }
        }
    }


}


/**
 * A filename filter for identifying the run directory
 *
 * @author Karan Vahi vahi@isi.edu
 */
class RunDirectoryFilenameFilter implements FilenameFilter {

    /**
     * Store the regular expressions necessary to parse kickstart output files
     */
    private static final String mRegexExpression =
                                     "(" + CPlanner.SUBMIT_DIRECTORY_PREFIX + ")([0-9][0-9][0-9][0-9])";

    /**
     * Stores compiled patterns at first use, quasi-Singleton.
     */
    private static Pattern mPattern = null;



    /***
     * Tests if a specified file should be included in a file list.
     *
     * @param dir the directory in which the file was found.
     * @param name - the name of the file.
     *
     * @return  true if and only if the name should be included in the file list
     *          false otherwise.
     *
     *
     */
     public boolean accept( File dir, String name) {
         //compile the pattern only once.
         if( mPattern == null ){
             mPattern = Pattern.compile( mRegexExpression );
         }
         return mPattern.matcher( name ).matches();
     }


}


