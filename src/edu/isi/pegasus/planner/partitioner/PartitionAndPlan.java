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

package edu.isi.pegasus.planner.partitioner;

import edu.isi.pegasus.common.logging.LoggingKeys;

import edu.isi.pegasus.planner.parser.dax.DAXCallbackFactory;
import edu.isi.pegasus.planner.parser.dax.Callback;

import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.PlannerOptions;
import edu.isi.pegasus.planner.classes.PegasusBag;

import edu.isi.pegasus.common.logging.LogManager;
import java.io.BufferedReader;
import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.planner.common.RunDirectoryFilenameFilter;

import edu.isi.pegasus.planner.parser.DaxParser;


import edu.isi.pegasus.planner.client.CPlanner;
import edu.isi.pegasus.planner.client.PartitionDAX;

import java.util.Map;

import java.io.File;
import java.io.IOException;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.NumberFormat;
import java.text.DecimalFormat;
import java.util.Collection;



/**
 * The class that triggers the partition and plan structure in pegasus.
 *
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class PartitionAndPlan{


    /**
     * The username of the user running the program.
     */
    private String mUser;

    /**
     * The number formatter to format the run submit dir entries.
     */
    private NumberFormat mNumFormatter;

    /**
     * The object containing all the options passed to the Concrete Planner.
     */
    private PlannerOptions mPegasusPlanOptions;

    /**
     * The handle to Pegasus Properties.
     */
    private PegasusProperties mProps;

    /**
     * Handle to the logging manager.
     */
    private LogManager mLogger;

    /**
     * Bag of Pegasus objects
     */
    private PegasusBag mBag;

    /**
     * The default constructor.
     */
    public PartitionAndPlan() {
        mNumFormatter = new DecimalFormat( "0000" );


    }


    /**
     * Initializes the class.
     *
     * @param bag  the bag of objects required for initialization
     */
    public void initialize( PegasusBag bag ){
        mBag = bag;
        mProps  = bag.getPegasusProperties();
        mLogger = bag.getLogger();
        this.mPegasusPlanOptions  = bag.getPlannerOptions();

        mUser = mProps.getProperty( "user.name" ) ;
        if ( mUser == null ){ mUser = "user"; }

        //hardcoded options for time being.
        mPegasusPlanOptions.setPartitioningType( "Whole" );

    }


    /**
     * This function is passed command line arguments. In this function you
     * generate the valid options and parse the options specified at run time.
     *
     * @param arguments  the arguments passed at runtime
     *
     * @return the Collection of <code>File</code> objects for the files written
     *         out.
     */
    public Collection<File> doPartitionAndPlan( String arguments ){
        String [] args = arguments.split( " " );
        mLogger.log( "Arguments passed to Partition and Plan are " + arguments,
                     LogManager.DEBUG_MESSAGE_LEVEL );
        
        //convert the args to pegasus-plan options
        PlannerOptions options = new CPlanner().parseCommandLineArguments( args, false );

        String submit = options.getSubmitDirectory();
        mLogger.log( "Submit directory in dax specified is " + submit,
                     LogManager.DEBUG_MESSAGE_LEVEL );

        if( submit == null || !submit.startsWith( File.separator ) ){
            //then set the submit directory relative to the parent workflow basedir
            String innerBase     = mPegasusPlanOptions.getBaseSubmitDirectory();
            String innerRelative = mPegasusPlanOptions.getRelativeDirectory();
            innerRelative = ( innerRelative == null && mPegasusPlanOptions.partOfDeferredRun() )?
                             mPegasusPlanOptions.getRandomDir(): //the random dir is the relative submit dir?
                             innerRelative;
            
            //FIX for JIRA bug 65 to ensure innerRelative is resolved correctly
            //in case of innerRelative being ./ . We dont want inner relative
            //to compute to .// Instead we want it to compute to ././
            //innerRelative += File.separator + submit  ;
            innerRelative =  new File( innerRelative, submit ).getPath();
            
            //options.setSubmitDirectory( mPegasusPlanOptions.getSubmitDirectory(), submit );
            options.setSubmitDirectory( innerBase, innerRelative );
            mLogger.log( "Base Submit directory for inner workflow set to " + innerBase,
                         LogManager.DEBUG_MESSAGE_LEVEL );
            mLogger.log( "Relative Submit Directory for inner workflow set to " + innerRelative,
                         LogManager.DEBUG_MESSAGE_LEVEL );
            mLogger.log( "Submit directory for inner workflow set to " + options.getSubmitDirectory(),
                         LogManager.DEBUG_MESSAGE_LEVEL );
        }

        if( options.getExecutionSites().isEmpty() ){
            //for JIRA feature request PM-64
            //no sites are specified. use the execution sites for
            //the parent workflow
            mLogger.log( "Setting list of execution sites to the same as outer workflow",
                         LogManager.DEBUG_MESSAGE_LEVEL );
            options.getExecutionSites().addAll( mPegasusPlanOptions.getExecutionSites() );
        }

        options.setPartitioningType( "Whole" );

        //do some sanitization of the path to the dax file.
        //if it is a relative path, then ???

        //
        options.setSanitizePath( true );
        return this.doPartitionAndPlan( mProps, options );

    }

    /**
     * Partitions and plans the workflow. First step of merging DAGMan and
     * Condor
     *
     * @param properties   the properties passed to the planner.
     * @param options      the options passed to the planner.
     *
     * @return the Collection of <code>File</code> objects for the files written
     *         out.
     */
    public Collection<File> doPartitionAndPlan( PegasusProperties properties, PlannerOptions options ) {
        //we first need to get the label of DAX
        Callback cb =  DAXCallbackFactory.loadInstance( properties, options.getDAX(), "DAX2Metadata" );

        try{
            DaxParser daxParser = new DaxParser(options.getDAX(), mBag, cb);
        }
        catch( RuntimeException e ){
            //check explicity for file not found exception
            if( e.getCause() != null && e.getCause() instanceof java.io.IOException){
                //rethrow 
                throw e;
            }
            
            //ignore only if the parsing is completed
            mLogger.log( e.getMessage(), LogManager.DEBUG_MESSAGE_LEVEL );
        }

        Map metadata = ( Map ) cb.getConstructedObject();
        String label = (String) metadata.get( "name" );

        String baseDir = options.getBaseSubmitDirectory();
        String relativeDir = null;
        //construct the submit directory structure
        try{
            relativeDir = (options.getRelativeDirectory() == null) ?
                                 //create our own relative dir
                                 createSubmitDirectory(label,
                                                       baseDir,
                                                       mUser,
                                                       options.getVOGroup(),
                                                       properties.useTimestampForDirectoryStructure()) :
                                 options.getRelativeDirectory();
        }
        catch( IOException ioe ){
            String error = "Unable to write  to directory" ;
            throw new RuntimeException( error + options.getSubmitDirectory() , ioe );

        }

        options.setSubmitDirectory( baseDir, relativeDir  );
        mLogger.log( "Submit Directory for workflow is " + options.getSubmitDirectory() , LogManager.DEBUG_MESSAGE_LEVEL );

        //now let us run partitiondax
        mLogger.logEventStart( LoggingKeys.EVENT_PEGASUS_PARTITION, LoggingKeys.DAX_ID, label );
        PartitionDAX partitionDAX = new PartitionDAX();
        File dir = new File( options.getSubmitDirectory(), "dax" );
        
        String pdax = null;
        
        //bypass partitioning for time being
        //as the dax is already created
        boolean partitionWorkflow = false;
        
        if( partitionWorkflow ){
            pdax = partitionDAX.partitionDAX(
                                              properties,
                                              options.getDAX(),
                                              dir.getAbsolutePath(),
                                              options.getPartitioningType() );
        }
        else{
            try {
                //create a shallow pdax file and a symbolic link
                //to the dax in the dax directory referred to by the dir variable
                String dax = options.getDAX();
                String daxBasename = new File(dax).getName();
                WriterCallback callback = new WriterCallback();
                
                //create the dir if it does not exist
                sanityCheck( dir );
                
                callback.initialize(properties, dax, daxBasename, dir.getAbsolutePath());
                pdax = callback.getPDAX();
                PDAXWriter writer = callback.getHandletoPDAXWriter(dax, label, dir.getAbsolutePath());
                writer.writeHeader();
                
                //create an empty Partition
                Partition p = new Partition();
                String id = "1";
                String partitionLabel = "partition_" + label;
                p.setName(partitionLabel);
                p.setIndex( Integer.parseInt( id ) );
                p.setID( id );
                p.constructPartition();
                
                //write out the partition and close
                writer.write(p);
                writer.close();
                
                //we have the partition written out
                //now create a symlink to the DAX file
                // partition_blackdiamond_1.dax
                StringBuffer destinationDAX = new StringBuffer();
                destinationDAX.append( dir ).append( File.separator ).
                               append( "partition_" ).append( label ).
                               append( "_" ).append( id ).append( ".dax" );
                if ( !createSymbolicLink( dax , destinationDAX.toString() ) ){
                    throw new RuntimeException( "Unable to create symbolic link between " +
                                                dax + " and " + destinationDAX.toString() );
                }
            } catch ( IOException ioe ) {
                String error = "Unable to generate the pdax file" ;
                throw new RuntimeException( error + options.getSubmitDirectory() , ioe );
            }
        }

        mLogger.log( "PDAX file generated is " + pdax , LogManager.DEBUG_MESSAGE_LEVEL );
        mLogger.logEventCompletion();

        //now run pegasus-plan with pdax option
        CPlanner pegasusPlan = new CPlanner();
        options.setDAX( null );
        options.setPDAX( pdax );
        options.setPartitioningType( null );

        return pegasusPlan.executeCommand( options );

        //we still need to create the condor submit file for submitting
        //the outer level dag created by pap

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
            leaf.append( mPegasusPlanOptions.getDateTime( mProps.useExtendedTimeStamp() ) );
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
                //try to get around JVM bug. JIRA PM-91
                if( dir.getPath().endsWith( "." ) ){
                    //just try to create the parent directory
                    if( !dir.getParentFile().mkdirs() ){
                        //tried everything and failed
                        throw new IOException( "Unable to create  directory " +
                                       dir.getPath() );
                    }
                    return;
                }


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
     * This method generates a symlink between two files
     *
     * @param source       the file that has to be symlinked 
     * @param destination  the destination of the symlink
     * 
     * @return boolean indicating if creation of symlink was successful or not
     */
    protected boolean createSymbolicLink( String source, String destination ) {
        try{
            Runtime rt = Runtime.getRuntime();
            String command = "ln -sf " + source + " " + destination;
            mLogger.log( "Creating symlink between " + source + " " + destination,
                         LogManager.DEBUG_MESSAGE_LEVEL);
            Process p = rt.exec( command, null );

            // set up to read subprogram output
            InputStream is = p.getInputStream();
            InputStreamReader isr = new InputStreamReader(is);
            BufferedReader br = new BufferedReader(isr);

            // set up to read subprogram error
            InputStream er = p.getErrorStream();
            InputStreamReader err = new InputStreamReader(er);
            BufferedReader ebr = new BufferedReader(err);

            // read output from subprogram
            // and display it

            String s,se=null;
            while ( ((s = br.readLine()) != null) || ((se = ebr.readLine()) != null ) ) {
               if(s!=null){
                   mLogger.log(s,LogManager.DEBUG_MESSAGE_LEVEL);
               }
               else {
                   mLogger.log(se,LogManager.ERROR_MESSAGE_LEVEL );
               }
            }

            br.close();
            return true;
        }
        catch(Exception ex){
            mLogger.log("Unable to create symlink to the log file" , ex,
                        LogManager.ERROR_MESSAGE_LEVEL);
            return false;
       }

    }




}
