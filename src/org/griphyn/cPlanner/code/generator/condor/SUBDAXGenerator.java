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

package org.griphyn.cPlanner.code.generator.condor;



import edu.isi.pegasus.common.logging.LogManager;

import org.griphyn.cPlanner.classes.ADag;
import org.griphyn.cPlanner.classes.PlannerOptions;
import org.griphyn.cPlanner.classes.PegasusBag;
import org.griphyn.cPlanner.classes.SubInfo;

import org.griphyn.cPlanner.namespace.Dagman;

import org.griphyn.cPlanner.parser.dax.DAXCallbackFactory;
import org.griphyn.cPlanner.parser.dax.Callback;
import org.griphyn.cPlanner.parser.DaxParser;

import org.griphyn.cPlanner.common.PegasusProperties;
import org.griphyn.cPlanner.common.RunDirectoryFilenameFilter;

import org.griphyn.cPlanner.toolkit.CPlanner;

import org.griphyn.common.catalog.TransformationCatalog;
import org.griphyn.common.catalog.TransformationCatalogEntry;
import org.griphyn.common.classes.TCType;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;

import java.text.NumberFormat;
import java.text.DecimalFormat;

import java.util.List;
import java.util.Properties;
import java.util.Map;

/**
 * The class that takes in a dax job specified in the DAX and renders it into
 * a SUBDAG with pegasus-plan as the appropriate prescript.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class SUBDAXGenerator{


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
     * The print writer handle to DAG file being written out.
     */
    private PrintWriter mDAGWriter;
    
    /**
     * The handle to the transformation catalog
     */
    private TransformationCatalog mTCHandle;
    
    /**
     * The cleanup scope for the workflows.
     */
    private PegasusProperties.CLEANUP_SCOPE mCleanupScope;
    
    /**
     * The default constructor.
     */
    public SUBDAXGenerator() {
        mNumFormatter = new DecimalFormat( "0000" );


    }


    /**
     * Initializes the class.
     *
     * @param bag  the bag of objects required for initialization
     */
    public void initialize( PegasusBag bag, PrintWriter dagWriter ){
        mBag = bag;
        mDAGWriter = dagWriter;
        mProps  = bag.getPegasusProperties();
        mLogger = bag.getLogger();
        mTCHandle = bag.getHandleToTransformationCatalog();
        this.mPegasusPlanOptions  = bag.getPlannerOptions();
        mCleanupScope = mProps.getCleanupScope();

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
    public void generateCode( SubInfo job ){
        String arguments = job.getArguments();
        String [] args = arguments.split( " " );
        mLogger.log( "Arguments passed to SUBDAX Generator are " + arguments,
                     LogManager.DEBUG_MESSAGE_LEVEL );
        
        //convert the args to pegasus-plan options
        PlannerOptions options = new CPlanner( mLogger ).parseCommandLineArguments( args, false );

        String submit = options.getSubmitDirectory();
        mLogger.log( "Submit directory in sub dax specified is " + submit,
                     LogManager.DEBUG_MESSAGE_LEVEL );

        if( submit == null || !submit.startsWith( File.separator ) ){
            //then set the submit directory relative to the parent workflow basedir
            String innerBase     = mPegasusPlanOptions.getBaseSubmitDirectory();
            String innerRelative = mPegasusPlanOptions.getRelativeSubmitDirectory();
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

        //do some sanitization of the path to the dax file.
        //if it is a relative path, then ???
        options.setSanitizePath( true );
        
        //retrieve the metadata in the subdax.
        //means the the dax needs to be generated beforehand.
        Map metadata = getDAXMetadata( options.getDAX() ); 
       
        String label = (String) metadata.get( "name" );
        String index = (String) metadata.get( "index" );

        String baseDir = options.getBaseSubmitDirectory();
        String relativeDir = null;
        //construct the submit directory structure for subdax
        try{
            relativeDir = (options.getRelativeSubmitDirectory() == null) ?
                                 //create our own relative dir
                                 createSubmitDirectory(label,
                                                       baseDir,
                                                       mUser,
                                                       options.getVOGroup(),
                                                       mProps.useTimestampForDirectoryStructure()) :
                                 options.getRelativeSubmitDirectory();
        }
        catch( IOException ioe ){
            String error = "Unable to write  to directory" ;
            throw new RuntimeException( error + options.getSubmitDirectory() , ioe );

        }

        options.setSubmitDirectory( baseDir, relativeDir  );
        mLogger.log( "Submit Directory for SUB DAX  is " + options.getSubmitDirectory() , LogManager.DEBUG_MESSAGE_LEVEL );

        //create a symbolic link to dax in the subdax submit directory
        String linkedDAX = createSymbolicLinktoDAX( options.getSubmitDirectory(),
                                                    options.getDAX() );
        
        //update options with the linked dax
        options.setDAX( linkedDAX );
        
        //write out the properties in the submit directory
        String propertiesFile = null;
        try{
            propertiesFile = this.writeOutProperties( options.getSubmitDirectory() );
        }
        catch( IOException ioe ){
            throw new RuntimeException( "Unable to write out properties to directory " + options.getSubmitDirectory() );
        }
        
        //construct  the pegasus-plan prescript for the JOB
        //the log file for the prescript should be in the
        //submit directory of the outer level workflow
        StringBuffer log = new StringBuffer();
        log.append( mPegasusPlanOptions.getSubmitDirectory() ).append( File.separator ).
            append( job.getName() ).append( ".pre.log" );
        String prescript = constructPegasusPlanPrescript( job, options, propertiesFile, log.toString() );
        job.setPreScript( prescript );
        
        //determine the path to the dag file that will be constructed.
        StringBuffer dag = new StringBuffer();
        dag.append( options.getSubmitDirectory() ).append( File.separator ).
            append( CondorGenerator.getDAGFilename( options, label, index, ".dag") );
        
        //print out the SUBDAG keyword for the job
        StringBuffer sb = new StringBuffer();
        sb.append( Dagman.SUBDAG_EXTERNAL_KEY ).append( " " ).append( job.getName() ).
                   append( " " ).append( dag.toString() );
        mDAGWriter.println( sb.toString() );
        

    }


    /**
     * Constructs the pegasus plan prescript for the subdax
     * 
     * @param job        the subdax job
     * @param options    the planner options with which subdax has to be invoked
     * @param properties the properties file.
     * @param log        the log for the prescript output
     * 
     * @return  the prescript
     */
    public String constructPegasusPlanPrescript( SubInfo job,
                                                 PlannerOptions options,
                                                 String properties,
                                                 String log ){
        StringBuffer prescript = new StringBuffer();

    
        String site = job.getSiteHandle();
        TransformationCatalogEntry entry = null;

        //get the path to script wrapper from the
        try{
            List entries = mTCHandle.getTCEntries( "pegasus",
                                                   "pegasus-plan",
                                                   null,
                                                   "local",
                                                   TCType.INSTALLED);

            //get the first entry from the list returned
            entry = ( entries == null ) ?
                     null :
                     //Gaurang assures that if no record is found then
                     //TC Mechanism returns null
                     ((TransformationCatalogEntry) entries.get(0));
        }
        catch(Exception e){
            throw new RuntimeException( "ERROR: While accessing the Transformation Catalog",e);
        }

        //construct the prescript path
        StringBuffer script = new StringBuffer();
        if(entry == null){
            //log to debug
            mLogger.log("Constructing the default path to the pegasus-plan",
                        LogManager.DEBUG_MESSAGE_LEVEL);

            //construct the default path to the executable
            script.append( mProps.getPegasusHome()).append( File.separator ).
                   append( "bin" ).append( File.separator ).
                   append( "pegasus-plan" );
        }
        else{
            script.append(entry.getPhysicalTransformation());
        }


        //set the flag designating that the planning invocation is part
        //of a deferred planning run
        options.setPartOfDeferredRun( true );

        //in case of deferred planning cleanup wont work
        //explicitly turn it off if the file cleanup scope if fullahead
        if( mCleanupScope.equals( PegasusProperties.CLEANUP_SCOPE.fullahead ) ){
            options.setCleanup( false );
        }

        //we want monitoring to happen
        options.setMonitoring( true );

        //construct the argument string.
        //add the jvm options and the pegasus options if any
        StringBuffer arguments = new StringBuffer();
        arguments./*append( mPOptions.toJVMOptions())*/
                  append(" -Dpegasus.user.properties=").append( properties ).
                  append( " -Dpegasus.log.*=").append(log).
                  //add other jvm options that user may have specified
                  append( options.toJVMOptions() ).
                  //put in all the other options.
                  append( options.toOptions());
        
        //add the --dax option explicitly in the end
        arguments.append( " --dax " ).append( options.getDAX() );

        prescript.append( script ).append( " " ).append( arguments );
        
        return prescript.toString();
    }
    
    /**
     * Creates a symbolic link to the DAX file in a dax sub directory in the 
     * submit directory 
     * 
     * 
     * @param submitDirectory    the submit directory for the sub workflow.
     * @param dax                the dax file to which the symbolic link has
     *                           to be created.
     * 
     * @return  the symbolic link created.
     */
    public String createSymbolicLinktoDAX( String submitDirectory , String dax ){
        File dir = new File( submitDirectory, "dax" );
        
        //create a symbolic in the dax subdirectory to the
        //dax file specified in the sub dax
                
        //create the dir if it does not exist
        try{
            sanityCheck( dir );
        }
        catch( IOException ioe ){
            throw new RuntimeException( "Unable to create the submit directory for sub dax " + dir );
        }
                
        //we have the partition written out
        //now create a symlink to the DAX file
        StringBuffer destinationDAX = new StringBuffer();
        destinationDAX.append( dir ).append( File.separator ).
                       append( new File(dax).getName() );
        
        if ( !createSymbolicLink( dax , destinationDAX.toString() ) ){
                throw new RuntimeException( "Unable to create symbolic link between " +
                                                dax + " and " + destinationDAX.toString() );
        }    
        
        return destinationDAX.toString();
    }
    
    /**
     * Returns the metadata stored in the root adag element in the DAX
     * 
     * @param dax   the dax file.
     * 
     * @return Map containing the metadata.
     */
    public Map getDAXMetadata( String dax ){
        Callback cb =  DAXCallbackFactory.loadInstance( mProps, dax, "DAX2Metadata" );

        try{
            DaxParser daxParser = new DaxParser( dax, mBag, cb);
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

        return ( Map ) cb.getConstructedObject();
        
    }
    
    
    /**
     * Writes out the properties to a temporary file in the directory passed.
     *
     * @param directory   the directory in which the properties file needs to
     *                    be written to.
     *
     * @return the absolute path to the properties file written in the directory.
     *
     * @throws IOException in case of error while writing out file.
     */
    protected String writeOutProperties( String directory ) throws IOException{
        File dir = new File(directory);

        //sanity check on the directory
        sanityCheck( dir );

        //we only want to write out the VDS properties for time being
        Properties properties = mProps.matchingSubset( "pegasus", true );

        //create a temporary file in directory
        File f = File.createTempFile( "pegasus.", ".properties", dir );

        //the header of the file
        StringBuffer header = new StringBuffer(64);
        header.append("PEGASUS USER PROPERTIES AT RUNTIME \n")
              .append("#ESCAPES IN VALUES ARE INTRODUCED");

        //create an output stream to this file and write out the properties
        OutputStream os = new FileOutputStream(f);
        properties.store( os, header.toString() );
        os.close();

        return f.getAbsolutePath();
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
