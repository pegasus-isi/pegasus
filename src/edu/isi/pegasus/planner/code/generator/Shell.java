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

import edu.isi.pegasus.planner.catalog.site.classes.SiteCatalogEntry;
import edu.isi.pegasus.planner.catalog.site.classes.SiteStore;

import org.griphyn.cPlanner.classes.ADag;
import org.griphyn.cPlanner.classes.SiteInfo;
import org.griphyn.cPlanner.classes.SubInfo;
import org.griphyn.cPlanner.classes.PegasusBag;

import edu.isi.pegasus.planner.code.CodeGeneratorException;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.code.GridStart;
import edu.isi.pegasus.planner.code.GridStartFactory;
import org.griphyn.cPlanner.common.PegasusProperties;

import org.griphyn.cPlanner.namespace.ENV;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;

import java.util.Collection;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.Vector;
import org.griphyn.cPlanner.classes.Profile;
import org.griphyn.cPlanner.namespace.Condor;
import org.griphyn.cPlanner.partitioner.graph.Adapter;
import org.griphyn.cPlanner.partitioner.graph.Graph;
import org.griphyn.cPlanner.partitioner.graph.GraphNode;

/**
 * This code generator generates a shell script in the submit directory.
 * The shell script can be executed on the submit host to run the workflow
 * locally.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class Shell extends Abstract {

    /**
     * The "official" namespace URI of the GRMS workflow schema.
     */
    public static final String SCHEMA_NAMESPACE = "";

    /**
     * The "not-so-official" location URL of the GRMS workflow schema definition.
     */
    public static final String SCHEMA_LOCATION = "";

    /**
     * The workflow schema to which this writer conforms.
     */
    public static final String SCHEMA = "grms-workflow-schema_10.xsd";

    /**
     * The prefix that needs to be added to the stdout to make GRMS aware of
     * a kickstart output.
     */
    public static final String STDOUT_PREFIX = "kickstart__exitcode__of__";

    /**
     * The version to report.
     */
    public static final String SCHEMA_VERSION = "10";


    /**
     * The LogManager object which is used to log all the messages.
     */
    private LogManager mLogger;

    /**
     * The handle to the output file that is being written to.
     */
    private PrintWriter mWriteHandle;

    /**
     * The handle to the properties file.
     */
    private PegasusProperties mProps;

    /**
     * Handle to the pool provider.
     */
    //private PoolInfoProvider mPoolHandle;
    
    /**
     * Handle to the Site Store.
     */
    private SiteStore mSiteStore;

    /**
     * The handle to the GridStart Factory.
     */
    protected GridStartFactory mGridStartFactory;

    /**
     * A boolean indicating whether grid start has been initialized or not.
     */
    protected boolean mInitializeGridStart;

    /**
     * The default constructor.
     */
    public Shell( ){
        super();
        mInitializeGridStart = true;
        mGridStartFactory = new GridStartFactory();
    }

    /**
     * Initializes the Code Generator implementation.
     *
     *  @param bag   the bag of initialization objects.
     *
     * @throws CodeGeneratorException in case of any error occuring code generation.
     */
    public void initialize( PegasusBag bag ) throws CodeGeneratorException{
        super.initialize( bag );
        mLogger = bag.getLogger();

        //create the base directory recovery
        File wdir = new File(mSubmitFileDir);
        wdir.mkdirs();

        //get the handle to pool file
        mSiteStore = bag.getHandleToSiteStore();

    }

    /**
     * Generates the code for the concrete workflow in the GRMS input format.
     * The GRMS input format is xml based. One XML file is generated per
     * workflow.
     *
     * @param dag  the concrete workflow.
     *
     * @return handle to the GRMS output file.
     *
     * @throws CodeGeneratorException in case of any error occuring code generation.
     */
    public Collection<File> generateCode( ADag dag ) throws CodeGeneratorException{
        String opFileName = this.mSubmitFileDir + File.separator +
                            dag.dagInfo.nameOfADag + ".sh";


        initializeWriteHandle( opFileName );
        Collection result = new ArrayList( 1 );
        result.add( new File( opFileName ) );
        

        //write out the script header
        writeString(this.getScriptHeader( mSubmitFileDir ) );

        //we first need to convert internally into graph format
        Graph workflow =    Adapter.convert( dag );

        //traverse the workflow in topological sort order
        for( Iterator<GraphNode> it = workflow.topologicalSortIterator(); it.hasNext(); ){
            GraphNode node = it.next();
            SubInfo job = (SubInfo)node.getContent();
            generateCode( dag, job );
        }

        //write out the footer
        writeString(this.getScriptFooter());
        mWriteHandle.close();

        return result;
    }

    /**
     * Generates the code for a single job in the input format of the workflow
     * executor being used.
     *
     * @param dag    the dag of which the job is a part of.
     * @param job    the <code>SubInfo</code> object holding the information about
     *               that particular job.
     *
     * @throws CodeGeneratorException in case of any error occuring code generation.
     */
    public void generateCode( ADag dag, SubInfo job ) throws CodeGeneratorException{
        mLogger.log( "Generating code for job " + job.getID() , LogManager.DEBUG_MESSAGE_LEVEL );


        //sanity check
        if( !job.getSiteHandle().equals( "local" ) ){
            throw new CodeGeneratorException( "Shell Code generator only works for jobs scheduled to site local" );
        }

        //initialize GridStart if required.
        if ( mInitializeGridStart ){
            mGridStartFactory.initialize( mBag, dag );
            mInitializeGridStart = false;
        }

        //determine the work directory for the job
        String execDir = getExecutionDirectory( job );
        
        //for local jobs we need initialdir
        //instead of remote_initialdir
        job.condorVariables.construct("initialdir", execDir );
        job.condorVariables.construct( "universe", "local" );

        SiteCatalogEntry site = mSiteStore.lookup( job.getSiteHandle() );
        String gridStartPath = site.getKickstartPath();
        GridStart gridStart = mGridStartFactory.loadGridStart( job , gridStartPath );

        //enable the job
        if( !gridStart.enable( job,false ) ){
            String msg = "Job " +  job.getName() + " cannot be enabled by " +
                         gridStart.shortDescribe() + " to run at " +
                         job.getSiteHandle();
            mLogger.log( msg, LogManager.FATAL_MESSAGE_LEVEL );
            throw new CodeGeneratorException( msg );
        }

        //gridstart modules right now store the executable
        //and arguments as condor profiles. Should be fixed.
        //Should only happen in Condor Generator
        String executable = (String) job.condorVariables.get( "executable" );
        String arguments = (String)job.condorVariables.get( Condor.ARGUMENTS_KEY );
        arguments = ( arguments == null ) ? "" : arguments;

        
        StringBuffer sb = new StringBuffer();

        //generate the call to execute job function
        //execute_job $jobstate test1 /tmp /bin/echo "Karan Vahi" "stdin file" "k=v" "g=m"
        sb.append( "execute_job" ).append( " " ).
           append( job.getID() ).append( " " ).
           append( execDir ).append( " " ).
           append( executable ).append( " " ).
           append( "\"" ).append( arguments ).append( "\"" ).append( " " );


        //handle stdin for jobs
        String stdin = job.getStdIn();
        if( stdin == null || stdin.length() == 0 ){
            sb.append( "\"\"" );
        }
        else{
            if( stdin.startsWith( File.separator ) ){
                sb.append( stdin );
            }
            else{
                sb.append( this.mSubmitFileDir ).append( File.separator ).append( stdin );
            }
        }
        sb.append( " " );

        
        //add the environment variables
        for( Iterator it = job.envVariables.getProfileKeyIterator(); it.hasNext(); ){
            String key = (String)it.next();
            sb.append( "\"" ).
               append( key ).append( "=" ).append( job.envVariables.get( key ) ).
               append( "\"" ).append( " " );
        }

        sb.append( "\n" );

        //generate the call to check_exitcode
        //check_exitcode $jobstate test1 $?
        sb.append( "check_exitcode" ).append( " " ).
           append(  job.getID() ).append( " " ).
           append(  "$?" );
        sb.append( "\n" );

        writeString( sb.toString() );
    }

    
 
    /**
     * Returns the header for the generated shell script. The header contains
     * the code block that sources the common plan script from $PEGASUS_HOME/bin
     * and initializes the jobstate.log file.
     *
     * @param submitDirectory       the submit directory for the workflow.
     *
     * @return the script header
     */
    protected String getScriptHeader( String submitDirectory ){

        StringBuffer sb = new StringBuffer();
        sb.append( "#!/bin/bash" ).append( "\n" ).
           append( "#" ).append( "\n" ).
           append( "# executes the workflow in shell mode " ).append( "\n" ).
           append( "#" ).append( "\n" ).
           append( "\n");

        //check for PEGASUS_HOME
        sb.append( "if [ \"X${PEGASUS_HOME}\" = \"X\" ]; then" ).append( "\n" ).
           append( "   echo \"ERROR: Set your PEGASUS_HOME variable\" 1>&2").append( "\n" ).
           append( "   exit 1" ).append( "\n" ).
           append( "fi" ).append( "\n" ).
           append( "\n" );


        //source the common shell script
        sb.append( ".  ${PEGASUS_HOME}/bin/common-sh-plan.sh" ).append( "\n" ).
           append( "" ).append( "\n" );

        sb.append( "PEGASUS_SUBMIT_DIR" ).append( "=" ).append( submitDirectory ).append( "\n" );


        sb.append( "#initialize jobstate.log file" ).append( "\n" ).
           append( "JOBSTATE_LOG=jobstate.log" ).append( "\n" ).
           append( "touch $JOBSTATE_LOG" ).append( "\n" ).
           append( "echo \"INTERNAL *** SHELL_SCRIPT_STARTED ***\" >> $JOBSTATE_LOG" ).append( "\n" );

        return sb.toString();

    }

    /**
     * Returns the footer for the generated shell script.
     *
     * @return the script footer.
     */
    protected String getScriptFooter(){

        StringBuffer sb = new StringBuffer();
        sb.append( "echo \"INTERNAL *** SHELL_SCRIPT_FINISHED 0 ***\" >> $JOBSTATE_LOG" );

        return sb.toString();

    }

    /**
     * It initializes the write handle to the output file.
     *
     * @param filename  the name of the file to which you want the write handle.
     */
    private void initializeWriteHandle(String filename) throws CodeGeneratorException{
        try {
            File f = new File( filename );
            mWriteHandle = new PrintWriter(new FileWriter( f ));
            //set the xbit for all users
            f.setExecutable( true , false );
            
            mLogger.log("Writing to file " + filename , LogManager.DEBUG_MESSAGE_LEVEL);
        }
        catch (Exception e) {
            throw new CodeGeneratorException( "Unable to initialize file handle for shell script ", e );
            
        }

        
    }

    /**
     * Writes a string to the associated write handle with the class
     *
     * @param st  the string to be written.
     */
    protected void writeString(String st){
        //try{
            //write the xml header
            mWriteHandle.println(st);
        /*}
        catch(IOException ex){
            System.out.println("Error while writing to xml " + ex.getMessage());
        }*/
    }

    /**
     * Returns the directory in which a job should be executed.
     *
     * @param job  the job.
     *
     * @return  the directory
     */
    protected String getExecutionDirectory(SubInfo job) {
       String execSiteWorkDir = mSiteStore.getWorkDirectory(job);
       String workdir = (String) job.globusRSL.removeKey("directory"); // returns old value
       workdir = (workdir == null)?execSiteWorkDir:workdir;

       return workdir;
    }

}
