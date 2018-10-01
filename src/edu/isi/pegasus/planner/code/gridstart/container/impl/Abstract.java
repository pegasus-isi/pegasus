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
package edu.isi.pegasus.planner.code.gridstart.container.impl;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.DAGJob;
import edu.isi.pegasus.planner.classes.DAXJob;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.PlannerOptions;
import edu.isi.pegasus.planner.code.gridstart.Integrity;
import edu.isi.pegasus.planner.code.gridstart.container.ContainerShellWrapper;
import edu.isi.pegasus.planner.common.PegasusProperties;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

/**
 *
 * @author Karan Vahi
 */
public abstract class Abstract implements ContainerShellWrapper{
    
    public static final String SEPARATOR = "########################";
    public static final char SEPARATOR_CHAR = '#';
    public static final int  MESSAGE_STRING_LENGTH = 80;
    
    
    public static final String  PEGASUS_LITE_MESSAGE_PREFIX = "[Pegasus Lite]";
    
    public static final String  CONTAINER_MESSAGE_PREFIX = "[Container]";
    
    /**
     * The LogManager object which is used to log all the messages.
     */
    protected LogManager mLogger;

    /**
     * The object holding all the properties pertaining to Pegasus.
     */
    protected PegasusProperties mProps;

    /**
     * The submit directory where the submit files are being generated for
     * the workflow.
     */
    protected String mSubmitDir;
    
    /**
     * the planner options.
     */
    protected PlannerOptions mPOptions;
    
    
    
    /**
     * Whether to do integrity checking or not.
     */
    protected boolean mDoIntegrityChecking ;
    
    private Integrity mIntegrityHandler;
    
    /**
     * Appends a fragment to the pegasus lite script that logs a message to
     * stderr
     * 
     * @param sb       string buffer
     * @param prefix
     * @param message  the message  
     */
    protected static void appendStderrFragment(StringBuilder sb, String prefix, String message ) {
        //prefix + 1 + message
        int len = prefix.length() + 1 + message.length();
        if( len > Abstract.MESSAGE_STRING_LENGTH ){
            throw new RuntimeException( "Message string for ContainerShellWrapper exceedss " + Abstract.MESSAGE_STRING_LENGTH + " characters");
        }
        
        int pad = ( Abstract.MESSAGE_STRING_LENGTH - len )/2;
        sb.append( "echo -e \"\\n" );
        for( int i = 0; i <= pad ; i ++ ){
            sb.append( Abstract.SEPARATOR_CHAR );
        }
        sb.append( prefix ).append( " " ).append( message ).append( " " );
        for( int i = 0; i <= pad ; i ++ ){
            sb.append( Abstract.SEPARATOR_CHAR );
        }
        sb.append( "\"  1>&2").append( "\n" );
        
    }
    
    public Abstract(){
        
    }
    
    
    /**
     * Initiailizes the Container  shell wrapper
     * @param bag 
     * @param dag 
     */
    public void initialize( PegasusBag bag, ADag dag ){
        mLogger    = bag.getLogger();
        mProps     = bag.getPegasusProperties();
        mPOptions  = bag.getPlannerOptions();
        mSubmitDir = mPOptions.getSubmitDirectory();
        mDoIntegrityChecking  = mProps.doIntegrityChecking();
        mIntegrityHandler = new Integrity();
        mIntegrityHandler.initialize(bag, dag);
    }
    
    /**
     * Enables a job for integrity checking 
     * 
     * @param job
     * @return 
     */
    protected StringBuilder enableForIntegrity( Job job ){
        StringBuilder sb = new StringBuilder();
        boolean isCompute = job.getJobType() == Job.COMPUTE_JOB;
        //PM-1190 we do integrity checks only for compute jobs
        if( mDoIntegrityChecking && isCompute){
            //we cannot enable integrity checking for DAX or dag jobs
            //as the prescript is not run as a full condor job
            if( !(job instanceof DAXJob || job instanceof DAGJob) ){
                appendStderrFragment( sb, "", "Checking file integrity for input files" );
                sb.append( "# do file integrity checks" ).append( '\n' );
                mIntegrityHandler.addIntegrityCheckInvocation( sb,  job.getInputFiles() );

                //check if planner knows of any checksums from the replica catalog
                //and generate an input meta file!
                File metaFile = mIntegrityHandler.generateChecksumMetadataFile( job.getFileFullPath( mSubmitDir,  ".in.meta"),
                                                              job.getInputFiles() );

                //modify job for transferring the .meta files
                if( !mIntegrityHandler.modifyJobForIntegrityChecks( job , metaFile, this.mSubmitDir )) {
                    throw new RuntimeException( "Unable to modify job for integrity checks" );
                }
                sb.append( "\n" );
            }
        }
        
        return sb;
    }
    
    
    /**
     * Convenience method to slurp in contents of a file into memory.
     *
     * @param directory  the directory where the file resides
     * @param file    the file to be slurped in.
     * 
     * @return StringBuffer containing the contents
     */
    protected StringBuffer slurpInFile( String directory, String file ) throws  IOException{
        StringBuffer result = new StringBuffer();
        //sanity check
        if( file == null ){
            return result;
        }

        BufferedReader in = new BufferedReader( new FileReader( new File(  directory, file )) );

        String line = null;

        while(( line = in.readLine() ) != null ){
            //System.out.println( line );
            result.append( line ).append( '\n' );
        }

        in.close();


        return result;
    }
    
}
