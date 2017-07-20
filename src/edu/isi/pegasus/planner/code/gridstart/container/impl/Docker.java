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

import edu.isi.pegasus.planner.catalog.transformation.classes.Container;
import edu.isi.pegasus.planner.classes.AggregatedJob;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusBag;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

/**
 * An interface to determine how a job gets wrapped to be launched on various 
 * containers, as a shell-script snippet that can be embedded in PegasusLite
 *
 * @author vahi
 */
public class Docker extends Abstract{

    /**
     * The suffix for the shell script created on the remote worker node, that
     * actually launches the job in the container.
     */
    public static final String CONTAINER_JOB_LAUNCH_SCRIPT_SUFFIX = "-cont.sh";
    
    /**
     * The directory in the container to be used as working directory 
     */
    public static final String CONTAINER_WORKING_DIRECTORY = "/scratch";
    
    private static String WORKER_PACKAGE_SETUP_SNIPPET = null; 
     
    
    /**
     * Initiailizes the Container  shell wrapper
     * @param bag 
     */
    public void initialize( PegasusBag bag ){
        super.initialize(bag);
    }
    
    /**
     * Returns the snippet to wrap a single job execution
     * In this implementation we don't wrap with any container, just plain
     * shell invocation is returned.
     * 
     * @param job
     * 
     * @return 
     */
    public String wrap( Job job ){
        StringBuilder sb = new StringBuilder();
        
        sb.append( "set -e" ).append( "\n" );
        
        //within the pegasus lite script create a wrapper
        //to launch job in the container. wrapper is required to
        //deploy pegasus worker package in the container and launch the user job
        String scriptName = job.getID() + Docker.CONTAINER_JOB_LAUNCH_SCRIPT_SUFFIX;
        sb.append( constructJobLaunchScriptInContainer( job, scriptName ) );
        
        sb.append( "chmod +x " ).append( scriptName ).append( "\n" );

        //copy pegasus lite common from the directory where condor transferred it via it's file transfer.
        sb.append( "if ! [ $pegasus_lite_start_dir -ef . ]; then").append( "\n" );
        sb.append( "\tcp $pegasus_lite_start_dir/pegasus-lite-common.sh . ").append( "\n" );
        sb.append( "fi" ).append( "\n" );
        sb.append( "\n" );
        
        sb.append( "set +e" ).append( "\n" );
        
        //sets up the variables used for docker run command
        //FIXME docker_init has to be passed the name of the tar file?
        Container c = job.getContainer();
        sb.append( "docker_init").append( " " ).append( c.getName() ).append( "\n" );
        
        sb.append( "job_ec=$(($job_ec + $?))" ).append( "\n" ).append( "\n" );;
        
        //assume docker is available in path
        sb.append( "docker run ");
        
        //environment variables are set in the job as -e
        for( Iterator it = job.envVariables.getProfileKeyIterator(); it.hasNext(); ){
            String key = (String)it.next();
            String value = (String) job.envVariables.get( key );
            
            //check for env variables that are constructed based on condor job classds 
            //such asCONDOR_JOBID=$(cluster).$(process). these are set by condor
            //and can only picked up from the shell when a job runs on a node
            //so we only set the key
            boolean fromShell = value.contains( "$(" );
            sb.append( "-e ").append( key );
            if( !fromShell ){
                //append the value
                sb.append( "=" ).
                append( "\"" ).append( value ).append( "\"" );
            }
            sb.append( " " );
        }
        
        //directory where job is run is mounted as scratch
        sb.append( "-v $PWD:").append( CONTAINER_WORKING_DIRECTORY ).append( " ");
        sb.append( "-w=").append( CONTAINER_WORKING_DIRECTORY ).append( " ");     
        
        sb.append( "--name $cont_name ");
        sb.append( " $cont_image ");
        
        //track 
        
        //invoke the command to run as user who launched the job
        sb.append( "bash -c ").
           append( "\"").
            
            append( "set -e ;" ).
            append( "if ! grep -q -E  \"^$cont_group:\" /etc/group ; then ").
            append( "groupadd --gid $cont_groupid $cont_group ;").
            append( "fi; ").
            append( "if ! id $cont_user 2>/dev/null >/dev/null; then ").
            append( "useradd --uid $cont_userid --gid $cont_groupid $cont_user; ").
            append( "fi; ").
            append( "su $cont_user -c ");
                sb.append( "\\\"");
                sb.append( "./" ).append( scriptName ).append( " " );
                sb.append( "\\\"");
                
          sb.append( "\"");      
        
        sb.append( "\n" );
        
        sb.append( "job_ec=$(($job_ec + $?))" ).append( "\n" ).append( "\n" );
        
        //remove the docker container
        sb.append( "docker rm $cont_name " ).append( " 1>&2" ).append( "\n" );
        sb.append( "job_ec=$(($job_ec + $?))" ).append( "\n" ).append( "\n" );
        
        return sb.toString();
    }
    
    /**
     * Returns the snippet to wrap a single job execution
     * 
     * @param job
     * 
     * @return 
     */
    public String wrap( AggregatedJob job ){
        String snippet = this.wrap( (Job)job );
        
        //rest the jobs stdin
        job.setStdIn( "" );
        job.condorVariables.removeKey( "input" );
       
        return snippet;
    }
    
    /**
     * Return the description 
     * @return 
     */
    public String describe(){
        return "Docker";
    }

  
    /**
     * Return the container package snippet. Construct the snippet that generates the 
     * shell script responsible for setting up the worker package in the container
     * and launch the job in the container.
     * 
     * @param job  the job
     * @param scriptName basename of the script
     * @return 
     */
    protected String constructJobLaunchScriptInContainer( Job job, String scriptName ) {
        if( WORKER_PACKAGE_SETUP_SNIPPET == null ){
            WORKER_PACKAGE_SETUP_SNIPPET = Docker.constructContainerWorkerPackagePreamble();
        }
        StringBuilder sb = new StringBuilder();
        sb.append( "\n" );
        appendStderrFragment( sb, "Writing out script to launch job in docker container (START)" );
        sb.append( "\n" );
        sb.append( "cat <<EOF > " ).append( scriptName ).append( "\n" );
        
        if( WORKER_PACKAGE_SETUP_SNIPPET == null ){
            WORKER_PACKAGE_SETUP_SNIPPET = Docker.constructContainerWorkerPackagePreamble();
        }
        sb.append( WORKER_PACKAGE_SETUP_SNIPPET );
        
        appendStderrFragment( sb, "launching job in the container");
        sb.append( "\n" );
        //sb.append( "\\$kickstart \"\\${original_args[@]}\" ").append( "\n" );
        
        if( job instanceof AggregatedJob ){
            try{
                //for clustered jobs we embed the contents of the input
                //file in the shell wrapper itself
                sb.append( job.getRemoteExecutable() ).append( " " ).append( job.getArguments() );
                sb.append( " << CLUSTER" ).append( '\n' );

                //PM-833 figure out the job submit directory
                String jobSubmitDirectory = new File( job.getFileFullPath( mSubmitDir, ".in" )).getParent();

                sb.append( slurpInFile( jobSubmitDirectory, job.getStdIn() ) );
                sb.append( "CLUSTER" ).append( '\n' );
            }
            catch( IOException ioe ){
                throw new RuntimeException( "[Pegasus-Lite] Error while Docker wrapping job " + job.getID(), ioe );
            }
        }
        else{
                sb.append( job.getRemoteExecutable()).append( " " ).
                   append( job.getArguments() ).append( "\n" );
        }
        sb.append( "EOF").append( "\n" );
        appendStderrFragment( sb, "Writing out script to launch job in docker container (END)" );
        sb.append( "\n" );
        sb.append( "\n" );
        
        return sb.toString();
    }
    
      
    /**
     * Construct the snippet that generates the shell script responsible for
     * setting up the worker package in the container.
     * 
     * @return 
     */
    protected static String constructContainerWorkerPackagePreamble() {
        StringBuffer sb = new StringBuffer();
        sb.append( "#!/bin/bash" ).append( "\n" );
        sb.append( "set -e" ).append( "\n" );
        sb.append( "pegasus_lite_version_major=$pegasus_lite_version_major" ).append( "\n" );
        sb.append( "pegasus_lite_version_minor=$pegasus_lite_version_minor" ).append( "\n" );
        sb.append( "pegasus_lite_version_patch=$pegasus_lite_version_patch" ).append( "\n" );
        sb.append( "pegasus_lite_enforce_strict_wp_check=$pegasus_lite_enforce_strict_wp_check" ).append( "\n" );
        sb.append( "pegasus_lite_version_allow_wp_auto_download=$pegasus_lite_version_allow_wp_auto_download" ).append( "\n" );
        sb.append( "pegasus_lite_work_dir=" ).append( Singularity.CONTAINER_WORKING_DIRECTORY ).append( "\n" );
        sb.append( "echo \\$PWD" ).append( "  1>&2" ).append( "\n" );
        /*
        sb.append( "echo \"Arguments passed \\$@\"" ).append( "  1>&2" ).append( "\n" );
        sb.append( "kickstart=\"\\$1\" ").append( "\n" );
        sb.append( "shift" ).append( "\n" );
        sb.append( "original_args=(\"\\$@\")" ).append( "\n" ).append( "\n" );
        */
        sb.append( ". pegasus-lite-common.sh" ).append( "\n" );
        sb.append( "pegasus_lite_init" ).append( "\n" ).append( "\n" );

        
        sb.append( "\n" );
        sb.append( "echo -e \"\\n###################### figuring out the worker package to use in the container ######################\"  1>&2" ).append( "\n" );
        sb.append( "# figure out the worker package to use" ).append( "\n" );

        sb.append( "pegasus_lite_worker_package" ).append( "\n" );

        sb.append( "echo \"PATH in container is set to is set to \\$PATH\"").append( "  1>&2" ).append( "\n" ); 
        sb.append( "\n" );
        
        return sb.toString();
        
    }
}
