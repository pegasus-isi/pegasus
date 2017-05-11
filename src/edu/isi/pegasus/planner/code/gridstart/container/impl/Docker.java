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

import edu.isi.pegasus.planner.classes.AggregatedJob;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusBag;

import edu.isi.pegasus.planner.code.gridstart.container.ContainerShellWrapper;

/**
 * An interface to determine how a job gets wrapped to be launched on various 
 * containers, as a shell-script snippet that can be embedded in PegasusLite
 *
 * @author vahi
 */
public class Docker implements ContainerShellWrapper {

    /**
     * The basename of the worker package setup shell script created on the
     * remote worker node and that launches the job.
     */
    public static final String WORKER_PACKAGE_SETUP_SCRIPT_NAME = "pegasus-container-setup-launch.sh";
    
    private static String WORKER_PACKAGE_SETUP_SNIPPET = null; 
     
    
    /**
     * Initiailizes the Container  shell wrapper
     * @param bag 
     */
    public void initialize( PegasusBag bag ){
        
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
        //deploy pegasus worker package in the container
        sb.append( getContainerWorkerPackageSnippet() );
        
        sb.append( "chmod +x " ).append( Docker.WORKER_PACKAGE_SETUP_SCRIPT_NAME).append( "\n" );

        //copy pegasus lite common from the directory where condor transferred it via it's file transfer.
        sb.append( "cp $pegasus_lite_start_dir/pegasus-lite-common.sh . ").append( "\n" );
 
        sb.append( "set +e" ).append( "\n" );
        
        //sets up the variables used for docker run command
        //FIXME docker_init has to be passed the name of the tar file?
        sb.append( "docker_init").append( "\n" );
        
        sb.append( "job_ec=$(($job_ec + $?))" ).append( "\n" ).append( "\n" );;
        
        //assume docker is available in path
        sb.append( "docker run ");
        //directory where job is run is mounted as scratch
        sb.append( "-v $PWD:/scratch -w=/scratch ");     
        //hardcoded image for time being
        sb.append( "-t ");
        sb.append( "--name $cont_name ");
        sb.append( " $cont_image ");
        
        //track 
        
        //invoke the command to run as user who launched the job
        sb.append( "bash -c ").
           append( "\"").
            
            append( "set -e ;" ).
            append( "groupadd --gid $cont_groupid $cont_group ;").
            append( "useradd --uid $cont_userid --gid $cont_groupid $cont_user;").
            append( "su $cont_user -c ");
                sb.append( "\\\"");
                sb.append( Docker.WORKER_PACKAGE_SETUP_SCRIPT_NAME ).append( " " );
                sb.append( job.getRemoteExecutable()).append( " " ).
                   append( job.getArguments() );
                
                sb.append( "\\\"");
                
          sb.append( "\"");      
        
        sb.append( "\n" );
        
        sb.append( "job_ec=$(($job_ec + $?))" ).append( "\n" ).append( "\n" );;
        
        //remove the docker container
        sb.append( "docker rm $cont_name " ).append( " 1>&2" ).append( "\n" );;
        sb.append( "job_ec=$(($job_ec + $?))" ).append( "\n" ).append( "\n" );;
        
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
    
        throw new UnsupportedOperationException("Method not implemented");

    }
    
    /**
     * Return the description 
     * @return 
     */
    public String describe(){
        return "No container wrapping";
    }

  
    /**
     * Return the container package snippet.
     * 
     * @return 
     */
    protected String getContainerWorkerPackageSnippet() {
        if( WORKER_PACKAGE_SETUP_SNIPPET == null ){
            WORKER_PACKAGE_SETUP_SNIPPET = Docker.constructContainerWorkerPackageSnippet();
        }
        return WORKER_PACKAGE_SETUP_SNIPPET;
    }
    
      
    /**
     * Construct the snippet that generates the shell script responsible for
     * setting up the worker package in the container
     * 
     * @return 
     */
    protected static String constructContainerWorkerPackageSnippet() {
        StringBuffer sb = new StringBuffer();
        sb.append( "\n" );
        sb.append( "############################# Writing out script to launch job in docker container (START) #############################" ).append( "\n" );
        sb.append( "cat <<EOF > " ).append( Docker.WORKER_PACKAGE_SETUP_SCRIPT_NAME).append( "\n" );
        sb.append( "set -e" ).append( "\n" );
        sb.append( "pegasus_lite_version_major=$pegasus_lite_version_major" ).append( "\n" );
        sb.append( "pegasus_lite_version_minor=$pegasus_lite_version_minor" ).append( "\n" );
        sb.append( "pegasus_lite_version_patch=$pegasus_lite_version_patch" ).append( "\n" );
        sb.append( "pegasus_lite_enforce_strict_wp_check=$pegasus_lite_enforce_strict_wp_check" ).append( "\n" );
        sb.append( "pegasus_lite_version_allow_wp_auto_download=$pegasus_lite_version_allow_wp_auto_download" ).append( "\n" );
        sb.append( "pegasus_lite_work_dir=/scratch" ).append( "\n" );
        sb.append( "echo \\$PWD" ).append( "\n" );
        sb.append( "echo \"Arguments passed \\$@\"" ).append( "\n" );
        sb.append( "kickstart=\"\\$1\" ").append( "\n" );
        sb.append( "shift" ).append( "\n" );
        sb.append( "original_args=(\"\\$@\")" ).append( "\n" ).append( "\n" );

        sb.append( ". pegasus-lite-common.sh" ).append( "\n" );
        sb.append( "pegasus_lite_init" ).append( "\n" ).append( "\n" );

        
        sb.append( "\n" );
        sb.append( "echo -e \"\\n###################### figuring out the worker package to use in the container ######################\"  1>&2" ).append( "\n" );
        sb.append( "# figure out the worker package to use" ).append( "\n" );

        sb.append( "pegasus_lite_worker_package" ).append( "\n" );

        sb.append( "echo \"PATH in container is set to is set to \\$PATH\" ").append( "\n" ); 
        
        sb.append( "\n" );
        sb.append( "echo -e \"\\n############################# launching job in the container #############################\"  1>&2" ).append( "\n" );
        sb.append( "\\$kickstart \"\\${original_args[@]}\" ").append( "\n" );
        sb.append( "EOF").append( "\n" );
        sb.append( "############################# Writing out script to launch job in docker container (END) #############################" ).append( "\n" );
        sb.append( "\n" );
        return sb.toString();
        
    }
}
