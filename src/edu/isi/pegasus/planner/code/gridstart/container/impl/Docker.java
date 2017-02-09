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
        
        //sets up the variables used for docker run command
        sb.append( "docker_init").append( "\n" );
        
        
        //assume docker is available in path
        sb.append( "docker run ");
        //directory where job is run is mounted as scratch
        sb.append( "-v $PWD:/scratch -w=/scratch ");     
        //hardcoded image for time being
        sb.append( "-t ");
        sb.append( "--name $cont_name ");
        sb.append( " centos-pegasus-root ");
        
        //invoke the command to run as user who launched the job
        sb.append( "bash -c ").
           append( "\"").
            
            append( "set -e ;" ).
            append( "groupadd --gid $cont_groupid $cont_group ;").
            append( "useradd --uid $cont_userid --gid $cont_groupid $cont_user;").
            append( "su $cont_user -c ");
                sb.append( "\\\"");
                
                sb.append( job.getRemoteExecutable()).append( " " ).
                   append( job.getArguments() );
                
                sb.append( "\\\"");
                
          sb.append( "\"");      
        
        sb.append( "\n" );
        //remove the docker container
        sb.append( "docker rm $cont_name " ).append( " 1>&2" );
        
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
}
