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
        /**
         * docker run -e USER=$USER  -e USERID=$UID -v $PWD:/scratch -w=/scratch -t centos-pegasus-root bash -c "usertouse=vahi; set -e ; echo $GROUPS; groupadd --gid $GROUP\                          
S gridstaff && echo $UID; useradd --uid $UID --gid $GROUPS \$usertouse; su \$usertouse -c \"env PATH=\$PATH /usr/bin/pegasus-kickstart  -n pegasus::preprocess:4.0 -N j1 -R local  -s f.b2=f\
.b2 -s f.b1=f.b1 -L blackdiamond -T 2017-02-01T13:22:04-08:00 /usr/bin/pegasus-keg  -a preprocess -T 60 -i  f.a  -o  f.b1  f.b2\""
         */
        
        //assume docker is available in path
        sb.append( "docker run ");
        //directory where job is run is mounted as scratch
        sb.append( "-v $PWD:/scratch -w=/scratch ");     
        //hardcoded image for time being
        sb.append( "-t centos-pegasus-root ");
        
        //invoke the command to run as user who launched the job
        sb.append( "bash -c ").
           append( "\"").
            
            append( "usertouse=$USER ; ").
            append( "set -e ;" ).
            append( "groupadd --gid $GROUPS ;").
            append( "useradd --uid $UID --gid $GROUPS \\$usertouse;").
            append( "su \\$usertouse -c ");
                sb.append( "\\\"");
                
                sb.append( job.getRemoteExecutable()).append( " " ).
                   append( job.getArguments() );
                
                sb.append( "\\\"");
                
          sb.append( "\"");      
                
        
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
