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

import java.io.File;
import java.io.IOException;

/**
 * An interface to determine how a job gets wrapped to be launched on various 
 * containers, as a shell-script snippet that can be embedded in PegasusLite
 *
 * @author vahi
 */
public class None extends Abstract {
    
    
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
        appendStderrFragment( sb, "executing the user task" );
        sb.append( job.getRemoteExecutable() ).append( job.getArguments() ).append( '\n' );
        //capture exitcode of the job
        sb.append( "job_ec=$?" ).append( "\n" );
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
        StringBuilder sb = new StringBuilder();
        
        try{
            appendStderrFragment( sb, "executing the user's clustered task" );
            //for clustered jobs we embed the contents of the input
            //file in the shell wrapper itself
            sb.append( job.getRemoteExecutable() ).append( " " ).append( job.getArguments() );
            sb.append( " << EOF" ).append( '\n' );

            //PM-833 figure out the job submit directory
            String jobSubmitDirectory = new File( job.getFileFullPath( mSubmitDir, ".in" )).getParent();

            sb.append( slurpInFile( jobSubmitDirectory, job.getStdIn() ) );
            sb.append( "EOF" ).append( '\n' );

            //capture exitcode of the job
            sb.append( "job_ec=$?" ).append( "\n" );
        
            
            //rest the jobs stdin
            job.setStdIn( "" );
            job.condorVariables.removeKey( "input" );
        }
        catch( IOException ioe ){
            throw new RuntimeException( "[Pegasus-Lite] Error while wrapping job " + job.getID(), ioe );
        }
        return sb.toString();
    }
    
    /**
     * Return the description 
     * @return 
     */
    public String describe(){
        return "No container wrapping";
    }
}
