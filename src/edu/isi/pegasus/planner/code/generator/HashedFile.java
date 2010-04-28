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

import org.griphyn.cPlanner.classes.ADag;
import org.griphyn.cPlanner.classes.SubInfo;
import org.griphyn.cPlanner.classes.PlannerOptions;

import edu.isi.pegasus.planner.code.CodeGenerator;
import edu.isi.pegasus.planner.code.generator.condor.CondorGenerator;

import org.griphyn.cPlanner.common.PegasusProperties;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.io.IOException;

/**
 * A Condor Submit Writer, that understands the notion of hashed file directories.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class HashedFile extends CondorGenerator {

    /**
     * The default constructor.
     */
    public HashedFile() {
        super();
    }


    /**
     * Returns an open stream to the file that is used for writing out the
     * job information for the job.
     *
     * @param job  the job whose job information needs to be written.
     *
     * @return  the writer to the open file.
     * @exception IOException if unable to open a write handle to the file.
     */
    public PrintWriter getWriter(SubInfo job) throws IOException{
//        String jobDir = job.getSubmitDirectory();
        StringBuffer sb = new StringBuffer();

        //determine the absolute submit directory for the job
//        sb.append( GridStart.getSubmitDirectory( mSubmitFileDir, job ));
        sb.append(mSubmitFileDir);

        //append the base name of the job
        sb.append( File.separatorChar ).append(getFileBaseName(job));

        // intialize the print stream to the file
        return new PrintWriter(new BufferedWriter(new FileWriter(sb.toString())));
    }

    /**
     * Returns the path relative to the workflow submit directory of the file to
     * which the job is written to.
     *
     * @param job  the job whose job information needs to be written.
     *
     * @return  the relative path of the file.
     */
     /*
    public String getDAGMANFilename(SubInfo job){
        //do the correct but the inefficient way.
        String name = "";

        //get the absolute directory first
        String absolute = GridStart.getSubmitDirectory( mSubmitFileDir, job );
        if (absolute.indexOf( mSubmitFileDir ) == 0){

            if(absolute.length() > mSubmitFileDir.length()){
                name = absolute.substring(mSubmitFileDir.length());

                //remove the file separator if present at the starting
                name = (name.indexOf( File.separatorChar) == 0)?
                        name.substring(1):
                        name;

                name += File.separatorChar;
            }
            else{
                //empty. no relative directory
            }
        }
        else{
            //the absolute path does not contain the submit file directory
            //root. Should not really be the case.
            name = absolute;
            name += File.separatorChar;
        }

        name += this.getFileBaseName(job);

        return name;
    }
    */

}
