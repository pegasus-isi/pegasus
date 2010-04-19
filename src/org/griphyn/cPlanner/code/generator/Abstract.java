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

package org.griphyn.cPlanner.code.generator;


import org.griphyn.cPlanner.classes.ADag;
import org.griphyn.cPlanner.classes.SubInfo;
import org.griphyn.cPlanner.classes.PlannerOptions;

import org.griphyn.cPlanner.code.CodeGenerator;
import org.griphyn.cPlanner.code.CodeGeneratorException;

import org.griphyn.cPlanner.common.PegasusProperties;

import edu.isi.pegasus.common.util.DynamicLoader;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import org.griphyn.cPlanner.classes.PegasusBag;

/**
 * An Abstract Base class implementing the CodeGenerator interface. Introduces
 * helper methods for determining basenames of files, that contain concrete
 * job descriptions.
 *
 *
 * @author Karan Vahi
 * @author Gaurang Mehta
 *
 * @version $Revision$
 */
public abstract class Abstract implements CodeGenerator{

    /**
     * The bag of initialization objects.
     */
    protected PegasusBag mBag;


    /**
     * The directory where all the submit files are to be generated.
     */
    protected String mSubmitFileDir;

    /**
     * The object holding all the properties pertaining to Pegasus.
     */
    protected PegasusProperties mProps;

    /**
     * The object containing the command line options specified to the planner
     * at runtime.
     */
    protected PlannerOptions mPOptions;


    /**
     * Initializes the Code Generator implementation.
     *
     * @param bag   the bag of initialization objects.
     *
     * @throws CodeGeneratorException in case of any error occuring code generation.
     */
    public void initialize( PegasusBag bag ) throws CodeGeneratorException{
        mBag           = bag;
        mProps         = bag.getPegasusProperties();
        mPOptions      = bag.getPlannerOptions();
        mSubmitFileDir = mPOptions.getSubmitDirectory();
    }



    /**
     * Starts monitoring of the workflow by invoking a workflow monitor daemon.
     * The monitoring should start only after the output files have been generated.
     * FIXME: It should actually happen after the workflow has been submitted.
     *        Eventually should be a separate monitor interface, and submit writers
     *        should be loaded by an AbstractFactory.
     *
     * @return boolean indicating whether could successfully start the monitor
     *         daemon or not.
     */
    public boolean startMonitoring(){
        //by default not all code generators support monitoring.
        return false;
    }


    /**
     * Resets the Code Generator implementation.
     *
     * @throws CodeGeneratorException in case of any error occuring code generation.
     */
    public void reset( )throws CodeGeneratorException{
        mSubmitFileDir = null;
        mProps         = null;
        mPOptions      = null;
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
    public PrintWriter getWriter( SubInfo job ) throws IOException{
//        String jobDir = job.getSubmitDirectory();
        StringBuffer sb = new StringBuffer();

        //determine the absolute submit directory for the job
//        sb.append( GridStart.getSubmitDirectory( mSubmitFileDir, job ));
        sb.append( mSubmitFileDir );

        //append the base name of the job
        sb.append( File.separatorChar ).append( getFileBaseName(job) );

        // intialize the print stream to the file
        return new PrintWriter(new BufferedWriter(new FileWriter(sb.toString())));
    }

    /**
     * Returns the basename of the file to which the job is written to.
     *
     * @param job  the job whose job information needs to be written.
     *
     * @return  the basename of the file.
     */
    public String getFileBaseName(SubInfo job){
        StringBuffer sb = new StringBuffer();
        sb.append(job.jobName).append(".sub");
        return sb.toString();
    }


}
