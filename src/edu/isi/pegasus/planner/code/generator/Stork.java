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


import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.TransferJob;
import edu.isi.pegasus.planner.classes.PegasusBag;

import edu.isi.pegasus.planner.code.CodeGenerator;
import edu.isi.pegasus.planner.code.CodeGeneratorException;

import edu.isi.pegasus.common.logging.LogManager;

import java.io.File;
import java.io.PrintWriter;
import java.io.IOException;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;



/**
 * This implementation generates files that can be understood by Stork.
 *
 * @author Karan Vahi
 * @version $Revision$
 */

public class Stork extends Abstract {

    /**
     * The nice start separator, define once, use often.
     */
    public final static String mStartSeparator =
        "/**********************************************************************";

    /**
     * The nice end separator, define once, use often.
     */
    public final static String mEndSeparator =
        " **********************************************************************/";


    /**
     * The LogManager object which is used to log all the messages.
     */
    private LogManager mLogger;

    /**
     * The name of the credential that is to be used for submitting the stork
     * job.
     */
//    private String mCredName;


    /**
     * The default constructor.
     */
    public Stork(){
        super();
    }

    /**
     * Initializes the Code Generator implementation.
     *
     * @param bag   the bag of initialization objects.
     *
     * @throws CodeGeneratorException in case of any error occuring code generation.
     */
    public void initialize( PegasusBag bag ) throws CodeGeneratorException{
        super.initialize( bag );
        mLogger = bag.getLogger();
//        mCredName = mProps.getCredName();
    }

    /**
     * Generates the code for the concrete workflow in the input format of the
     * workflow executor being used. The method is not yet implemented.
     *
     * @param dag  the concrete workflow.
     *
     * @return null
     *
     * @throws CodeGeneratorException in case of any error occuring code generation.
     */
    public Collection<File> generateCode( ADag dag ) throws CodeGeneratorException{
        throw new CodeGeneratorException(
            new UnsupportedOperationException(
                 "Stork Code Generator: Method generateCode( ADag) not implemeneted"));
    }

    /**
     * Generates the code for a single job in the Stork format.
     *
     * @param dag    the dag of which the job is a part of.
     * @param job    the <code>Job</code> object holding the information about
     *               that particular job.
     *
     *
     * @throws CodeGeneratorException in case of any error occuring code generation.
     */
    public void generateCode( ADag dag, Job job ) throws CodeGeneratorException{
        String dagname  = dag.dagInfo.nameOfADag;
        String dagindex = dag.dagInfo.index;
        String dagcount = dag.dagInfo.count;


        StringTokenizer st = new StringTokenizer(job.strargs,"\n");
        String srcUrl = (st.hasMoreTokens())?st.nextToken():null;
        String dstUrl = (st.hasMoreTokens())?st.nextToken():null;

        //sanity check
        // Credential name is no longer required. Karan Feb 04, 2008
//        if(mCredName == null){
//            mLogger.log("Credential name needs to be specified for " +
//                        " stork job. Set pegasus.transfer.stork.cred property",
//                        LogManager.ERROR_MESSAGE_LEVEL);
//            throw new CodeGeneratorException(
//                "Credential name needs to be specified for " +
//                " stork job. Set pegasus.transfer.stork.cred property");
//
//        }

        //check for type of job. Stork only understands Transfer Jobs
        if (!(job instanceof TransferJob )){
            throw new CodeGeneratorException(
                "Stork Code Generator can only generate code for transfer jobs" );
        }

        PrintWriter writer = null;
        try{
            writer = this.getWriter( job );
        }
        catch( IOException ioe ){
            throw new RuntimeException( "Unable to get Writer to write the Stork Submit file", ioe );
        }

        writer.println(this.mStartSeparator);
        writer.println(" * PEGASUS WMS STORK FILE GENERATOR");
        writer.println(" * DAG : " + dagname + ", Index = " + dagindex +
                       ", Count = " + dagcount);
        writer.println(" * STORK FILE NAME : " + this.getFileBaseName(job));
        writer.println(this.mEndSeparator);

        writer.println("[");

        writer.println("\tdap_type = \"" + "transfer" + "\";");
        writer.println("\tsrc_url = \"" + srcUrl + "\";");
        writer.println("\tdest_url = \"" + dstUrl + "\";");
        writer.println("\tx509proxy = \"" + "default" + "\";");
	writer.println("\tlog = \"" + this.getFileBaseName(job) + ".log" + "\";");

        // DONE
        writer.println("]");

        writer.println(this.mStartSeparator);
        writer.println(" * END OF STORK FILE");
        writer.println(this.mEndSeparator);

        //flush the contents
        writer.close();
    }

    /**
     * Returns the basename of the file to which the job is written to.
     *
     * @param job  the job whose job information needs to be written.
     *
     * @return  the basename of the file.
     */
    public String getFileBaseName(Job job){
        StringBuffer sb = new StringBuffer();
        sb.append(job.jobName).append(".stork");
        return sb.toString();
    }

    /**
     * Returns an empty map
     *
     * @param workflow   the workflow.
     *
     * @return map containing extra entries
     */
    public Map<String, String> getAdditionalBraindumpEntries(ADag workflow) {
        return new HashMap();
    }

}
