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

package edu.isi.pegasus.planner.code.generator.condor;

import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.DagInfo;
import edu.isi.pegasus.planner.classes.Job;

import edu.isi.pegasus.planner.namespace.Pegasus;

import java.io.PrintWriter;

/**
 * A helper class, that generates Pegasus specific classads for the jobs.
 *
 * @author Karan Vahi
 * @version $Revision$
 */

public class ClassADSGenerator {

    /**
     * The name of the generator.
     */
    public static final String GENERATOR = "Pegasus";


    /**
     * The complete classad designating Pegasus as the generator.
     */
    public static final String GENERATOR_AD_KEY = "pegasus_generator";

    /**
     * The class ad key for the version id.
     *
     * @see org.griphyn.cPlanner.classes.DagInfo#releaseVersion
     */
    public static final String VERSION_AD_KEY  = "pegasus_version";

    /**
     * The classad key for the pegasus build.
     */
    public static final String BUILD_AD_KEY = "pegasus_build";

    /**
     * The classad for the flow id.
     *
     * @see org.griphyn.cPlanner.classes.DagInfo#flowIDName
     */
    public static final String WF_NAME_AD_KEY = "pegasus_wf_name";

    /**
     * The classad for the timestamp.
     *
     * @see org.griphyn.cPlanner.classes.DagInfo#mFlowTimestamp
     */
    public static final String WF_TIME_AD_KEY = "pegasus_wf_time";

    /**
     * The classad for the complete transformation name.
     */
    public static final String XFORMATION_AD_KEY = "pegasus_wf_xformation";

    /**
     * The classad for generating the DAX ID
     */
    public static final String DAX_ID_KEY = "pegasus_wf_dax_id";

    /**
     * The class ad for job Class.
     *
     * @see org.griphyn.cPlanner.classes.Job#jobClass
     */
    public static final String JOB_CLASS_AD_KEY = "pegasus_job_class";

    /**
     * The class ad for the jobId.
     *
     * @see org.griphyn.cPlanner.classes.Job#jobID
     */
    public static final String JOB_ID_AD_KEY = "pegasus_job_id";
    
    /**
     * The class ad for the expected job value
     */
    public static final String JOB_RUNTIME_AD_KEY = "pegasus_job_runtime";

    /**
     * The class ad to store the execution pool at which the job is run. The
     * globusscheduler specified in the submit file refers to the jobmanager on
     * this execution pool.
     */
    public static final String RESOURCE_AD_KEY = "pegasus_site";


    /**
     * Writes out the classads for a workflow to corresponding writer stream.
     *
     * @param writer is an open stream for the Condor submit file.
     * @param dag    the workflow object containing metadata about the workflow
     *               like the workflow id and the release version.
     */
    public static void generate( PrintWriter writer, ADag dag ) {
        //get hold of the object holding the metadata
        //information about the workflow
        DagInfo dinfo = dag.dagInfo;

        //pegasus is the generator
        writer.println(generateClassAdAttribute(GENERATOR_AD_KEY, GENERATOR));

        //the vds version
        if (dinfo.releaseVersion != null) {
            writer.println(
                generateClassAdAttribute(VERSION_AD_KEY, dinfo.releaseVersion));
        }

        //the workflow name
        if (dinfo.flowIDName != null) {
            writer.println(
                generateClassAdAttribute(WF_NAME_AD_KEY, dinfo.flowIDName));
        }
        //the workflow time
        if (dinfo.getMTime() != null) {
            writer.println(
                generateClassAdAttribute(WF_TIME_AD_KEY, dinfo.getFlowTimestamp()));
        }
    }



    /**
     * Writes out the classads for a job to corresponding writer stream.
     * The writer stream points to a Condor Submit file for the job.
     *
     * @param writer is an open stream for the Condor submit file.
     * @param dag    the workflow object containing metadata about the workflow
     *               like the workflow id and the release version.
     * @param job    the <code>Job</code> object for which the writer stream
     *               is passed.
     **/
    public static void generate( PrintWriter writer, ADag dag, Job job ) {

        //get all the workflow classads
        generate( writer, dag );

        //get the job classads

        //the tranformation name
        writer.println(
            generateClassAdAttribute( ClassADSGenerator.XFORMATION_AD_KEY, job.getCompleteTCName() ) );

        //put in the DAX 
        writer.println(
            generateClassAdAttribute( DAX_ID_KEY, job.getDAXID() ) );

        //the class of the job
        writer.println(generateClassAdAttribute( ClassADSGenerator.JOB_CLASS_AD_KEY, job.getJobType() ) );

        //the supernode id
        writer.println(generateClassAdAttribute( ClassADSGenerator.JOB_ID_AD_KEY, job.jobID ));

        //the resource on which the job is scheduled
        writer.println(generateClassAdAttribute( ClassADSGenerator.RESOURCE_AD_KEY, job.getSiteHandle() ) );

        //add the pegasus value if defined.
        String value = (String)job.vdsNS.getStringValue( Pegasus.RUNTIME_KEY );
        //else see if globus maxwalltime defined
        value = ( value == null )? (String)job.globusRSL.get( "maxwalltime" ) : value;
        int runtime = 0;
        try{
            runtime = ( value ==  null )? 0: Integer.parseInt(value);
        }
        catch( Exception e ){
            //ignore
        }
        writer.println(generateClassAdAttribute( ClassADSGenerator.JOB_RUNTIME_AD_KEY, runtime  ) );
        
    }




    /**
     * Generates a classad attribute given the name and the value.
     *
     * @param name  the attribute name.
     * @param value the value/expression making the classad attribute.
     *
     * @return  the classad attriubute.
     */
    private static String generateClassAdAttribute(String name, String value) {
        return generateClassAdAttribute( name, value, false);
    }

    /**
     * Generates a classad attribute given the name and the value. It by default
     * adds a new line character at start of each attribute.
     *
     * @param name  the attribute name.
     * @param value the value/expression making the classad attribute.
     *
     * @return  the classad attriubute.
     */
    private static String generateClassAdAttribute(String name, int value) {
        StringBuffer sb = new StringBuffer(10);

        sb.append("+");
        sb.append(name).append(" = ");
        sb.append(value);

        return sb.toString();
    }


    /**
     * Generates a classad attribute given the name and the value.
     *
     * @param name     the attribute name.
     * @param value    the value/expression making the classad attribute.
     * @param newLine  boolean denoting whether to add a new line character at
     *                 start or not.
     *
     * @return  the classad attriubute.
     */
    private static String generateClassAdAttribute(  String name,
                                                     String value,
                                                     boolean newLine) {

        StringBuffer sb = new StringBuffer(10);
        if(newLine)
            sb.append("\n");

        sb.append("+");
        sb.append(name).append(" = ");
        sb.append("\"");
        sb.append(value);
        sb.append("\"");


        return sb.toString();
    }

}
