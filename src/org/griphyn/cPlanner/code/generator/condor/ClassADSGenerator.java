/**
 * This file or a portion of this file is licensed under the terms of
 * the Globus Toolkit Public License, found in file GTPL, or at
 * http://www.globus.org/toolkit/download/license.html. This notice must
 * appear in redistributions of this file, with or without modification.
 *
 * Redistributions of this Software, with or without modification, must
 * reproduce the GTPL in: (1) the Software, or (2) the Documentation or
 * some other similar material which is provided with the Software (if
 * any).
 *
 * Copyright 1999-2004 University of Chicago and The University of
 * Southern California. All rights reserved.
 */
package org.griphyn.cPlanner.code.generator.condor;

import org.griphyn.cPlanner.classes.ADag;
import org.griphyn.cPlanner.classes.DagInfo;
import org.griphyn.cPlanner.classes.SubInfo;

import java.io.PrintWriter;

/**
 * A helper class, that generates VDS specific classads for the jobs.
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
     * The classad for the corresponding vdl derivation.
     */
    public static final String DERIVATION_AD_KEY = "pegasus_wf_derivation";

    /**
     * The class ad for job Class.
     *
     * @see org.griphyn.cPlanner.classes.SubInfo#jobClass
     */
    public static final String JOB_CLASS_AD_KEY = "pegasus_job_class";

    /**
     * The class ad for the jobId.
     *
     * @see org.griphyn.cPlanner.classes.SubInfo#jobID
     */
    public static final String JOB_ID_AD_KEY = "pegasus_job_id";

    /**
     * The class ad to store the execution pool at which the job is run. The
     * globusscheduler specified in the submit file refers to the jobmanager on
     * this execution pool.
     */
    public static final String RESOURCE_AD_KEY = "pegasus_site";



    /**
     * Writes out the classads for a job to corresponding writer stream.
     * The writer stream points to a Condor Submit file for the job.
     *
     * @param writer is an open stream for the Condor submit file.
     * @param dag    the workflow object containing metadata about the workflow
     *               like the workflow id and the release version.
     * @param job    the <code>SubInfo</code> object for which the writer stream
     *               is passed.
     **/
    public static void generate( PrintWriter writer, ADag dag, SubInfo job ) {
        //get hold of the object holding the metadata
        //information about the workflow
        DagInfo dinfo = dag.dagInfo;

        //pegasus is the generator
        writer.println(generateClassAdAttribute( GENERATOR_AD_KEY, GENERATOR ) );

        //the vds version
        if ( dinfo.releaseVersion != null ) {
            writer.println(
                generateClassAdAttribute( VERSION_AD_KEY, dinfo.releaseVersion ) );
        }

        //the workflow name
        if (dinfo.flowIDName != null) {
            writer.println(
                generateClassAdAttribute( WF_NAME_AD_KEY, dinfo.flowIDName ) );
        }
        //the workflow time
        if (dinfo.getMTime() != null) {
            writer.println(
                generateClassAdAttribute( WF_TIME_AD_KEY, dinfo.getFlowTimestamp() ) );
        }
        //the tranformation name
        writer.println(
            generateClassAdAttribute( XFORMATION_AD_KEY, job.getCompleteTCName() ) );
        //the derivation name
        writer.println(
            generateClassAdAttribute( DERIVATION_AD_KEY, job.getCompleteDVName() ) );

        //the job class ads
        //the class of the job
        writer.println(generateClassAdAttribute( JOB_CLASS_AD_KEY, job.getJobType() ) );

        //the supernode id
        writer.println(generateClassAdAttribute( JOB_ID_AD_KEY, job.jobID ));

        //the resource on which the job is scheduled
        writer.println(generateClassAdAttribute( RESOURCE_AD_KEY, job.getSiteHandle() ) );

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