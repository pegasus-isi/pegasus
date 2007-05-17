/*
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

package org.griphyn.cPlanner.code.generator;


import org.griphyn.cPlanner.classes.ADag;
import org.griphyn.cPlanner.classes.SubInfo;
import org.griphyn.cPlanner.classes.TransferJob;
import org.griphyn.cPlanner.classes.PlannerOptions;

import org.griphyn.cPlanner.code.CodeGenerator;
import org.griphyn.cPlanner.code.CodeGeneratorException;

import org.griphyn.cPlanner.common.LogManager;
import org.griphyn.cPlanner.common.PegasusProperties;

import java.io.PrintWriter;
import java.io.IOException;

import java.util.StringTokenizer;


/**
 * This implementation generates files that can be understood by Stork.
 *
 * @author Karan Vahi
 * @version $Revision: 1.1 $
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
    private String mCredName;


    /**
     * The default constructor.
     */
    public Stork(){
        super();
        mLogger = LogManager.getInstance();
    }

    /**
     * Initializes the Code Generator implementation.
     *
     * @param properties the <code>PegasusProperties</code> object containing all
     *                   the properties required by Pegasus.
     * @param directory  the base directory where the generated code should reside.
     * @param options    the options passed to the planner at runtime.
     *
     * @throws CodeGeneratorException in case of any error occuring code generation.
     */
    public void initialize( PegasusProperties properties,
                            String directory,
                            PlannerOptions options) throws CodeGeneratorException{
        super.initialize( properties, directory, options );
        mCredName = mProps.getCredName();
    }

    /**
     * Generates the code for the concrete workflow in the input format of the
     * workflow executor being used. The method is not yet implemented.
     *
     * @param dag  the concrete workflow.
     *
     * @throws CodeGeneratorException in case of any error occuring code generation.
     */
    public void generateCode( ADag dag ) throws CodeGeneratorException{
        throw new CodeGeneratorException(
            new UnsupportedOperationException(
                 "Stork Code Generator: Method generateCode( ADag) not implemeneted"));
    }

    /**
     * Generates the code for a single job in the Stork format.
     *
     * @param dag    the dag of which the job is a part of.
     * @param job    the <code>SubInfo</code> object holding the information about
     *               that particular job.
     *
     *
     * @throws CodeGeneratorException in case of any error occuring code generation.
     */
    public void generateCode( ADag dag, SubInfo job ) throws CodeGeneratorException{
        String dagname  = dag.dagInfo.nameOfADag;
        String dagindex = dag.dagInfo.index;
        String dagcount = dag.dagInfo.count;


        StringTokenizer st = new StringTokenizer(job.strargs,"\n");
        String srcUrl = (st.hasMoreTokens())?st.nextToken():null;
        String dstUrl = (st.hasMoreTokens())?st.nextToken():null;

        //sanity check
        if(mCredName == null){
            mLogger.log("Credential name needs to be specified for " +
                        " stork job. Set pegasus.transfer.stork.cred property",
                        LogManager.ERROR_MESSAGE_LEVEL);
            throw new CodeGeneratorException(
                "Credential name needs to be specified for " +
                " stork job. Set pegasus.transfer.stork.cred property");

        }

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
        writer.println(" * GRIPHYN VDS STORK FILE GENERATOR");
        writer.println(" * DAG : " + dagname + ", Index = " + dagindex +
                       ", Count = " + dagcount);
        writer.println(" * STORK FILE NAME : " + this.getFileBaseName(job));
        writer.println(this.mEndSeparator);

        writer.println("[");

        writer.println("\tdap_type = \"" + job.logicalName + "\";");
        writer.println("\tsrc_url = \"" + srcUrl + "\";");
        writer.println("\tdest_url = \"" + dstUrl + "\";");
        writer.println("\tcred_name = \"" + mCredName + "\";");

        // DONE
        writer.println("]");

        writer.println(this.mStartSeparator);
        writer.println(" * END OF STORK FILE");
        writer.println(this.mEndSeparator);

        //flush the contents
        writer.close();
    }
}