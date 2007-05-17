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
package org.griphyn.cPlanner.code.generator;

import org.griphyn.cPlanner.classes.ADag;
import org.griphyn.cPlanner.classes.SubInfo;
import org.griphyn.cPlanner.classes.PlannerOptions;

import org.griphyn.cPlanner.code.CodeGenerator;
import org.griphyn.cPlanner.code.generator.condor.CondorGenerator;

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
 * @version $Revision: 1.1 $
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