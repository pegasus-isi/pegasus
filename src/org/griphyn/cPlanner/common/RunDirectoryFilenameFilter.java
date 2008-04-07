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
package org.griphyn.cPlanner.common;

import java.io.FilenameFilter;
import java.util.regex.Pattern;
import java.io.File;



/**
 * A filename filter for identifying the run directory
 *
 * @author Karan Vahi vahi@isi.edu
 */
public class RunDirectoryFilenameFilter implements FilenameFilter {

    /**
     * The prefix for the submit directory.
     */
    public static final String SUBMIT_DIRECTORY_PREFIX = "run";


    /**
     * Store the regular expressions necessary to parse kickstart output files
     */
    private static final String mRegexExpression =
                                     "(" + SUBMIT_DIRECTORY_PREFIX + ")([0-9][0-9][0-9][0-9])";

    /**
     * Stores compiled patterns at first use, quasi-Singleton.
     */
    private static Pattern mPattern = null;



    /***
     * Tests if a specified file should be included in a file list.
     *
     * @param dir the directory in which the file was found.
     * @param name - the name of the file.
     *
     * @return  true if and only if the name should be included in the file list
     *          false otherwise.
     *
     *
     */
     public boolean accept( File dir, String name) {
         //compile the pattern only once.
         if( mPattern == null ){
             mPattern = Pattern.compile( mRegexExpression );
         }
         return mPattern.matcher( name ).matches();
     }


}
