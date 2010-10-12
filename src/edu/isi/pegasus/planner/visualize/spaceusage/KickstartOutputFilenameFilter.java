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

package edu.isi.pegasus.planner.visualize.spaceusage;

import java.io.FilenameFilter;
import java.io.File;

import java.util.regex.Pattern;

/**
 * A filename filter for identifying the kickstart output files.
 *
 * @author Karan Vahi vahi@isi.edu
 * @version $Revision$
 */

public class KickstartOutputFilenameFilter implements FilenameFilter {

    /**
     * Store the regular expressions necessary to parse kickstart output files
     */
    private static final String mRegexExpression =
                                     "[a-zA-Z_0-9]*(.out)([.][0-9][0-9][0-9])*";

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
         boolean result = mPattern.matcher( name ).matches();

         //we want to ignore jobs starting with chmod
         return  result ? !name.startsWith( "chmod" ) : result;
     }

     public static void main( String[] args){
         KickstartOutputFilenameFilter f = new KickstartOutputFilenameFilter();
         System.out.println( f.accept( new java.io.File("."),
                                       "rc_tx_preprocess_ID000001_0.out.000" ));
         System.out.println( f.accept( new java.io.File("."),
                                       "blackdiamond-0.dag.dagman.out " ));

     }
}
