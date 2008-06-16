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

package org.griphyn.cPlanner.visualize;

import org.griphyn.vdl.invocation.Data;
import org.griphyn.vdl.invocation.Job;
import org.griphyn.vdl.invocation.StatInfo;

import java.util.List;
import java.util.Map;

/**
 * This callback interface has methods to handle the data sections
 * for stdout, stderr and stdin.
 *
 * @author Karan Vahi vahi@isi.edu
 * @version $Revision$
 */
public interface Callback {

    /**
     * The version of the API.
     */
    public static final String VERSION ="1.3";

    /**
     * Initializes the callback.
     *
     * @param directory   the directory where all the files reside.
     * @param useStatInfo  boolean indicating whether to use stat info or not.
     */
    public void initialize( String directory, boolean useStatInfo );

    /**
     * Callback for the starting of an invocation record.
     *
     * @param job      the job/file being parsed.
     * @param resource  the site id where the job was executed.
     */
    public void cbInvocationStart( String job, String resource );

    /**
     * Callback function for the data section of stdin. Since the jobs
     * ( setup, prejob, main, postjob, cleanup)
     * do not have separate stdout etc, all are passed.
     *
     * @param jobs  all the jobs specified in the kickstart record.
     * @param data  the data contents as String.
     *
     */
    public void cbStdIN( List jobs, String data );

    /**
     * Callback function for the data section of stdout. Since the jobs
     * ( setup, prejob, main, postjob, cleanup)
     * do not have separate stdout etc, all are passed.
     *
     * @param jobs  all the jobs specified in the kickstart record.
     * @param data  the data contents as String.
     *
     */
    public void cbStdOut( List jobs, String data );

    /**
     * Callback function for the data section of stderr. Since the jobs
     * ( setup, prejob, main, postjob, cleanup)
     * do not have separate stdout etc, all are passed.
     *
     * @param jobs  all the jobs specified in the kickstart record.
     * @param data  the data contents as String.
     *
     */
    public void cbStdERR( List jobs, String data );

    /**
     * Callback function for when stat information for an input file is
     * encountered
     *
     * @param filename  the name of the file.
     * @param info      the <code>StatInfo</code> about the file.
     *
     */
    public void cbInputFile( String filename, StatInfo info );

    /**
     * Callback function for when stat information for an output file is
     * encountered
     *
     * @param filename  the name of the file.
     * @param info      the <code>StatInfo</code> about the file.
     *
     */
    public void cbOutputFile( String filename, StatInfo info );



    /**
     * Callback signalling that an invocation record has been parsed.
     *
     */
    public void cbInvocationEnd( );

    /**
     * Callback signalling that we are done with the parsing of the files.
     */
    public void done();

    /**
     * Returns the object constructed.
     *
     * @return the <code>Object</code> constructed.
     */
    public Object getConstructedObject();
}
