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

package edu.isi.pegasus.planner.transfer;

import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.Job;



import java.io.File;

/**
 * This interface defines the second level staging process, that manages
 * the transfer of files from the headnode to the worker node temp and back.
 *
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public interface SLS {

    /**
     * The version associated with the API.
     */
    public static final String VERSION = "1.1";

    /**
     * Initializes the SLS implementation.
     *
     * @param bag   the bag of objects. Contains access to catalogs etc.
     */
    public void initialize( PegasusBag bag );


    /**
     * Returns a boolean whether the SLS implementation does a condor based
     * modification or not. By condor based modification we mean whether it
     * uses condor specific classads to achieve the second level staging or not.
     *
     * @return boolean
     */
    public boolean doesCondorModifications();

    /**
     * Constructs a command line invocation for a job, with a given sls file.
     * The SLS maybe null. In the case where SLS impl does not read from a file,
     * it is advised to create a file in generateSLSXXX methods, and then read
     * the file in this function and put it on the command line.
     *
     * @param job          the job that is being sls enabled
     * @param slsFile      the slsFile  that is accessible on the worker node. Can be null
     *
     * @return invocation string
     */
    public String invocationString( Job job, File slsFile );

    /**
     * Returns a boolean indicating whether it will an input file for a job
     * to do the transfers. Transfer reads from stdin the file transfers that
     * it needs to do.
     *
     * @param job the job being detected.
     *
     * @return true
     */
    public boolean needsSLSInput( Job job );

    /**
     * Returns a boolean indicating whether it will an output file for a job
     * to do the transfers. Transfer reads from stdin the file transfers that
     * it needs to do.
     *
     * @param job the job being detected.
     *
     * @return true
     */
    public boolean needsSLSOutput( Job job );


    /**
     * Returns the LFN of sls input file.
     *
     * @param job Job
     *
     * @return the name of the sls input file.
     */
    public String getSLSInputLFN( Job job );


    /**
     * Returns the LFN of sls output file.
     *
     * @param job Job
     *
     * @return the name of the sls input file.
     */
    public String getSLSOutputLFN( Job job );


    /**
     * Generates a second level staging file of the input files to the worker node
     * directory. It should be  consistent with the function needsSLSFile( Job )
     *
     * @param job        the job for which the file is being created
     * @param submitDir  the submit directory where it has to be written out.
     * @param fileName    the name of the file that needs to be written out.
     * @param stagingSiteDirectory  the directory on the head node of the staging site.
     * @param workerNodeDirectory  the worker node directory
     *
     * @return the full path to lof file created, else null if no file is written out.
     *
     * @see #needsSLSInput( Job)
     */
    public File generateSLSInputFile( Job job,
                                      String fileName,
                                      String submitDir,
                                      String stagingSiteDirectory,
                                      String workerNodeDirectory );

    /**
     * Generates a second level staging file of the input files to the worker node
     * directory. It should be  consistent with the function needsSLSFile( Job )
     *
     * @param job        the job for which the file is being created
     * @param submitDir  the submit directory where it has to be written out.
     * @param fileName    the name of the file that needs to be written out.
     * @param stagingSiteDirectory  the directory on the head node of the staging site.
     * @param workerNodeDirectory  the worker node directory
     *
     * @return the full path to lof file created, else null if no file is written out.
     *
     * @see #needsSLSOutput( Job)
     */
    public File generateSLSOutputFile( Job job,
                                       String fileName,
                                       String submitDir,
                                       String stagingSiteDirectory,
                                       String workerNodeDirectory );





    /**
     * Modifies a job for the first level staging to headnode.This is to add
     * any files that needs to be staged to the head node for a job specific
     * to the SLS implementation. If any file needs to be added, a <code>FileTransfer</code>
     * object should be created and added as an input or an output file.
     *
     *
     * @param job           the job
     * @param submitDir     the submit directory
     * @param slsInputLFN   the sls input file if required, that is used for
     *                      staging in from the head node to worker node directory.
     * @param slsOutputLFN  the sls output file if required, that is used
     *                      for staging in from the head node to worker node directory.
     * @return boolean
     */
    public boolean modifyJobForFirstLevelStaging( Job job,
                                                  String submitDir,
                                                  String slsInputLFN,
                                                  String slsOutputLFN );





    /**
     * Modifies a compute job for second level staging.
     *
     * @param job                 the job to be modified.
     * @param stagingSiteURLPrefix   the url prefix for the server on the staging site
     * @param stagingSitedirectory   the directory on the staging site, where the input data is
     *                            read from and the output data written out.
     * @param headNodeURLPrefix   the url prefix for the server on the headnode
     * @param headNodeDirectory   the directory on the headnode, where the input data is
     *                            read from and the output data written out.
     * @param workerNodeDirectory the directory in the worker node tmp
     *
     * @return boolean indicating whether job was successfully modified or not.
     */
    public boolean modifyJobForWorkerNodeExecution( Job job, 
                                                    String stagingSiteURLPrefix,
                                                    String stagingSitedirectory,
                                                    String headNodeURLPrefix,
                                                    String headNodeDirectory,
                                                    String workerNodeDirectory  );
    }
