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

package edu.isi.pegasus.planner.transfer.sls;


import edu.isi.pegasus.common.util.S3cfg;
import edu.isi.pegasus.planner.classes.Job;


/**
 * This implementation of the SLS API allows us to use pegasus-transfer to retrieve
 * data from S3 bucket for worker node execution.
 *
 * @author Karan Vahi
 * @author Mats Rynge
 * @version $Revision$
 */
public class S3 extends Transfer3 {

    /**
     * Modifies a job for the first level staging to headnode.This is to add
     * any files that needs to be staged to the head node for a job specific
     * to the SLS implementation. If any file needs to be added, a <code>FileTransfer</code>
     * object should be created and added as an input or an output file.
     * A job is not modified the staging of the sls file is turned of by
     * setting the property specified by STAGE_SLS_FILE_PROPERTY_KEY
     *
     *
     * @param job           the job
     * @param submitDir     the submit directory
     * @param slsInputLFN   the sls input file if required, that is used for
     *                      staging in from the head node to worker node directory.
     * @param slsOutputLFN  the sls output file if required, that is used
     *                      for staging in from the head node to worker node directory.
     * @return boolean
     *
     * @see #STAGE_SLS_FILE_PROPERTY_KEY
     */
    public boolean modifyJobForFirstLevelStaging( Job job,
                                                  String submitDir,
                                                  String slsInputLFN,
                                                  String slsOutputLFN ) {
        
        return super.modifyJobForFirstLevelStaging(job, submitDir, slsInputLFN, slsOutputLFN);
    }


}
