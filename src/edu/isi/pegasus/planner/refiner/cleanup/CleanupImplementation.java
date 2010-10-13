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


package edu.isi.pegasus.planner.refiner.cleanup;

import edu.isi.pegasus.planner.classes.SubInfo;

import edu.isi.pegasus.planner.classes.PegasusBag;


import java.util.List;

/**
 * The interface that defines how the cleanup job is invoked and created.
 *
 * @author  Karan Vahi
 * @version $Revision$
 */
public interface CleanupImplementation {


    /**
     * The version number associated with this API Cleanup CleanupImplementation.
     */
    public static final String VERSION = "1.1";

    /**
     * Intializes the class.
     *
     * @param bag      bag of initialization objects
     */
    public void initialize( PegasusBag bag ) ;
    
    /**
     * Creates a cleanup job that removes the files from remote working directory.
     * This will eventually make way to it's own interface.
     *
     * @param id         the identifier to be assigned to the job.
     * @param files      the list of <code>PegasusFile</code> that need to be
     *                   cleaned up.
     * @param job        the primary compute job with which this cleanup job is associated.
     *
     * @return the cleanup job.
     */
    public SubInfo createCleanupJob( String id, List files, SubInfo job );



}
