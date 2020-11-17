/**
 * Copyright 2007-2008 University Of Southern California
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.isi.pegasus.planner.refiner.createdir;

import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusBag;

/**
 * The interface that defines how the create dir job is created.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public interface Implementation {

    /** The version number associated with this API. */
    public static final String VERSION = "1.1";

    /**
     * Intializes the class.
     *
     * @param bag bag of initialization objects
     */
    public void initialize(PegasusBag bag);

    /**
     * It creates a make directory job that creates a directory on the remote pool using the perl
     * executable that Gaurang wrote. It access mkdir underneath.
     *
     * @param site the execution site for which the create dir job is to be created.
     * @param name the name that is to be assigned to the job.
     * @param directoryURL the externally accessible URL to the directory that is created
     * @return create dir job.
     */
    public Job makeCreateDirJob(String site, String name, String directoryURL);
}
