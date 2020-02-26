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

import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.PegasusBag;

/**
 * The interface that defines how the cleanup job is invoked and created.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public interface Strategy {

    /** The version number associated with this API Cleanup Strategy. */
    public static final String VERSION = "1.0";

    /**
     * Intializes the class.
     *
     * @param bag bag of initialization objects
     * @param impl the implementation instance that creates create dir job
     */
    public void initialize(PegasusBag bag, Implementation impl);

    /**
     * Modifies the workflow to add create directory nodes. The workflow passed is a worklow, where
     * the jobs have been mapped to sites.
     *
     * @param dag the workflow to which the nodes have to be added.
     * @return the added workflow
     */
    public ADag addCreateDirectoryNodes(ADag dag);
}
