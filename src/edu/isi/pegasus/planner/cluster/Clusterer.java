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
package edu.isi.pegasus.planner.cluster;

import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.partitioner.Partition;
import java.util.List;

/**
 * The clustering API, that constructs clusters of jobs out of a single partition.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public interface Clusterer {

    /** The version number associated with this API of Code Generator. */
    public static final String VERSION = "1.1";

    /**
     * Initializes the Clusterer impelementation
     *
     * @param dag the workflow that is being clustered.
     * @param bag the bag of objects that is useful for initialization.
     * @throws ClustererException in case of error.
     */
    public void initialize(ADag dag, PegasusBag bag) throws ClustererException;

    /**
     * Determine the clusters for a partition.
     *
     * @param partition the partition for which the clusters need to be determined.
     * @throws ClustererException in case of error.
     */
    public void determineClusters(Partition partition) throws ClustererException;

    /**
     * Associates the relations between the partitions with the corresponding relations between the
     * clustered jobs that are created for each Partition.
     *
     * @param partitionID the id of a partition.
     * @param parents the list of <code>String</code> objects that contain the id's of the parents
     *     of the partition.
     * @throws ClustererException in case of error.
     */
    public void parents(String partitionID, List parents) throws ClustererException;

    /**
     * Returns the clustered workflow.
     *
     * @return the <code>ADag</code> object corresponding to the clustered workflow.
     * @throws ClustererException in case of error.
     */
    public ADag getClusteredDAG() throws ClustererException;

    /**
     * Returns a textual description of the transfer implementation.
     *
     * @return a short textual description
     */
    public String description();
}
