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
package edu.isi.pegasus.planner.partitioner;

import java.util.List;

/**
 * This interface defines the callback calls from the partitioners. The partitioners call out to the
 * appropriate callback methods as and when they determine that a partition has been constructed.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public interface Callback {

    /**
     * Callback for when a partitioner determines that partition has been constructed.
     *
     * @param partition the constructed partition.
     */
    public void cbPartition(Partition partition);

    /**
     * Callback for when a partitioner determines the relations between partitions that it has
     * previously constructed.
     *
     * @param child the id of a partition.
     * @param parents the list of <code>String</code> objects that contain the id's of the parents
     *     of the partition.
     */
    public void cbParents(String child, List parents);

    /** Callback for the partitioner to signal that it is done with the processing. */
    public void cbDone();
}
