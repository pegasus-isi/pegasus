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

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.common.logging.LogManagerFactory;
import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.planner.partitioner.graph.GraphNode;
import java.util.Map;

/**
 * The abstract class that lays out the api to do the partitioning of the dax into smaller daxes. It
 * defines additional functions to get and set the name of the partitions etc.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public abstract class Partitioner {

    /** The package name where the implementing classes of this interface reside. */
    public static final String PACKAGE_NAME = "org.griphyn.cPlanner.partitioner";

    /** The version number associated with this API of Code Generator. */
    public static final String VERSION = "1.2";

    /** The root node of the graph from where to start the BFS. */
    protected GraphNode mRoot;

    /**
     * The map containing all the graph nodes. The key to the map are the logical id's of the jobs
     * as identified in the dax and the values are the corresponding Graph Node objects.
     */
    protected Map mGraph;

    /** The handle to the internal logging object. */
    protected LogManager mLogger;

    /** The object holding all the properties pertaining to Pegasus. */
    protected PegasusProperties mProps;

    /**
     * The overloaded constructor.
     *
     * @param root the dummy root node of the graph.
     * @param graph the map containing all the nodes of the graph keyed by the logical id of the
     *     nodes.
     * @param properties the properties passed out to the planner.
     */
    public Partitioner(GraphNode root, Map graph, PegasusProperties properties) {
        mRoot = root;
        mGraph = graph;
        mLogger = LogManagerFactory.loadSingletonInstance(properties);
        mProps = properties;
        // set a default name to the partition dax
        // mPDAXWriter = null;

    }

    /**
     * The main function that ends up traversing the graph structure corrsponding to the dax and
     * creates the smaller dax files(one dax file per partition) and the .pdax file that illustrates
     * the partition graph. It is recommended that the implementing classes use the already
     * initialized handles to the DAXWriter and PDAXWriter interfaces to write out the xml files.
     * The advantage of using these preinitialized handles is that they already are correctly
     * configured for the directories where Pegasus expects the submit files and dax files to
     * reside.
     *
     * @param c the callback object that the partitioner calls out to.
     */
    public abstract void determinePartitions(Callback c);

    /**
     * Returns a textual description of the transfer implementation.
     *
     * @return a short textual description
     */
    public abstract String description();
}
