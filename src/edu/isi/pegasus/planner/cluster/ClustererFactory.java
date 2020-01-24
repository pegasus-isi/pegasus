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

import edu.isi.pegasus.common.util.DynamicLoader;
import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.planner.partitioner.Partitioner;
import edu.isi.pegasus.planner.partitioner.PartitionerFactory;
import edu.isi.pegasus.planner.partitioner.PartitionerFactoryException;
import edu.isi.pegasus.planner.partitioner.graph.GraphNode;
import java.util.HashMap;
import java.util.Map;

/**
 * A factory class to load the appropriate Partitioner, and Clusterer Callback for clustering. An
 * abstract factory, as it loads the appropriate partitioner matching a clustering technique.
 *
 * @author Karan Vahi vahi@isi.edu
 * @version $Revision$
 */
public class ClustererFactory {

    /** The default package where all the implementations reside. */
    public static final String DEFAULT_PACKAGE_NAME = "edu.isi.pegasus.planner.cluster";

    /** The name of the class implementing horizontal clustering. */
    public static final String HORIZONTAL_CLUSTERING_CLASS = "Horizontal";

    /** The name of the class implementing vertical clustering. */
    public static final String VERTICAL_CLUSTERING_CLASS = "Vertical";

    /** The type corresponding to label based clustering. */
    private static final String LABEL_CLUSTERING_TYPE = "label";

    /** The table that maps clustering technique to a partitioner. */
    private static Map mPartitionerTable;

    /** The table that maps a clustering technique to a clustering impelemntation. */
    private static Map mClustererTable;

    /**
     * Loads the appropriate partitioner on the basis of the clustering type specified in the
     * options passed to the planner.
     *
     * @param properties the <code>PegasusProperties</code> object containing all the properties
     *     required by Pegasus.
     * @param type type of clustering to be used.
     * @param root the dummy root node of the graph.
     * @param graph the map containing all the nodes of the graph keyed by the logical id of the
     *     nodes.
     * @return the instance of the appropriate partitioner.
     * @throws ClustererFactoryException that nests any error that might occur during the
     *     instantiation
     * @see #DEFAULT_PACKAGE_NAME
     */
    public static Partitioner loadPartitioner(
            PegasusProperties properties, String type, GraphNode root, Map graph)
            throws ClustererFactoryException {

        String clusterer = type;

        // sanity check
        if (clusterer == null) {
            throw new ClustererFactoryException("No Clustering Technique Specified ");
        }

        // try to find the appropriate partitioner
        Object partitionerClass = partitionerTable().get(clusterer);
        if (partitionerClass == null) {
            throw new ClustererFactoryException(
                    "No matching partitioner found for clustering technique " + clusterer);
        }

        // now load the partitioner
        Partitioner partitioner = null;
        try {
            partitioner =
                    PartitionerFactory.loadInstance(
                            properties, root, graph, (String) partitionerClass);
        } catch (PartitionerFactoryException e) {
            throw new ClustererFactoryException(
                    " Unable to instantiate partitioner " + partitionerClass, e);
        }
        return partitioner;
    }

    /**
     * Loads the appropriate clusterer on the basis of the clustering type specified in the options
     * passed to the planner.
     *
     * @param dag the workflow being clustered.
     * @param bag the bag of initialization objects.
     * @param type type of clustering to be used.
     * @return the instance of the appropriate clusterer.
     * @throws ClustererFactoryException that nests any error that might occur during the
     *     instantiation
     * @see #DEFAULT_PACKAGE_NAME
     */
    public static Clusterer loadClusterer(ADag dag, PegasusBag bag, String type)
            throws ClustererFactoryException {

        // sanity check
        if (type == null) {
            throw new ClustererFactoryException("No Clustering Technique Specified ");
        }

        // try to find the appropriate clusterer
        Object clustererClass = clustererTable().get(type);
        if (clustererClass == null) {
            throw new ClustererFactoryException(
                    "No matching clusterer found for clustering technique " + type);
        }

        // now load the clusterer
        Clusterer clusterer = null;
        String className = (String) clustererClass;
        try {

            // prepend the package name if required
            className =
                    (className.indexOf('.') == -1)
                            ?
                            // pick up from the default package
                            DEFAULT_PACKAGE_NAME + "." + className
                            :
                            // load directly
                            className;

            // try loading the class dynamically
            DynamicLoader dl = new DynamicLoader(className);
            clusterer = (Clusterer) dl.instantiate(new Object[0]);
            clusterer.initialize(dag, bag);
        } catch (Exception e) {
            throw new ClustererFactoryException(
                    " Unable to instantiate partitioner ", className, e);
        }
        return clusterer;
    }

    /**
     * Returns a table that maps, the clustering technique to an appropriate class implementing that
     * clustering technique.
     *
     * @return a Map indexed by clustering styles, and values as corresponding implementing
     *     Clustering classes.
     */
    private static Map clustererTable() {
        if (mClustererTable == null) {
            mClustererTable = new HashMap(3);
            mClustererTable.put(
                    HORIZONTAL_CLUSTERING_CLASS.toLowerCase(), HORIZONTAL_CLUSTERING_CLASS);
            mClustererTable.put(VERTICAL_CLUSTERING_CLASS.toLowerCase(), VERTICAL_CLUSTERING_CLASS);
            mClustererTable.put(LABEL_CLUSTERING_TYPE.toLowerCase(), VERTICAL_CLUSTERING_CLASS);
        }
        return mClustererTable;
    }

    /**
     * Returns a table that maps, the clustering technique to an appropriate partitioning technique.
     *
     * @return a Map indexed by clustering styles, and values as corresponding Partitioners.
     */
    private static Map partitionerTable() {
        if (mPartitionerTable == null) {
            mPartitionerTable = new HashMap(3);
            mPartitionerTable.put(
                    HORIZONTAL_CLUSTERING_CLASS.toLowerCase(),
                    PartitionerFactory.LEVEL_BASED_PARTITIONING_CLASS);
            mPartitionerTable.put(
                    VERTICAL_CLUSTERING_CLASS.toLowerCase(),
                    PartitionerFactory.LABEL_BASED_PARTITIONING_CLASS);
            mPartitionerTable.put(
                    LABEL_CLUSTERING_TYPE.toLowerCase(),
                    PartitionerFactory.LABEL_BASED_PARTITIONING_CLASS);
        }
        return mPartitionerTable;
    }
}
