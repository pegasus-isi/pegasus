/*
 * This file or a portion of this file is licensed under the terms of
 * the Globus Toolkit Public License, found in file GTPL, or at
 * http://www.globus.org/toolkit/download/license.html. This notice must
 * appear in redistributions of this file, with or without modification.
 *
 * Redistributions of this Software, with or without modification, must
 * reproduce the GTPL in: (1) the Software, or (2) the Documentation or
 * some other similar material which is provided with the Software (if
 * any).
 *
 * Copyright 1999-2004 University of Chicago and The University of
 * Southern California. All rights reserved.
 */

package org.griphyn.cPlanner.cluster;

import org.griphyn.cPlanner.classes.ADag;
import org.griphyn.cPlanner.classes.PegasusBag;

import org.griphyn.cPlanner.common.PegasusProperties;

import org.griphyn.cPlanner.partitioner.Partitioner;
import org.griphyn.cPlanner.partitioner.PartitionerFactory;
import org.griphyn.cPlanner.partitioner.PartitionerFactoryException;
import org.griphyn.cPlanner.partitioner.graph.GraphNode;

import org.griphyn.common.util.DynamicLoader;

import java.util.Map;
import java.util.HashMap;

/**
 * A factory class to load the appropriate Partitioner, and Clusterer Callback
 * for clustering. An abstract factory, as it loads the appropriate partitioner
 * matching a clustering technique.
 *
 *
 * @author Karan Vahi vahi@isi.edu
 * @version $Revision$
 */

public class ClustererFactory {

    /**
     * The default package where all the implementations reside.
     */
    public static final String DEFAULT_PACKAGE_NAME = "org.griphyn.cPlanner.cluster";

    /**
     * The name of the class implementing horizontal clustering.
     */
    public static final String HORIZONTAL_CLUSTERING_CLASS = "Horizontal";

    /**
     * The name of the class implementing vertical clustering.
     */
    public static final String VERTICAL_CLUSTERING_CLASS = "Vertical";

    /**
     * The type corresponding to label based clustering.
     */
    private static final String LABEL_CLUSTERING_TYPE = "label";

    /**
     * The table that maps clustering technique to a partitioner.
     */
    private static Map mPartitionerTable;

    /**
     * The table that maps a clustering technique to a clustering impelemntation.
     */
    private static Map mClustererTable;


    /**
     * Loads the appropriate partitioner on the basis of the clustering type
     * specified in the options passed to the planner.
     *
     * @param properties the <code>PegasusProperties</code> object containing all
     *                   the properties required by Pegasus.
     * @param type       type of clustering to be used.
     * @param root       the dummy root node of the graph.
     * @param graph      the map containing all the nodes of the graph keyed by
     *                   the logical id of the nodes.
     *
     * @return the instance of the appropriate partitioner.
     *
     * @throws ClustererFactoryException that nests any error that
     *         might occur during the instantiation
     *
     * @see #DEFAULT_PACKAGE_NAME
     */
    public static  Partitioner loadPartitioner(PegasusProperties properties,
                                               String type,
                                               GraphNode root,
                                               Map graph )
                                               throws ClustererFactoryException{

        String clusterer = type;

        //sanity check
        if( clusterer == null ){
            throw new ClustererFactoryException( "No Clustering Technique Specified ");
        }

        //try to find the appropriate partitioner
        Object partitionerClass = partitionerTable().get( clusterer );
        if ( partitionerClass == null ){
            throw new ClustererFactoryException(
                "No matching partitioner found for clustering technique " + clusterer );
        }

        //now load the partitioner
        Partitioner partitioner = null;
        try{
            partitioner = PartitionerFactory.loadInstance( properties,
                                                           root,
                                                           graph,
                                                           (String) partitionerClass );
        }
        catch ( PartitionerFactoryException e ){
            throw new ClustererFactoryException( " Unable to instantiate partitioner " + partitionerClass,
                                                 e );
        }
        return partitioner;
    }

    /**
     * Loads the appropriate clusterer on the basis of the clustering type
     * specified in the options passed to the planner.
     *
     * @param dag        the workflow being clustered.
     * @param bag        the bag of initialization objects.
     * @param type       type of clustering to be used.
     *
     * @return the instance of the appropriate clusterer.
     *
     * @throws ClustererFactoryException that nests any error that
     *         might occur during the instantiation
     *
     * @see #DEFAULT_PACKAGE_NAME
     */
    public static  Clusterer loadClusterer( ADag dag,
                                            PegasusBag bag,
                                            String type )
                                            throws ClustererFactoryException{


        //sanity check
        if( type == null ){
            throw new ClustererFactoryException( "No Clustering Technique Specified ");
        }

        //try to find the appropriate clusterer
        Object clustererClass = clustererTable().get( type );
        if ( clustererClass == null ){
            throw new ClustererFactoryException(
                "No matching clusterer found for clustering technique " + type );
        }

        //now load the clusterer
        Clusterer clusterer = null;
        String className = ( String )clustererClass;
        try{

            //prepend the package name if required
            className = ( className.indexOf('.') == -1 )?
                        //pick up from the default package
                        DEFAULT_PACKAGE_NAME + "." + className:
                        //load directly
                        className;

            //try loading the class dynamically
            DynamicLoader dl = new DynamicLoader( className );
            clusterer = (Clusterer) dl.instantiate( new Object[0] );
            clusterer.initialize( dag, bag );
        }
        catch ( Exception e ){
            throw new ClustererFactoryException( " Unable to instantiate partitioner ",
                                                 className,
                                                 e );
        }
        return clusterer;
    }



    /**
     * Returns a table that maps, the clustering technique to an appropriate
     * class implementing that clustering technique.
     *
     * @return a Map indexed by clustering styles, and values as corresponding
     *         implementing Clustering classes.
     */
    private static Map clustererTable(){
        if( mClustererTable == null ){
            mClustererTable = new HashMap(3);
            mClustererTable.put( HORIZONTAL_CLUSTERING_CLASS.toLowerCase(),
                                 HORIZONTAL_CLUSTERING_CLASS );
            mClustererTable.put( VERTICAL_CLUSTERING_CLASS.toLowerCase(),
                                 VERTICAL_CLUSTERING_CLASS );
            mClustererTable.put( LABEL_CLUSTERING_TYPE.toLowerCase(),
                                 VERTICAL_CLUSTERING_CLASS );
        }
        return mClustererTable;
    }



    /**
     * Returns a table that maps, the clustering technique to an appropriate
     * partitioning technique.
     *
     * @return a Map indexed by clustering styles, and values as corresponding
     *         Partitioners.
     */
    private static Map partitionerTable(){
        if( mPartitionerTable == null ){
            mPartitionerTable = new HashMap(3);
            mPartitionerTable.put( HORIZONTAL_CLUSTERING_CLASS.toLowerCase(),
                                   PartitionerFactory.LEVEL_BASED_PARTITIONING_CLASS );
            mPartitionerTable.put( VERTICAL_CLUSTERING_CLASS.toLowerCase(),
                                   PartitionerFactory.LABEL_BASED_PARTITIONING_CLASS );
            mPartitionerTable.put( LABEL_CLUSTERING_TYPE.toLowerCase(),
                                   PartitionerFactory.LABEL_BASED_PARTITIONING_CLASS );
        }
        return mPartitionerTable;
    }


}
