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
package org.griphyn.cPlanner.partitioner;


import org.griphyn.cPlanner.classes.PlannerOptions;

import org.griphyn.cPlanner.common.PegasusProperties;

import org.griphyn.common.util.DynamicLoader;

import org.griphyn.cPlanner.partitioner.graph.GraphNode;

import java.io.IOException;

import java.lang.reflect.InvocationTargetException;

import java.util.Map;

/**
 * A Factory class to load the right type of partitioner at runtime, as
 * specified by the Properties. Each invocation, results in a new partitioner
 * being loaded.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class PartitionerFactory {

    /**
     * Package to prefix "just" class names with.
     */
    public static final String DEFAULT_PACKAGE_NAME =
                                             "org.griphyn.cPlanner.partitioner";

    /**
     * The name of the class that does level based partitioning.
     */
    public static final String LEVEL_BASED_PARTITIONING_CLASS = "BFS";

    /**
     * The name of the class that does label based partitioning.
     */
    public static final String LABEL_BASED_PARTITIONING_CLASS = "Label";

    /**
     * The name of the class that does horizontal based partitioning.
     */
    public static final String HORIZONTAL_PARTITIONING_CLASS = "Horizontal";


    /**
     * The name of the class that does level based partitioning.
     */
    public static final String DEFAULT_PARTITIONING_CLASS = LEVEL_BASED_PARTITIONING_CLASS;


    /**
     * An array of known partitioning classes.
     */
    private static final String[] PARTITIONING_CLASSES = { LEVEL_BASED_PARTITIONING_CLASS,
                                                           LABEL_BASED_PARTITIONING_CLASS ,
                                                           HORIZONTAL_PARTITIONING_CLASS
                                                         };



    /**
     * Loads the implementing class corresponding to the type specified by the user.
     * The properties object passed should not be null.
     *
     * @param properties the <code>PegasusProperties</code> object containing all
     *                   the properties required by Pegasus.
     * @param root       the dummy root node of the graph.
     * @param graph      the map containing all the nodes of the graph keyed by
     *                   the logical id of the nodes.
     * @param className  the name of the implementing class.
     *
     * @return the instance of the class implementing this interface.
     *
     * @throws PartitionerFactoryException that nests any error that
     *         might occur during the instantiation
     *
     * @see #DEFAULT_PACKAGE_NAME
     */
    public static  Partitioner loadInstance(PegasusProperties properties,
                                            GraphNode root,
                                            Map graph,
                                            String className)
        throws PartitionerFactoryException{

        //sanity check
        if(properties == null){
            throw new NullPointerException("Invalid properties passed");
        }

        if( className.indexOf( '.' ) == -1 ){
            //compare with the known classes to ensure classnames
            //passed are case insensitive
            for( int i = 0; i < PARTITIONING_CLASSES.length; i++ ){
                if( className.equalsIgnoreCase( PARTITIONING_CLASSES[i] )){
                    className = PARTITIONING_CLASSES[i];
                    break;
                }
            }
            className = DEFAULT_PACKAGE_NAME + "." + className;
        }

        //try loading the class dynamically
        Partitioner partitioner = null;

        try{
            DynamicLoader dl = new DynamicLoader(className);
            Object argList[] = new Object[3];
            Class classList[] = new Class[3];
            argList[0] = root;
            //classList[0] = Class.forName( "org.griphyn.cPlanner.partitioner.GraphNode" );
            classList[0] = new GraphNode().getClass();//to circumvent root being null
            argList[1] = graph;
            classList[1] = Class.forName("java.util.Map");
            argList[2] = properties;
            classList[2] = Class.forName(
                "org.griphyn.cPlanner.common.PegasusProperties");
            partitioner = (Partitioner) dl.instantiate(classList, argList);
        }
        catch( Exception e ){
            throw new PartitionerFactoryException("Instantiating Partitioner ",
                                                  className, e);
        }

        return partitioner;
    }

}
