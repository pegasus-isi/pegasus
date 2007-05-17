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

package org.griphyn.cPlanner.cluster.aggregator;

import org.griphyn.cPlanner.cluster.JobAggregator;

import org.griphyn.cPlanner.common.PegasusProperties;

import org.griphyn.cPlanner.classes.ADag;

import org.griphyn.common.util.DynamicLoader;


/**
 * A factory class to load the appropriate JobAggregator implementations while
 * clustering jobs.
 *
 * @author Karan Vahi
 * @version $Revision$
 */

public class JobAggregatorFactory {

    /**
     * Package to prefix "just" class names with.
     */
    public static final String DEFAULT_PACKAGE_NAME =
                                             "org.griphyn.cPlanner.cluster.aggregator";


    /**
     * The name of the class in this package, that corresponds to seqexec.
     * This is required to load the correct class, even though the user
     * specifyies a class that matches on ignoring case, but not directly.
     */
    public static final String SEQ_EXEC_CLASS = "SeqExec";


    /**
     * The name of the class in this package, that corresponds to mpiexec.
     * This is required to load the correct class, even though the user
     * specifyies a class that matches on ignoring case, but not directly.
     */
    public static final String MPI_EXEC_CLASS = "MPIExec";

    /**
     * Loads the implementing class corresponding to the mode specified by the user
     * at runtime in the properties file. The properties object passed should not
     * be null.
     *
     * @param properties the <code>PegasusProperties</code> object containing all
     *                   the properties required by Pegasus.
     * @param submitDir  the submit directory where the submit files for the job
     *                   has to be generated.
     * @param dag        the workflow that is being clustered.
     *
     * @return the instance of the class implementing this interface.
     *
     * @throws JobAggregatorFactoryException that nests any error that
     *            might occur during the instantiation
     *
     * @see #DEFAULT_PACKAGE_NAME
     */
    public static JobAggregator loadInstance( PegasusProperties properties,
                                              String submitDir,
                                              ADag dag) {

        return loadInstance(
                      properties.getJobAggregator(), properties, submitDir, dag);
    }


    /**
     * Loads the implementing class corresponding to the class. If the package
     * name is not specified with the class, then class is assumed to be
     * in the DEFAULT_PACKAGE. The properties object passed should not be null.
     *
     * @param className  the name of the class that implements the mode. It is the
     *                   name of the class, not the complete name with package. That
     *                   is added by itself.
     * @param properties the <code>PegasusProperties</code> object containing all
     *                   the properties required by Pegasus.
     * @param submitDir  the submit directory where the submit file for the job
     *                   has to be generated.
     * @param dag        the workflow that is being clustered.
     *
     * @return the instance of the class implementing this interface.
     *
     * @throws JobAggregatorFactoryException that nests any error that
     *            might occur during the instantiation
     *
     * @see #DEFAULT_PACKAGE_NAME
     */
    public static JobAggregator loadInstance(String className,
                                             PegasusProperties properties,
                                             String submitDir,
                                             ADag dag) {

        //sanity check
        if(properties == null){
            throw new RuntimeException("Invalid properties passed");
        }
        if(className == null){
            throw new RuntimeException("Invalid class specified to load");
        }

        JobAggregator ja = null;
        try{
            //ensure that correct class is picked up in case
            //of mpiexec and seqexec
            if(className.equalsIgnoreCase(MPI_EXEC_CLASS)){
                className = MPI_EXEC_CLASS;
            }
            else if(className.equalsIgnoreCase(SEQ_EXEC_CLASS)){
                className = SEQ_EXEC_CLASS;
            }

            //prepend the package name if required
            className = (className.indexOf('.') == -1)?
                        //pick up from the default package
                        DEFAULT_PACKAGE_NAME + "." + className:
                        //load directly
                        className;

            //try loading the class dynamically
            DynamicLoader dl = new DynamicLoader( className);
            Object argList[] = new Object[3];
            argList[0] = properties;
            argList[1] = (submitDir == null) ? ".":submitDir;
            argList[2] = dag;
            ja = (JobAggregator) dl.instantiate(argList);
        }
        catch ( Exception e ) {
            throw new JobAggregatorFactoryException("Instantiating JobAggregator ",
                                                     className, e);
        }

        return ja;
    }



}