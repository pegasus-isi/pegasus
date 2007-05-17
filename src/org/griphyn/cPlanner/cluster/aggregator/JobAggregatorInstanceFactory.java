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

import org.griphyn.cPlanner.classes.ADag;
import org.griphyn.cPlanner.classes.SubInfo;

import org.griphyn.cPlanner.common.PegasusProperties;

import org.griphyn.cPlanner.namespace.VDS;

import java.util.Iterator;
import java.util.Map;
import java.util.HashMap;

/**
 * A JobAggergator factory that caches up the loaded implementations.
 * It loads a new implementation only if it has not loaded it earlier.
 * However, it is different from a Singleton Factory, as the implementations
 * are not stored in static instances. Hence, two different instances of this
 * Factory can load different instances of the same implementation.
 *
 *
 * @author Karan Vahi
 * @version $Revision$
 *
 * @see JobAggregatorFactory
 */

public class JobAggregatorInstanceFactory {
    /**
     * A table that maps, VDS style keys to the names of the corresponding classes
     * implementing the CondorStyle interface.
     */
    private static Map mImplementingClassNameTable;


    /**
     * A table that maps, VDS style keys to appropriate classes implementing the
     * JobAggregator interface
     */
    private  Map mImplementingClassTable ;



    /**
     * The handle to the properties object holding all the properties.
     */
    protected PegasusProperties mProps;

    /**
     * ADag object containing the jobs that have been scheduled by the site
     * selector.
     */
    private ADag mDAG;

    /**
     * The submit directory.
     */
    private String mSubmitDir;

    /**
     * A boolean indicating that the factory has been initialized.
     */
    private boolean mInitialized;


    /**
     * The default constructor.
     */
    public JobAggregatorInstanceFactory() {
        mInitialized = false;
        mImplementingClassTable = new HashMap(3);
    }

    /**
     * Initializes the Factory. Loads all the implementations just once.
     *
     * @param properties  the <code>PegasusProperties</code> object containing all
     *                    the properties required by Pegasus.
     * @param dag        the workflow that is being clustered.
     * @param submitDir  the submit directory where the submit files for the job
     *                   has to be generated.
     *
     * @throws JobAggregatorFactoryException that nests any error that
     *            might occur during the instantiation of the implementation.
     */
    public void initialize( PegasusProperties properties,
                            ADag dag,
                            String submitDir
                            ) throws JobAggregatorFactoryException{


        mProps     = properties;
        mDAG       = dag;
        mSubmitDir = submitDir;

        //load all the implementations that correspond to the VDS style keys
        for( Iterator it = this.implementingClassNameTable().entrySet().iterator(); it.hasNext(); ){
            Map.Entry entry = (Map.Entry) it.next();
            String aggregator = (String)entry.getKey();
            String className  = (String)entry.getValue();

            //load via reflection. not required in this case though
            put( aggregator, JobAggregatorFactory.loadInstance( className, mProps, mSubmitDir, mDAG ));
        }

        //we have successfully loaded all implementations
        mInitialized = true;
    }



    /**
     * Returns the appropriate handle to the JobAggregator that is to be used
     * for a particular type of job. Aggregators for mpiexec and seqexec are
     * already loaded in the constructor, and just the reference is returned.
     * For any other aggregator it is dynamically loaded.
     *
     * @param job  the job corresponding to which the aggregator is to be
     *             loaded.
     *
     * @return the appropriate JobAggregator
     *
     * @throws JobAggregatorFactoryException that nests any error that
     *            might occur during the instantiation
     *
     */
    public JobAggregator loadInstance( SubInfo job ) throws JobAggregatorFactoryException{
        //sanity checks first
        if( !mInitialized ){
            throw new JobAggregatorFactoryException(
                "JobAggregatorFactory needs to be initialized first before using" );
        }


        Object obj;
        String shortName = ((obj =job.vdsNS.get(VDS.COLLAPSER_KEY))==null)?
                            //pick the one from the properties
                            mProps.getJobAggregator():
                            (String)obj;

        //now look up the job aggregator
        Object aggregator = this.get( shortName.toLowerCase() );
        if ( aggregator == null ) {
            //load via reflection
            aggregator = JobAggregatorFactory.loadInstance( shortName, mProps, mSubmitDir, mDAG);

            //throw exception if still null
            if (aggregator == null ){
                throw new JobAggregatorFactoryException( "Unsupported aggregator " + shortName);
            }

            //register in cache
            this.put( shortName, aggregator );
        }


        return (JobAggregator)aggregator;
    }

    /**
     * Returns the implementation from the implementing class table.
     *
     * @param style           the aggregator style
     *
     * @return implementation  the class implementing that style, else null
     */
    private Object get( String style ){
        return mImplementingClassTable.get( style);
    }


    /**
     * Inserts an entry into the implementing class table.
     *
     * @param style           the aggregator style
     * @param implementation  the class implementing that aggregator.
     */
    private void put( String style, Object implementation){
        mImplementingClassTable.put( style.toLowerCase(), implementation );
    }


    /**
     * Returns a table that maps, the VDS style keys to the names of implementing
     * classes.
     *
     * @return a Map indexed by VDS styles, and values as names of implementing
     *         classes.
     */
    private static Map implementingClassNameTable(){
        if( mImplementingClassNameTable == null ){
            mImplementingClassNameTable = new HashMap(3);
            mImplementingClassNameTable.put( JobAggregatorFactory.SEQ_EXEC_CLASS.toLowerCase(),
                                             JobAggregatorFactory.SEQ_EXEC_CLASS);
            mImplementingClassNameTable.put( JobAggregatorFactory.MPI_EXEC_CLASS.toLowerCase(),
                                             JobAggregatorFactory.MPI_EXEC_CLASS);
        }
        return mImplementingClassNameTable;
    }



}