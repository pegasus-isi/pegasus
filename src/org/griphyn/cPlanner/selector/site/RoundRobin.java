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
package org.griphyn.cPlanner.selector.site;

import org.griphyn.cPlanner.poolinfo.PoolInfoProvider;
import org.griphyn.cPlanner.poolinfo.PoolMode;

import org.griphyn.cPlanner.selector.SiteSelector;

import org.griphyn.cPlanner.classes.SubInfo;

import org.griphyn.cPlanner.common.LogManager;
import org.griphyn.cPlanner.common.PegasusProperties;

import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

/**
 * This ends up scheduling the jobs in a round robin manner. In order to avoid
 * starvation, the jobs are scheduled in a round robin manner per level, and
 * the queue is initialised for each level.
 *
 * @author Karan Vahi
 * @version $Revision$
 */

public class RoundRobin
    extends SiteSelector {

    /**
     * The current level in the abstract workflow. It is level that is designated
     * by Chimera while constructing the graph bottom up.
     */
    private int mCurrentLevel;

    /**
     * The list of pools that have been given by the user at run time or has been
     * authenticated against. At present these are the same as the list of pools
     * that is passed for site selection at each function.
     */
    private java.util.LinkedList mExecPools;

    /**
     * The handle to the pool configuration file.
     */
    private PoolInfoProvider mPoolHandle;

    /**
     * The handle to the properties file.
     */
    private PegasusProperties mProps;

    /**
     * The handle to the logger.
     */
    private LogManager mLogger;

    /**
     * The default constructor. Not to be used.
     */
    public RoundRobin() {
        mCurrentLevel = -1;
    }

    /**
     * The overloaded constructor.
     * @param path String
     */
    public RoundRobin( String path ) {
        super( path );
        mCurrentLevel = -1;

        mProps = PegasusProperties.nonSingletonInstance();
        mLogger = LogManager.getInstance();

        String poolClass = PoolMode.getImplementingClass( mProps.getPoolMode() );
        mPoolHandle = PoolMode.loadPoolInstance( poolClass, mProps.getPoolFile(),
            PoolMode.SINGLETON_LOAD );

    }

    /**
     * Returns a brief description of the site selection techinque implemented by
     * this class.
     * @return String
     */
    public String description() {
        String st = "Round Robin Scheduling per level of the workflow";
        return st;
    }

    /**
     * The main function that performs the round robin scheduling on the job.
     *
     * @param job   the <code>SubInfo</code> object  corresponding to the
     *                  job whose execution pool we want to determine.
     * @param pools     the list of <code>String</code> objects representing the
     *                  execution pools that can be used.
     *
     * @return if the pool is found to which the job can be mapped, a string of the
     *         form <code>executionpool:jobmanager</code> where the jobmanager can
     *          be null. If the pool is not found, then set poolhandle to NONE.
     *          null - if some error occured .
     */
    public String mapJob2ExecPool( SubInfo job, List pools ) {
        ListIterator it;
        NameValue current;
        NameValue next;

        if ( mExecPools == null ) {
            initialiseList( pools );
        }

        if ( job.level != mCurrentLevel ) {
            //reinitialize stuff
            mCurrentLevel = job.level;
            it = mExecPools.listIterator();
            while ( it.hasNext() ) {
                ( ( NameValue ) it.next() ).setValue( 0 );
            }
        }

        //go around the list and schedule it to the first one where it can
        it = mExecPools.listIterator();
        while ( it.hasNext() ) {
            current = ( NameValue ) it.next();
            //check if job can run on pool
            if ( mTCMapper.isSiteValid( job.namespace, job.logicalName,
                job.version, current.getName() ) ) {
                String mapping = constructResult( current.getName(),
                    job.condorUniverse );
                //update the the number of times used and place it at the
                //correct position in the list
                current.increment();

                //the current element stays at it's place if it is the only one
                //in the list or it's value is less than the next one.
                if ( it.hasNext() ) {
                    next = ( NameValue ) it.next();
                    if ( current.getValue() <= next.getValue() ) {
                        return mapping;
                    } else {
                        current = ( NameValue ) it.previous();
                        current = ( NameValue ) it.previous();
                        System.out.print( "" );
                    }
                }
                it.remove();

                //now go thru the list and insert in the correct position
                while ( it.hasNext() ) {
                    next = ( NameValue ) it.next();

                    if ( current.getValue() <= next.getValue() ) {
                        //current has to be inserted before next
                        next = ( NameValue ) it.previous();
                        it.add( current );
                        return mapping;
                    }
                }
                //current goes to the end of the list
                it.add( current );
                return mapping;
            }
        }

        //means no pool has been found to which the job could be mapped to.
        return ( SiteSelector.POOL_NOT_FOUND + ":null" );
    }

    /**
     * This helper method constructs the result in the format required by the
     * SiteSelector API.
     *
     * @param siteid  The siteid selected by the selector.
     * @param universe  The universe in which the job has to run.
     *
     * @return  the formatted result.
     */
    private String constructResult( String siteid, String universe ) {
        String mapping = siteid + ":";

        //Let Interpool Engine take care of determining
        //the jobmanager for the time being. Karan Apr 12, 2005
        /*
        Pool pool = mPoolHandle.getPoolEntry( siteid, universe );
        mapping += ( pool == null || pool.job_mgr_string == null ) ? "null" :
            pool.job_mgr_string;
        */
        mapping += null;
        return mapping;

    }

    /**
     * It initialises the internal list. A node in the list corresponds to a pool
     * that can be used, and has the value associated with it which is the
     * number of jobs in the current level have been scheduled to it.
     * @param pools List
     */
    private void initialiseList( List pools ) {
        if ( mExecPools == null ) {
            mExecPools = new java.util.LinkedList();

            Iterator it = pools.iterator();
            while ( it.hasNext() ) {
                mExecPools.add( new NameValue( ( String ) it.next(), 0 ) );
            }
        }
    }



    /**
     * A inner name value class that associates a string with an int value.
     * This is used to populate the round robin list that is used by this
     * scheduler.
     */
    class NameValue {
        /**
         * Stores the name of the pair (the left handside of the mapping).
         */
        private String name;

        /**
         * Stores the corresponding value to the name in the pair.
         */
        private int value;

        /**
         * The default constructor  which initialises the class member variables.
         */
        public NameValue() {
            name = new String();
            value = -1;
        }

        /**
         * Initialises the class member variables to the values passed in the
         * arguments.
         *
         * @param name  corresponds to the name in the NameValue pair
         *
         * @param value corresponds to the value for the name in the NameValue pair
         */
        public NameValue( String name, int value ) {
            this.name = name;
            this.value = value;
        }

        /**
         * The set method to set the value.
         * @param value int
         */
        public void setValue( int value ) {
            this.value = value;
        }

        /**
         * Returns the value associated with this pair.
         * @return int
         */
        public int getValue() {
            return this.value;
        }

        /**
         * Returns the key of this pair, i.e the left hand side of the mapping.
         * @return String
         */
        public String getName() {
            return this.name;
        }

        /**
         * Increments the int value by one.
         */
        public void increment() {
            value += 1;
        }

        /**
         * Returns a copy of this object.
         * @return Object
         */
        public Object clone() {
            NameValue nv = new NameValue( this.name, this.value );
            return nv;

        }

        /**
         * Writes out the contents of the class to a String in form suitable for
         * displaying.
         * @return String
         */
        public String toString() {
            String str = name + "-->" + value;

            return str;
        }

    }
}
