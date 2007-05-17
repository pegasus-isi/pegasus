/**
 * This file or a portion of this file is licensed under the terms of
 * the Globus Toolkit Public License, found at $PEGASUS_HOME/GTPL or
 * http://www.globus.org/toolkit/download/license.html.
 * This notice must appear in redistributions of this file
 * with or without modification.
 *
 * Redistributions of this Software, with or without modification, must reproduce
 * the GTPL in:
 * (1) the Software, or
 * (2) the Documentation or
 * some other similar material which is provided with the Software (if any).
 *
 * Copyright 1999-2004
 * University of Chicago and The University of Southern California.
 * All rights reserved.
 */

package org.griphyn.cPlanner.engine;

import org.griphyn.cPlanner.classes.PegasusFile;
import org.griphyn.cPlanner.classes.PlannerOptions;

import org.griphyn.cPlanner.common.LogManager;
import org.griphyn.cPlanner.common.PegasusProperties;

import org.griphyn.cPlanner.poolinfo.PoolInfoProvider;
import org.griphyn.cPlanner.poolinfo.PoolMode;


import org.griphyn.common.catalog.TransformationCatalog;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;

/**
 * The  class which is a superclass of all the various Engine classes. It
 * defines common methods and member variables.
 *
 * @author Karan Vahi
 * @author Gaurang Mehta
 * @version $Revision: 1.17 $
 *
 */
public class Engine {

    //constants
    public static final String REGISTRATION_UNIVERSE = "registration";
    public static final String TRANSFER_UNIVERSE = "transfer";

    /**
     * The pool on which all the output data should be transferred.
     */
    protected static String mOutputPool;

    /**
     * The object holding all the properties pertaining to Pegasus.
     */
    protected PegasusProperties mProps;

    /**
     * The path to the file containing the pool information. By default it is
     * $PEGASUS_HOME/etc/pool.config
     */
    protected String mPoolFile;

    /**
     * The handle to the Transformation Catalog. It must be instantiated
     * in the implementing class.
     */
    protected TransformationCatalog mTCHandle;

    /**
     * The path to the file containing the pool information. By default it is
     * $PEGASUS_HOME/etc/tc.data.
     */
    protected String mTCFile;

    /**
     * The handle to the Pool Info Provider. It is instantiated in this class
     */
    protected PoolInfoProvider mPoolHandle;


    /**
     * Contains the message which is to be logged by Pegasus.
     */
    protected String mLogMsg = new String();

    /**
     * The Replica Location Index URL got from vds.rls.url property
     */
    protected String mRLIUrl = new String();


    /**
     * Defines the read mode for transformation catalog. Whether we want to read all
     * at once or as desired.
     *
     * @see org.griphyn.common.catalog.transformation.TCMode
     */
    protected String mTCMode;

    /**
     * Specifies the implementing class for the pool interface. Contains
     * the name of the class that implements the pool interface the
     * user has asked at runtime.
     */
    protected String mPoolClass;


    /**
     * The logging object which is used to log all the messages.
     *
     * @see org.griphyn.cPlanner.common.LogManager
     */
    protected LogManager mLogger = LogManager.getInstance();

    /**
     * Contains the various options to the Planner as passed by the user at
     * runtime.
     */
    protected PlannerOptions mPOptions;

    /**
     * Default constructor.
     *
     * @param props   the properties to be used.
     */
    public Engine( PegasusProperties props ) {
        mProps = props;
        loadProperties();

        mPoolHandle = PoolMode.loadPoolInstance(mPoolClass, mPoolFile,
                                                PoolMode.SINGLETON_LOAD);

    }

    /**
     * Loads all the properties that are needed by the Engine classes.
     */
    public void loadProperties() {

        //get from the properties object
        mPoolFile = mProps.getPoolFile();
        mTCFile = mProps.getTCPath();
        mRLIUrl = mProps.getRLIURL();
        String rmode = mProps.getReplicaMode();
        String tcmode = mProps.getTCMode();
        String poolmode = mProps.getPoolMode();

        mPoolClass = PoolMode.getImplementingClass(poolmode);

    }

    /**
     * Returns true if a particular String is in the Vector of strings.
     *
     * @param stringName  the String which has to be searched for in the Vector.
     * @param vector      the Vector of Strings in which to search for a
     *                    particular String.
     *
     * @return    boolean on the basis of whether the String  in Vector or not.
     */
    public boolean stringInVector(String stringName, Vector vector) {
        Enumeration e = vector.elements();
        while (e.hasMoreElements()) {
            if (stringName.equalsIgnoreCase( (String) e.nextElement())) {
                return true;
            }
        }
        return false;
    }

    public boolean stringInList(String stringName, List list) {
        if (list.contains(stringName)) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * Returns true if a particular String is in the Vector of PegasusFile objects.
     *
     * @param stringName the String which has to be searched for in the Vector.
     * @param vector     the Vector of Strings in which to search for a particular
     *                   String
     *
     * @return    boolean on the basis of whether the String in Vector or not.
     *
     */
    public boolean stringInPegVector(String stringName, Vector vector) {
        Enumeration e = vector.elements();
        while (e.hasMoreElements()) {
            PegasusFile pf = (PegasusFile) e.nextElement();
            if (stringName.equalsIgnoreCase(pf.getLFN())) {
                return true;
            }
        }
        return false;
    }

    /**
     * Adds elements (PegasusFile type) in a Vector to another Vector and
     * returns the new Vector.
     *
     * @return   Vector of PegasusFile objects
     */
    public Vector addVector(Vector from_vector, Vector to_vector) {
        Enumeration e = from_vector.elements();
        Vector newVector = (Vector) to_vector.clone();

        while (e.hasMoreElements()) {
            PegasusFile pf = (PegasusFile) e.nextElement();
            newVector.addElement(pf);
            /*String elem = new String((String)e.nextElement());
                         if(!stringInVector(elem,to_vector)){
                newVector.addElement(elem);
                         }*/
        }

        return newVector;
    }

    /**
     * It prints the contents of the Vector, with the first line being the heading.
     *
     * @param heading   The heading you want to give to the text which is printed.
     * @param vector    The <code>Vector</code> whose elements you want to print.
     */
    public void printVector(String heading, Vector vector) {
        mLogger.log(heading, LogManager.DEBUG_MESSAGE_LEVEL);
        for(Iterator it = vector.iterator() ; it.hasNext() ;) {
            mLogger.log( it.next().toString() , LogManager.DEBUG_MESSAGE_LEVEL);
        }
    }

    /**
     * It prints the contents of the Vector, to a String with the first line being
     * the heading.
     *
     * @param heading   The heading you want to give to the text which is printed.
     * @param vector    The <code>Vector</code> whose elements you want to print.
     */
    public String vectorToString(String heading, Vector vector) {
        Enumeration e = vector.elements();
        String st = heading;
        while (e.hasMoreElements()) {
            st += "\t" + e.nextElement();
        }
        return st;
    }

    /**
     * It appends the source list at the end of the destination list.
     *
     * @param dest    the destination list
     * @param source  the source list
     */
    public void appendArrayList(ArrayList dest, ArrayList source) {

        Iterator iter = source.iterator();
        while (iter.hasNext()) {
            dest.add(iter.next());
        }
    }

}
