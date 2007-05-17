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
package org.griphyn.cPlanner.classes;

import java.util.Enumeration;
import java.util.Iterator;
import java.util.Set;
import java.util.Vector;

import org.griphyn.cPlanner.common.LogManager;

/**
 * This is the container for all the Data classes.
 *
 * @author Karan Vahi
 * @author Gaurang Mehta
 * @version $Revision: 1.7 $
 */
public abstract class Data implements Cloneable {

    /**
     * The LogManager object which is used to log all the messages.
     *
     * @see org.griphyn.cPlanner.common.LogManager
     */
    public LogManager mLogger = LogManager.getInstance();

    /**
     * The String which stores the message to be stored.
     */
    public String mLogMsg;


    /**
     * The default constructor.
     */
    public Data(){
        mLogMsg = new String();
    }


    /**
     * Returns the String version of the data object, which is in human readable
     * form.
     */
    public abstract String toString();


    /**
     * It converts the contents of the Vector to a String and returns it.
     * For this to work , all the objects making up the vector should be having
     * a valid toString() method.
     *
     * @param heading   The heading you want to give
     *                  to the text which is printed
     *
     * @param vector    The <code>Vector</code> whose
     *                  elements you want to print
     */
    public String vectorToString(String heading,Vector vector){
        Enumeration e = vector.elements();

        String st = "\n" + heading;
        while(e.hasMoreElements()){
            st += " " + e.nextElement().toString();
        }

        return st;
    }

    /**
     * A small helper method that displays the contents of a Set in a String.
     *
     * @param delim  The delimited between the members of the set.
     * @return  String
     */
    public String setToString(Set s, String delim){
        Iterator it = s.iterator();
        String st = new String();
        while(it.hasNext()){
            st += (String)it.next() + delim;
        }
        st = (st.length() > 0)?
             st.substring(0,st.lastIndexOf(delim)):
             st;
        return st;
    }


}