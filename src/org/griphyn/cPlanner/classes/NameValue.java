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
/**
 * The object of this class holds the name value pair.
 * At present to be used for environment variables. Will be used more
 * after integration of Spitfire.
 *
 * @author Karan Vahi
 * @author Gaurang Mehta
 * @version $Revision: 1.4 $
 */
public class NameValue extends Data
implements Comparable{

    /**
     * stores the name of the pair.
     */
    private String name;

    /**
     * stores the corresponding value to the name in the pair.
     */
    private String value;

    /**
     * the default constructor  which initialises the class member variables.
     */
    public NameValue(){
        name  = new String();
        value = new String();
    }

    /**
     * Initialises the class member variables to the values passed in the
     * arguments.
     *
     * @param name  corresponds to the name in the NameValue pair.
     * @param value corresponds to the value for the name in the NameValue pair.
     */
    public NameValue(String name,String value){
        this.name  = name;
        this.value = value;
    }

    /**
     * Returns the key associated with this tuple.
     *
     * @return the key associated with the tuple.
     */
    public String getKey(){
        return this.name;
    }

    /**
     * Returns the value associated with this tuple.
     *
     * @return value associated with the tuple.
     */
    public String getValue(){
        return this.value;
    }

   /**
     * Returns a copy of this object
     *
     * @return object containing a cloned copy of the tuple.
     */
    public Object clone(){
        NameValue nv = new NameValue(this.name,this.value) ;
        return nv;

    }

    /**
     * Writes out the contents of the class to a String
     * in form suitable for displaying.
     *
     * @return the textual description.
     */
    public String toString(){
        String str = this.getKey() + "=" + this.getValue();
        return str;
    }

    /**
     * Implementation of the {@link java.lang.Comparable} interface.
     * Compares this object with the specified object for order. Returns a
     * negative integer, zero, or a positive integer as this object is
     * less than, equal to, or greater than the specified object. The
     * NameValue are compared by their keys.
     *
     * @param o is the object to be compared
     * @return a negative number, zero, or a positive number, if the
     * object compared against is less than, equals or greater than
     * this object.
     * @exception ClassCastException if the specified object's type
     * prevents it from being compared to this Object.
     */
    public int compareTo( Object o ){
        if ( o instanceof NameValue ) {
            NameValue nv = (NameValue) o;
            return this.name.compareTo(nv.name);
        } else {
            throw new ClassCastException( "Object is not a NameValue" );
        }
    }


}