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
package org.griphyn.cPlanner.partitioner.graph;

/**
 * A bag implementation that just holds a particular value for the label key.
 * This bag implements just contains one object, and a null value is associated
 * by default with the label.
 *
 * @author Karan Vahi
 * @version $Revision: 1.1 $
 */

public class LabelBag implements Bag {

    /**
     * The default key that is associated with label.
     */
    public static String LABEL_KEY = "label";

    /**
     * The key that designates the partition to which a node belongs to.
     */
    public static final String PARTITION_KEY = "partition";

    /**
     * The value for the Label.
     */
    private Object mValue;

    /**
     * The value for the partition key.
     */
    private Object mPartitionID;

    /**
     * Sets the label key that is to be associated with the bag.
     */
    public static void setLabelKey(String key){
        LABEL_KEY = key;
    }


    /**
     * The default constructor.
     */
    public LabelBag(){
        mValue = null;
        mPartitionID = null;
    }

    /**
     * Returns an objects corresponding to the key passed.
     *
     * @param key  the key corresponding to which the objects need to be returned.
     *
     * @return  the object that is found corresponding to the key or null.
     */
    public Object get(Object key){
        return (key.equals(this.LABEL_KEY)?mValue:
                                          key.equals(this.PARTITION_KEY)?
                                                             mPartitionID:null);
    }

    /**
     * Adds an object to the underlying bag corresponding to a particular key.
     *
     * @param key   the key with which the value has to be associated.
     * @param value the value to be associated with the key.
     */
    public boolean add(Object key, Object value){
        boolean result = false;
        if(key.equals(LABEL_KEY)){
            mValue = value;
            result = true;
        }
        else if(key.equals(PARTITION_KEY)){
            mPartitionID = value;
            result = true;
        }
        return result;
    }

    /**
     * Returns true if the namespace contains a mapping for the specified key.
     * More formally, returns true if and only if this map contains at a mapping
     * for a key k such that (key==null ? k==null : key.equals(k)).
     * (There can be at most one such mapping.)
     *
     * @param key   The key that you want to search for in the bag.
     */
    public boolean containsKey(Object key){
        return key.equals(this.LABEL_KEY) || key.equals(this.PARTITION_KEY);
    }

    /**
     * Returns a textual description of the Bag.
     *
     * @return String
     */
    public String toString(){
        StringBuffer sb = new StringBuffer(32);
        sb.append('{').append(mValue).append(',').append(mPartitionID).append('}');
        return sb.toString();
    }

}