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
 * An interface to define a BAG of objects. The bag can be then associated
 * with other data structures, like Graph Nodes.
 *
 * @author Karan Vahi
 * @version $Revision$
 */

public interface Bag {

    /**
     * Returns an objects corresponding to the key passed.
     *
     * @param key  the key corresponding to which the objects need to be returned.
     *
     * @return  the object that is found corresponding to the key or null.
     */
    public Object get(Object key);

    /**
     * Adds an object to the underlying bag corresponding to a particular key.
     *
     * @param key   the key with which the value has to be associated.
     * @param value the value to be associated with the key.
     *
     * @return boolean indicating if insertion was successful.
     */
    public boolean add(Object key, Object value);

    /**
     * Returns true if the namespace contains a mapping for the specified key.
     * More formally, returns true if and only if this map contains at a mapping
     * for a key k such that (key==null ? k==null : key.equals(k)).
     * (There can be at most one such mapping.)
     *
     * @param key   The key that you want to search for in the bag.
     */
    public boolean containsKey(Object key);


}