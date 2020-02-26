/*
 * This file or a portion of this file is licensed under the terms of
 * the Globus Toolkit Public License, found in file ../GTPL, or at
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

package org.griphyn.vdl.classes;

import java.io.Serializable;
import java.util.*;

/**
 * This class is the base class for {@link Scalar} and {@link List} child classes.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 */
public abstract class Value extends VDL implements Cloneable, Serializable {
    /**
     * @deprecated This is the name for a value element. Originally we thought to employ this value,
     *     but it is currently unused.
     */
    private String m_name;

    /**
     * Denotes a scalar component, which is always a leaf.
     *
     * @see #getContainerType()
     * @see Scalar#getContainerType()
     */
    public static final int SCALAR = 0;

    /**
     * Denotes a list component.
     *
     * @see #getContainerType()
     * @see List#getContainerType()
     */
    public static final int LIST = 1;

    /**
     * This method determines which container is being used in the abstract base class in order to
     * avoid <code>typeof</code> constructs.
     *
     * @return either the constant to designate a <code>Scalar</code>, or a constant to designate a
     *     <code>List</code>.
     * @see #SCALAR
     * @see #LIST
     */
    public abstract int getContainerType();

    /**
     * This method determines which container is being used in the abstract base class in order to
     * kludgy statements when printing debug info.
     *
     * @return the symblic identifier for the type of the Value.
     */
    public abstract String getSymbolicType();

    /**
     * Creates and returns a copy of this object.
     *
     * @return a new instance.
     */
    public abstract Object clone();

    /**
     * Determines all LFN instances of a given scalar that match the specified linkage. This is a
     * higher-level method employing the given API.
     *
     * @param linkage is the linkage to check for, -1 for all filenames.
     * @return a set of logical filename instances that match the linkage and were part of the
     *     scalar. The result may be an empty set, if no such result were to be found.
     * @see LFN
     */
    public abstract java.util.List getAllLFN(int linkage);

    /**
     * Determines all LFN instances of a given scalar that match the specified linkage. This is a
     * higher-level method employing the given API. Note that also linkage of NONE will not be found
     * in wildcard search mode.
     *
     * @param linkage is the linkage to check for, -1 for all filenames.
     * @return a set of all logical filenames that match the linkage and were part of the scalar.
     *     The result may be an empty set, if no such result were to be found. For a linkage of -1,
     *     complete LFNs will be returned, for any other linkage, just the filename will be
     *     returned.
     * @see Derivation#getLFNList( int )
     * @see LFN
     */
    public abstract java.util.List getLFNList(int linkage);

    /**
     * Determines if the Value contains an LFN of the specified linkage. The logic uses
     * short-circuit evaluation, thus finding things is faster than not finding things. Searching a
     * list is a potentially expensive method.
     *
     * @param filename is the name of the LFN
     * @param linkage is the linkage to check for, -1 for any linkage type.
     * @return true if the LFN is contained in the scalar, false otherwise.
     * @see org.griphyn.vdl.classes.LFN
     * @see Derivation#containsLFN( String, int )
     */
    public abstract boolean containsLFN(String filename, int linkage);

    /**
     * Predicate function to determine, if a container type is within the legal range of values.
     *
     * @param x is the container type
     * @return true, if the type is within the legal range, or false, if the container type is
     *     outside legal range.
     * @see #SCALAR
     * @see #LIST
     */
    public static boolean isInRange(int x) {
        return ((x >= Value.SCALAR) && (x <= Value.LIST));
    }

    /** Default ctor. */
    public Value() {
        // empty
    }

    /**
     * Accessor method. Obtains the current state of the thus named attribute.
     *
     * @return the current state of the m_name attribute.
     * @see #setName(String)
     * @deprecated the attribute is currently unused.
     */
    public String getName() {
        return this.m_name;
    }

    /**
     * Accessor method. Sets the attribute of same name to a new value.
     *
     * @param name is the new value to overwrite the current state with.
     * @see #getName()
     * @deprecated the name attribute is currently unused.
     */
    public void setName(String name) {
        this.m_name = name;
    }
}
