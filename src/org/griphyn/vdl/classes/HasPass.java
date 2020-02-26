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

import java.util.Enumeration;

/**
 * This interface defines a common base for {@link Derivation} and {@link Call}. The latter is in a
 * sense an anonymous DV.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 */
public interface HasPass {
    /**
     * Accessor: Adds an actual argument to the bag of arguments.
     *
     * @param vPass is the new actual argument to add.
     * @see Pass
     */
    public void addPass(Pass vPass) throws NullPointerException;

    /**
     * Accessor: Provides an iterator for the bag of actual arguments.
     *
     * @return the iterator for <code>Pass</code> elements.
     * @see Pass
     * @see java.util.Enumeration
     */
    public Enumeration enumeratePass();

    /**
     * Determines all LFN instances from the actual arguments of a given derivation that match the
     * specified linkage. This is a higher-level method employing the given interface. Note that
     * also linkage of NONE will not be found in wildcard search mode.
     *
     * @param linkage is the linkage type to match against, -1 for all files.
     * @return a list of logical filenames from the given derivation which match the given linkage.
     *     For a linkage of -1, complete LFNs will be returned, for any other linkage, just the
     *     filename will be returned.
     * @see Value#getLFNList( int )
     * @see LFN
     */
    public java.util.List getLFNList(int linkage);

    /**
     * Determines if the list contains an LFN of the specified linkage. The logic uses short-circuit
     * evaluation, thus finding things is faster than not finding things. Searching a list is a
     * potentially expensive method.
     *
     * @param filename is the name of the LFN
     * @param linkage is the linkage to check for, -1 for any linkage type.
     * @return true if the LFN is contained in the scalar, false otherwise.
     * @see Value#containsLFN( String, int )
     * @see LFN
     */
    public boolean containsLFN(String filename, int linkage);

    /**
     * Accessor: Obtains an actual argument identified by the bound variable.
     *
     * @param name is the binding name.
     * @return the bound value to the given name.
     * @see Pass
     */
    public Pass getPass(String name);

    /**
     * Accessor: Obtains the bag of actual arguments as array. Note that the order is arbitrary.
     *
     * @return an array containing all bound variables.
     * @see Pass
     */
    public Pass[] getPass();

    /**
     * Accessor: Counts the number of actual arguments.
     *
     * @return the number of actual arguments in the internal bag.
     */
    public int getPassCount();

    /** Accessor: Removes all actual arguments. Effectively empties the bag. */
    public void removeAllPass();

    /**
     * Accessor: Removes a specific actual argument.
     *
     * @param name is the bound variable name of the argument to remove.
     * @return the object that was removed, or null, if not found.
     * @see Pass
     */
    public Pass removePass(String name);

    /**
     * Accessor: Adds a new or overwrites an existing actual argument.
     *
     * @param vPass is a new actual argument with bound name and value.
     * @see Pass
     */
    public void setPass(Pass vPass);

    /**
     * Accessor: Replaces the bag of actual argument with new arguments.
     *
     * @param passArray is the new actual argument list.
     * @see Pass
     */
    public void setPass(Pass[] passArray);

    /**
     * Accessor: Obtains the name of the logical {@link Transformation} that this derivation refers
     * to.
     */
    public java.lang.String getUses();

    /**
     * Accessor: Obtains the namespace of the logical {@link Transformation} that this derivation
     * refers to.
     */
    public java.lang.String getUsesspace();

    /**
     * Accessor: Obtains the maximum inclusive version permissable for binding to a {@link
     * Transformation}.
     *
     * @return the maximum inclusive version number.
     */
    public String getMaxIncludeVersion();

    /**
     * Accessor: Obtains the minimum inclusive version permissable for binding to a {@link
     * Transformation}.
     *
     * @return the minimum inclusive version number.
     */
    public String getMinIncludeVersion();

    /** Identify the transformation or derivation by its name. */
    public String identify();

    /**
     * Constructs dynamically a short descriptive, hopefully unique identifier for this derivation
     * w/o referring to any transformation. FIXME: Anonymous derivations get their hash code, which
     * is well for the first versions working without database. Later versions with database must
     * use some unique sequence mechanism instead.
     *
     * @return a string describing the derivation
     * @see Object#hashCode()
     */
    public String shortID();
}
