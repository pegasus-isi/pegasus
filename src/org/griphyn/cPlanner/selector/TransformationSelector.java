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
package org.griphyn.cPlanner.selector;

/**
 *
 * This class is an abstract class for the Transformation Catalog Selector.
 * Its purpose is to provide a generic api to select one valid transformation
 * among the many transformations.
 * @author Gaurang Mehta
 * @version $Revision$
 *
 */

import org.griphyn.cPlanner.common.LogManager;

import org.griphyn.common.util.DynamicLoader;
import org.griphyn.common.util.FactoryException;

import java.util.List;

public abstract class TransformationSelector {

    public static final String PACKAGE_NAME =
        "org.griphyn.cPlanner.selector.transformation";

    protected LogManager mLogger;

    public TransformationSelector() {
        mLogger = LogManager.getInstance();
    }

    /**
     * Takes a list of TransformationCatalogEntry objects and returns 1 or many
     * TransformationCatalogEntry objects as a list depending on the type of selection algorithm.
     * The Random and RoundRobin implementation ensure that only one entry is
     * returned and should be run last when chaining multiple selectors
     * @param tcentries List
     * @return List
     */
    public abstract List getTCEntry( List tcentries );

    /**
     * Loads the implementing class corresponding to the mode specified by the
     * user at runtime in the properties file.
     *
     * @param className  String The name of the class that implements the mode.
     *                   It is the name of the class, not the complete name with
     *                   package. That  is added by itself.
     *
     * @return TransformationSelector
     *
     * @throws FactoryException that nests any error that
     *         might occur during the instantiation of the implementation.
     */
    public static TransformationSelector loadTXSelector( String className )
           throws FactoryException {

        //prepend the package name
        className = PACKAGE_NAME + "." + className;

        //try loading the class dynamically
        TransformationSelector ss = null;
        DynamicLoader dl = new DynamicLoader( className );
        try {
            Object argList[] = new Object[0 ];
            //argList[ 0 ] = ( path == null ) ? new String() : path;
            ss = ( TransformationSelector ) dl.instantiate( argList );
        } catch ( Exception e ) {
            throw new FactoryException( "Instantiating Create Directory",
                                        className,
                                        e );
        }

        return ss;
    }

}
