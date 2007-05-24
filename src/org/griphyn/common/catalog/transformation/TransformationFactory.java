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


package org.griphyn.common.catalog.transformation;

import org.griphyn.cPlanner.common.*;
import org.griphyn.common.catalog.*;
import org.griphyn.common.util.*;

/**
 * A factory class to load the appropriate implementation of Transformation
 * Catalog as specified by properties.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class TransformationFactory {

    /**
     * Some Constants for backward compatibility.
     */

    public static final String DEFAULT_TC_CLASS = "File";

    /**
     * The default package where all the implementations reside.
     */
    public static final String DEFAULT_PACKAGE_NAME =
        "org.griphyn.common.catalog.transformation";

    /**
     * Connects the interface with the transformation catalog implementation. The
     * choice of backend is configured through properties. This method uses default
     * properties from the property singleton.
     *
     * @return handle to the Transformation Catalog.
     *
     * @throws TransformationFactoryException that nests any error that
     *         might occur during the instantiation
     *
     * @see #DEFAULT_PACKAGE_NAME
     */
    public static TransformationCatalog loadInstance() throws
        TransformationFactoryException {
        return loadInstance(PegasusProperties.getInstance());
    }

    /**
     * Connects the interface with the transformation catalog implementation. The
     * choice of backend is configured through properties. This class is
     * useful for non-singleton instances that may require changing
     * properties.
     *
     * @param properties is an instance of properties to use.
     *
     * @return handle to the Transformation Catalog.
     *
     * @throws TransformationFactoryException that nests any error that
     *         might occur during the instantiation
     *
     * @see #DEFAULT_PACKAGE_NAME
     */
    public static TransformationCatalog loadInstance(PegasusProperties
        properties) throws
        TransformationFactoryException {

        TransformationCatalog tc = null;
        String methodName = "getInstance";

        /* get the implementor from properties */
        String catalogImplementor = properties.getTCMode().trim();

        /* prepend the package name if required */
        catalogImplementor = (catalogImplementor.indexOf('.') == -1) ?
            //pick up from the default package
            DEFAULT_PACKAGE_NAME + "." + catalogImplementor :
            //load directly
            catalogImplementor;

        TransformationCatalog catalog;

        /* try loading the catalog implementation dynamically */
        try {
            DynamicLoader dl = new DynamicLoader(catalogImplementor);
            catalog = (TransformationCatalog) dl.static_method(methodName,
                new String[0]);

        }
        catch (Exception e) {
            throw new TransformationFactoryException(
                " Unable to instantiate Transformation Catalog ",
                catalogImplementor,
                e);
        }
        if (catalog == null) {
            throw new TransformationFactoryException(
                " Unable to instantiate Transformation Catalog ",
                catalogImplementor);
        }
        return catalog;

    }

}
