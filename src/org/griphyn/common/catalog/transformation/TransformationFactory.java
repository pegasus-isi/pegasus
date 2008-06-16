/**
 *  Copyright 2007-2008 University Of Southern California
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
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
