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


package org.griphyn.cPlanner.poolinfo;

import edu.isi.pegasus.common.util.DynamicLoader;

import org.griphyn.cPlanner.common.PegasusProperties;


/**
 * A factory class to load the appropriate implementation of Transformation
 * Catalog as specified by properties.
 *
 * @author Karan Vahi
 * @version $Revision$
 */

public class SiteFactory {

    /**
     * The default package where all the implementations reside.
     */
    public static final String DEFAULT_PACKAGE_NAME =
                                   "org.griphyn.cPlanner.poolinfo";


    /**
     * Constants to specify how to load the class, as singleton or non singleton.
     */
    public static final int SINGLETON_LOAD = 0;
    public static final int NON_SINGLETON_LOAD = 1;


    /**
     * The name of the class that connects to an XML based Site Catalog.
     */
    public static final String XML_IMPLEMENTING_CLASS = "XML";

    /**
     * The name of the class that connects to an multi line Text based Site Catalog.
     */
    public static final String TEXT_IMPLEMENTING_CLASS = "Text";

    /**
     * Connects the interface with the site catalog implementation. The
     * choice of backend is configured through properties. This method uses
     * default properties from the property singleton.
     *
     * @param singleton  indicates whether to load the singleton implementation
     *                   to Site Catalog backend or not.
     *
     * @return handle to the Site Catalog.
     *
     * @throws SiteFactoryException that nests any error that
     *         might occur during the instantiation
     *
     * @see #DEFAULT_PACKAGE_NAME
     */
//    public static PoolInfoProvider loadInstance( boolean singleton )
//                             throws SiteFactoryException{
//        return loadInstance( PegasusProperties.getsingletonInstance(), singleton );
//    }


    /**
     * Connects the interface with the site catalog implementation. The
     * choice of backend is configured through properties. This class is
     * useful for non-singleton instances that may require changing
     * properties.
     *
     *
     * @param properties is an instance of properties to use.
     * @param singleton  indicates whether to load the singleton implementation
     *                   to Site Catalog backend or not. It should be set to false
     *                   for portals.
     *
     * @return handle to the Site Catalog.
     *
     * @throws SiteFactoryException that nests any error that
     *         might occur during the instantiation
     *
     * @see #DEFAULT_PACKAGE_NAME
     */
    public static PoolInfoProvider loadInstance( PegasusProperties properties, boolean singleton )
                             throws SiteFactoryException{


        /* get the implementor from properties */
        String catalogImplementor = properties.getPoolMode().trim();

        /* pick up the right value on basis of case insensitive match */
        if ( catalogImplementor.equalsIgnoreCase( XML_IMPLEMENTING_CLASS ) ){
             catalogImplementor = XML_IMPLEMENTING_CLASS;
        }
        else if ( catalogImplementor.equalsIgnoreCase( TEXT_IMPLEMENTING_CLASS ) ){
            catalogImplementor = TEXT_IMPLEMENTING_CLASS;
        }

        /* prepend the package name if required */
        catalogImplementor = ( catalogImplementor.indexOf('.') == -1 )?
                             //pick up from the default package
                             DEFAULT_PACKAGE_NAME + "." + catalogImplementor:
                             //load directly
                             catalogImplementor;

        /* determine the method name to invoke */
        String methodName = ( singleton )? "singletonInstance" : "nonSingletonInstance";

        /* construct arguments to the method */
        Object args[] = new Object[2 ];
        args[ 0 ] = properties.getPoolFile();
        /* this should not be reqd. the Site Catalog interface should take
           the properties object while being instantiated */
        args[ 1 ] = edu.isi.pegasus.common.util.VDSProperties.PROPERTY_FILENAME;


        PoolInfoProvider catalog;

        /* try loading the catalog implementation dynamically */
        try{
            DynamicLoader dl = new DynamicLoader( catalogImplementor );
            catalog = ( PoolInfoProvider ) dl.static_method( methodName, args );
        }
        catch ( Exception e ){
            throw new SiteFactoryException(
                                           " Unable to instantiate Site Catalog ",
                                            catalogImplementor,
                                            e );
        }

        return catalog;

    }

}
