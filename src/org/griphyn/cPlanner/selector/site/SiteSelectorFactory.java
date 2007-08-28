/**
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

package org.griphyn.cPlanner.selector.site;

import org.griphyn.cPlanner.selector.SiteSelector;

import org.griphyn.cPlanner.classes.PegasusBag;

import org.griphyn.cPlanner.common.PegasusProperties;

import org.griphyn.common.util.DynamicLoader;


/**
 * A factory class to load the appropriate type of Site Selector, as
 * specified by the user at runtime in properties. Each invocation of the
 * factory results in a SiteSelector being instantiated.
 *
 * @author Karan Vahi
 * @version $Revision$
 */

public class SiteSelectorFactory {

    /**
     * The default package where the all the implementing classes provided with
     * the VDS reside.
     */
    public static final String DEFAULT_PACKAGE_NAME =
                                        "org.griphyn.cPlanner.selector.site";

    /**
     * The name of the class in the DEFAULT package, that corresponds to the
     * default site selector.
     */
    public static final String DEFAULT_SITE_SELECTOR = "Random";



    /**
     * Loads the implementing class corresponding to the mode specified by the user
     * at runtime in the properties file. A default replica selector is loaded
     * if property is not specified in the properties.
     *
     * @param bag    the bag of objects that is required.
     *
     * @return the instance of the class implementing this interface.
     *
     * @exception SiteSelectorFactoryException that chains any error that
     *            might occur during the instantiation
     *
     * @see #DEFAULT_PACKAGE_NAME
     * @see #DEFAULT_SITE_SELECTOR
     */
    public static SiteSelector loadInstance( PegasusBag bag )
                                         throws SiteSelectorFactoryException {

        PegasusProperties properties = ( PegasusProperties )bag.get( PegasusBag.PEGASUS_PROPERTIES );
        String className = null;
        SiteSelector selector;

        //sanity check
        try{
            if (properties == null) {
                throw new RuntimeException("Invalid properties passed");
            }

            //figure out the implementing class
            //that needs to be instantiated.
            className = properties.getSiteSelectorMode();
            className = ( className == null || className.trim().length() < 2) ?
                          DEFAULT_SITE_SELECTOR :
                          className;

            //prepend the package name if required
            className = (className.indexOf('.') == -1)?
                         //pick up from the default package
                         DEFAULT_PACKAGE_NAME + "." + className:
                         //load directly
                         className;

            //try loading the class dynamically
            DynamicLoader dl = new DynamicLoader(className);
            selector = ( SiteSelector ) dl.instantiate( new Object[ 0 ] );
            selector.initialize( bag );
        }
        catch(Exception e){
            //chain the exception caught into the appropriate Factory Exception
            throw new SiteSelectorFactoryException( "Instantiating SiteSelector ",
                                                     className, e );
        }

        return selector;
    }

}
