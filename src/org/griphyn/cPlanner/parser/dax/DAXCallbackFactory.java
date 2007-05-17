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
package org.griphyn.cPlanner.parser.dax;

import org.griphyn.cPlanner.common.PegasusProperties;

import org.griphyn.common.util.DynamicLoader;

import java.io.IOException;

import java.lang.reflect.InvocationTargetException;


/**
 * A factory class to load the appropriate DAX callback implementations that need
 * to be passed to the DAX Parser.
 *
 * @author Karan Vahi
 * @version $Revision: 1.3 $
 */
public class DAXCallbackFactory {

    /**
     * Package to prefix "just" class names with.
     */
    public static final String DEFAULT_PACKAGE_NAME =
                                             "org.griphyn.cPlanner.parser.dax";

    /**
     * Loads the implementing class corresponding to the type specified by the user.
     * The properties object passed should not be null. The callback that is
     * loaded, is the one referred to in the properties by the user.
     *
     * @param properties the <code>PegasusProperties</code> object containing all
     *                   the properties required by Pegasus.
     * @param dax        the path to the DAX file that has to be parsed.
     *
     * @return the instance of the class implementing this interface.
     *
     * @exception DAXCallbackFactoryException that nests any error that
     *            might occur during the instantiation
     *
     * @see #DEFAULT_PACKAGE_NAME
     * @see org.griphyn.cPlanner.common.PegasusProperties#getDAXCallback()
     */
    public static Callback loadInstance( PegasusProperties properties,
                                         String dax )
        throws DAXCallbackFactoryException{

        return loadInstance( properties, dax, properties.getDAXCallback());

    }


    /**
     * Loads the implementing class corresponding to the type specified by the user.
     * The properties object passed should not be null. The callback that is
     * loaded, is the one referred to by the className parameter passed.
     *
     * @param properties the <code>PegasusProperties</code> object containing all
     *                   the properties required by Pegasus.
     * @param dax        the path to the DAX file that has to be parsed.
     * @param className  the name of the implementing class.
     *
     * @return the instance of the class implementing this interface.
     *
     * @exception DAXCallbackFactoryException that nests any error that
     *            might occur during the instantiation
     *
     * @see #DEFAULT_PACKAGE_NAME
     */
    public static  Callback loadInstance(PegasusProperties properties,
                                         String dax,
                                         String className)
        throws DAXCallbackFactoryException{

        //try loading the class dynamically
        Callback callback = null;

        try{
            //sanity check
            if(properties == null){
                throw new RuntimeException("Invalid properties passed");
            }
            if(className == null){
                throw new RuntimeException("Invalid class specified to load");
            }

            //prepend the package name
            className = (className.indexOf('.') == -1)?
                        //pick up from the default package
                        DEFAULT_PACKAGE_NAME + "." + className:
                        //load directly
                        className;

            DynamicLoader dl  = new DynamicLoader( className);
            Object argList[]  = new Object[2];
            argList[0] = properties;
            argList[1] = dax;
            callback = (Callback)dl.instantiate(argList);
        }
        catch(Exception e){
            throw new DAXCallbackFactoryException("Instantiating DAXCallback ",
                                                  className, e);
        }
        return callback;
    }


}
