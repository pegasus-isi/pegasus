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
package org.griphyn.cPlanner.transfer.sls;


import org.griphyn.cPlanner.classes.PlannerOptions;
import org.griphyn.cPlanner.classes.PegasusBag;

import org.griphyn.cPlanner.transfer.SLS;

import org.griphyn.cPlanner.common.PegasusProperties;
import org.griphyn.common.util.DynamicLoader;



/**
 * A factory class to load the appropriate type of SLS Implementation to do
 * the Second Level Staging.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class SLSFactory {

    /**
     * The default package where the all the implementing classes are supposed to
     * reside.
     */
    public static final String DEFAULT_PACKAGE_NAME =
                                          "org.griphyn.cPlanner.transfer.sls";

    /**
     * The name of the class implementing the condor code generator.
     */
    public static final String DEFAULT_SLS_IMPL_CLASS  =  "Transfer";


    /**
     * This method loads the appropriate implementing code generator as specified
     * by the user at runtime. If the megadag mode is specified in the options,
     * then that is used to load the implementing class, overriding the submit
     * mode specified in the properties file.
     *
     *
     * @param bag   the bag of initialization objects.
     *
     * @return the instance of the class implementing this interface.
     *
     * @exception CodeGeneratorFactoryException that nests any error that
     *            might occur during the instantiation of the implementation.
     *
     * @see #DEFAULT_PACKAGE_NAME
     * @see org.griphyn.cPlanner.common.PegasusProperties#getDAXCallback()
     *
     * @throws SLSFactoryException
     */
    public static SLS loadInstance( PegasusBag bag )  throws SLSFactoryException{

        PegasusProperties properties = bag.getPegasusProperties();
        PlannerOptions options       = bag.getPlannerOptions();

        //sanity check
        if(properties == null){
            throw new SLSFactoryException( "Invalid properties passed" );
        }
        if(options == null){
            throw new SLSFactoryException( "Invalid Options specified" );
        }

        String className = properties.getSLSTransferImplementation();
        if( className == null ){
            className = DEFAULT_SLS_IMPL_CLASS; //to be picked up from properties eventually
        }

        return loadInstance( bag, className );

    }

    /**
     * This method loads the appropriate code generator as specified by the
     * user at runtime.
     *
     *
     * @param bag   the bag of initialization objects.
     * @param className  the name of the implementing class.
     *
     * @return the instance of the class implementing this interface.
     *
     * @exception CodeGeneratorFactoryException that nests any error that
     *            might occur during the instantiation of the implementation.
     *
     * @see #DEFAULT_PACKAGE_NAME
     *
     * @throws SLSFactoryException
     */
    public static SLS loadInstance( PegasusBag bag, String className)
        throws SLSFactoryException{


        PegasusProperties properties = bag.getPegasusProperties();
        PlannerOptions options       = bag.getPlannerOptions();


        //sanity check
        if (properties == null) {
            throw new SLSFactoryException( "Invalid properties passed" );
        }
        if (className == null) {
            throw new SLSFactoryException( "Invalid className specified" );
        }

        //prepend the package name if classname is actually just a basename
        className = (className.indexOf('.') == -1) ?
            //pick up from the default package
            DEFAULT_PACKAGE_NAME + "." + className :
            //load directly
            className;

        //try loading the class dynamically
        SLS sls = null;
        try {
            DynamicLoader dl = new DynamicLoader( className );
            sls = ( SLS ) dl.instantiate( new Object[0] );
            //initialize the loaded code generator
            sls.initialize( bag );
        }
        catch ( Exception e ) {
            throw new SLSFactoryException( "Instantiating SLS Implementor ", className, e );
        }

        return sls;
    }

}
