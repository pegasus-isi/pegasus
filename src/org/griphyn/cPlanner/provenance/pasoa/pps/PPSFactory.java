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

package org.griphyn.cPlanner.provenance.pasoa.pps;

import org.griphyn.cPlanner.common.PegasusProperties;
import org.griphyn.common.util.DynamicLoader;

import org.griphyn.cPlanner.provenance.pasoa.PPS;

/**
 * The factory for instantiating an XMLProducer.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class PPSFactory {

    /**
     * The default package where all the implementations reside.
     */
    public static final String DEFAULT_PACKAGE_NAME = "org.griphyn.cPlanner.provenance.pasoa.pps";


    /**
     * The default PPS implementation to be used.
     */
    public static final String DEFAULT_PPS_PROVIDER = "Empty";

    /**
     * The default Pasoa PPS implementation to be used.
     */
    public static final String PASOA_PPS_PROVIDER = "Pasoa";


    /**
     * The singleton instance of the PPS implementation that is returned.
     */
    private static PPS mInstance = null;


    /**
     * Loads the appropriate PPS implementation on the basis of the property set in the
     * properties.
     *
     *
     * @param properties the <code>PegasusProperties</code> object containing all
     *                   the properties required by Pegasus.
     *
     * @return the instance of the appropriate XML Producer.
     *
     * @throws PPSFactoryException that nests any error that
     *         might occur during the instantiation
     *
     * @see #DEFAULT_PACKAGE_NAME
     */
    public static PPS loadPPS( PegasusProperties properties )
                                              throws PPSFactoryException{

        //sanity check
        if( properties == null ){
            throw new PPSFactoryException( "No properties passed to factory " );
        }


        //check for singleton
        if( mInstance != null ){
            return mInstance;
        }


        String className = properties.getRefinementProvenanceStore();
        if( className == null ){
            className = DEFAULT_PPS_PROVIDER;
        }
        else if ( className.equalsIgnoreCase( "pasoa" ) ){
            className = PASOA_PPS_PROVIDER;
        }

        PPS pps = null;
        try{
            //prepend the package name if required
            className = ( className.indexOf('.') == -1 )?
                        //pick up from the default package
                        DEFAULT_PACKAGE_NAME + "." + className:
                        //load directly
                        className;

            //try loading the class dynamically
            DynamicLoader dl = new DynamicLoader( className );
            pps = ( PPS ) dl.instantiate( new Object[0] );
        }
        catch( Exception e ){
            throw new PPSFactoryException( " Unable to instantiate PPS ",
                                                 className,
                                                 e );
        }
        mInstance = pps;
        return pps;
    }


}
