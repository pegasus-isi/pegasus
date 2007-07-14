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

package org.griphyn.cPlanner.provenance.pasoa.producer;

import org.griphyn.cPlanner.common.PegasusProperties;
import org.griphyn.common.util.DynamicLoader;


import org.griphyn.cPlanner.provenance.pasoa.XMLProducer;

/**
 *
 * The factory for instantiating an XMLProducer.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class XMLProducerFactory {
    /**
     * The default package where all the implementations reside.
     */
    public static final String DEFAULT_PACKAGE_NAME = "org.griphyn.cPlanner.provenance.pasoa.producer";


    /**
     * The default XML producer implementation to be used.
     */
    public static final String DEFAULT_XML_PRODUCER = "InMemory";


    /**
     * Loads the appropriate XMLProducer on the basis of the property set in the
     * properties.
     *
     *
     * @param properties the <code>PegasusProperties</code> object containing all
     *                   the properties required by Pegasus.
     *
     * @return the instance of the appropriate XML Producer.
     *
     * @throws XMLProducerFactoryException that nests any error that
     *         might occur during the instantiation
     *
     * @see #DEFAULT_PACKAGE_NAME
     */
    public static XMLProducer loadXMLProducer( PegasusProperties properties )
                                              throws XMLProducerFactoryException{


        //sanity check

        String className = DEFAULT_XML_PRODUCER;
        XMLProducer producer = null;
        try{

            //prepend the package name if required
            className = ( className.indexOf('.') == -1 )?
                        //pick up from the default package
                        DEFAULT_PACKAGE_NAME + "." + className:
                        //load directly
                        className;

            //try loading the class dynamically
            DynamicLoader dl = new DynamicLoader( className );
            producer = ( XMLProducer ) dl.instantiate( new Object[0] );
        }
        catch ( Exception e ){
            throw new XMLProducerFactoryException( " Unable to instantiate XMLProducer ",
                                                 className,
                                                 e );
        }
        return producer;
    }


}
