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
package org.griphyn.cPlanner.code.generator.condor.style;


import org.griphyn.cPlanner.code.generator.condor.CondorStyle;

import org.griphyn.cPlanner.classes.SubInfo;

import org.griphyn.cPlanner.common.PegasusProperties;

import org.griphyn.cPlanner.poolinfo.PoolInfoProvider;

import org.griphyn.cPlanner.namespace.VDS;

import org.griphyn.common.util.DynamicLoader;

import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;

import java.io.IOException;

/**
 * A factory class to load the appropriate type of Condor Style impelementations.
 * This factory class is different from other factories, in the sense that it
 * must be instantiated first and intialized first before calling out to any
 * of the Factory methods.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class CondorStyleFactory {

    /**
     * The default package where the all the implementing classes are supposed to
     * reside.
     */
    public static final String DEFAULT_PACKAGE_NAME =
                                          "org.griphyn.cPlanner.code.generator.condor.style";
    //

    /**
     * The name of the class implementing the Condor Style.
     */
    private static final String CONDOR_STYLE_IMPLEMENTING_CLASS = "Condor";

    /**
     * The name of the class implementing the Condor GlideIN Style.
     */
    private static final String GLIDEIN_STYLE_IMPLEMENTING_CLASS = "CondorGlideIN";

    /**
     * The name of the class implementing the CondorG Style.
     */
    private static final String GLOBUS_STYLE_IMPLEMENTING_CLASS = "CondorG";


    /**
     * A table that maps, VDS style keys to the names of the corresponding classes
     * implementing the CondorStyle interface.
     */
    private static Map mImplementingClassNameTable;


    /**
     * A table that maps, VDS style keys to appropriate classes implementing the
     * CondorStyle interface
     */
    private  Map mImplementingClassTable ;

    /**
     * A boolean indicating that the factory has been initialized.
     */
    private boolean mInitialized;


    /**
     * The default constructor.
     */
    public CondorStyleFactory(){
        mImplementingClassTable = new HashMap(3);
        mInitialized = false;
    }

    /**
     * Initializes the Factory. Loads all the implementations just once.
     *
     * @param properties  the <code>PegasusProperties</code> object containing all
     *                    the properties required by Pegasus.
     * @param siteCatalog a handle to the Site Catalog being used.
     *
     * @throws CondorStyleFactoryException that nests any error that
     *            might occur during the instantiation of the implementation.
     */
    public void initialize( PegasusProperties properties,
                            PoolInfoProvider siteCatalog) throws CondorStyleFactoryException{

        //load all the implementations that correspond to the VDS style keys
        for( Iterator it = this.implementingClassNameTable().entrySet().iterator(); it.hasNext(); ){
            Map.Entry entry = (Map.Entry) it.next();
            String style    = (String)entry.getKey();
            String className= (String)entry.getValue();

            //load via reflection. not required in this case though
            put( style, this.loadInstance( properties, siteCatalog, className ));
        }

        //we have successfully loaded all implementations
        mInitialized = true;
    }


    /**
     * This method loads the appropriate implementing CondorStyle as specified
     * by the user at runtime. The CondorStyle is initialized and returned.
     *
     * @param job         the job for which the corresponding style is required.
     *
     * @throws CondorStyleFactoryException that nests any error that
     *            might occur during the instantiation of the implementation.
     */
    public CondorStyle loadInstance( SubInfo job )
                                            throws CondorStyleFactoryException{

        //sanity checks first
        if( !mInitialized ){
            throw new CondorStyleFactoryException(
                "CondorStyleFactory needs to be initialized first before using" );
        }
        String defaultStyle = job.getSiteHandle().equalsIgnoreCase( "local" )?
                              //jobs scheduled on local site have
                              //default style as condor
                              VDS.CONDOR_STYLE:
                              VDS.GLOBUS_STYLE;

        String style = job.vdsNS.containsKey( VDS.STYLE_KEY )?
                       (String)job.vdsNS.get( VDS.STYLE_KEY ):
                       defaultStyle;

        //need to check if the style isvalid or not
        //missing for now.

        //update the job with style determined
        job.vdsNS.construct( VDS.STYLE_KEY, style );

        //now just load from the implementing classes
        Object cs = this.get( style );
        if ( cs == null ) {
            throw new CondorStyleFactoryException( "Unsupported style " + style);
        }

        return (CondorStyle)cs;
    }

    /**
     * This method loads the appropriate Condor Style using reflection.
     *
     *
     * @param properties  the <code>PegasusProperties</code> object containing all
     *                    the properties required by Pegasus.
     * @param siteCatalog a handle to the Site Catalog being used.
     * @param className  the name of the implementing class.
     *
     * @return the instance of the class implementing this interface.
     *
     * @throws CondorStyleFactoryException that nests any error that
     *            might occur during the instantiation of the implementation.
     *
     * @see #DEFAULT_PACKAGE_NAME
     */
    private  CondorStyle loadInstance( PegasusProperties properties,
                                       PoolInfoProvider siteCatalog,
                                       String className )
                                              throws CondorStyleFactoryException{

        //sanity check
        if (properties == null) {
            throw new RuntimeException( "Invalid properties passed" );
        }
        if (className == null) {
            throw new RuntimeException( "Invalid className specified" );
        }

        //prepend the package name if classname is actually just a basename
        className = (className.indexOf('.') == -1) ?
            //pick up from the default package
            DEFAULT_PACKAGE_NAME + "." + className :
            //load directly
            className;

        //try loading the class dynamically
        CondorStyle cs = null;
        try {
            DynamicLoader dl = new DynamicLoader( className );
            cs = (CondorStyle) dl.instantiate( new Object[0] );
            //initialize the loaded condor style
            cs.initialize( properties, siteCatalog );
        }
        catch (Exception e) {
            throw new CondorStyleFactoryException( "Instantiating Condor Style ",
                                                   className,
                                                   e);
        }

        return cs;
    }

    /**
     * Returns the implementation from the implementing class table.
     *
     * @param style           the VDS style
     *
     * @return implementation  the class implementing that style, else null
     */
    private Object get( String style ){
        return mImplementingClassTable.get( style);
    }


    /**
     * Inserts an entry into the implementing class table.
     *
     * @param style           the VDS style
     * @param implementation  the class implementing that style.
     */
    private void put( String style, Object implementation){
        mImplementingClassTable.put( style, implementation );
    }


    /**
     * Returns a table that maps, the VDS style keys to the names of implementing
     * classes.
     *
     * @return a Map indexed by VDS styles, and values as names of implementing
     *         classes.
     */
    private static Map implementingClassNameTable(){
        if( mImplementingClassNameTable == null ){
            mImplementingClassNameTable = new HashMap(3);
            mImplementingClassNameTable.put( VDS.CONDOR_STYLE, CONDOR_STYLE_IMPLEMENTING_CLASS);
            mImplementingClassNameTable.put( VDS.GLIDEIN_STYLE, GLIDEIN_STYLE_IMPLEMENTING_CLASS);
            mImplementingClassNameTable.put( VDS.GLOBUS_STYLE, GLOBUS_STYLE_IMPLEMENTING_CLASS);
        }
        return mImplementingClassNameTable;
    }

}
