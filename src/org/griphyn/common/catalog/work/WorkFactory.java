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
package org.griphyn.common.catalog.work;

import java.lang.reflect.*;
import java.io.IOException;
import java.util.Properties;
import java.util.Enumeration;
import java.util.MissingResourceException;

import org.griphyn.common.util.*;
import org.griphyn.common.catalog.*;

import org.griphyn.cPlanner.common.PegasusProperties;

/**
 * This factory loads a work catalog, as specified by the properties.
 * Each invocation of the factory will result in a new instance of a
 * connection to the replica catalog.
 *
 * @author Karan Vahi
 * @author Jens-S. Vöckler
 * @version $Revision: 50 $
 *
 * @see org.griphyn.common.catalog.WorkCatalog
 */
public class WorkFactory{

    /**
     * Package to prefix "just" class names with.
     */
    public static final String DEFAULT_PACKAGE =   "org.griphyn.common.catalog.work";



    /**
     * Connects the interface with the work catalog implementation. The
     * choice of backend is configured through properties. This class is
     * useful for non-singleton instances that may require changing
     * properties.
     *
     * @param props is an instance of properties to use.
     *
     * @exception ClassNotFoundException if the schema for the database
     * cannot be loaded. You might want to check your CLASSPATH, too.
     * @exception NoSuchMethodException if the schema's constructor interface
     * does not comply with the database driver API.
     * @exception InstantiationException if the schema class is an abstract
     * class instead of a concrete implementation.
     * @exception IllegalAccessException if the constructor for the schema
     * class it not publicly accessible to this package.
     * @exception InvocationTargetException if the constructor of the schema
     * throws an exception while being dynamically loaded.
     *
     * @see org.griphyn.common.util.VDSProperties
     * @see #loadInstance()
     */
    static public WorkCatalog loadInstance( PegasusProperties props )
           throws WorkFactoryException {

        return loadInstance( props.getVDSProperties() );
    }


    /**
     * Connects the interface with the work catalog implementation. The
     * choice of backend is configured through properties. This class is
     * useful for non-singleton instances that may require changing
     * properties.
     *
     * @param props is an instance of properties to use.
     *
     * @exception ClassNotFoundException if the schema for the database
     * cannot be loaded. You might want to check your CLASSPATH, too.
     * @exception NoSuchMethodException if the schema's constructor interface
     * does not comply with the database driver API.
     * @exception InstantiationException if the schema class is an abstract
     * class instead of a concrete implementation.
     * @exception IllegalAccessException if the constructor for the schema
     * class it not publicly accessible to this package.
     * @exception InvocationTargetException if the constructor of the schema
     * throws an exception while being dynamically loaded.
     *
     * @see org.griphyn.common.util.VDSProperties
     * @see #loadInstance()
     */
    static public WorkCatalog loadInstance( VDSProperties props )
      throws WorkFactoryException
    {
        // sanity check
        if ( props == null ) throw new NullPointerException("invalid properties");


        Properties connect = props.matchingSubset( WorkCatalog.c_prefix, false );

        //get the default db driver properties in first pegasus.catalog.*.db.driver.*
        Properties db = props.matchingSubset( WorkCatalog.DBDRIVER_ALL_PREFIX, false );
        //now overload with the work catalog specific db properties.
        //pegasus.catalog.work.db.driver.*
        db.putAll( props.matchingSubset( WorkCatalog.DBDRIVER_PREFIX , false ) );


        //to make sure that no confusion happens.
        //add the db prefix to all the db properties
        for( Enumeration e = db.propertyNames(); e.hasMoreElements(); ){
            String key = (String)e.nextElement();
            connect.put( "db.driver." + key, db.getProperty( key ));
        }

        //put the driver property back into the DB property
        String driver = props.getProperty( WorkCatalog.DBDRIVER_PREFIX );
        if( driver == null ){ driver = props.getProperty( WorkCatalog.DBDRIVER_ALL_PREFIX ); }
        connect.put( "db.driver", driver );



        // determine the class that implements the work catalog
        return loadInstance( props.getProperty( WorkCatalog.c_prefix ),
                             connect );
    }



  /**
   * Connects the interface with the work catalog implementation. The
   * choice of backend is configured through properties. This class is
   * useful for non-singleton instances that may require changing
   * properties.
   *
   * @param props is an instance of properties to use.
   *
   * @exception ClassNotFoundException if the schema for the database
   * cannot be loaded. You might want to check your CLASSPATH, too.
   * @exception NoSuchMethodException if the schema's constructor interface
   * does not comply with the database driver API.
   * @exception InstantiationException if the schema class is an abstract
   * class instead of a concrete implementation.
   * @exception IllegalAccessException if the constructor for the schema
   * class it not publicly accessible to this package.
   * @exception InvocationTargetException if the constructor of the schema
   * throws an exception while being dynamically loaded.
   *
   * @see org.griphyn.common.util.VDSProperties
   * @see #loadInstance()
   */
  static private WorkCatalog loadInstance( String catalogImplementor,
                                          Properties props )
    throws WorkFactoryException
  {
    WorkCatalog result = null;


    try{
        if ( catalogImplementor == null )
            throw new RuntimeException( "You need to specify the " +
                                        WorkCatalog.c_prefix + " property" );


        // syntactic sugar adds absolute class prefix
        if ( catalogImplementor.indexOf('.') == -1 )
            catalogImplementor = DEFAULT_PACKAGE + "." + catalogImplementor;
            // POSTCONDITION: we have now a fully-qualified classname

        DynamicLoader dl = new DynamicLoader( catalogImplementor );
        result = (WorkCatalog) dl.instantiate( new Object[0] );

        if ( ! result.connect( props ) )
            throw new RuntimeException( "Unable to connect to work catalog implementation" );
    }
    catch( Exception e ) {
        throw new WorkFactoryException(
                " Unable to instantiate Work Catalog ",
                catalogImplementor,
                e );

    }

    // done
    return result;
  }

}
