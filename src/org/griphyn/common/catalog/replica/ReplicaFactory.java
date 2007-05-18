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
package org.griphyn.common.catalog.replica;

import java.lang.reflect.*;
import java.io.IOException;
import java.util.Properties;
import java.util.MissingResourceException;

import org.griphyn.common.util.*;
import org.griphyn.common.catalog.*;

import org.griphyn.cPlanner.common.PegasusProperties;

/**
 * This factory loads a replica catalog, as specified by the properties.
 * Each invocation of the factory will result in a new instance of a
 * connection to the replica catalog.
 *
 * @author Karan Vahi
 * @author Jens-S. Vöckler
 * @version $Revision$
 *
 * @see org.griphyn.common.catalog.ReplicaCatalog
 * @see org.griphyn.common.catalog.ReplicaCatalogEntry
 * @see org.griphyn.common.catalog.replica.JDBCRC
 */
public class ReplicaFactory{

    /**
     * Package to prefix "just" class names with.
     */
    public static final String DEFAULT_PACKAGE =   "org.griphyn.common.catalog.replica";


    /**
     * Connects the interface with the replica catalog implementation. The
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
    static public ReplicaCatalog loadInstance( PegasusProperties props )
           throws ClassNotFoundException, IOException,
           NoSuchMethodException, InstantiationException,
           IllegalAccessException, InvocationTargetException {

        return loadInstance( props.getVDSProperties() );
    }


    /**
     * Connects the interface with the replica catalog implementation. The
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
    static public ReplicaCatalog loadInstance( VDSProperties props )
      throws ClassNotFoundException, IOException,
             NoSuchMethodException, InstantiationException,
             IllegalAccessException, InvocationTargetException
    {
        // sanity check
        if ( props == null ) throw new NullPointerException("invalid properties");

        // determine the class that implements the replica catalog
        return loadInstance( props.getProperty( ReplicaCatalog.c_prefix ),
                             props.matchingSubset( ReplicaCatalog.c_prefix, false )
                             );
    }


  /**
   * Connects the interface with the replica catalog implementation. The
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
  static public ReplicaCatalog loadInstance( String catalogImplementor,
                                             Properties props )
    throws ClassNotFoundException, IOException,
	   NoSuchMethodException, InstantiationException,
	   IllegalAccessException, InvocationTargetException
  {
    ReplicaCatalog result = null;



    if ( catalogImplementor == null )
      throw new RuntimeException( "You need to specify the " +
				  ReplicaCatalog.c_prefix + " property" );
    // for Karan: 2005-10-27
    if ( catalogImplementor.equalsIgnoreCase("rls") )
      catalogImplementor = "RLI";

    // syntactic sugar adds absolute class prefix
    if ( catalogImplementor.indexOf('.') == -1 )
      catalogImplementor = DEFAULT_PACKAGE + "." + catalogImplementor;
    // POSTCONDITION: we have now a fully-qualified classname

    DynamicLoader dl = new DynamicLoader( catalogImplementor );
    result = (ReplicaCatalog) dl.instantiate( new Object[0] );
    if ( result == null )
      throw new RuntimeException( "Unable to load " + catalogImplementor );


    if ( ! result.connect( props ) )
      throw new RuntimeException( "Unable to connect to replica catalog implementation" );

    // done
    return result;
  }

  /**
   * Connects the interface with the replica catalog implementation. The
   * choice of backend is configured through properties. This method uses
   * default properties from the property singleton.
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
   * @exception MissingResourceException if the properties could not
   * be loaded properly.
   *
   * @see org.griphyn.common.util.VDSProperties
   * @see #loadInstance( org.griphyn.common.util.VDSProperties )
   */
  static public ReplicaCatalog loadInstance()
    throws ClassNotFoundException, IOException,
	   NoSuchMethodException, InstantiationException,
	   IllegalAccessException, InvocationTargetException,
	   MissingResourceException
  {
    return loadInstance( VDSProperties.instance() );
  }
}
