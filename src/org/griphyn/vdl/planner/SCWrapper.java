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

package org.griphyn.vdl.planner;

import java.io.*;
import java.util.*;

import org.griphyn.vdl.util.Logging;
import edu.isi.pegasus.planner.catalog.site.impl.oldimpl.classes.SiteInfo;
import edu.isi.pegasus.planner.classes.Profile;
import edu.isi.pegasus.planner.catalog.site.impl.oldimpl.classes.WorkDir;
import edu.isi.pegasus.planner.catalog.site.impl.oldimpl.PoolMode;
import edu.isi.pegasus.planner.catalog.site.impl.oldimpl.PoolInfoProvider;
import edu.isi.pegasus.planner.common.PegasusProperties;

/**
 * This class wraps the shell planner's request into the new site
 * catalog API. The site catalog is only queried for the contents of its
 * "local" special site. 
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 *
 */
public class SCWrapper implements Wrapper
{
  /**
   * site catalog API reference.
   */
  private PoolInfoProvider m_sc = null;

  /**
   * Connects the interface with the site catalog implementation. The
   * choice of backend is configured through properties.
   */
  public SCWrapper()
  {
    try {
      PegasusProperties p = PegasusProperties.nonSingletonInstance();
      String poolClass = PoolMode.getImplementingClass( p.getPoolMode() );
      m_sc = PoolMode.loadPoolInstance( poolClass, p.getPoolFile(),
					PoolMode.NON_SINGLETON_LOAD );
    } catch ( Exception e ) {
      Logging.instance().log( "planner", 0, "Warning: While loading SC: " +
			      e.getMessage() + ", ignoring" );
      m_sc = null;
    }
  }

  /**
   * Frees resources taken by the instance of the replica catalog. This
   * method is safe to be called on failed or already closed catalogs.
   */
  public void close()
  {
    if ( m_sc != null )  m_sc = null;
  }

  /**
   * garbage collection.
   */
  protected void finalize() 
  {
    close();
  }

  /**
   * Determines the working directory for the site "local". 
   *
   * @return the working directory, of <code>null</code>, if
   * not available.
   */
  public String getWorkingDirectory() 
  {
    // sanity check
    if ( m_sc == null ) return null;

    String result = null;
    try {
      result = m_sc.getExecPoolWorkDir("local");
    } catch ( NullPointerException npe ) {
      // noop
    }

    // sanitization
    if ( result != null && result.length() == 0 ) result = null;
    return result;
  }

  /**
   * Determines the path to the local installation of a grid launcher
   * for site "local".
   *
   * @return the path to the local kickstart, or <code>null</code>, if
   * not available.
   */
  public String getGridLaunch()
  {
    // sanity check
    if ( m_sc == null ) return null;

    String result = null;
    try {
      SiteInfo siv = m_sc.getPoolEntry( "local", "vanilla" );
      SiteInfo sit = m_sc.getPoolEntry( "local", "transfer" );
      if ( siv != null ) {
	result = siv.getKickstartPath();
      } else if ( sit != null ) {
	result = sit.getKickstartPath();
      }
    } catch ( NullPointerException npe ) {
      // noop
    }

    // sanitization
    if ( result != null && result.length() == 0 ) result = null;
    return result;
  }

  /**
   * Gathers all profiles declared for pool local. 
   *
   * @return a map of maps, the outer map indexed by the profile
   * namespace, and the inner map indexed by the key in the profile.
   * Returns <code>null</code> in case of error.
   */
  public Map getProfiles() 
  { 
    Map result = new HashMap();

    // sanity checks
    if ( m_sc == null ) return null;

    // ask site catalog
    List lop = m_sc.getPoolProfile("local");

    // return empty maps now, if there are no profiles
    if ( lop == null || lop.size() == 0 ) return result;

    Map submap;
    for ( Iterator i=lop.iterator(); i.hasNext(); ) {
            edu.isi.pegasus.planner.classes.Profile p =
	(   edu.isi.pegasus.planner.classes.Profile) i.next();
      String ns = p.getProfileNamespace().trim().toLowerCase();
      String key = p.getProfileKey().trim();
      String value = p.getProfileValue();

      // insert at the right place into the result map
      if ( result.containsKey(ns) ) {
	submap = (Map) result.get(ns);
      } else {
	result.put( ns, (submap = new HashMap()) );
      }
      submap.put( key, value );
    }
      
    return result;
  }

  /**
   * Obtains the name of the class implementing the replica catalog.
   * 
   * @return class name of the replica catalog implementor.
   */
  public String getName() 
  { 
    return ( m_sc == null ? null : m_sc.getClass().getName() );
  }

  /**
   * Shows the contents of the catalog as one string. Warning, this may
   * be very large, slow, and memory expensive.
   *
   * @return the string with the complete catalog contents.
   * @throws RuntimeException because the method is not implemented. 
   */
  public String toString() 
  {
    throw new RuntimeException("method not implemented");
  }
}
