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
package org.griphyn.common.util;

import java.io.*;
import java.util.*;
import java.util.regex.*;

/**
 * This class creates a common interface to handle package properties.
 * The package properties are meant as read-only (so far, until
 * requirements crop up for write access). The class is implemented
 * as a Singleton pattern.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @author Karan Vahi
 * @version $Revision$
 * */
public class VDSProperties
{
  /**
   * implements the singleton access via class variable.
   */
  private static VDSProperties m_instance = null;

  /**
   * internal set of properties. Direct access is expressly forbidden.
   */
  private Properties m_props;

  /**
   * internal copy of a system property. There is read-only access.
   */
  private String m_home;

  /**
   * GNU: read-only architecture-independent data in DIR [PREFIX/share].
   * The files in this directory have a low change frequency, are
   * effectively read-only, can be shared via a networked FS, and they
   * are usually valid for multiple users.
   */
  private File m_dataDir;

  /**
   * GNU: read-only single-machine data in DIR [PREFIX/etc].
   * The files in this directory have a low change frequency, are
   * effectively read-only, they reside on a per-machine basis, and they
   * are usually valid for a single user.
   */
  private File m_sysConfDir;

  /**
   * GNU: modifiable architecture-independent data in DIR [PREFIX/com].
   * The files in this directory have a high change frequency, are
   * effectively read-write, can be shared via a networked FS, and they
   * are usually valid for multiple users.
   */
  private File m_sharedStateDir;

  /**
   * GNU: modifiable single-machine data in DIR [PREFIX/var].
   * The files in this directory have a high change frequency, are
   * effectively read-write, they reside on a per-machine basis, and they
   * are usually valid for a single user.
   */
  private File m_localStateDir;

  /**
   * Basename of the file to read to obtain system properties
   */
  public static final String PROPERTY_FILENAME = "properties";

  /**
   * Basename of the (old) file to read for user properties.
   * Warning, the old name will eventually fall prey to bit rot.
   */
  public static final String OLD_USER_PROPERTY_FILENAME = ".chimerarc";

  /**
   * Basename of the (new) file to read for user properties.
   */
  public static final String USER_PROPERTY_FILENAME = ".pegasusrc";


  /**
   * Adds new properties to an existing set of properties while
   * substituting variables. This function will allow value
   * substitutions based on other property values. Value substitutions
   * may not be nested. A value substitution will be ${property.key},
   * where the dollar-brace and close-brace are being stripped before
   * looking up the value to replace it with. Note that the ${..}
   * combination must be escaped from the shell.
   *
   * @param a is the initial set of known properties (besides System ones)
   * @param b is the set of properties to add to a
   * @return the combined set of properties from a and b.
   */
  protected static Properties addProperties( Properties a, Properties b )
  {
    // initial
    Properties result = new Properties(a);
    Properties sys = System.getProperties();
    Pattern pattern = Pattern.compile( "\\$\\{[-a-zA-Z0-9._]+\\}" );

    for ( Enumeration e = b.propertyNames(); e.hasMoreElements(); ) {
      String key = (String) e.nextElement();
      String value = b.getProperty(key);

      // unparse value ${prop.key} inside braces
      Matcher matcher = pattern.matcher(value);
      StringBuffer sb = new StringBuffer();
      while ( matcher.find() ) {
	// extract name of properties from braces
	String newKey = value.substring( matcher.start()+2, matcher.end()-1 );

	// try to find a matching value in result properties
	String newVal = result.getProperty(newKey);

	/*
	 * // if not found, try b's properties
	 * if ( newVal == null ) newVal = b.getProperty(newKey);
	 */

	// if still not found, try system properties
	if ( newVal == null ) newVal = sys.getProperty(newKey);

	// replace braced string with the actual value or empty string
	matcher.appendReplacement( sb, newVal == null ? "" : newVal );
      }
      matcher.appendTail( sb );
      result.setProperty( key, sb.toString() );
    }

    // final
    return result;
  }

  /**
   * Set some defaults, should values be missing in the dataset.
   *
   */
  private static Properties defaultProps()
  {
    // initial
    Properties result = new Properties();

    // copy pegasus keys as specified in the system properties to defaults
    Properties sys = System.getProperties();
    for ( Enumeration e = sys.propertyNames(); e.hasMoreElements(); ) {
      String key = (String) e.nextElement();
      if ( key.startsWith("pegasus.") )
	result.setProperty( key, sys.getProperty(key) );
    }

    // INSERT HERE!

    // final
    return addProperties( new Properties(), result );
  }

  /**
   * ctor. This initializes the local instance of properties
   * from a central file.
   *
   * @param propFilename is the basename of the file to read. This file
   * will be looked for in the $PEGASUS_HOME/etc directory. Usually, the name
   * will be set from the PROPERTY_FILENAME constant above.
   * Alternatively, the name will be ignored, if an alternative properties
   * location is specified via <code>-Dpegasus.properties</code>.
   * @exception IOException will be thrown if reading the property file
   * goes awry.
   * @exception MissingResourceException will be thrown if you forgot
   * to specify the <code>-Dpegasus.home=$PEGASUS_HOME</code> to the runtime
   * environment.
   */
  protected VDSProperties( String propFilename )
    throws IOException, MissingResourceException
  {
    // create empty new instance
    this.m_props = new Properties( defaultProps() );

    // We have to write the appropriate shell scripts to translate
    // between $PEGASUS_HOME and "java -Dpegasus.home=$PEGASUS_HOME <class>..."
    this.m_home = System.getProperty( "pegasus.home" );

    // if this is a valid path, continue
    if ( this.m_home != null && this.m_home.length() > 0 ) {
      // check for a file called "$PEGASUS_HOME/etc/<propFilename>"
      // in a system-independent fashion, allowing for overrides
      String alternative = System.getProperty( "pegasus.home.sysconfdir" );
      File etcDir = ( alternative == null ?
		      new File( this.m_home, "etc" ) :
		      new File( alternative ) );

      // check for an alternative property file spec
      alternative = System.getProperty("pegasus.properties" );
      File props = ( alternative == null ?
		     new File( etcDir, propFilename ) :
		     new File( alternative ) );

      if ( props.exists() ) {
	// if this file exists, read the properties (will throw IOException)
	Properties temp = new Properties();
	InputStream stream =
	  new BufferedInputStream( new FileInputStream(props) );
	temp.load( stream );
	stream.close();

	this.m_props = addProperties( this.m_props, temp );
      }

      // add user properties afterwards to have higher precedence
      String userHome = System.getProperty( "user.home", "." );
      alternative = System.getProperty( "pegasus.user.properties" );
      if ( alternative == null ) {
	// Prefer $HOME/.pegasusrc over $HOME/.chimerarc
	props = new File( userHome, VDSProperties.USER_PROPERTY_FILENAME );
	if ( props.exists() ) {
	  // new user props file does exist, sanity check for old one
	  File old = new File( userHome,
			       VDSProperties.OLD_USER_PROPERTY_FILENAME );
	  if ( old.exists() ) {
	    // both user props exist, does the user know what he's doing?
	    System.out.println( "INFO: Both user property files " +
				old.getName() + " and " + props.getName() +
				" exist, using " +
				VDSProperties.USER_PROPERTY_FILENAME );
	  }
	} else {
	  // new user props file did not exist, check for old user props file
	  props = new File( userHome, VDSProperties.OLD_USER_PROPERTY_FILENAME );
	}
      } else {
	// was overwritten -- use user's overwrite
	props = new File(alternative);
      }

      if ( props.exists() ) {
	// if this file exists, read the properties (will throw IOException)
	Properties temp = new Properties();
	InputStream stream =
	  new BufferedInputStream( new FileInputStream(props) );
	temp.load( stream );
	stream.close();

	this.m_props = addProperties( this.m_props, temp );
      }

      // now set the paths: set sysconfdir to correct latest value
      alternative = this.m_props.getProperty( "pegasus.home.datadir" );
      this.m_dataDir = ( alternative == null ?
			 new File( this.m_home, "share" ) :
			 new File( alternative ) );
      alternative = this.m_props.getProperty( "pegasus.home.sysconfdir" );
      this.m_sysConfDir = ( alternative == null ?
			    new File( this.m_home, "etc" ) :
			    new File( alternative ) );
      alternative = this.m_props.getProperty( "pegasus.home.sharedstatedir" );
      this.m_sharedStateDir = ( alternative == null ?
				new File( this.m_home, "com" ) :
				new File( alternative ) );
      alternative = this.m_props.getProperty( "pegasus.home.localstatedir" );
      this.m_localStateDir = ( alternative == null ?
			       new File( this.m_home, "var" ) :
			       new File( alternative ) );
    } else {
      // die on (forgotten) missing property home
      throw new MissingResourceException( "The pegasus.home property was not set!",
					  "java.util.Properties", "pegasus.home" );
    }
  }

  /**
   * Singleton threading: Creates the one and only instance of the
   * properties in the current application.
   *
   * @return a reference to the properties.
   * @exception IOException will be thrown if reading the property file
   * goes awry.
   * @exception MissingResourceException will be thrown if you forgot
   * to specify the <code>-Dpegasus.home=$PEGASUS_HOME</code> to the runtime
   * environment.
   * @see #noHassleInstance()
   */
  public static VDSProperties instance()
    throws IOException, MissingResourceException
  {
    if ( VDSProperties.m_instance == null )
      VDSProperties.m_instance =
	new VDSProperties( VDSProperties.PROPERTY_FILENAME );
    return VDSProperties.m_instance;
  }

  /**
   * Create a temporary property that is not attached to the Singleton.
   * This may be helpful with portal, which do magic things during the
   * lifetime of a process.
   *
   * @param propFilename is the full path name to the location of the
   * properties file to read. In case of null, the default location
   * will be taken
   * @return a reference to the parsed properties.
   * @exception IOException will be thrown if reading the property file
   * goes awry.
   * @exception MissingResourceException will be thrown if you forgot
   * to specify the <code>-Dpegasus.home=$PEGASUS_HOME</code> to the runtime
   * environment.
   * @see #instance()
   */
  public static VDSProperties nonSingletonInstance( String propFilename )
    throws IOException, MissingResourceException
  {
    return new VDSProperties( (propFilename == null) ?
			      // pick up the default value
			      VDSProperties.PROPERTY_FILENAME :
			      // pick up the file mentioned
			      propFilename );
  }


  /**
   * Singleton interface: Creates the one and only instance of the
   * properties in the current application, and does not bother the
   * programmer with exceptions. Rather, exceptions from the underlying
   * <code>instance()</code> call are caught, converted to an error
   * message on stderr, and the program is exited.
   *
   * @return a reference to the properties.
   * @see #instance()
   */
  public static VDSProperties noHassleInstance()
  {
    VDSProperties result = null;
    try {
      result = instance();
    } catch ( IOException e ) {
      System.err.println( "While reading property file: " + e.getMessage() );
      System.exit(1);
    } catch ( MissingResourceException mre ) {
      System.err.println( "You probably forgot to set the -Dpegasus.home=$PEGASUS_HOME" );
      System.exit(1);
    }
    return result;
  }

  /**
   * Accessor: Obtains the root directory of the VDS runtime
   * system. Kept to make ChimeraProperties.java to compile for time being
   *
   * @return the root directory of the Pegasus runtime system, as initially
   * set from the system properties.
   */
  public String getVDSHome()
  {
    return this.m_home;
  }


  /**
   * Accessor: Obtains the root directory of the Pegasus runtime
   * system.
   *
   * @return the root directory of the Pegasus runtime system, as initially
   * set from the system properties.
   */
  public String getPegasusHome()
  {
    return this.m_home;
  }

  /**
   * Accessor to $PEGASUS_HOME/share. The files in this directory have a low
   * change frequency, are effectively read-only, can be shared via a
   * networked FS, and they are valid for multiple users.
   *
   * @return the "share" directory of the VDS runtime system.
   */
  public File getDataDir()
  {
    return this.m_dataDir;
  }

  /**
   * Accessor to $PEGASUS_HOME/etc. The files in this directory have a low
   * change frequency, are effectively read-only, they reside on a
   * per-machine basis, and they are valid usually for a single user.
   *
   * @return the "etc" directory of the VDS runtime system.
   */
  public File getSysConfDir()
  {
    return this.m_sysConfDir;
  }

  /**
   * Accessor to $PEGASUS_HOME/com. The files in this directory have a high
   * change frequency, are effectively read-write, they reside on a
   * per-machine basis, and they are valid usually for a single user.
   *
   * @return the "com" directory of the VDS runtime system.
   */
  public File getSharedStateDir()
  {
    return this.m_sharedStateDir;
  }

  /**
   * Accessor to $PEGASUS_HOME/var. The files in this directory have a high
   * change frequency, are effectively read-write, they reside on a
   * per-machine basis, and they are valid usually for a single user.
   *
   * @return the "var" directory of the VDS runtime system.
   */
  public File getLocalStateDir()
  {
    return this.m_localStateDir;
  }

  /**
   * Accessor: Obtains the number of properties known to the project.
   *
   * @return number of properties in the project property space.
   */
  public int size()
  {
    return this.m_props.size();
  }

  /**
   * Accessor: access to the internal properties as read from file.
   * An existing system property of the same key will have precedence
   * over any project property. This method will remove leading and
   * trailing ASCII control characters and whitespaces.
   *
   * @param key is the key to look up
   * @return the value for the key, or null, if not found.
   */
  public String getProperty( String key )
  {
    String result =
      System.getProperty( key, this.m_props.getProperty(key) );
    return ( result == null ? result : result.trim() );
  }

  /**
   * Accessor: access to the internal properties as read from file
   * An existing system property of the same key will have precedence
   * over any project property. This method will remove leading and
   * trailing ASCII control characters and whitespaces.
   *
   * @param key is the key to look up
   * @param defValue is a default to use, if no value can be found for the key.
   * @return the value for the key, or the default value, if not found.
   */
  public String getProperty( String key, String defValue )
  {
    String result =
      System.getProperty( key, this.m_props.getProperty(key,defValue) );
    return ( result == null ? result : result.trim() );
  }

  /**
   * Accessor: Overwrite any properties from within the program.
   *
   * @param key is the key to look up
   * @param value is the new property value to place in the system.
   * @return the old value, or null if it didn't exist before.
   */
  public Object setProperty( String key, String value )
  {
    return System.setProperty( key, value );
  }

  /**
   * Accessor: enumerate all keys known to this property collection
   * @return an enumerator for the keys of the properties.
   */
  public Enumeration propertyNames()
  {
    return this.m_props.propertyNames();
  }

  /**
   * Extracts a specific property key subset from the known properties.
   * The prefix may be removed from the keys in the resulting dictionary,
   * or it may be kept. In the latter case, exact matches on the prefix
   * will also be copied into the resulting dictionary.
   *
   * @param prefix is the key prefix to filter the properties by.
   * @param keepPrefix if true, the key prefix is kept in the resulting
   * dictionary. As side-effect, a key that matches the prefix exactly
   * will also be copied. If false, the resulting dictionary's keys are
   * shortened by the prefix. An exact prefix match will not be copied,
   * as it would result in an empty string key.
   * @return a property dictionary matching the filter key. May be
   * an empty dictionary, if no prefix matches were found.
   *
   * @see #getProperty( String ) is used to assemble matches
   */
  public Properties matchingSubset( String prefix, boolean keepPrefix )
  {
    Properties result = new Properties();

    // sanity check
    if ( prefix == null || prefix.length() == 0 ) return result;

    String prefixMatch;  // match prefix strings with this
    String prefixSelf;   // match self with this
    if ( prefix.charAt(prefix.length()-1) != '.' ) {
      // prefix does not end in a dot
      prefixSelf = prefix;
      prefixMatch = prefix + '.';
    } else {
      // prefix does end in one dot, remove for exact matches
      prefixSelf = prefix.substring( 0, prefix.length()-1 );
      prefixMatch = prefix;
    }
    // POSTCONDITION: prefixMatch and prefixSelf are initialized!

    // now add all matches into the resulting properties.
    // Remark 1: #propertyNames() will contain the System properties!
    // Remark 2: We need to give priority to System properties. This is done
    // automatically by calling this class's getProperty method.
    String key;
    for ( Enumeration e = propertyNames(); e.hasMoreElements(); ) {
      key = (String) e.nextElement();

      if ( keepPrefix ) {
	// keep full prefix in result, also copy direct matches
	if ( key.startsWith(prefixMatch) || key.equals(prefixSelf) )
	  result.setProperty( key,
			      getProperty(key) );
      } else {
	// remove full prefix in result, dont copy direct matches
	if ( key.startsWith(prefixMatch) )
	  result.setProperty( key.substring( prefixMatch.length() ),
			      getProperty(key) );
      }
    }

    // done
    return result;
  }
}
