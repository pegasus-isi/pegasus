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
import org.apache.tools.ant.*;
import edu.isi.pegasus.common.util.Version;

/**
 * This class exists only for compilation-with-ant purpose. It extends
 * the ant property to report the chosen version number to the
 * compilation process.
 *
 * @author Gaurang Mehta
 * @author Karan Vahi
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision: 50 $
 */
public class VersionTask extends Task
{
  /**
   * member variable to obatain the major version number.
   * @see org.griphyn.common.util.Version#MAJOR
   */
  private int major;

  /**
   * member variable to obatain the minor version number.
   * @see org.griphyn.common.util.Version#MINOR
   */
  private int minor;

  /**
   * member variable to obatain the patch-level number.
   * @see org.griphyn.common.util.Version#PLEVEL
   */
  private int patch;

  /**
   * member variable to maintain the version string.
   * @see org.griphyn.common.util.Version#toString()
   */
  private String version;

  /**
   * Accessor to set the major version number.
   * @param major is the new major version number.
   */
  public void setMajor( int major )
  { this.major = major; }

  /**
   * Accessor to set the minor version number.
   * @param minor is the new minor version number.
   */
  public void setMinor( int minor )
  { this.minor = minor; }

  /**
   * Accessor to set the patch-level number.
   * @param patch is the new patch-level number.
   */
  public void setPatch( int patch )
  { this.patch = patch; }

  /**
   * Accessor to set the version string. No matching is
   * done to verify consistency with the simple numbers.
   * @param version is the version string. 
   */
  public void setVersion( String version )
  { this.version = version; }

  /**
   * Initializes the member variables for an ant task.
   *
   * @exception BuildException if something goes bump with ant.
   * @see org.griphyn.common.util.Version
   */
  public void init()
    throws BuildException
  {
    super.init();

    this.major = Version.MAJOR;
    this.minor = Version.MINOR;
    this.patch = Version.PLEVEL;
    this.version = Version.instance().toString();
  }

  /**
   * Executes the ant task, and populates the environmental properties
   * for the current project. The environmental properties propagate
   * the Java-encoded version number of the software into the build
   * process.
   *
   * @exception BuildException if something goes bump with ant.
   */
  public void execute()
    throws BuildException
  {
    Project p = getProject();
    p.setProperty( "pegasus.version.major", Integer.toString(this.major) );
    p.setProperty( "pegasus.version.minor", Integer.toString(this.minor) );
    p.setProperty( "pegasus.version.patch", Integer.toString(this.patch) );
    p.setProperty( "pegasus.version", this.version );
  }
}
