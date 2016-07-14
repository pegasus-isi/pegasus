/*
 * This file or a portion of this file is licensed under the terms of
 * the Globus Toolkit Public License, found in file ../GTPL, or at
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

package org.griphyn.vdl.euryale;

import java.io.IOException;
import java.io.File;

/**
 *
 * A Virtual Hashed File Factory that does not do any existence checks while
 * creating a directory. The factory, is used to create remote paths without
 * checking for correctness.
 *
 *
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class VirtualHashedFileFactory
                                     extends HashedFileFactory {


   /**
   * Constructor: Creates the base directory and employs sanity checks.
   * @param baseDirectory is the place where the other dirs are created,
   * and where the DAG file resides.
   * @throws IOException if the location is not a writable directory,
   * or cannot be created as such.
   */
  public VirtualHashedFileFactory( File baseDirectory )
                                                           throws IOException  {
    super(baseDirectory);
  }

  /**
   * Constructor: Creates the directory and employs sanity checks.
   * @param baseDirectory is the place where the other dirs are created,
   * and where the DAG file resides.
   * @throws IOException if the location is not a writable directory,
   * or cannot be created as such.
   */
  public VirtualHashedFileFactory( String baseDirectory )
                                                           throws IOException  {
    super(baseDirectory);
  }

  /**
   * Constructor: Creates the base directory and employs sanity checks.
   * @param baseDirectory is the place where the other dirs are created,
   * and where the DAG file resides.
   * @param totalFiles is the number of files to support, and the number
   * of times, the virtual constructor is expected to be called.
   * @throws IOException if the location is not a writable directory,
   * or cannot be created as such.
   */
  public VirtualHashedFileFactory( File baseDirectory, int totalFiles )
                                                           throws IOException  {
    super(baseDirectory, totalFiles );
  }

  /**
   * Constructor: Creates the directory and employs sanity checks.
   * @param baseDirectory is the place where the other dirs are created,
   * and where the DAG file resides.
   * @param totalFiles is the number of files to support, and the number
   * of times, the virtual constructor is expected to be called.
   * @throws IOException if the location is not a writable directory,
   * or cannot be created as such.
   */
  public VirtualHashedFileFactory( String baseDirectory, int totalFiles )
                                                           throws IOException  {
    super(baseDirectory, totalFiles);
  }

 

  /**
   * Checks the destination location for existence, if it can
   * be created, if it is writable etc. Does no check as it is
   * virtual.
   *
   * @param dir is the new base directory to optionally create
   */
   protected void sanityCheck( File dir )    throws IOException{

   }

   /**
   * Creates a directory for the hashed file directory structure on the
   * submit host. It only creates the File with correct path name, however
   * does not physically create the file.
   *
   *
   * @return the File structure to the created directory
   *
   * @throws IOException the exception.
   */
  protected File createDirectory(  ) throws IOException{
      // create directory, as necessary
      File d = getBaseDirectory();
      for ( int i=0; i<m_levels; ++i ) {
          d = new File( d, format( mh_level[i] ) );
      }
      return d;
  }


}
