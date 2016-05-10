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

import java.io.*;

/**
 * This interface describes what file factories should look like.
 *
 * @author Kavitha Ranganathan
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 *
 * @see DAX2DAG
 */
public interface FileFactory {
    
    /**
     * Virtual constructor: Creates the next file with the given basename.
     *
     * @param basename is the filename to create. Don't specify dirs here.
     * @return a File structure which points to the new file.
     * @see #getCount()
     */
    public File createFile(String basename)
            throws IOException;

    /**
     * Virtual constructor: Creates the next file with the given basename.
     *
     * @param basename is the filename to create. Don't specify dirs here.
     * 
     * @return a relative File structure (relative to the base directory)
     * which points to the new file.
     * 
     * @see #getCount()
     */
    public File createRelativeFile(String basename)
            throws IOException;
    
    /**
     * Returns the number of times the regular virtual constructor for
     * structured entries was called.
     *
     * @return the count for createFile invocations.
     * @see #createFile( String )
     */
    public int getCount();

    /**
     * Virtual constructor: Creates the next file with the given basename which
     * is guaranteed to be created in the base directory, and never in any
     * structured directories that child classes may implement.
     *
     * @param basename is the filename to create. Don't specify dirs here.
     * @return a File structure which points to the new file.
     * @see #getFlatCount()
     */
    public File createFlatFile(String basename)
            throws IOException;

    /**
     * Returns the number of times the virtual constructor for flat files was
     * called.
     *
     * @return the count for createFlatFile invocations.
     * @see #createFlatFile( String )
     */
    public int getFlatCount();
    
    /**
     * Resets the helper structures after changing layout parameters. You will
     * also need to call this function after you invoked the virtual
     * constructors, but want to change parameter pertaining to the directory
     * structure. The file counters will also be reset!
     */
    public void reset();
}
