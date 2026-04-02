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
package edu.isi.pegasus.planner.invocation;

/**
 * This interface defines a common base for all File elements in an invocation record that carry a
 * filename in their values. It exists primarily for grouping purposes and for easier access for the
 * database manager.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 */
public interface HasFilename {
    /**
     * Accessor: Obtains the name of the file
     *
     * @return the name of the file objects. Null is legal.
     * @see #setFilename(String)
     */
    public String getFilename();

    /**
     * Accessor: Sets the name of an file object.
     *
     * @param filename is the new name to store as filename.
     * @see #getFilename()
     */
    public void setFilename(String filename);
}
