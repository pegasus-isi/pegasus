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

import edu.isi.pegasus.common.util.Separator;
import edu.isi.pegasus.common.util.Currently;
import java.io.*;
import java.util.*;

/**
 * This file factory generates a stream of submit files within the same toplevel
 * directory. There is no subdirectory structuring.
 *
 * @author Kavitha Ranganathan
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 *
 * @see DAX2DAG
 */
public class FlatFileFactory implements FileFactory {

    /**
     * Contains the base directory where to store all files into.
     */
    private File m_directory;

    /**
     * Counts the number of times the virtual constructor was called.
     */
    private int m_count;

    /**
     * Helping structure to avoid repeated memory requests. Stores the path
     * string of the base directory for later perusal.
     *
     * @see #getBaseDirectory()
     */
    private String mh_directory;

    /**
     * Resets the helper structures after changing layout parameters. You will
     * also need to call this function after you invoked the virtual
     * constructors, but want to change parameter pertaining to the directory
     * structure. The structured file count will also be reset!
     */
    public void reset() {
        m_count = 0;
        mh_directory = m_directory.getPath();
    }

    /**
     * Constructor: Creates the directory and employs sanity checks.
     *
     * @param directory is the place where files should be placed.
     * @throws IOException if the location is not a writable directory, or
     * cannot be created as such.
     */
    public FlatFileFactory(File directory)
            throws IOException {
        sanityCheck(directory);
        m_directory = directory;
        m_count = 0;
        mh_directory = m_directory.getPath();
    }

    /**
     * Constructor: Creates the directory and employs sanity checks.
     *
     * @param directory is the place where files should be placed.
     * @throws IOException if the location is not a writable directory, or
     * cannot be created as such.
     */
    public FlatFileFactory(String directory)
            throws IOException {
        File base = new File(directory);
        sanityCheck(base);
        m_directory = base;
        m_count = 0;
        mh_directory = m_directory.getPath();
    }

    /**
     * Virtual constructor: Creates the next file with the given basename.
     *
     * @param basename is the filename to create. Don't specify dirs here.
     * @return a File structure which points to the new file. Nothing is created
     * through this method, and creation may still fail.
     * @see #getCount()
     */
    public File createFile(String basename)
            throws IOException {
        return createFlatFile(basename);
    }
    
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
            throws IOException{
        m_count++;
        return new File( ".", basename);
    }

    /**
     * Virtual constructor: Creates the next file with the given basename.
     *
     * @param basename is the filename to create. Don't specify dirs here.
     * @return a File structure which points to the new file. Nothing is created
     * through this method, and creation may still fail.
     * @see #getCount()
     */
    public File createFlatFile(String basename)
            throws IOException {
        m_count++;
        return new File(m_directory, basename);
    }

    /**
     * Returns the number of times the virtual constructor for structured files
     * was called. Since this is a flat file class, it will be 0.
     *
     * @return the count for createFile invocations.
     * @see #createFile( String )
     */
    public int getCount() {
        return 0;
    }

    /**
     * Returns the number of times the virtual constructor was called.
     *
     * @return the count for createFile invocations.
     * @see #createFile( String )
     */
    public int getFlatCount() {
        return m_count;
    }

    /**
     * Returns base directory. We want to be on the safe side.
     *
     * @return the directory passed to the constructor.
     * @see #setBaseDirectory( File )
     */
    public File getBaseDirectory() {
        return m_directory;
    }

    /**
     * Checks the destination location for existence, if it can be created, if
     * it is writable etc.
     *
     * @param dir is the new base directory to optionally create
     */
    protected void sanityCheck(File dir)
            throws IOException {
        if (dir.exists()) {
            // location exists
            if (dir.isDirectory()) {
                // ok, isa directory
                if (dir.canWrite()) {
                    // can write, all is well
                    return;
                } else {
                    // all is there, but I cannot write to dir
                    throw new IOException("Cannot write to existing directory "
                            + dir.getPath());
                }
            } else {
                // exists but not a directory
                throw new IOException("Destination " + dir.getPath() + " already "
                        + "exists, but is not a directory.");
            }
        } else {
            // does not exist, try to make it
            if (!dir.mkdirs()) {
                throw new IOException("Unable to create directory destination "
                        + dir.getPath());
            }
        }
    }

    /**
     * Accessor to set a different base directory. Note that this accessor may
     * only be called before any of the virtual constructors were invoked the
     * first time.
     *
     * @param base is the new directory to set things to
     * @throws VTorInUseException if the virtual constructor is already in use.
     * @throws IOException if there is something wrong with the directory
     * @see #getBaseDirectory()
     * @see #reset()
     */
    public void setBaseDirectory(File base)
            throws IOException, VTorInUseException {
        if (m_count != 0) {
            throw new VTorInUseException();
        }
        sanityCheck(base);
        m_directory = base;
        reset(); // will invoke _virtual_ reset :-)
    }

    /**
     * Constructs a virtual basename by removing the directory from a given
     * file. If the file's directory does not match our stored directory prefix,
     * nothing will be removed. This method is essential in order to assemble
     * relative pathnames to the base directory.
     *
     * @param file is an arbitrary file, should have been constructed using the
     * virtual constructor.
     * @return a relative pathname with the base directory removed. Note that
     * may will still contain directories. In case of an arbitrary file, the
     * full filename may be returned.
     */
    public String getName(File file) {
        String path = file.getPath();
        if (path.startsWith(mh_directory)) {
            // yes, one of ours
            return path.substring(mh_directory.length() + 1);
        } else {
            // eek, an alien
            return path;
        }
    }
}
