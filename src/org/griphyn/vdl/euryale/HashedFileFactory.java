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
import java.util.*;

/**
 * This file factory generates a stream of submit files in a dynamically
 * determinable directory structure. By default, a 2-level subdirectory
 * structure is assumed, which should be able to accomodate about 500k files.
 *
 * <pre>
 * mult=16, offset=30, fpd=254: nr=15 => l=1
 * mult=16, offset=30, fpd=254: nr=4047 => l=2
 * mult=16, offset=30, fpd=254: nr=1028222 => l=3
 * </pre>
 *
 * With the given multiplicator, offset and files per directory, nr is smallest
 * number of jobs at which a level change to l occurs.
 *
 * @author Kavitha Ranganathan
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 *
 * @see DAX2DAG
 */
public class HashedFileFactory extends FlatFileFactory {

    /**
     * Determines dynamically the number of directory levels required to
     * accomodate a certain number of files.
     *
     * <pre>
     *    levels = |log   ( tf * m + offset )|
     *                 fpd
     * </pre>
     *
     * @param totalFiles is the total number of files estimated to generate
     * @param multiplicator is a corrective factor to account for files that are
     * created by the run-time system on the fly. For Euryale and Pegasus it is
     * safe to assume a multiplicator of at least 8.
     * @param filesPerDirectory is the optimal maximum number of directory
     * entries in any directory. The value of 254 for Linux ext2, and thus ext3,
     * is a safe bet.
     * @param offset is the number of (expected) files in the top level.
     * @return the number of directory levels necessary to accomodate the given
     * number of files.
     */
    public static int calculateLevels(int totalFiles,
            int multiplicator,
            int filesPerDirectory,
            int offset) {
        // total files to accomodate, including ones cropping up later
        long total = totalFiles * multiplicator + offset;

    // "count" the levels
        // return (int) Math.floor( Math.log(total) / Math.log(filesPerDirectory) );
        int levels = 0;
        while (total > filesPerDirectory) {
            ++levels;
            total /= filesPerDirectory;
        }
        return levels;
    }

    /**
     * Counts the number of times the structured virtual constructor was called.
     *
     * @see #getCount()
     */
    protected int m_count;

    /**
     * Contains the total number of directory levels. Defaults to a reasonable
     * level for hashing.
     */
    protected int m_levels = 2;

    /**
     * Number of entries per level. The number 254 is optimized for the Linux
     * VFS ext2, and consequently ext3, which works fastest, if the number of
     * entries per directory, including dot and dotdot, don't exceed 256.
     */
    protected int m_filesPerDirectory = 254;

    /**
     * Multiplicative factor to estimate the number of result leaf filenames for
     * each virtual constructor invocation. We assume that Euryale produces ~12
     * files per submit file. It is better to err on the larger side than
     * makeing the multiplicator too small.
     */
    protected int m_multiplicator = 16;

    /**
     * Offset of files expected to reside at the top level directory. This is
     * counted in addition to the directories being created.
     */
    protected int m_offset = 30;

    /**
     * Helping structure to avoid repeated memory requests. Stores the directory
     * number for each level.
     *
     * @see #createFile( String )
     */
    protected int mh_level[];

    /**
     * Helping structure to avoid repeated memory requests. Stores the digits
     * necessary to create one level's directory name.
     *
     * @see #format( int )
     */
    protected StringBuffer mh_buffer;

    /**
     * Helping structure to avoid repeated memory requests. Stores the number of
     * digits for hexadecimal formatting.
     *
     * @see #createFile( String )
     */
    protected int mh_digits;

    /**
     * Resets the helper structures after changing layout parameters. You will
     * also need to call this function after you invoked the virtual
     * constructors, but want to change parameter pertaining to the directory
     * structure. The structured file count will also be reset!
     */
    public void reset() {
        super.reset();
        m_count = 0;
        mh_level = new int[m_levels];
        mh_digits = (int) Math.ceil(Math.log(m_filesPerDirectory) / Math.log(16));
        mh_buffer = new StringBuffer(mh_digits);
    }

    /**
     * Constructor: Creates the base directory and employs sanity checks.
     *
     * @param baseDirectory is the place where the other dirs are created, and
     * where the DAG file resides.
     * @throws IOException if the location is not a writable directory, or
     * cannot be created as such.
     */
    public HashedFileFactory(File baseDirectory)
            throws IOException {
        super(baseDirectory);
        reset();
    }

    /**
     * Constructor: Creates the directory and employs sanity checks.
     *
     * @param baseDirectory is the place where the other dirs are created, and
     * where the DAG file resides.
     * @throws IOException if the location is not a writable directory, or
     * cannot be created as such.
     */
    public HashedFileFactory(String baseDirectory)
            throws IOException {
        super(baseDirectory);
        reset();
    }

    /**
     * Constructor: Creates the base directory and employs sanity checks.
     *
     * @param baseDirectory is the place where the other dirs are created, and
     * where the DAG file resides.
     * @param totalFiles is the number of files to support, and the number of
     * times, the virtual constructor is expected to be called.
     * @throws IOException if the location is not a writable directory, or
     * cannot be created as such.
     */
    public HashedFileFactory(File baseDirectory, int totalFiles)
            throws IOException {
        super(baseDirectory);
        m_levels = calculateLevels(totalFiles,
                m_multiplicator,
                m_filesPerDirectory,
                m_offset);
        reset();
    }

    /**
     * Constructor: Creates the directory and employs sanity checks.
     *
     * @param baseDirectory is the place where the other dirs are created, and
     * where the DAG file resides.
     * @param totalFiles is the number of files to support, and the number of
     * times, the virtual constructor is expected to be called.
     * @throws IOException if the location is not a writable directory, or
     * cannot be created as such.
     */
    public HashedFileFactory(String baseDirectory, int totalFiles)
            throws IOException {
        super(baseDirectory);
        m_levels = calculateLevels(totalFiles,
                m_multiplicator,
                m_filesPerDirectory,
                m_offset);
        reset();
    }

    /**
     * Converts the given integer into hexadecimal notation, using the given
     * number of digits, prefixing with zeros as necessary.
     *
     * @param number is the number to format.
     * @return a string of appropriate length, filled with leading zeros,
     * representing the number hexadecimally.
     */
    public String format(int number) {
        mh_buffer.delete(0, mh_digits);
        mh_buffer.append(Integer.toHexString(number).toUpperCase());
        while (mh_buffer.length() < mh_digits) {
            mh_buffer.insert(0, '0');
        }
        return mh_buffer.toString();
    }

    /**
     * Virtual constructor: Creates the next file with the given basename.
     *
     * @param basename is the filename to create. Don't specify dirs here.
     * 
     * @return a relative File structure (relative to the base directory)
     * which points to the new file.
     * @throws java.io.IOException
     * 
     * @see #getCount()
     */
    @Override
    public File createRelativeFile(String basename)
            throws IOException{
        
        File f = this.createFile(basename);       
        
        StringBuffer relative = new StringBuffer();
        //figure out the relative path
        //from the base directory
        File base = this.getBaseDirectory();
        Stack<String> s = new Stack();
        File parent = null;
        while( (parent = f.getParentFile()) != null ){
            String child = f.getName();
            s.push( child );
            
            if( parent.equals( base ) ){
                //have the relative path created by 
                //poping the stack
                String comp = null;
                while( !s.isEmpty() ){
                    comp = s.pop();
                    relative.append( comp );
                    if( !s.empty() ){
                        relative.append( File.separator );
                    }
                }
                break;
            }
            
            f = parent;
        }
        //System.out.println( relative );
        return new File( relative.toString() );
    }
    
    /**
     * Creates the next file with the given basename. This is the factory
     * standard virtual constructor. Once invoked, the directory structure can
     * not be changed any more.
     *
     * @param basename is the filename to create. Don't specify dirs here.
     * @return a File structure which points to the new file. Nothing is created
     * through this method, and creation may still fail.
     * @see #getCount()
     */
    public File createFile(String basename)
            throws IOException {
    // calculate the directory which this goes into
        ////int estimate = m_count++ * m_multiplicator;
        int estimate = (m_count++ * m_multiplicator) + m_offset;
        for (int i = m_levels - 1; i >= 0; --i) {
            estimate /= m_filesPerDirectory;
            mh_level[i] = estimate % m_filesPerDirectory;
        }
        if (estimate > m_filesPerDirectory) {
            throw new RuntimeException("ERROR! Wrap-around of generator.");
        }

        //create the base directory if required
        File d = createDirectory();

        // return position in new (or old) directory
        return new File(d, basename);
    }

    /**
     * Creates a directory for the hashed file directory structure on the submit
     * host.
     *
     *
     * @return the File structure to the created directory
     *
     * @throws IOException the exception.
     */
    protected File createDirectory() throws IOException {
        // create directory, as necessary
        File d = getBaseDirectory();
        for (int i = 0; i < m_levels; ++i) {
            d = new File(d, format(mh_level[i]));
            if (d.exists()) {
                if (!d.isDirectory()) {
                    throw new IOException(d.getPath() + " is not a directory");
                }
            } else {
                if (!d.mkdir()) {
                    throw new IOException("unable to create directory " + d.getPath());
                }
            }
        }
        return d;
    }

    /**
     * Returns the number of times the regular virtual constructor for
     * structured entries was called.
     *
     * @return the count for createFile invocations.
     * @see #createFile( String )
     */
    public int getCount() {
        return m_count;
    }

    /**
     * Accessor: Obtains the total number of directory levels.
     *
     * @return the total number of directory levels chosen.
     */
    public int getLevels() {
        return m_levels;
    }

    /**
     * Accessor: Sets the number of directory levels. Note that this modificator
     * can only be called before the virtual constructor is called the first
     * time.
     *
     * @param levels is the number of directory levels to use
     * @throws VTorInUseException if the virtual constructor is already in use.
     * @throws IllegalArgumentException if the argument is less than zero.
     * @see #getLevels()
     */
    public void setLevels(int levels) {
        if (m_count != 0) {
            throw new VTorInUseException();
        }
        if (levels < 0) {
            throw new IllegalArgumentException();
        }
        m_levels = levels;
        reset();
    }

    /**
     * Accessor: Sets the number of directory levels. Note that this modificator
     * can only be called before the virtual constructor is called the first
     * time. It takes as argument the total number of expected files instead of
     * the level.
     *
     * @param totalFiles is the total number of files to accomodate.
     * @throws VTorInUseException if the virtual constructor is already in use.
     * @throws IllegalArgumentException if the argument is less than zero.
     * @see #getLevels()
     */
    public void setLevelsFromTotals(int totalFiles) {
        if (m_count != 0) {
            throw new VTorInUseException();
        }
        if (totalFiles < 0) {
            throw new IllegalArgumentException();
        }
        m_levels = calculateLevels(totalFiles,
                m_multiplicator,
                m_filesPerDirectory,
                m_offset);
        reset();
    }

    /**
     * Accessor: Obtains the number of entries per directory.
     *
     * @return the chosen number of entries per directory excluding the dot and
     * dotdot files.
     */
    public int getFilesPerDirectory() {
        return m_filesPerDirectory;
    }

    /**
     * Accessor: Sets the optimal maximum number of files per directory
     * excluding dot and dotdot. For a Linux ext2 and thus ext3 system, the
     * optimal maximum number is 254.
     *
     * @param entries is the number of optimal maximum entries per dir.
     * @throws VTorInUseException if the virtual constructor is already in use.
     * @throws IllegalArgumentException if the argument is less than one.
     * @see #getFilesPerDirectory()
     */
    public void setFilesPerDirectory(int entries) {
        if (m_count != 0) {
            throw new VTorInUseException();
        }
        if (entries <= 0) {
            throw new IllegalArgumentException();
        }
        m_filesPerDirectory = entries;
        reset();
    }

    /**
     * Accessor: Obtains the multiplicative factor for an estimation of total
     * files from calls to the virtual constructor.
     *
     * @return the multiplicator.
     * @see #setMultiplicator(int)
     */
    public int getMultiplicator() {
        return m_multiplicator;
    }

    /**
     * Accessor: Sets the multiplicative factor to account for files which may
     * be created without calling the virtual constructor.
     *
     * @param multiplicator is the new multiplicator.
     * @throws VTorInUseException if the virtual constructor is already in use.
     * @throws IllegalArgumentException if the argument is less than one.
     * @see #getMultiplicator()
     */
    public void setMultiplicator(int multiplicator) {
        if (m_count != 0) {
            throw new VTorInUseException();
        }
        if (multiplicator < 1) {
            throw new IllegalArgumentException();
        }
        m_multiplicator = multiplicator;
        reset();
    }

    /**
     * Accessor: Obtains the offset for an estimation of total files from calls
     * to the virtual constructor.
     *
     * @return the offset
     * @see #setOffset(int)
     */
    public int getOffset() {
        return m_offset;
    }

    /**
     * Accessor: Sets the offset for files which may be created without calling
     * the virtual constructor.
     *
     * @param offset is the new offset
     * @throws VTorInUseException if the virtual constructor is already in use.
     * @throws IllegalArgumentException if the argument is less than zero.
     * @see #getOffset()
     */
    public void setOffset(int offset) {
        if (m_count != 0) {
            throw new VTorInUseException();
        }
        if (offset < 0) {
            throw new IllegalArgumentException();
        }
        m_offset = offset;
        reset();
    }

    /**
     * test function
     */
    public static void main(String arg[])
            throws Exception {
        if (arg.length == 0) {
            // no arguments, spit out at which point levels change
            HashedFileFactory def = new HashedFileFactory("/tmp");
            int level = 0;
            for (int i = 1; i < 4; ++i) {
                def.reset();
                int nr = 1;
                for (int n = i; n > 0; n--) {
                    nr *= def.getFilesPerDirectory();
                }
                nr -= def.getOffset();
                nr /= def.getMultiplicator();
                for (int j = -2; j < Integer.MAX_VALUE; ++j) {
                    int n = nr + j;
                    def.reset();
                    def.setLevelsFromTotals(n);
                    if (level < def.getLevels()) {
                        ++level;
                        System.out.println("mult=" + def.getMultiplicator()
                                + ", offset=" + def.getOffset()
                                + ", fpd=" + def.getFilesPerDirectory()
                                + ": nr=" + n + " => l=" + level);
                        break;
                    }
                }
            }

        } else {
            // arguments, assume numeric strings
            for (int i = 0; i < arg.length; ++i) {
                int nr = Integer.parseInt(arg[i]);
                HashedFileFactory hff = new HashedFileFactory("/tmp");
                hff.setLevelsFromTotals(nr);
                System.out.println();
                System.out.println("filesPerDirectory = " + hff.getFilesPerDirectory());
                System.out.println("multiplicator = " + hff.getMultiplicator());
                System.out.println("offset = " + hff.getOffset());
                System.out.println("totalFiles = " + nr);
                System.out.println("levels = " + hff.getLevels());

                File f = hff.createFile("ID000001");
                System.out.println("example = \"" + f.getAbsolutePath() + "\"");
            }
            System.out.println();
        }
    }
}
