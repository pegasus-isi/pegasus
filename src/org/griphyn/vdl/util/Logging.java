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
package org.griphyn.vdl.util;

import edu.isi.pegasus.common.util.Currently;
import java.io.*;
import java.util.*;

/**
 * Create a common interface to handle logging of messages, debugging and streaming. In order to
 * avoid conflicts with JDK 1.4.*, this class is named Logging instead of Logger.
 *
 * <p>The logging mechanism works similar to syslog. There is an arbitrary number of user-named
 * queues, and a "default" queue. Each queue has a level associated with it. The higher the level,
 * the less important the message. If the message to be logged exceeds the level, it will not be
 * logged. Level 0 will always be logged, if a queue exists for it.
 *
 * <p>Usage is simple. Each queue has to be registered before use. The registrations associated the
 * output stream and maximum debug level.
 *
 * <p>Each log line will be prefixed by a time stamp. The logging class maintains internal state for
 * each queue, if it requested a line feed to be printed. Thus, you are able to construct a message
 * in several pieces, or a multi-line message by smuggling line feeds within the message.
 *
 * <p>
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 */
public class Logging {
    /** Keeper of the Singleton. */
    private static Logging m_instance = null;

    /** maintains the map with associated output streams. */
    private Hashtable m_queues = null;

    /** maintains the map with the maximum debug level per queue. */
    private Hashtable m_levels = null;

    /** maintains the line feed state for each queue. */
    private Hashtable m_newline = null;

    /** This is used to format the time stamp. */
    private static Currently m_formatter = null;

    /**
     * This is the verbose option. Any queue will be protocolled up to the verbose level, iff the
     * level is 0 or above. Verbose messages are dumped on the stream associated with "default",
     * which defaults to stderr.
     */
    private int m_verbose = -1;

    /** implement the Singleton pattern */
    public static Logging instance() {
        if (m_instance == null) m_instance = new Logging();
        return m_instance;
    }

    /** Ctor. */
    private Logging() {
        this.m_queues = new Hashtable();
        this.m_levels = new Hashtable();
        this.m_newline = new Hashtable();
        // Logging.m_formatter = new Currently( "yyyy-MM-dd HH:mm:ss.SSSZZZZZ: " );
        Logging.m_formatter = new Currently("yyyyMMdd'T'HHmmss.SSS: ");
        register("default", System.err, 0);
    }

    /**
     * Accessor: Obtains the default timestamp format for all queues.
     *
     * @return the currently active timestamp prefix format.
     */
    public static Currently getDateFormat() {
        return Logging.m_formatter;
    }

    /**
     * Accessor: Sets the default timestamp format for all queues.
     *
     * @param format is the new timestamp prefix format.
     */
    public static void setDateFormat(Currently format) {
        if (format != null) Logging.m_formatter = format;
    }

    /**
     * Registers a stream with a name to use for logging. The queue will be set up for maximum
     * logging, e.g. virtually all levels for this queue are logged.
     *
     * @param handle is the queue identifier
     * @param out is the name of a file to append to. Special names are <code>stdout</code> and
     *     <code>stderr</code>, which map to the system's respective streams.
     * @see #register( String, OutputStream, int )
     */
    public void register(String handle, String out) {
        if (out.equals("stdout")) {
            this.register(handle, System.out, Integer.MAX_VALUE);
        } else if (out.equals("stderr")) {
            this.register(handle, System.err, Integer.MAX_VALUE);
        } else {
            try {
                FileOutputStream fout = new FileOutputStream(out, true);
                this.register(handle, new BufferedOutputStream(fout), Integer.MAX_VALUE);
            } catch (FileNotFoundException e) {
                log(
                        "default",
                        0,
                        "unable to append \"" + handle + "\" to " + out + ": " + e.getMessage());
            } catch (SecurityException e) {
                log(
                        "default",
                        0,
                        "unable to append \"" + handle + "\" to " + out + ": " + e.getMessage());
            }
        }
    }

    /**
     * Registers a stream with a name to use for logging. The queue will be set up for maximum
     * logging, e.g. virtually all levels for this queue are logged.
     *
     * @param handle is the queue identifier
     * @param out is the new output stream
     * @see #register( String, OutputStream, int )
     */
    public void register(String handle, OutputStream out) {
        this.register(handle, out, Integer.MAX_VALUE);
    }

    /**
     * Registers a stream with a name to use for logging. The queue will be set up to use the output
     * stream. If there was another stream previously registered, it will be closed!
     *
     * @param handle is the queue identifier
     * @param out is the output stream associated with the queue
     * @param level is the maximum debug level to put into the queue
     */
    public void register(String handle, OutputStream out, int level) {
        PrintWriter previous = (PrintWriter) m_queues.put(handle, new PrintWriter(out, true));
        // don't close System.out nor System.err. So, rely on Java to close
        // if ( previous != null ) previous.close();
        m_levels.put(handle, new Integer(level));
        m_newline.put(handle, new Boolean(true));
    }

    /**
     * Determines the maximum level up to which messages on the given queue are protocolled. The
     * associated stream is unaffected.
     *
     * @param handle is the queue identifier
     * @return the maximum inclusive log level, or -1 for error
     * @see #setLevel( String, int )
     */
    public int getLevel(String handle) {
        if (isUnset(handle)) return -1;
        Integer i = (Integer) m_levels.get(handle);
        return (i != null ? i.intValue() : -1);
    }

    /**
     * Set the maximum level up to which messages on the given queue are protocolled. The associated
     * stream is unaffected.
     *
     * @param handle is the queue identifier
     * @param level is the new maximum log level (non-negative integer)
     * @see #setLevel( String, int )
     */
    public void setLevel(String handle, int level) {
        if (isUnset(handle)) return;
        if (level < 0) return;
        m_levels.put(handle, new Integer(level));
    }

    /**
     * Obtains the current verbosity level.
     *
     * @return -1 for no verbosity, or the level up to which messages are logged.
     * @see #setVerbose( int )
     */
    public int getVerbose() {
        return this.m_verbose;
    }

    /**
     * Sets the maximum verbosity.
     *
     * @see #resetVerbose()
     */
    public void setVerbose() {
        this.m_verbose = Integer.MAX_VALUE;
    }

    /**
     * Deactivates any verbosity.
     *
     * @see #setVerbose()
     */
    public void resetVerbose() {
        this.m_verbose = -1;
    }

    /**
     * Sets or resets the verbosity level.
     *
     * @param max is the maximum inclusive level to which messages on any queue should be logged. A
     *     value of -1 (or any negative value) will deactivate verbosity mode.
     * @see #getVerbose()
     */
    public void setVerbose(int max) {
        this.m_verbose = max;
    }

    /**
     * Prints a message on a previously registered stream.
     *
     * @param handle is the symbolic queue handle.
     * @param level is a verbosity level. The higher the level, the more debug like the message.
     *     Messages of level 0 will always be printed.
     * @param msg is the message to put onto the stream. Please note that this function will
     *     automatically add the line break.
     */
    public void log(String handle, int level, String msg) {
        this.log(handle, level, msg, true);
    }

    /**
     * Checks if a queue is free to be set up. This is important for initialization to setup default
     * queues, but allow user overrides.
     *
     * @param handle names the queue to check for a stream.
     * @return true, if the queue is not yet connected.
     */
    public boolean isUnset(String handle) {
        // sanity check
        if (handle == null) return false;

        return (this.m_queues.get(handle) == null);
    }

    /**
     * Prints a message on a previously registered stream.
     *
     * @param handle is the symbolic queue handle.
     * @param level is a verbosity level. The higher the level, the more debug like the message.
     *     Messages of level 0 will always be printed.
     * @param msg is the message to put onto the stream.
     * @param newline is a boolean, which will call invoke the println method.
     */
    public void log(String handle, int level, String msg, boolean newline) {
        Integer maximum = (Integer) this.m_levels.get(handle);

        // do something, if verbosity if active
        boolean verbose = this.m_verbose >= 0 && level <= this.m_verbose;

        // do nothing, if we don't know about this level
        // do nothing, if the maximum level is below chosen debug level
        if (verbose || (maximum != null && (level == 0 || level <= maximum.intValue()))) {
            // determine stream to dump message upon
            PrintWriter pw = (PrintWriter) this.m_queues.get(verbose ? "default" : handle);

            // if stream is known and without fault, dump message
            if (pw != null && !pw.checkError()) {
                String prefix = new String();

                // determine state of last message
                Boolean nl = (Boolean) this.m_newline.get(handle);

                // if last message had a newline attached, prefix with new timestamp
                if (nl == null || nl.booleanValue())
                    prefix += Logging.m_formatter.now() + '[' + handle + "] ";

                // print message
                if (newline) pw.println(prefix + msg);
                else pw.print(prefix + msg);

                // save new newline state for the stream.
                this.m_newline.put(handle, new Boolean(newline));
            }
        }
    }
}
