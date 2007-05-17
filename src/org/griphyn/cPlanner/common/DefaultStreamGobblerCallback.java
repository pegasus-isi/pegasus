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
package org.griphyn.cPlanner.common;

/**
 * The default callback for the stream gobbler, that logs all the messages to
 * a particular logging level. By default all the messages are logged onto the
 * DEBUG level.
 *
 * @author Karan Vahi
 * @version $Revision: 1.1 $
 */
public class DefaultStreamGobblerCallback implements StreamGobblerCallback {

    /**
     * The level on which the messages are to be logged.
     */
    private int mLevel;

    /**
     * The instance to the logger to log messages.
     */
    private LogManager mLogger;

    /**
     * The overloaded constructor.
     *
     * @param level   the level on which to log.
     */
    public DefaultStreamGobblerCallback(int level) {
        //should do a sanity check on the levels
        mLevel  = level;
        mLogger = LogManager.getInstance();
    }

    /**
     * Callback whenever a line is read from the stream by the StreamGobbler.
     * The line is logged to the level specified while initializing the
     * class.
     *
     * @param line   the line that is read.
     */
    public void work(String line) {
        mLogger.log( line , mLevel);
    }

}