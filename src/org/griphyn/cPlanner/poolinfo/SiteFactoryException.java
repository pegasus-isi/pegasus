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

package org.griphyn.cPlanner.poolinfo;

import org.griphyn.common.util.FactoryException;

/**
 * Class to notify of failures while instantiating Site Catalog
 * implementations.
 *
 * @author Karan Vahi
 * @version $Revision$
 */

public class SiteFactoryException extends FactoryException {

    /**
     * The default classname that is associated with the exception.
     */
    public static final String DEFAULT_NAME = "Site Catalog";


    /**
     * Constructs a <code>SiteFactoryException</code> with no detail
     * message. The associated classname is set to value specified by
     * <code>DEFAULT_NAME</code>.
     *
     * @param msg the detailed message.
     *
     * @see #DEFAULT_NAME
     */
    public SiteFactoryException( String msg ) {
        super( msg );
        mClassname = this.DEFAULT_NAME;
    }

    /**
     * Constructs a <code>SiteFactoryException</code> with the specified detailed
     * message.
     *
     * @param msg       is the detailed message.
     * @param classname the name of class that was trying to be instantiated or
     *                  some other signifier like module name.
     */
    public SiteFactoryException( String msg, String classname ) {
        super( msg , classname );
    }


    /**
     * Constructs a <code>SiteFactoryException</code> with the
     * specified detailed message and a cause. The associated classname is set
     * to value specified by <code>DEFAULT_NAME</code>.
     *
     * @param msg       is the detailed message that is to be logged.
     * @param cause is the cause (which is saved for later retrieval by the
     * {@link java.lang.Throwable#getCause()} method). A <code>null</code>
     * value is permitted, and indicates that the cause is nonexistent or
     * unknown.
     *
     * @see #DEFAULT_NAME
     */
    public SiteFactoryException( String msg, Throwable cause ) {
        super(msg, cause);
        mClassname = this.DEFAULT_NAME;
    }


    /**
     * Constructs a <code>SiteFactoryException</code> with the
     * specified detailed message and a cause.
     *
     * @param msg       is the detailed message that is to be logged.
     * @param classname the name of class that was trying to be instantiated.
     * @param cause is the cause (which is saved for later retrieval by the
     * {@link java.lang.Throwable#getCause()} method). A <code>null</code>
     * value is permitted, and indicates that the cause is nonexistent or
     * unknown.
     */
    public SiteFactoryException( String msg,
                                 String classname,
                                 Throwable cause ) {

        super( msg, cause );
        mClassname = classname;
    }


}
