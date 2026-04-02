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
 * This interface defines a common base for all elements in an invocation record that can carry text
 * in their values. It exists primarily for grouping purposes and for easier access through the
 * character SAX callback. Due to the fact that SAX may present text in several chunks, all
 * text-carrying classes must also provide the append function.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 */
public interface HasText {
    /**
     * Appends a piece of text to the existing text.
     *
     * @param fragment is a piece of text to append to existing text. Appending <code>null</code> is
     *     a noop.
     */
    public void appendValue(String fragment);

    /**
     * Accessor
     *
     * @see #setValue(String)
     */
    public String getValue();

    /**
     * Accessor.
     *
     * @param value is the new value to set.
     * @see #getValue()
     */
    public void setValue(String value);
}
