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

import java.io.IOException;
import java.io.Writer;
import java.util.*;

/**
 * This class is the base class for a file object.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 */
public abstract class File extends Invocation implements HasText {
    /** optional first 16 byte of file, or less if shorter. */
    protected String m_hexbyte;

    /** Default c'tor: Construct a hollow shell and allow further information to be added later. */
    public File() {
        m_hexbyte = null;
    }

    /**
     * C'tor: Constructs the value in the base class.
     *
     * @param value is all or part of the hex bytes.
     */
    public File(String value) {
        m_hexbyte = value;
    }

    /**
     * Appends a piece of text to the existing text.
     *
     * @param fragment is a piece of text to append to existing text. Appending <code>null</code> is
     *     a noop.
     */
    public void appendValue(String fragment) {
        if (fragment != null) {
            if (this.m_hexbyte == null) this.m_hexbyte = new String(fragment);
            else this.m_hexbyte += fragment;
        }
    }

    /**
     * Accessor
     *
     * @see #setValue(String)
     */
    public String getValue() {
        return this.m_hexbyte;
    }

    /**
     * Accessor.
     *
     * @param hexbyte
     * @see #getValue()
     */
    public void setValue(String hexbyte) {
        this.m_hexbyte = hexbyte;
    }

    /**
     * Converts the active state into something meant for human consumption. The method will be
     * called when recursively traversing the instance tree.
     *
     * @param stream is a stream opened and ready for writing. This can also be a string stream for
     *     efficient output.
     */
    public void toString(Writer stream) throws IOException {
        throw new IOException("method not implemented, please contact vds-support@griphyn.org");
    }
}
