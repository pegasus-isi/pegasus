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
package org.griphyn.vdl.classes;

import java.io.Serializable;
import java.util.*;

/**
 * <code>Leaf</code> is an abstract base class for leaf nodes in the instance tree. FIXME: The
 * content value is actually handled in the child classes, and needs to be unified into this class.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 * @see Text
 * @see Use
 * @see LFN
 */
public abstract class Leaf extends VDL implements Cloneable, Serializable {
    /**
     * Creates and returns a copy of this object.
     *
     * @return a new instance.
     */
    public abstract Object clone();

    /**
     * This is the name for a value element.
     *
     * @deprecated Originally we thought to employ this value, but it is currently unused.
     */
    private transient String m_value;

    /**
     * Accessor method. Obtains the current state of the thus named attribute.
     *
     * @return the current state of the m_name attribute.
     * @see #setValue(String)
     * @deprecated the attribute is currently unused.
     */
    public String getValue() {
        return this.m_value;
    }

    /**
     * Accessor method. Sets the attribute of same name to a new value.
     *
     * @param value is the new value to overwrite the current state with.
     * @see #getValue()
     * @deprecated the name attribute is currently unused.
     */
    public void setValue(String value) {
        this.m_value = value;
    }
}
