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
package org.griphyn.vdl.annotation;


/**
 * This abstract base class defines the key value tuple that define the heart of Yong's annotations.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 */
public abstract class Tuple {
    /** Defines the key for which we store the annotation. */
    private String m_key;

    /** Array constructor, please don't use. */
    public Tuple() {
        m_key = null;
    }

    /**
     * Constructs the key portion of a tuple.
     *
     * @param key is the key to store annotations for.
     */
    public Tuple(String key) {
        m_key = key;
    }

    /**
     * Obtains the current value of the key.
     *
     * @return the current value of the key.
     * @see #setKey( String )
     */
    public String getKey() {
        return m_key;
    }

    /**
     * Overwrites the key with a different name.
     *
     * @param key is the new key to use from now on.
     * @see #getKey()
     */
    public void setKey(String key) {
        m_key = key;
    }

    /**
     * Generate interface to obtain the value from an instance. The value will be wrapped into the
     * appropriate Java class. For the timestamp object, the Java SQL Date will be used.
     *
     * @return the value wrapped as Java object.
     * @see #setValue( Object )
     */
    public abstract Object getValue();

    /**
     * Generic interface to set a value in an instance. Note that this action may fail, if the
     * instance is of an incompatible type.
     *
     * @param value is the data object to set.
     * @see #getValue()
     * @exception ClassCastException if the actual argument type of the value is incompatible with
     *     the value maintained by the instance.
     */
    public abstract void setValue(Object value) throws ClassCastException;

    /**
     * Returns the type of the value as SQL Type.
     *
     * @return a constant from the set of SQL types.
     * @see java.sql.Types
     */
    public abstract int getType();

    /** Returns the string representation of the type. */
    public abstract String getTypeString();

    /** Return a string representation of the tuple */
    public String toString() {
        return m_key + "\t" + getTypeString() + "\t" + getValue();
    }
}
