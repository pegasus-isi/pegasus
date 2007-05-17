/**
 * This file or a portion of this file is licensed under the terms of
 * the Globus Toolkit Public License, found at $PEGASUS_HOME/GTPL or
 * http://www.globus.org/toolkit/download/license.html.
 * This notice must appear in redistributions of this file
 * with or without modification.
 *
 * Redistributions of this Software, with or without modification, must reproduce
 * the GTPL in:
 * (1) the Software, or
 * (2) the Documentation or
 * some other similar material which is provided with the Software (if any).
 *
 * Copyright 1999-2004
 * University of Chicago and The University of Southern California.
 * All rights reserved.
 */

package org.griphyn.common.classes;

/**
 * This is an enumerated data class for the different types of operating systems.
 *
 * @author Gaurang Mehta gmehta@isi.edu
 * @version $Revision: 1.4 $
 */

import java.io.Serializable;
import java.util.HashMap;

public class Os
    implements Serializable {
    private String _value_;
    private static HashMap _table_ = new HashMap(5);

    protected Os(String value) {
        _value_ = value;
        _table_.put(_value_, this);
    }

    private static final String _LINUX = "LINUX";
    private static final String _SUNOS = "SUNOS";
    private static final String _AIX = "AIX";

    public static final Os LINUX = new Os(_LINUX);
    public static final Os SUNOS = new Os(_SUNOS);
    public static final Os AIX = new Os(_AIX);

    public static final String err = "Error: Illegal Operating System defined. Please specify one of the predefined types \n [LINUX, SUNOS, AIX]";

    /**
     * Returns the value of the operating system as string.
     * @return String
     */
    public String getValue() {
        return _value_;
    }

    /**
     * Creates a new Os object given an os string.
     * @param value String
     * @throws IllegalStateException Throws Exception if the operating system is not defined in this class.
     * @return Os
     */
    public static Os fromValue(String value) throws IllegalStateException {
        Os m_enum = (Os) _table_.get(value.toUpperCase());
        if (m_enum == null) {
            throw new IllegalStateException(err);
        }
        return m_enum;
    }

    /**
     * Creates a new Os object given an os string.
     * @param value String
     * @throws IllegalStateException Throws Exception if the operating system is not defined in this class.
     * @return Os
     */
    public static Os fromString(String value) throws IllegalStateException {
        return fromValue(value);
    }

    /**
     * Compares if a given Os object is equal to this.
     * @param obj Object
     * @return boolean
     */
    public boolean equals(Object obj) {
        return (obj == this);
    }

    public int hashCode() {
        return toString().hashCode();
    }

    /**
     * Returns the string value of the operating system.
     * @return String
     */
    public String toString() {
        return _value_;
    }

}
