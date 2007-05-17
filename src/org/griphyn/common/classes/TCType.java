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
 * This is an enumerated data class for the different types of transformation.
 *
 * @author Gaurang Mehta gmehta@isi.edu
 * @version $Revision$
 */

import java.io.Serializable;
import java.util.HashMap;

public class TCType
    implements Serializable {
    private String _value_;
    private static HashMap _table_ = new HashMap(6);

    protected TCType(String value) {
        _value_ = value;
        _table_.put(_value_, this);
    }

    public static final String _STATIC_BINARY = "STATIC_BINARY";
    public static final String _DYNAMIC_BINARY = "DYNAMIC_BINARY";
    public static final String _INSTALLED = "INSTALLED";
    public static final String _SOURCE = "SOURCE";
    public static final String _PACMAN_PACKAGE = "PACMAN_PACKAGE";
    public static final String _SCRIPT = "SCRIPT";

    public static final TCType STATIC_BINARY = new TCType(_STATIC_BINARY);
    public static final TCType DYNAMIC_BINARY = new TCType(_DYNAMIC_BINARY);
    public static final TCType INSTALLED = new TCType(_INSTALLED);
    public static final TCType SOURCE = new TCType(_SOURCE);
    public static final TCType PACMAN_PACKAGE = new TCType(_PACMAN_PACKAGE);
    public static final TCType SCRIPT = new TCType(_SCRIPT);

    public static final String err = "Error : Illegal TCType defined. Please specify one of the predefined types \n [INSTALLED, SOURCE, STATIC_BINARY, DYNAMIC_BINARY, PACMAN_PACKAGE, SCRIPT]";

    /**
     * Returns the string value of the type of transformation.
     * @return String
     */
    public String getValue() {
        return _value_;
    }

    /**
     * Creates an objct of TCType given a string.
     * @param value String The type value as string
     * @throws IllegalStateException Throws Exception if the type is not defined in this class
     * @return TCType
     */
    public static TCType fromValue(String value) throws IllegalStateException {
        TCType m_enum = (TCType) _table_.get(value.toUpperCase());
        if (m_enum == null) {
            throw new IllegalStateException(err);
        }
        return m_enum;
    }

    /**
     * Creates an object of TCType given a string.
     * @param value String The type value as string.
     * @throws IllegalStateException Throws Exception if the type is not defined in this class.
     * @return TCType
     */
    public static TCType fromString(String value) throws IllegalStateException {
        return fromValue(value);
    }

    /**
     * Compares if a given TCType object equals this TCType object.
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
     * Returns the string value of the type of transformation.
     * @return String
     */
    public String toString() {
        return _value_;
    }

}
