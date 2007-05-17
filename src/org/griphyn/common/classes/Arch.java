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
 * This is an enumerated data class for the different types of architecture.
 *
 * @author Gaurang Mehta gmehta@isi.edu
 * @version $Revision: 1.5 $
 */

import java.io.Serializable;
import java.util.HashMap;

public class Arch
    implements Serializable {
    private String _value_;
    private static HashMap _table_ = new HashMap(5);

    protected Arch(String value) {
        _value_ = value;
        _table_.put(_value_, this);
    }

    private static final String _INTEL32 = "INTEL32";
    private static final String _INTEL64 = "INTEL64";
    private static final String _SPARCV7 = "SPARCV7";
    private static final String _SPARCV9 = "SPARCV9";
    private static final String _AMD64 = "AMD64";

    public static final Arch INTEL32 = new Arch(_INTEL32);
    public static final Arch INTEL64 = new Arch(_INTEL64);
    public static final Arch SPARCV7 = new Arch(_SPARCV7);
    public static final Arch SPARCV9 = new Arch(_SPARCV9);
    public static final Arch AMD64 = new Arch(_AMD64);

    public static final String err = "Error: Illegal Architecture defined. Please specify one of the predefined types \n [INTEL32, INTEL64, AMD64, SPARCV7, SPARCV9]";

    /**
     * Returns the value of the architecture as string.
     * @return String
     */
    public String getValue() {
        return _value_;
    }

    /**
     * Creates a new Arch Object givan a arch string.
     * @param value String
     * @throws IllegalStateException Throws Exception if the architecure is not defined in this class.
     * @return Arch
     */
    public static Arch fromValue(String value) throws IllegalStateException {
        Arch m_enum = (Arch) _table_.get(value.toUpperCase());
        if (m_enum == null) {
            throw new IllegalStateException(err);
        }
        return m_enum;
    }

    /**
     * Creates a new Arch object given a arch string.
     * @param value String
     * @throws IllegalStateException Throws Exception if the architecure is not defined in this class.
     * @return Arch
     */
    public static Arch fromString(String value) throws IllegalStateException {
        return fromValue(value);
    }

    /**
     * Compares if a given Arch object is equal to this.
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
     * Returns the string value of the architecture.
     * @return String
     */
    public String toString() {
        return _value_;
    }

}
