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
package org.griphyn.vdl.parser;

/**
 * Class to pass the name of a definition from scanner to parser. The class is used for both, the
 * namespace::name:version info as well as the namespace::name:min,max mapping. This class is
 * module-local on purpose.
 *
 * @author Jens-S. Vöckler
 * @version $Revision$
 */
class VDLtFQDN implements VDLtToken {
    /** The name of the identifier */
    private String m_value[];

    /** Contructs an empty fqdn to pass. */
    public VDLtFQDN() {
        this.m_value = new String[4];
    }

    /**
     * Sets a part of the fqdn to the given value.
     *
     * @param index is the part to set
     * @param value is the new value to set the part for
     * @return the old value at the slot. For an index out of bounds, null will always be returned.
     */
    public String setValue(int index, String value) {
        if (index >= 0 && index < m_value.length) {
            String old = m_value[index];
            m_value[index] = value;
            return old;
        } else {
            // out of bounds, ignore
            return null;
        }
    }

    /**
     * Obtains the current name value of an fqdn part.
     *
     * @param index is the part to obtain the value for
     * @return the name of the identifer, which may be null. null is always returned for index out
     *     of bounds.
     */
    public String getValue(int index) {
        if (index >= 0 && index < m_value.length) return this.m_value[index];
        else return null;
    }
}
