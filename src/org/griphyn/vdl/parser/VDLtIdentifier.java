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
 * Class to pass the name of a regular identifier from scanner to parser. This class is module-local
 * on purpose.
 *
 * @author Jens-S. Vöckler
 * @version $Revision$
 */
class VDLtIdentifier implements VDLtToken {
    /** The name of the identifier */
    private String m_value;

    /**
     * Contructs a new identifiert to pass.
     *
     * @param value is the name of the identifier
     */
    public VDLtIdentifier(String value) {
        this.m_value = value == null ? null : new String(value);
    }

    /**
     * Obtains the current name of the identifier passed.
     *
     * @return the name of the identifer
     */
    public String getValue() {
        return this.m_value;
    }
}
