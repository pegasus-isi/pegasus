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
 * Class to pass the content from a quoted string from scanner to parser. This class is module-local
 * on purpose.
 *
 * @author Jens-S. Vöckler
 * @version $Revision$
 */
class VDLtQuotedString implements VDLtToken {
    /** The content of the string to be passed. */
    private String m_value;

    /**
     * Contructs a new string value to pass.
     *
     * @param value is the content of the string.
     */
    public VDLtQuotedString(String value) {
        this.m_value = value == null ? null : new String(value);
    }

    /**
     * Obtains the current, unmodified content of the string. This means that quote characters
     * inside the string will remain as-is.
     *
     * @return the content of the quoted string.
     */
    public String getValue() {
        return this.m_value;
    }

    /**
     * Turns the content of a string into a quoted version of itself. The quote and backslash
     * character are quoted by prepending a backslash in front of them. Single quotes (apostrophe)
     * are not touched.
     *
     * @param unquoted is the raw string that may require quoting.
     * @return null if the input was null, or the quoted string.
     */
    public static String getQuotedValue(String unquoted) {
        if (unquoted == null) return null;

        StringBuffer result = new StringBuffer(unquoted.length());
        for (int i = 0; i < unquoted.length(); ++i) {
            char ch = unquoted.charAt(i);
            if (ch == '\\' || ch == '\"') result.append('\\');
            result.append(ch);
        }

        return result.toString();
    }
}
