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
package org.griphyn.vdl.util;

import java.text.DecimalFormat;

/**
 * Each instance of this class produces a row of sequence numbers. By default, the sequence string
 * has six digits.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 */
public class SequenceGenerator {
    /**
     * The sequence counter employed by this generator. Please note that multiple instances of this
     * class will produce the same sequences!
     */
    private long m_count = 0;

    /** The formatting of the number. */
    private DecimalFormat m_format;

    /** C'tor: This will generate an six digit sequence strings. */
    public SequenceGenerator() {
        this.m_format = new DecimalFormat("######000000");
    }

    /**
     * C'tor: Instances from this contructor will generate ${prefix}xxxxx. Please note that multiple
     * instances of this class with the same prefix will produce the same sequences!
     *
     * @param digits are the number of digits to use. This value must be at least one.
     * @exception IllegalArgumentException if the number of digits is negative
     */
    public SequenceGenerator(int digits) {
        if (digits < 0) throw new IllegalArgumentException(digits + " < 1");
        if (digits == 0) this.m_format = null;
        else {
            StringBuffer pattern = new StringBuffer(digits << 1);
            pattern.setLength(digits << 1);
            for (int i = 0; i < digits; ++i) {
                pattern.setCharAt(i, '#');
                pattern.setCharAt(i + digits, '0');
            }
            this.m_format = new DecimalFormat(pattern.toString());
        }
    }

    /**
     * Creates the next sequence number as textual string.
     *
     * @return the next sequence number string.
     * @exception RuntimeException, if the maximum permissable value was reached.
     */
    public String generate() throws RuntimeException {
        if (this.m_count == Long.MAX_VALUE) throw new RuntimeException("Reached end of sequence");
        else this.m_count++;

        return (this.m_format == null
                ? Long.toString(this.m_count)
                : this.m_format.format(this.m_count));
    }
}
