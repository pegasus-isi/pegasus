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

import java.sql.Timestamp;
import java.text.*;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * This simple class defines the Date values for the values at the heart of Yong's annotations.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 */
public class TupleDate extends Tuple {
    /** Defines the key for which we store the annotation. */
    private Timestamp m_value;

    /**
     * Constructs an instance of a Date tuple.
     *
     * @param key is the key to store annotations for.
     * @param value is the Date value of the annotation.
     */
    public TupleDate(String key, Timestamp value) {
        super(key);
        m_value = value;
    }

    /**
     * Constructs an instance of a Date tuple.
     *
     * @param key is the key to store annotations for.
     * @param value is the Date value of the annotation.
     */
    public TupleDate(String key, Date value) {
        super(key);
        m_value = new Timestamp(value.getTime());
    }

    /**
     * Constructs an instance of a Date tuple.
     *
     * @param key is the key to store annotations for.
     * @param value is the information in milliseconds since epoch.
     */
    public TupleDate(String key, long value) {
        super(key);
        m_value = new Timestamp(value);
    }

    /**
     * Obtains the current value of the value part.
     *
     * @return the current value as Date.
     * @see #setDate( Timestamp )
     * @see #setDate( Timestamp )
     * @see #setDate( Date )
     * @see #setDate( long )
     */
    public Timestamp getDate() {
        return m_value;
    }

    /**
     * Overwrites the current value of the value part.
     *
     * @param value is the new value to use from now on.
     * @see #getDate()
     * @see #setDate( Date )
     * @see #setDate( long )
     */
    public void setDate(Timestamp value) {
        m_value = value;
    }

    /**
     * Overwrites the current value of the value part.
     *
     * @param value is the new value to use from now on.
     * @see #getDate()
     * @see #setDate( Timestamp )
     * @see #setDate( long )
     */
    public void setDate(Date value) {
        m_value = new Timestamp(value.getTime());
    }

    /**
     * Overwrites the current value of the value part.
     *
     * @param value is the new value to use from now on.
     * @see #getDate()
     * @see #setDate( Timestamp )
     * @see #setDate( Date )
     */
    public void setDate(long value) {
        m_value = new Timestamp(value);
    }

    /**
     * Return generically the value as a copy of the original.
     *
     * @return the value wrapped as Java object.
     * @see #setValue( Object )
     * @see java.sql.Timestamp
     */
    public Object getValue() {
        return new Timestamp(m_value.getTime());
    }

    /**
     * Generic interface to set a value in an instance. Note that this action may fail, if the
     * instance is of an incompatible type.
     *
     * @param value is the data object to set.
     * @see #getValue()
     * @exception ClassCastException if the actual argument type of the value is incompatible with
     *     the value maintained by the instance.
     */
    public void oldSetValue(Object value) throws ClassCastException {
        if (value instanceof java.sql.Timestamp) m_value = (Timestamp) value;
        else if (value instanceof java.util.Date) m_value = new Timestamp(((Date) value).getTime());
        else if (value instanceof java.lang.Number)
            m_value = new Timestamp(((Number) value).longValue());
        else if (value instanceof java.lang.String) {
            try {
                SimpleDateFormat df = new SimpleDateFormat("MM/dd/yy");
                Date d = df.parse((String) value);
                m_value = new Timestamp(d.getTime());
            } catch (java.text.ParseException e) {
                throw new ClassCastException("type cast failed: not a Date");
            }
        } else throw new ClassCastException("type cast failed: not a Date");
    }

    public void setValue(Object value) throws ClassCastException {
        if (value instanceof java.sql.Timestamp) m_value = (Timestamp) value;
        else if (value instanceof java.util.Date) m_value = new Timestamp(((Date) value).getTime());
        else if (value instanceof java.lang.Number)
            m_value = new Timestamp(((Number) value).longValue());
        else if (value instanceof java.lang.String) {
            try {
                Date d = TupleDate.parse((String) value);
                m_value = new Timestamp(d.getTime());
            } catch (ClassCastException e) {
                throw e;
            }
        } else throw new ClassCastException("type cast failed: not a Date");
    }

    /**
     * Returns the type of the value as SQL Type.
     *
     * @return a constant from the set of SQL types.
     * @see java.sql.Types
     */
    public int getType() {
        return java.sql.Types.TIMESTAMP;
    }

    /** Returns the string representation of the type. */
    public String getTypeString() {
        return "date";
    }

    // Utility Methods

    public static Date parse(String s) throws ClassCastException {
        Date d = null;
        SimpleDateFormat fmt[] = {
            new SimpleDateFormat("MM/dd/yy HH:mm:ss.SSS"),
            new SimpleDateFormat("MM/dd/yy HH:mm:ss"),
            new SimpleDateFormat("MM/dd/yy HH:mm"),
            new SimpleDateFormat("MM/dd/yy"),
            new SimpleDateFormat("MM.dd.yy HH:mm:ss.SSS"),
            new SimpleDateFormat("MM.dd.yy HH:mm:ss"),
            new SimpleDateFormat("MM.dd.yy HH:mm"),
            new SimpleDateFormat("MM.dd.yy"),
            new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS"),
            new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"),
            new SimpleDateFormat("yyyy-MM-dd HH:mm"),
            new SimpleDateFormat("yyyy-MM-dd"),
            new SimpleDateFormat("yyyy.MM.dd HH:mm:ss.SSS"),
            new SimpleDateFormat("yyyy.MM.dd HH:mm:ss"),
            new SimpleDateFormat("yyyy.MM.dd HH:mm"),
            new SimpleDateFormat("yyyy.MM.dd"),
            new SimpleDateFormat("HH:mm:ss.SSS"),
            new SimpleDateFormat("HH:mm:ss"),
            new SimpleDateFormat("HH:mm")
        };
        for (int i = 0; i < fmt.length; i++) {
            try {
                d = fmt[i].parse(s);
                break;
            } catch (ParseException e) {
                continue;
            }
        }
        if (d != null) return d;
        else {
            throw new ClassCastException("type cast failed: not a Date");
        }
    }
}
