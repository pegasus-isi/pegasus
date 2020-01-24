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

import org.griphyn.vdl.dbschema.Annotation;

/**
 * This class defines basic predicates used for query annotations. Currently included: AND, OR, NOT,
 * EXISTS, LIKE, BETWEEN_AND, CONTAINS, EQ, NE, GT, LT, GE, LE.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 */
public class Predicate {
    /** Defines constants for predicates */
    public static final int AND = 0;

    public static final int OR = 1;
    public static final int NOT = 2;
    public static final int EXISTS = 3;
    public static final int LIKE = 4;
    public static final int BETWEEN = 5;
    public static final int CONTAINS = 6;
    public static final int EQ = 7;
    public static final int NE = 8;
    public static final int GT = 9;
    public static final int LT = 10;
    public static final int GE = 11;
    public static final int LE = 12;

    /** Defines corresponding strings for predicates */
    public static final String[] PREDICATE_STRING = {
        "AND", "OR", "NOT", "EXISTS", "LIKE", "BETWEEN", "CONTAINS", "=", "<>", ">", "<", ">=", "<="
    };

    /** Defines constants for value types */
    public static final int TYPE_STRING = 0;

    public static final int TYPE_INT = 1;
    public static final int TYPE_FLOAT = 2;
    public static final int TYPE_BOOL = 3;
    public static final int TYPE_DATE = 4;

    /** Defines the predicate for the query. */
    private int m_predicate = EQ;

    /** Defines the attribute name */
    private String m_key;

    /** Defines the value type */
    private int m_type = TYPE_STRING;

    /** Defines the value for the attribute */
    private String m_value1 = null;

    /** Defines the second value for the attribute used only in BETWEEN_AND. */
    private String m_value2 = null;

    /** Constructor */
    public Predicate(int predicate) {
        m_predicate = predicate;
    }

    /** Constructor assume the value type is string */
    public Predicate(int predicate, String key) {
        m_predicate = predicate;
        m_key = key;
    }

    /** Constructs the predicate and key. */
    public Predicate(int predicate, String key, String value) {
        m_predicate = predicate;
        m_key = key;
        m_value1 = value;
    }

    /** Constructs the predicate and key. */
    public Predicate(int predicate, String key, int type, String value) {
        m_predicate = predicate;
        m_key = key;
        m_type = type;
        m_value1 = value;
    }

    /** Constructs the predicate and key. */
    public Predicate(int predicate, String key, int type, String value1, String value2) {
        m_predicate = predicate;
        m_key = key;
        m_type = type;
        m_value1 = value1;
        m_value2 = value2;
    }

    /** Obtains the current value of the predicate */
    public int getPredicate() {
        return m_predicate;
    }

    /** Sets the predicate */
    public void setPredicate(int predicate) {
        m_predicate = predicate;
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
     * Returns the type of the value
     *
     * @return a constant from the value types.
     */
    public int getType() {
        return m_type;
    }

    /** Sets the value type */
    public void setType(int type) {
        m_type = type;
    }

    /** Returns the value */
    public String getValue() {
        return m_value1;
    }

    /** Sets the value */
    public void setValue(String value) {
        m_value1 = value;
    }

    /** Returns the value */
    public String getValue2() {
        return m_value2;
    }

    /** Sets the value type */
    public void setValue2(String value) {
        m_value2 = value;
    }

    /** Strips function part from key */
    private String getFunctionKey(String key) {
        int start = key.indexOf('(');
        int end = key.indexOf(')');
        if (start != -1 && end != -1 && start < end) {
            return key.substring(start + 1, end);
        } else return key;
    }

    /** Adds function part to value */
    private String getFunctionValue(String key, String default_value) {
        if (default_value == null) default_value = " value ";
        if (key == null) return default_value;
        int start = key.indexOf('(');
        int end = key.indexOf(')');
        if (start != -1 && end != -1 && start < end) {
            return key.substring(0, start) + "(" + default_value + ")";
        } else return default_value;
    }

    /** Returns a string representation */
    public String toString() {
        String str = "";
        if (m_key != null) str += m_key + " ";
        str += PREDICATE_STRING[m_predicate];
        if (m_value1 != null) {
            if (m_type == TYPE_INT || m_type == TYPE_FLOAT) str += " " + m_value1;
            else str += " '" + m_value1 + "'";
        }
        if (m_predicate == BETWEEN && m_value2 != null) {
            if (m_type == TYPE_INT || m_type == TYPE_FLOAT) str += " AND " + m_value2;
            else str += " AND '" + m_value2 + "'";
        }
        return str;
    }

    /**
     * Returns a SQL query statement for annotation search.
     *
     * @param annoClass is the class to search for
     * @param arg could be a String for TR arg, or a Integer for TR call position.
     * @see org.griphyn.vdl.dbschema.Annotation
     */
    public String toSQL(int annoClass, Object arg) {
        if (m_predicate == AND) return " INTERSECT ";

        if (m_predicate == OR) return " UNION ";

        String select = "SELECT DISTINCT ";
        String ktable = "";
        String comma = ", ";
        String vtable = "";
        String field = "";
        String from = " FROM ";
        String where = " WHERE ";
        String ext = "";
        String sql = "";

        switch (annoClass) {
            case Annotation.CLASS_FILENAME:
                ktable = "anno_lfn k";
                field = "name";
                break;
            case Annotation.CLASS_TRANSFORMATION:
                ktable = "anno_tr k";
                field = "did";
                break;
            case Annotation.CLASS_DERIVATION:
                ktable = "anno_dv k";
                field = "did";
                break;
            case Annotation.CLASS_DECLARE:
                ktable = "anno_targ k";
                field = "did";
                if (arg != null && arg instanceof String) ext = " AND name='" + (String) arg + "'";
                break;
            case Annotation.CLASS_CALL:
                ktable = "anno_call k";
                field = "did";
                if (arg != null && arg instanceof String) ext = " AND pos=" + (Integer) arg;
                break;
            default:
        }

        if (m_predicate == NOT) {
            where += field + " NOT IN ";
            sql = select + field + from + ktable + where;
            return sql;
        }

        if (m_key != null) where += " mkey='" + getFunctionKey(m_key) + "'";

        if (m_predicate == EXISTS) {
            // only need to check key
            sql = select + field + from + ktable + where + ext;
            return sql;
        }

        // need to check value
        switch (m_type) {
            case TYPE_STRING:
                vtable = "anno_text v";
                break;
            case TYPE_INT:
                vtable = "anno_int v";
                break;
            case TYPE_FLOAT:
                vtable = "anno_float v";
                break;
            case TYPE_DATE:
                vtable = "anno_date v";
                break;
            case TYPE_BOOL:
                vtable = "anno_bool v";
                break;
        }

        where += " AND " + getFunctionValue(m_key, null);
        if (m_predicate == CONTAINS) {
            where += " LIKE ";
            if (m_value1 != null) where += "'%" + m_value1 + "%'";
        } else {
            where += PREDICATE_STRING[m_predicate];
            if (m_value1 != null) {
                if (m_type == TYPE_INT || m_type == TYPE_FLOAT) where += " " + m_value1;
                else where += " '" + m_value1 + "'";
            }
        }

        if (m_predicate == BETWEEN && m_value2 != null) {
            if (m_type == TYPE_INT || m_type == TYPE_FLOAT) where += " AND " + m_value2;
            else where += " AND '" + m_value2 + "'";
        }

        where += " AND k.id=v.id ";
        sql = select + field + from + ktable + comma + vtable + where + ext;
        return sql;
    }

    public String toXQuery(String var) {
        String xquery = "";

        if (m_predicate == AND) return " and ";

        if (m_predicate == OR) return " or ";

        if (m_predicate == NOT) {
            return " not ";
        }

        String name = "";
        if (m_key != null) name = var + "[@name = '" + getFunctionKey(m_key) + "']";

        if (m_predicate == EXISTS) {
            // only need to check key
            xquery += "exists(" + name + ")";
            return xquery;
        }

        // need to check value
        /*
           switch (m_type) {
           case TYPE_STRING:
        break;
           case TYPE_INT:
               xquery += "[@type = 'int']";
        break;
           case TYPE_FLOAT:
               xquery += "[@type = 'float']";
        break;
           case TYPE_DATE:
        break;
           case TYPE_BOOL:
        break;
           }
           */

        String value = getFunctionValue(m_key, name);
        if (m_predicate == CONTAINS) {
            xquery += "contains(" + value + ",";
            if (m_value1 != null) {
                xquery += "'" + m_value1 + "'";
            }
            xquery += ")";

        } else {
            xquery += value;
            if (m_predicate == BETWEEN) xquery += " >= ";
            else xquery += " " + PREDICATE_STRING[m_predicate];
            if (m_value1 != null) {
                if (m_type == TYPE_INT || m_type == TYPE_FLOAT) xquery += " " + m_value1;
                else xquery += " '" + m_value1 + "'";
            }
        }

        if (m_predicate == BETWEEN && m_value2 != null) {
            if (m_type == TYPE_INT || m_type == TYPE_FLOAT)
                xquery += " and " + value + " <= " + m_value2;
            else xquery += " and " + value + " <= '" + m_value2 + "'";
        }

        return xquery;
    }
}
