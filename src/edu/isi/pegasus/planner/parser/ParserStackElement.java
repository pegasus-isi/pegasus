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

package edu.isi.pegasus.planner.parser;

/**
 * This class keeps the name of an element and its corresponding java object reference. The
 * structure is used by the stack in <code>SiteCatalogParser</code>.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class ParserStackElement {
    private String mElement;
    private Object mObject;

    /**
     * The overloaded constructor
     *
     * @param name
     * @param obj
     */
    public ParserStackElement(String name, Object obj) {
        mElement = name;
        mObject = obj;
    }

    /**
     * Returns the element name.
     *
     * @return name
     */
    public String getElementName() {
        return mElement;
    }

    /**
     * Returns the object referred to by the element.
     *
     * @return the object
     */
    public Object getElementObject() {
        return mObject;
    }
}
