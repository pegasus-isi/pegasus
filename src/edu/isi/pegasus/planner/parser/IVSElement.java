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

import edu.isi.pegasus.planner.invocation.Invocation;
import java.util.*;

/**
 * This class keeps the name of an element and its corresponding java object reference. The
 * structure is used by the stack in <code>InvocationParser</code>.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 * @see InvocationParser
 */
public class IVSElement {
    public String m_name;
    public Invocation m_obj;

    public IVSElement(String name, Invocation invocation) {
        m_name = new String(name);
        m_obj = invocation;
    }
}
