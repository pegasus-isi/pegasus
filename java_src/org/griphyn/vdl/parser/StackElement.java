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

import java.util.*;
import org.griphyn.vdl.classes.*;

/**
 * This class keeps the name of an element and its corresponding java object reference. The
 * structure is used by the stack in <code>VDLContentHandler</code>.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 * @see VDLContentHandler
 */
public class StackElement {
    public String m_name;
    public VDL m_obj;

    public StackElement(String name, VDL vdl) {
        m_name = new String(name);
        m_obj = vdl;
    }
}
