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
package org.griphyn.vdl.dbschema;

/**
 * This interface groups different but related catalog subinterfaces.
 *
 * <p>The interface will require implementing classes to provide a constructor that takes one String
 * as only argument. The class may ignore the string for now.
 *
 * <p>Subinterfaces are required to provide a constant PROPERTY_PREFIX. This constant select the
 * pieces of the <tt>vds.db.<i>catalog</i>.schema</tt> property space that corresponds to the
 * appropriate <i>catalog</i>.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 */
public interface Catalog {
    // empty
}
