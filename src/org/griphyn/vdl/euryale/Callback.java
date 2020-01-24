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
package org.griphyn.vdl.euryale;

import org.griphyn.vdl.dax.*;

/**
 * This interfaces defines the callback calls from DAX parsing. A slim and memory-efficient parser
 * of DAX is expected to implement these callbacks, and generate its own information on the fly.
 *
 * @author Kavitha Ranganathan
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 */
public interface Callback {
    /**
     * Callback when the opening tag was parsed. This contains all attributes and their raw values
     * within a map. This callback can also be used to initialize callback-specific resources.
     *
     * @param attributes is a map of attribute key to attribute value
     */
    public void cb_document(java.util.Map attributes);

    /**
     * Callback when the section 1 filenames are being parsed.
     *
     * @param filename is a DAX-style filename elements.
     */
    public void cb_filename(Filename filename);

    /**
     * Callback for the job from section 2 jobs. These jobs are completely assembled, but each is
     * passed separately.
     *
     * @param job is the DAX-style job.
     */
    public void cb_job(Job job);

    /**
     * Callback for child and parent relationships from section 3.
     *
     * @param child is the IDREF of the child element.
     * @param parents is a list of IDREFs of the included parents.
     */
    public void cb_parents(String child, java.util.List parents);

    /**
     * Callback when the parsing of the document is done. While this state could also be determined
     * from the return of the invocation of the parser, that return may be hidden in another place
     * of the code. This callback can be used to free callback-specific resources.
     */
    public void cb_done();
}
