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

package org.griphyn.vdl.directive;

import java.io.*;
import java.util.MissingResourceException;
import org.griphyn.vdl.classes.*;
import org.griphyn.vdl.parser.*;
import org.xml.sax.InputSource;

/**
 * This class uses the <code>VDLxParser</code> to parse VDL XML specification and output VDL textual
 * specification.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 * @see org.griphyn.vdl.parser.VDLxParser
 * @see org.griphyn.vdl.parser.DefinitionHandler
 */
public class VDLxConvert extends Directive implements DefinitionHandler {

    private Writer m_writer;

    /** Constructor */
    public VDLxConvert() throws IOException, MissingResourceException {
        super();
    }

    /**
     * Reads VDLx specification and outputs vdlt specification
     *
     * @param reader the source vdlx reader
     * @param writer the target vdlt writer
     */
    public void VDLx2VDLt(Reader reader, Writer writer) {
        m_writer = writer;

        org.griphyn.vdl.parser.VDLxParser parser =
                new org.griphyn.vdl.parser.VDLxParser(m_props.getVDLSchemaLocation());

        parser.parse(new InputSource(reader), this);
    }

    /**
     * This method implements the interface defined in DefinitionHandler to output the vdlt
     * specification.
     *
     * @param d is the Definition that is ready to be stored.
     * @return always true, if the write was successful.
     */
    public boolean store(Definition d) {
        boolean result = true;
        try {
            if (m_verbose) m_logger.log("directive", 3, d.shortID());
            d.toString(m_writer);
        } catch (IOException ioe) {
            m_logger.log("default", 0, "IO Error: " + ioe.getMessage());
            result = false;
        }
        return result;
    }
}
