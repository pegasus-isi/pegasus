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

import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.util.MissingResourceException;
import org.griphyn.vdl.classes.*;
import org.griphyn.vdl.parser.*;

/**
 * The class converts VDLt syntax into VDLx syntax.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 * @see org.griphyn.vdl.parser.VDLtParser
 */
public class VDLtConvert extends Directive {
    /** Constructor */
    public VDLtConvert() throws IOException, MissingResourceException {
        super();
    }

    /**
     * Reads VDLt specification and outputs VDLx specification
     *
     * @param reader the vdlt source reader
     * @param writer the vdlx taget writer
     */
    public void VDLt2VDLx(Reader reader, Writer writer)
            throws VDLtParserException, VDLtScannerException, IOException {
        VDLt2VDLx(reader, writer, null, null);
    }

    /**
     * Reads VDLt specification and outputs VDLx specification
     *
     * @param reader the vdlt source reader
     * @param writer the vdlx taget writer
     * @param namespace the common vdl namespace for all the definitions included
     * @param version the common version number for all the definitions included
     */
    public void VDLt2VDLx(Reader reader, Writer writer, String namespace, String version)
            throws VDLtParserException, VDLtScannerException, IOException {
        VDLtParser parser = new VDLtParser(reader);

        String newline = System.getProperty("line.separator", "\r\n");

        Definitions def = new Definitions();
        def.setVdlns(namespace);
        def.setVersion(version);
        def.writeXMLHeader(writer, "", null);

        do {
            Definition d = parser.parseDefinition();
            d.toXML(writer, "  ");
            if (m_verbose) m_logger.log("directive", 1, d.shortID());
        } while (parser.hasMoreTokens());

        writer.write("</definitions>");
        writer.write(newline);
        writer.flush();
    }
}
