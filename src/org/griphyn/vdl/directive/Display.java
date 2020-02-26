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
import java.util.*;
import org.griphyn.vdl.dax.*;
import org.griphyn.vdl.parser.DAXParser;
import org.griphyn.vdl.util.DAX2DOT;
import org.griphyn.vdl.util.Logging;

/**
 * The class converts a DAX specification into other formats for visualization purposes.
 *
 * @see org.griphyn.vdl.parser.VDLxParser
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 */
public class Display extends Directive {
    /** instance variable that is managed by this interface for web services. */
    private DAX2DOT m_dax;

    /**
     * Constructor.
     *
     * @throws IOException
     * @throws MissingResourceException
     */
    public Display() throws IOException, MissingResourceException {
        super();
        m_dax = new DAX2DOT();
    }

    /**
     * Sets the graph size.
     *
     * @param h is the height in inches?
     * @param w is the width in inches?
     */
    public void setSize(double h, double w) {
        m_dax.setSize(h, w);
    }

    /**
     * Determines whether to show derivations.
     *
     * @param showDV if true, also show the DVs
     */
    public void setShowDV(boolean showDV) {
        m_dax.setShowDV(showDV);
    }

    /**
     * Generates GraphViz dot format from the DAX specification.
     *
     * @param dax is the InputStream for the DAX
     * @param showFiles specifies whether to show input/output files in the graph.
     * @return a string of the GraphViz dot representation
     * @throws IOException if there is a problem reading or writing
     */
    public String DAX2DOT(InputStream dax, boolean showFiles) throws IOException {
        // parse the dax file
        Logging.instance().log("display", 0, "Initializing dax parser");
        DAXParser daxparser = new DAXParser(m_props.getDAXSchemaLocation());

        Logging.instance().log("display", 0, "parsing the dax...");
        ADAG adag = daxparser.parse(dax);

        if (adag == null) {
            Logging.instance().log("display", 0, "failed parsing the dax.");
            return null;
        }

        return m_dax.toDOT(adag, showFiles);
    }

    /**
     * Generates GraphViz dot format from the DAX specification.
     *
     * @param dax is the InputStream for the DAX
     * @param writer is the target to output GraphViz dot representation
     * @param showFiles specifies whether to show input/output files in the graph.
     * @throws IOException if there is a problem reading or writing
     */
    public void DAX2DOT(InputStream dax, Writer writer, boolean showFiles) throws IOException {
        // parse the dax file
        Logging.instance().log("display", 0, "Initializing dax parser");
        DAXParser daxparser = new DAXParser(m_props.getDAXSchemaLocation());

        Logging.instance().log("display", 0, "parsing the dax...");
        ADAG adag = daxparser.parse(dax);

        if (adag == null) {
            Logging.instance().log("display", 0, "failed parsing the dax.");
            return;
        }

        m_dax.toDOT(adag, writer, showFiles);
    }

    /**
     * Generates GraphViz dot format from the DAX specification.
     *
     * @param dax is the InputStream for the DAX
     * @param writer is the target to output GraphViz dot representation
     * @param showFiles specifies whether to show input/output files in the graph.
     * @param jobURL is the base URL for jobs
     * @param fileURL is the base URL for files
     * @throws IOException if there is a problem reading or writing
     */
    public void DAX2DOT(
            InputStream dax, Writer writer, boolean showFiles, String jobURL, String fileURL)
            throws IOException {
        // parse the dax file
        Logging.instance().log("display", 0, "Initializing dax parser");
        DAXParser daxparser = new DAXParser(m_props.getDAXSchemaLocation());

        Logging.instance().log("display", 0, "parsing the dax...");
        ADAG adag = daxparser.parse(dax);

        if (adag == null) {
            Logging.instance().log("display", 0, "failed parsing the dax.");
            return;
        }

        m_dax.toDOT(adag, writer, showFiles, jobURL, fileURL);
    }
}
