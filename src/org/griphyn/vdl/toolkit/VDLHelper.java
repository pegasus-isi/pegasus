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
package org.griphyn.vdl.toolkit;

import java.io.*;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.*;
import org.griphyn.vdl.directive.*;
import org.griphyn.vdl.parser.*;

/**
 * This class encapsulates common helper functions for VDL parsing.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 * @see UpdateVDC
 * @see VDLc
 * @see org.griphyn.vdl.parser.VDLtParser
 * @see org.griphyn.vdl.parser.VDLxParser
 */
public abstract class VDLHelper extends Toolkit {
    /**
     * Pattern to match against when recognizing VDLx from VDLt. recognizes: "<?xml " "<definition "
     * "<ns1:definition "
     */
    private static final String c_pattern = "^\\s*<(?:(?:\\w+:)?definitions|\\?\\s*xml)\\s+";

    /** Converter for VDLt to VDLx, is a singleton. */
    private static VDLtConvert c_t2x = null;

    /**
     * ctor: Constructs a new instance object with the given application name.
     *
     * @param appName is the name of the shell wrapper with which to report.
     */
    public VDLHelper(String appName) {
        super(appName);
    }

    /**
     * Default ctor: Constructs a new instance object with the given application name.
     *
     * @param appName is the name of the shell wrapper with which to report.
     * @param verbosity sets the verbosity level of the "app" logging queue.
     */
    public VDLHelper(String appName, int verbosity) {
        super(appName, verbosity);
    }

    /**
     * Determines if a file is VDLx or VDLt. It does so by expecting the ubiquitous XML preamble in
     * the VDLx.
     *
     * @param filename is the name of the file
     * @return true if the file is XML, false otherwise.
     */
    public boolean isXML(String filename) throws IOException {
        LineNumberReader lnr = new LineNumberReader(new FileReader(filename));
        String line = null;
        boolean result = false;
        java.util.regex.Pattern p = java.util.regex.Pattern.compile(VDLHelper.c_pattern);

        while ((line = lnr.readLine()) != null && !result && lnr.getLineNumber() < 64) {
            result = p.matcher(line).lookingAt();
        }
        lnr.close();
        return result;
    }

    /**
     * Determines, if input file is VDLt or VDLx. Parses VDLt into a temporary VDLx file. Adds VDLx
     * into the VDC.
     *
     * @param filename is the file to add, either VDLt or VDLx content.
     * @param define is a VDC connector to add to the VDC
     * @param reject is a writer to gather definitions that were reject from the VDC, depending on
     *     the overwrite mode. If <code>null</code>, no rejects will be gathered.
     * @param overwrite determines insert or update mode.
     * @return the result from adding the XML into the VDC
     * @see org.griphyn.vdl.directive.Define#updateVDC( Reader, Writer, boolean )
     */
    public boolean addFileToVDC(String filename, Define define, Writer reject, boolean overwrite)
            throws VDLtParserException, VDLtScannerException, IOException {
        // instantiate singleton, if necessary
        if (c_t2x == null) c_t2x = new VDLtConvert();

        File tempfn = null;
        if (isXML(filename)) {
            // VDLx file, no conversions necessary
            tempfn = new File(filename);
        } else {
            // VDLt file, convert to XML
            Reader r = new BufferedReader(new FileReader(filename));
            tempfn = File.createTempFile("vdlc-", ".xml", null);
            tempfn.deleteOnExit();
            Writer w = new BufferedWriter(new FileWriter(tempfn));
            c_t2x.VDLt2VDLx(r, w);
            r.close();
            w.flush();
            w.close();
        }

        return define.updateVDC(new BufferedReader(new FileReader(tempfn)), reject, overwrite);
    }

    /**
     * Determines, if input file is VDLt or VDLx. Parses VDLt into a temporary VDLx file. Adds VDLx
     * into the VDC. Skips to next file on error.
     *
     * @param args is the argument vector of main
     * @param start is the start of filenames in the argument vector.
     * @param define is a VDC connector to add to the VDC
     * @param reject is a writer to gather definitions that were reject from the VDC, depending on
     *     the overwrite mode. If <code>null</code>, no rejects will be gathered.
     * @param overwrite determines insert or update mode.
     * @return a set of filenames which reported errors.
     * @see #addFileToVDC( String, Define, Writer, boolean )
     */
    public Set addFilesToVDC(
            String[] args, int start, Define define, Writer reject, boolean overwrite) {
        Set result = new TreeSet();

        for (int i = start; i < args.length; ++i) {
            m_logger.log("app", 1, "parsing \"" + args[i] + "\"");

            try {
                if (!addFileToVDC(args[i], define, reject, overwrite)) {
                    // unsuccessful parsing
                    m_logger.log("default", 0, "XML parsing error, skipping to next file");
                    result.add(args[i]);
                }
            } catch (VDLtParserException e) {
                m_logger.log("default", 0, "syntactical error, skipping to next file");
                System.err.println(e.getMessage());
                result.add(args[i]);
            } catch (VDLtScannerException e) {
                m_logger.log("default", 0, "lexical error, skipping to next file");
                System.err.println(e.getMessage());
                result.add(args[i]);
            } catch (IOException e) {
                m_logger.log("default", 0, "I/O error, skipping to next file");
                System.err.println(e.getMessage());
                result.add(args[i]);
            }
        }

        return result;
    }
}
