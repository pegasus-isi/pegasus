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
 * Copyright 1999-2020 University of Chicago and The University of
 * Southern California. All rights reserved.
 */
package edu.isi.pegasus.common.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.regex.Pattern;

/**
 * Helper Class to detect type of a file.
 *
 * @author Karan Vahi
 */
public class FileDetector {

    /**
     * Store the regular expressions necessary to parse the the xml header <?xml version="1.0"
     * encoding="UTF-8"?>
     *
     * <p>e.g. $CondorVersion: 7.1.0 Apr 1 2008 BuildID: 80895$
     */
    private static final String mXMLHeaderRegexExpression =
            "<\\?xml\\s*version=\\\"[\\d\\.\\d]*\\\"\\s*encoding=\\\"[\\w\\W]*\\\"\\?>";

    /** Stores compiled patterns at first use, quasi-Singleton. */
    private static Pattern mPattern = null;

    static {
        if (mPattern == null) {
            mPattern = Pattern.compile(mXMLHeaderRegexExpression);
        }
    }

    /**
     * Returns whether type of file is XML or not only by inspecting first line and looking for XML
     * prefix <?xml version="1.0" encoding="UTF-8"?>
     *
     * @param file
     * @return
     */
    public static boolean isTypeXML(File file) {
        boolean isXML = false;
        if (!(file.exists() && file.canRead())) {
            return isXML;
        }

        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader(file));
        } catch (FileNotFoundException ex) {
            return isXML;
        }
        String line = "";
        try {
            line = br.readLine();
            br.close();
        } catch (IOException ex) {
            throw new RuntimeException("Unable to read from file " + file);
        }
        // match against regex
        return mPattern.matcher(line).matches();
    }
}
