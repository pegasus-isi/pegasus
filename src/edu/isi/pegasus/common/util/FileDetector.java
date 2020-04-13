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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
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
     */
    private static final String mXMLHeaderRegexExpression =
            "<\\?xml\\s*version=\\\"[\\d\\.\\d]*\\\"\\s*encoding=\\\"[\\w\\W]*\\\"\\?>";

    /** Stores compiled patterns at first use, quasi-Singleton. */
    private static Pattern mXMLPattern = null;

    /**
     * Store the regular expressions necessary to parse the the xml header <?xml version="1.0"
     * encoding="UTF-8"?>
     */
    private static final String mYAMLHeaderRegexExpression = "\\s*pegasus:\\s+\\d\\.\\d";

    /** Stores compiled patterns at first use, quasi-Singleton. */
    private static Pattern mYAMLPattern = null;

    /** the yaml extension file * */
    public static final String YAML_EXTENSION = ".yml";

    static {
        if (mXMLPattern == null) {
            mXMLPattern = Pattern.compile(mXMLHeaderRegexExpression);
        }
        if (mYAMLPattern == null) {
            mYAMLPattern = Pattern.compile(mYAMLHeaderRegexExpression);
        }
    }

    /**
     * Returns whether type of file is XML or not only by inspecting first line and looking for XML
     * prefix <?xml version="1.0" encoding="UTF-8"?>
     *
     * @param file
     * @return
     */
    public static boolean isTypeXML(String file) {
        return FileDetector.isTypeXML(new File(file));
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
            throw new RuntimeException(
                    "File does not exist or with read bit set to false " + file.getAbsolutePath());
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
        return mXMLPattern.matcher(line).matches();
    }

    /**
     * Returns whether type of file is type YAML or not
     *
     * @param file
     * @return
     */
    public static boolean isTypeYAML(String file) {
        return FileDetector.isTypeYAML(new File(file));
    }

    /**
     * Returns whether type of file is type YAML or not
     *
     * @param file
     * @return
     */
    public static boolean isTypeYAML(File file) {
        boolean isYAML = false;
        if (!(file.exists() && file.canRead())) {
            throw new RuntimeException(
                    "File does not exist or with read bit set to false " + file.getAbsolutePath());
        }

        if (file.getName().endsWith(YAML_EXTENSION)) {
            // filenames ending with yml
            return true;
        }

        // first try to parse in directly to a YAML parser
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        JsonNode root = null;
        try {
            root = mapper.readTree(new FileReader(file));
            isYAML = (root != null);
        } catch (Exception e) {
        }

        if (isYAML) {
            // most of RC and TC Text formats and even XML are parsed by YAML parser without
            // errors. so failsafe check
            if (!(root instanceof ObjectNode)) {
                isYAML = false;
            }
        }

        if (!isYAML) {
            // maybe it is yaml but with errors
            // try to parse the first line and look for pegasus: XX
            BufferedReader br = null;
            try {
                br = new BufferedReader(new FileReader(file));
            } catch (FileNotFoundException ex) {
                return isYAML;
            }
            String line = "";
            try {
                line = br.readLine();
                br.close();
            } catch (IOException ex) {
                throw new RuntimeException("Unable to read from file " + file);
            }
            // match against regex
            isYAML = line == null ? false : mYAMLPattern.matcher(line).matches();
        }

        return isYAML;
    }
}
