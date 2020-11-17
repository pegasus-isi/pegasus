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
package org.griphyn.vdl.parser;

import java.io.*;
import javax.xml.parsers.*;
import org.griphyn.vdl.classes.*;
import org.griphyn.vdl.util.Logging;
import org.xml.sax.*;

/**
 * This class uses the Xerces SAX2 parser to validate and parse an XML document. The content handler
 * <code>VDLContentHandler</code> and error handler <code>VDLErrorHandler</code> are necessary to
 * handle various callbacks.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 * @see VDLContentHandler
 * @see VDLErrorHandler
 */
public class VDLxParser {
    /** Default parser is the Xerces parser. */
    protected static final String vendorParserClass = "org.apache.xerces.parsers.SAXParser";

    /** Holds the instance of a {@link org.xml.sax.XMLReader} class. */
    private XMLReader m_parser;

    /**
     * Handles the filling in of content, and callbacks to the {@link DefinitionHandler} interface.
     */
    private VDLContentHandler m_contentHandler;

    /**
     * Sets a feature while capturing failed features right here.
     *
     * @param uri is the feature's URI to modify
     * @param flag is the new value to set.
     * @return true, if the feature could be set, false for an exception
     */
    private boolean set(String uri, boolean flag) {
        boolean result = false;
        try {
            this.m_parser.setFeature(uri, flag);
            result = true;
        } catch (SAXException se) {
            Logging.instance().log("default", 0, "Could not set parser feature " + se.getMessage());
        }
        return result;
    }

    /**
     * The class constructor. This function initializes the Xerces parser and the features that
     * enable schema validation.
     *
     * @param schemaLocation is the default location of the XML Schema which this parser is capable
     *     of parsing. It may be null to use the defaults provided in the document.
     */
    public VDLxParser(String schemaLocation) {
        try {
            m_parser = (XMLReader) Class.forName(vendorParserClass).newInstance();
            m_contentHandler = new VDLContentHandler();
            m_parser.setContentHandler(m_contentHandler);
            m_parser.setErrorHandler(new VDLErrorHandler());

            set("http://xml.org/sax/features/validation", true);
            set("http://apache.org/xml/features/validation/dynamic", true);
            set("http://apache.org/xml/features/validation/schema", true);
            // time+memory consuming, see http://xml.apache.org/xerces2-j/features.html
            // set( "http://apache.org/xml/features/validation/schema-full-checking", true );

            // Send XML Schema element default values via characters().
            set("http://apache.org/xml/features/validation/schema/element-default", true);
            set("http://apache.org/xml/features/validation/warn-on-duplicate-attdef", true);
            // mysteriously, this one fails with recent Xerces
            // set( "http://apache.org/xml/features/validation/warn-on-undeclared-elemdef", true );
            set("http://apache.org/xml/features/warn-on-duplicate-entitydef", true);

            // set the schema default location.
            if (schemaLocation != null) {
                setSchemaLocations(Definitions.SCHEMA_NAMESPACE + ' ' + schemaLocation);
                Logging.instance().log("parser", 0, "will use " + schemaLocation);
            } else {
                Logging.instance().log("parser", 0, "will use document schema hint");
            }
        } catch (ClassNotFoundException e) {
            Logging.instance().log("defaut", 0, "The SAXParser class was not found: " + e);
        } catch (InstantiationException e) {
            Logging.instance()
                    .log("default", 0, "The SAXParser class could not be instantiated: " + e);
        } catch (IllegalAccessException e) {
            Logging.instance().log("default", 0, "The SAXParser class could not be accessed: " + e);
        }
    }

    /**
     * Sets the list of external real locations where the XML schema may be found. Since this list
     * can be determined at run-time through properties etc., we expect this function to be called
     * between instantiating the parser, and using the parser.
     *
     * @param list is a list of strings representing schema locations. The content exists in pairs,
     *     one of the namespace URI, one of the location URL.
     */
    public void setSchemaLocations(String list) {
        /*
        // default place to add
        list += "http://www.griphyn.org/working_groups/VDS/vdl-1.24.xsd " +
          "http://www.griphyn.org/working_groups/VDS/vdl-1.24.xsd";
        */

        // schema location handling
        try {
            m_parser.setProperty(
                    "http://apache.org/xml/properties/schema/external-schemaLocation", list);
        } catch (SAXException se) {
            Logging.instance().log("default", 0, "The SAXParser reported an error: " + se);
        }
    }

    /**
     * Sets the list of external real locations where the XML schema may be found when no namespace
     * is active. Only one location can be specified. We expect this function to be called between
     * instantiating the parser, and using the parser.
     *
     * @param location is the location of the schema file (location URL).
     */
    public void setDefaultSchemaLocation(String location) {
        /*
        // default place to add
        list += "http://www.griphyn.org/working_groups/VDS/vdl-1.19.xsd " +
          "http://www.griphyn.org/working_groups/VDS/vdl-1.19.xsd";
        */

        // schema location handling
        try {
            m_parser.setProperty(
                    "http://apache.org/xml/properties/schema/external-noNamespaceSchemaLocation",
                    location);
        } catch (SAXException se) {
            Logging.instance().log("default", 0, "The SAXParser reported an error: " + se);
        }
    }

    /**
     * This function parses a XML source from an InputStream source, and creates java class
     * instances that correspond to different elements in the XML source.
     *
     * @param reader is a bytestream opened for reading.
     * @param definitions is a reference to the already known definitions in the system. The
     *     definitions may be empty, but must not be null.
     * @param overwrite is a flag to indicate the insertion mode. If set to <code>false</code>, an
     *     insert mode is assumed. Violations will be returned as clashes. With value <code>true
     *     </code>, an update mode is assumed. Old definitions of updates will be returned.
     * @param dontcare is a flag to minimize memory consumption. Clashes in insert mode will be
     *     signalled with an Exception. Old values in update mode will be ignored. Effectively, the
     *     resulting list is always empty in dontcare mode.
     * @return usually an empty list. If not empty, it contains clashes in insert, or old
     *     definitions in update mode. Please note that each element is a single Definition, either
     *     Transformation or Derivation. It is not a Definitions object, since multiple old versions
     *     may appear in update mode. Returns null on error!
     * @see org.griphyn.vdl.classes.Definitions
     */
    public java.util.List parse(
            java.io.InputStream reader,
            Definitions definitions,
            boolean overwrite,
            boolean dontcare) {
        try {
            MemoryStorage database = new MemoryStorage(definitions, overwrite, dontcare);
            m_contentHandler.setDefinitionHandler(database);

            m_parser.parse(new InputSource(reader));

            java.util.List result = database.getRejects();
            Logging.instance()
                    .log(
                            "parser",
                            1,
                            "Now with "
                                    + definitions.getDefinitionCount()
                                    + " definitions, and "
                                    + result.size()
                                    + " rejects");
            return result;
        } catch (SAXException e) {
            Logging.instance().log("default", 0, "SAX Error: " + e);
        } catch (IOException e) {
            Logging.instance().log("default", 0, "IO Error: " + e);
        }

        return null;
    }

    /**
     * This function parses an XML source (could be a document, a stream, etc.), and creates java
     * class instances that correspond to different elements in the XML source.
     *
     * @param reader is an XML input source, which may be a character stream, byte stream, or even
     *     an URI.
     * @param callback is a handler for store callbacks that will take one complete definition each
     *     time one is ready to be processed.
     * @return true for successful parsing, false in case of error.
     * @see org.griphyn.vdl.classes.Definitions
     */
    public boolean parse(InputSource reader, DefinitionHandler callback) {
        try {
            m_contentHandler.setDefinitionHandler(callback);
            m_parser.parse(reader);
            return true;
        } catch (SAXException e) {
            Logging.instance().log("default", 0, "SAX Error: " + e);
        } catch (IOException e) {
            Logging.instance().log("default", 0, "IO Error: " + e);
        }

        return false;
    }

    /**
     * This function parses an XML source (could be a document, a stream, etc.), and invokes a
     * callback for the top-level element with the corresponding Java class. Note: The finalizer
     * cannot be called for Definitions elements. This method should be used for "partial VDLx",
     * which contains XML for a Transformation or Derivation.
     *
     * @param reader is an XML input source, which may be a character stream, byte stream, or even
     *     an URI.
     * @param callback is a handler for store callbacks that will take one complete definition.
     * @return true for successful parsing, false in case of error.
     * @see org.griphyn.vdl.classes.Definitions
     */
    public boolean parse(InputSource reader, FinalizerHandler callback) {
        try {
            m_contentHandler.setFinalizerHandler(callback);
            m_parser.parse(reader);
            return true;
        } catch (SAXException e) {
            Logging.instance().log("default", 0, "SAX Error: " + e);
        } catch (IOException e) {
            Logging.instance().log("default", 0, "IO Error: " + e);
        }

        return false;
    }

    // public Definitions parse(String xmlURI);
    // public Definitions parse(InputStream stream);
    // public Definitions parse(java.io.Reader reader);
}
