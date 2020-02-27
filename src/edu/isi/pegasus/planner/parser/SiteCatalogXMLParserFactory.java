/**
 * Copyright 2007-2008 University Of Southern California
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.isi.pegasus.planner.parser;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.common.util.CondorVersion;
import edu.isi.pegasus.common.util.DynamicLoader;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.common.PegasusProperties;
import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;

/**
 * A factory class to load the appropriate Site Catalog Parser implementations based on version in
 * the site catalog element of the XML document
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class SiteCatalogXMLParserFactory {

    /** Package to prefix "just" class names with. */
    public static final String DEFAULT_PARSER_PACKAGE_NAME = "edu.isi.pegasus.planner.parser";

    /*
     * Predefined Constant for Site Catalog Version 2.0
     */
    private static final long SC_VERSION_2_0_0 = CondorVersion.numericValue("2.0.0");

    /*
     * Predefined Constant for Site Catalog Version 3.0
     */
    public static final long SC_VERSION_3_0_0 = CondorVersion.numericValue("3.0.0");

    /*
     * Predefined Constant for Site Catalog Version 4.0
     */
    public static final long SC_VERSION_4_0_0 = CondorVersion.numericValue("4.0.0");

    /** The default Site Catalog Parser */
    public static final String DEFAULT_SC_PARSER_CLASS = "SiteCatalogXMLParser4";

    /** The SC Parser classname */
    public static final String SC_PARSER3_CLASS = "SiteCatalogXMLParser3";

    /**
     * Loads the appropriate DAXParser looking at the dax schema that is specified by the user.
     *
     * @param bag bag of Pegasus intialization objects
     * @param connectProps the connection properties without the site catalog prefix
     * @param file the site catalog file
     * @param sites the list of sites that need to be parsed. * means all
     * @return the SiteCatalogXMLParser class that is loaded.
     * @exception SiteCatalogXMLParserFactoryException that nests any error that might occur during
     *     the instantiation
     * @see #DEFAULT_CALLBACK_PACKAGE_NAME
     */
    public static SiteCatalogXMLParser loadSiteCatalogXMLParser(
            PegasusBag bag, Properties connectProps, String file, List<String> sites)
            throws SiteCatalogXMLParserFactoryException {

        String scClass = SiteCatalogXMLParserFactory.DEFAULT_SC_PARSER_CLASS;
        LogManager logger = bag.getLogger();
        PegasusProperties properties = bag.getPegasusProperties();

        // sanity check
        if (properties == null) {
            throw new RuntimeException("Invalid properties passed");
        }
        if (logger == null) {
            throw new RuntimeException("Invalid logger passed");
        }

        // try to figure out the schema version by parsing the dax file
        String schemaVersion = null;

        try {
            Map m = getMetadata(bag, file);
            if (m.containsKey("version") && (schemaVersion = (String) m.get("version")) != null) {
                logger.log(
                        "Site Catalog Schema Version as determined from catalog file "
                                + schemaVersion,
                        LogManager.DEBUG_MESSAGE_LEVEL);

                // append .0 to the version number
                // to be able to convert to numberic value
                schemaVersion = schemaVersion + ".0";
            }

            // try to figure out the schema from the schemaLocation
            // attribute in the root element
            String schemaLocation = (String) m.get("schemaLocation");

            if (schemaVersion == null && schemaLocation != null) {
                logger.log(
                        "Guessing schema version from schema location " + schemaLocation,
                        LogManager.DEBUG_MESSAGE_LEVEL);
                // split on whitespace
                String constitutents[] = schemaLocation.split(file);
                schemaLocation = constitutents[constitutents.length - 1];

                // get the basename
                String schema = new File(schemaLocation).getName();
                if (schema.startsWith("sc-") && schema.endsWith(".xsd")) {
                    schemaVersion =
                            schema.substring(schema.indexOf("sc-") + 3, schema.lastIndexOf(".xsd"));

                    logger.log(
                            "Site Catalog XML Version as determined from schema location "
                                    + schemaVersion,
                            LogManager.DEBUG_MESSAGE_LEVEL);

                    // append .0 to the version number
                    // to be able to convert to numberic value
                    schemaVersion = schemaVersion + ".0";
                }
            }

        } catch (Exception e) {
            logger.log(
                    "Problem while determining the version of dax",
                    e,
                    LogManager.ERROR_MESSAGE_LEVEL);
        }

        if (schemaVersion != null) {
            long numericValue = CondorVersion.numericValue(schemaVersion);

            if (numericValue < SiteCatalogXMLParserFactory.SC_VERSION_2_0_0) {
                // log error
                throw new SiteCatalogXMLParserFactoryException(
                        "Site Catalog Schema Version2 is no longer supported");
            } else if (numericValue == SiteCatalogXMLParserFactory.SC_VERSION_3_0_0) {
                scClass = SiteCatalogXMLParserFactory.SC_PARSER3_CLASS;
            } else {
                scClass = SiteCatalogXMLParserFactory.DEFAULT_SC_PARSER_CLASS;
            }
        }

        logger.log(
                "Site Catalog Parser Class to be loaded is " + scClass,
                LogManager.CONFIG_MESSAGE_LEVEL);

        return loadSiteCatalogParser(scClass, bag, connectProps, sites);
    }

    /**
     * Loads the appropriate Site Catalog Parser looking at the dax schema that is specified by the
     * user.
     *
     * @param classname the classname of the parser class that needs to be loaded
     * @param bag bag of Pegasus intialization objects
     * @param connectProps the connection properties without the site catalog prefix
     * @param sites the list of sites that need to be parsed. * means all
     * @return the DAXParser loaded.
     * @exception SiteCatalogXMLParserFactoryException that nests any error that might occur during
     *     the instantiation
     * @see #DEFAULT_CALLBACK_PACKAGE_NAME
     */
    private static final SiteCatalogXMLParser loadSiteCatalogParser(
            String classname, PegasusBag bag, Properties connectProps, List<String> sites) {
        SiteCatalogXMLParser parser = null;
        try {
            // load the DAX Parser class
            // prepend the package name
            String daxClass =
                    (classname.indexOf('.') == -1)
                            ?
                            // pick up from the default package
                            DEFAULT_PARSER_PACKAGE_NAME + "." + classname
                            :
                            // load directly
                            classname;

            DynamicLoader dl = new DynamicLoader(daxClass);

            Object argList[] = new Object[3];
            Class classList[] = new Class[3];

            classList[0] = bag.getClass();
            classList[1] = connectProps.getClass();
            classList[2] = Class.forName("java.util.List");
            argList[0] = bag;
            argList[1] = connectProps;
            argList[2] = sites;

            parser = (SiteCatalogXMLParser) dl.instantiate(classList, argList);

        } catch (Exception e) {
            throw new SiteCatalogXMLParserFactoryException(
                    "Unable to instantiate SiteCatalogXMLParser class ", classname, e);
        }
        return parser;
    }

    /**
     * Returns the metadata stored in the root adag element in the DAX
     *
     * @param bag the bag of initialization objects
     * @param file the site catalog file.
     * @return Map containing the metadata, else an empty map
     */
    public static Map getMetadata(PegasusBag bag, String file) {

        LogManager logger = bag.getLogger();
        if (logger != null) {
            logger.log("Retrieving Metadata from the file " + file, LogManager.DEBUG_MESSAGE_LEVEL);
        }

        SiteCatalogXMLMetadataParser p = new SiteCatalogXMLMetadataParser(bag, "sitecatalog");
        p.startParser(file);

        return p.getMetadata();
    }
}

/**
 * A lightweight XML Parser class to just retrieve the meta data in first instance of an element in
 * a XML Document. It used to get the metadata in the root element.
 *
 * @author Karan Vahi
 */
class SiteCatalogXMLMetadataParser extends Parser {

    /** The root element name to look for. */
    private String mElement;

    /** The Metadata object that has to be returned. */
    private Map<String, String> mMetadata;

    /** A boolean indicating that parsing is done. */
    protected boolean mParsingDone;

    /**
     * The overloaded constructor
     *
     * @param bag the bag of intiialization documents
     * @param element the root element
     */
    public SiteCatalogXMLMetadataParser(PegasusBag bag, String element) {
        super(bag);
        mElement = element;
        mMetadata = new HashMap();
        mParsingDone = false;
    }

    /**
     * Returns the metadata/attributes assoicated with the element that was parsed for.
     *
     * @return
     */
    public Map<String, String> getMetadata() {
        if (mParsingDone) {
            return mMetadata;
        }
        throw new RuntimeException("Method called before parsing was started");
    }

    /**
     * Start Element.
     *
     * @param uri
     * @param local
     * @param raw
     * @param attrs
     * @throws SAXException
     */
    public void startElement(String uri, String local, String raw, Attributes attrs)
            throws SAXException {

        // check
        if (local == null || !local.equalsIgnoreCase(mElement)) {
            return;
        }

        // usually the element we want to search for is the first one
        for (int i = 0; i < attrs.getLength(); ++i) {
            String name = attrs.getLocalName(i);
            String value = attrs.getValue(i);
            mMetadata.put(name, value);
        }

        // call to end document
        this.endDocument();

        // we need to throw an exception to stop the parser
        // ugly
        throw new StopParserException();
    }

    /**
     * * An empty implementation
     *
     * @param uri
     * @param localName
     * @param qName
     * @throws SAXException
     */
    public void endElement(String uri, String localName, String qName) throws SAXException {}

    /** Sets the boolean indicating parsing is done */
    public void endDocument() {
        mParsingDone = true;
    }

    /**
     * The function that starts the parser
     *
     * @param file the file to be parsed
     */
    public void startParser(String file) {
        try {
            this.testForFile(file);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        // while determining the metadata we are just parsing adag element
        // we want the parser validation to be turned off.
        this.setParserFeature("http://xml.org/sax/features/validation", false);
        this.setParserFeature("http://apache.org/xml/features/validation/schema", false);

        try {
            mParser.parse(file);
        } catch (StopParserException e) {
            // ignore
        } catch (Exception e) {
            // if a locator error then
            String message =
                    (mLocator == null)
                            ? "SiteCatalogXMLMetadataParser While parsing the file " + file
                            : "SiteCatalogXMLMetadataParser While parsing file "
                                    + mLocator.getSystemId()
                                    + " at line "
                                    + mLocator.getLineNumber()
                                    + " at column "
                                    + mLocator.getColumnNumber();
            mLogger.logEventCompletion();
            throw new RuntimeException(message, e);
        }
    }

    /**
     * Not implemented as yet
     *
     * @return
     */
    public String getSchemaLocation() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    /**
     * Not implemented as yet
     *
     * @return
     */
    public String getSchemaNamespace() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    /** Private RuntimeException to stop the SAX Parser */
    private static class StopParserException extends RuntimeException {}
}
