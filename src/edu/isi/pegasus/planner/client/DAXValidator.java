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
package edu.isi.pegasus.planner.client;

import java.io.File;
import java.io.IOException;
import org.xml.sax.Attributes;
import org.xml.sax.Locator;
import org.xml.sax.SAXException;
import org.xml.sax.SAXNotRecognizedException;
import org.xml.sax.SAXNotSupportedException;
import org.xml.sax.SAXParseException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;
import org.xml.sax.helpers.XMLReaderFactory;

/**
 * This class reads to validate a DAX document. It requires at least Xerces-J 2.10.
 *
 * @author: Jens-S. Vöckler
 * @version: $Id$
 */
public class DAXValidator extends DefaultHandler {
    /** Default parser is the Xerces parser. */
    protected static final String vendorParserClass = "org.apache.xerces.parsers.SAXParser";

    /** URI namespace for DAX schema. */
    public static final String SCHEMA_NAMESPACE = "https://pegasus.isi.edu/schema/DAX";

    /** what is the name of the schema file in the filename hint? */
    private String m_schemafile = "dax-3.3.xsd";

    /** Holds the instance of a {@link org.xml.sax.XMLReader} class. */
    private XMLReader m_reader;

    /** Keep the location within the document. */
    private Locator m_location;

    /** How verbose should we be? */
    protected boolean m_verbose;

    /** Counts the number of warnings. */
    protected int m_warnings;

    /** Counts the number of errors. */
    protected int m_errors;

    /** Counts the number of fatal errors. */
    protected int m_fatals;

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
            this.m_reader.setFeature(uri, flag);
            result = true;
        } catch (SAXNotRecognizedException e) {
            System.err.println("Unrecognized feature " + uri + ": " + e);
        } catch (SAXNotSupportedException e) {
            System.err.println("Unsupported feature " + uri + ": " + e);
        } catch (SAXException e) {
            System.err.println("Parser feature error: " + e);
        }
        return result;
    }

    /**
     * Sets a SAX property while capturing failed features right here.
     *
     * @param uri is the property's URI to modify
     * @param value is the new value to set.
     * @return true, if the feature could be set, false for an exception
     */
    private boolean prop(String uri, Object value) {
        boolean result = false;
        try {
            this.m_reader.setProperty(uri, value);
            result = true;
        } catch (SAXNotRecognizedException e) {
            System.err.println("Unrecognized property " + uri + ": " + e);
        } catch (SAXNotSupportedException e) {
            System.err.println("Unsupported property " + uri + ": " + e);
        } catch (SAXException e) {
            System.err.println("Parser property error: " + e);
        }
        return result;
    }

    /** default c'tor */
    public DAXValidator(boolean verbose) throws Exception {
        m_reader = XMLReaderFactory.createXMLReader(vendorParserClass);
        m_reader.setContentHandler(this);
        m_reader.setErrorHandler(this);
        m_verbose = verbose;
        m_warnings = m_errors = m_fatals = 0;

        if (m_verbose) {
            System.err.println("# XMLReader is " + org.apache.xerces.impl.Version.getVersion());
        }

        //
        // turn on almost all features that we can safely turn on.
        // WARNING: The features below assume Xerces-J 2.10 or greater.
        //

        // Perform namespace processing: prefixes will be stripped off
        // element and attribute names and replaced with the corresponding
        // namespace URIs. By default, the two will simply be concatenated,
        // but the namespace-sep core property allows the application to
        // specify a delimiter string for separating the URI part and the
        // local part.
        set("http://xml.org/sax/features/namespaces", true);

        // The methods of the org.xml.sax.ext.EntityResolver2 interface will
        // be used when an object implementing this interface is registered
        // with the parser using setEntityResolver.
        //
        // If the disallow DOCTYPE declaration feature is set to true
        // org.xml.sax.ext.EntityResolver2.getExternalSubset() will not be
        // called when the document contains no DOCTYPE declaration.
        set("http://xml.org/sax/features/use-entity-resolver2", true);

        // Validate the document and report validity errors.
        //
        // If this feature is set to true, the document must specify a
        // grammar. By default, validation will occur against DTD. For more
        // information, please, refer to the FAQ. If this feature is set to
        // false, and document specifies a grammar that grammar might be
        // parsed but no validation of the document contents will be
        // performed.
        set("http://xml.org/sax/features/validation", true);

        // true: The parser will validate the document only if a grammar is
        // specified.
        // false: Validation is determined by the state of the validation
        // feature.
        set("http://apache.org/xml/features/validation/dynamic", false);

        // Turn on XML Schema validation by inserting an XML Schema
        // validator into the pipeline.
        //
        // Validation errors will only be reported if the validation feature
        // is set to true. For more information, please, refer to the FAQ.
        //
        // Checking of constraints on a schema grammar which are either
        // time-consuming or memory intensive such as unique particle
        // attribution will only occur if the schema full checking feature
        // is set to true.
        set("http://apache.org/xml/features/validation/schema", true);

        // Enable full schema grammar constraint checking, including
        // checking which may be time-consuming or memory
        // intensive. Currently, unique particle attribution constraint
        // checking and particle derivation restriction checking are
        // controlled by this option.
        //
        // This feature checks the Schema grammar itself for additional
        // errors that are time-consuming or memory intensive. It does not
        // affect the level of checking performed on document instances that
        // use Schema grammars.
        set("http://apache.org/xml/features/validation/schema-full-checking", true);

        // Expose via SAX and DOM XML Schema normalized values for
        // attributes and elements.
        //
        // XML Schema normalized values will be exposed only if both schema
        // validation and validation features are set to true.
        set("http://apache.org/xml/features/validation/schema/normalized-value", true);

        // Send XML Schema element default values via characters().
        //
        // XML Schema default values will be send via characters() if both
        // schema validation and validation features are set to true.
        set("http://apache.org/xml/features/validation/schema/element-default", true);

        // Augment Post-Schema-Validation-Infoset.
        //
        // This feature can be turned off to improve parsing performance.
        set("http://apache.org/xml/features/validation/schema/augment-psvi", true);

        // xsi:type attributes will be ignored until a global element
        // declaration has been found, at which point xsi:type attributes
        // will be processed on the element for which the global element
        // declaration was found as well as its descendants.
        set(
                "http://apache.org/xml/features/validation/schema/ignore-xsi-type-until-elemdecl",
                true);

        // Enable generation of synthetic annotations. A synthetic
        // annotation will be generated when a schema component has
        // non-schema attributes but no child annotation.
        set("http://apache.org/xml/features/generate-synthetic-annotations", true);

        // Schema annotations will be laxly validated against available
        // schema components.
        set("http://apache.org/xml/features/validate-annotations", true);

        // All schema location hints will be used to locate the components
        // for a given target namespace.
        set("http://apache.org/xml/features/honour-all-schemaLocations", true);

        // Include external general entities.
        set("http://xml.org/sax/features/external-general-entities", true);

        // Include external parameter entities and the external DTD subset.
        set("http://xml.org/sax/features/external-parameter-entities", true);

        // Construct an optimal representation for DTD content models to
        // significantly reduce the likelihood a StackOverflowError will
        // occur when large content models are processed.
        //
        // Enabling this feature may cost your application some performance
        // when DTDs are processed so it is recommended that it only be
        // turned on when necessary.
        set("http://apache.org/xml/features/validation/balance-syntax-trees", true);

        // Enable checking of ID/IDREF constraints.
        //
        // This feature only applies to schema validation.
        set("http://apache.org/xml/features/validation/id-idref-checking", true);

        // Enable identity constraint checking.
        set("http://apache.org/xml/features/validation/identity-constraint-checking", true);

        // Check that each value of type ENTITY matches the name of an
        // unparsed entity declared in the DTD.
        //
        // This feature only applies to schema validation.
        set("http://apache.org/xml/features/validation/unparsed-entity-checking", true);

        // Report a warning when a duplicate attribute is re-declared.
        set("http://apache.org/xml/features/validation/warn-on-duplicate-attdef", true);

        // Report a warning if an element referenced in a content model is
        // not declared.
        set("http://apache.org/xml/features/validation/warn-on-undeclared-elemdef", true);

        // Report a warning for duplicate entity declaration.
        set("http://apache.org/xml/features/warn-on-duplicate-entitydef", true);

        // Do not allow Java encoding names in XMLDecl and TextDecl line.
        //
        // A true value for this feature allows the encoding of the file to
        // be specified as a Java encoding name as well as the standard ISO
        // encoding name. Be aware that other parsers may not be able to use
        // Java encoding names. If this feature is set to false, an error
        // will be generated if Java encoding names are used.
        set("http://apache.org/xml/features/allow-java-encodings", false);

        // Attempt to continue parsing after a fatal error.
        //
        // The behavior of the parser when this feature is set to true is
        // undetermined! Therefore use this feature with extreme caution
        // because the parser may get stuck in an infinite loop or worse.
        set("http://apache.org/xml/features/continue-after-fatal-error", true);

        // Load the DTD and use it to add default attributes and set
        // attribute types when parsing.
        //
        // This feature is always on when validation is on.
        set("http://apache.org/xml/features/nonvalidating/load-dtd-grammar", true);

        // Load the external DTD.
        //
        // This feature is always on when validation is on.
        set("http://apache.org/xml/features/nonvalidating/load-external-dtd", true);

        // Notifies the handler of character reference boundaries in the
        // document via the start/endEntity callbacks.
        set("http://apache.org/xml/features/scanner/notify-char-refs", false);

        // Notifies the handler of built-in entity boundaries (e.g &amp;) in
        // the document via the start/endEntity callbacks.
        set("http://apache.org/xml/features/scanner/notify-builtin-refs", false);

        // A fatal error is thrown if the incoming document contains a
        // DOCTYPE declaration.
        set("http://apache.org/xml/features/disallow-doctype-decl", true);

        // Requires that a URI has to be provided where a URI is expected.
        //
        // It's recommended to set this feature to true if you want your
        // application/documents to be truly portable across different XML
        // processors.
        set("http://apache.org/xml/features/standard-uri-conformant", true);

        // Report the original prefixed names and attributes used for
        // namespace declarations.
        set("http://xml.org/sax/features/namespace-prefixes", true);

        // All element names, prefixes, attribute names, namespace URIs, and
        // local names are internalized using the
        // java.lang.String#intern(String):String method.
        set("http://xml.org/sax/features/string-interning", true);

        // Report the beginning and end of parameter entities to a
        // registered LexicalHandler.
        set("http://xml.org/sax/features/lexical-handler/parameter-entities", true);

        // set( "http://apache.org/xml/features/xinclude", true );
        // set( "http://apache.org/xml/features/xinclude/fixup-base-uris", true );
        // set( "http://apache.org/xml/features/xinclude/fixup-language", true );
        //
        // set( "http://xml.org/sax/features/is-standalone", true );
        // set( "http://xml.org/sax/features/unicode-normalization-checking", true );
        // set( "http://xml.org/sax/features/use-attributes2", true );
        // set( "http://xml.org/sax/features/use-locator2", true );

        // The system identifiers passed to the notationDecl,
        // unparsedEntityDecl, and externalEntityDecl events will be
        // absolutized relative to their base URIs before reporting.
        //
        // This feature does not apply to EntityResolver.resolveEntity(),
        // which is not used to report declarations, or to
        // LexicalHandler.startDTD(), which already provides the
        // non-absolutized URI.
        set("http://xml.org/sax/features/resolve-dtd-uris", true);

        // true: When the namespace-prefixes feature is set to true, namespace
        // declaration attributes will be reported as being in the
        // http://www.w3.org/2000/xmlns/ namespace.
        // false: Namespace declaration attributes are reported as having no
        // namespace.
        set("http://xml.org/sax/features/xmlns-uris", true);

        String schemaLocation = null;
        String pegasus_home = System.getenv("PEGASUS_HOME");
        if (pegasus_home != null) {
            File sl = new File(new File(pegasus_home, "etc"), m_schemafile);
            if (sl.canRead()) {
                schemaLocation = sl.toString();
            } else {
                System.err.println("Warning: Unable to read " + sl);
            }
        }

        // The XML Schema Recommendation explicitly states that the
        // inclusion of schemaLocation/noNamespaceSchemaLocation attributes
        // is only a hint; it does not mandate that these attributes must be
        // used to locate schemas. Similar situation happens to <import>
        // element in schema documents. This property allows the user to
        // specify a list of schemas to use. If the targetNamespace of a
        // schema (specified using this property) matches the
        // targetNamespace of a schema occurring in the instance document in
        // schemaLocation attribute, or if the targetNamespace matches the
        // namespace attribute of <import> element, the schema specified by
        // the user using this property will be used (i.e., the
        // schemaLocation attribute in the instance document or on the
        // <import> element will be effectively ignored).
        //
        // The syntax is the same as for schemaLocation attributes in
        // instance documents: e.g, "http://www.example.com
        // file_name.xsd". The user can specify more than one XML Schema in
        // the list.
        if (schemaLocation != null) {
            prop(
                    "http://apache.org/xml/properties/schema/external-schemaLocation",
                    SCHEMA_NAMESPACE + " " + schemaLocation);
            if (m_verbose) System.err.println("# will use " + schemaLocation);
        } else {
            if (m_verbose) System.err.println("# will use document schema hint");
        }

        // The size of the input buffer in the readers. This determines how
        // many bytes to read for each chunk. Some tests indicate that a
        // bigger buffer size can improve the parsing performance for
        // relatively large files. The default buffer size in Xerces is
        // 2K. This would give a good performance for small documents (less
        // than 10K). For documents larger than 10K, specifying the buffer
        // size to 4K or 8K will significantly improve the performance. But
        // it's not recommended to set it to a value larger than 16K. For
        // really tiny documents (1K, for example), you can also set it to a
        // value less than 2K, to get the best performance.
        prop("http://apache.org/xml/properties/input-buffer-size", 16384);
    }

    // --- ErrorHandler ---

    public void warning(SAXParseException ex) throws SAXException {
        m_warnings++;
        System.err.println("WARNING in " + full_where() + ": " + ex.getMessage());
    }

    public void error(SAXParseException ex) throws SAXException {
        m_errors++;
        System.err.println("ERROR in " + full_where() + ": " + ex.getMessage());
    }

    public void fatalError(SAXParseException ex) throws SAXException {
        m_fatals++;
        System.err.println("FATAL in " + full_where() + ": " + ex.getMessage());
    }

    // --- ContentHandler ---

    public void setDocumentLocator(Locator locator) {
        this.m_location = locator;
    }

    private String full_where() {
        return ("line " + m_location.getLineNumber() + ", col " + m_location.getColumnNumber());
    }

    private String where() {
        return (m_location.getLineNumber() + ":" + m_location.getColumnNumber());
    }

    public void startDocument() throws SAXException {
        if (m_verbose) {
            System.out.println(where() + " *** start of document ***");
        }
    }

    public void endDocument() {
        if (m_verbose) {
            System.out.println(where() + " *** end of document ***");
        }
    }

    public void startElement(String nsURI, String localName, String qName, Attributes attrs)
            throws SAXException {
        if (m_verbose) {
            System.out.print(where() + " <" + qName);
            for (int i = 0; i < attrs.getLength(); i++) {
                System.out.print(" " + attrs.getQName(i));
                System.out.print("=\"" + attrs.getValue(i) + "\"");
            }
            System.out.println(">");
        }
    }

    public void endElement(String nsURI, String localName, String qName) throws SAXException {
        if (m_verbose) {
            System.out.println(where() + " </" + qName + ">");
        }
    }

    public void characters(char[] ch, int start, int length) throws SAXException {
        if (m_verbose) {
            String s = new String(ch, start, length).trim();
            if (s.length() > 0) System.out.println(where() + " \"" + s + "\"");
        }
    }

    public void ignorableWhitespace(char[] ch, int start, int length) throws SAXException {
        //    if ( m_verbose ) {
        //      String s = new String( ch, start, length ).trim();
        //      if ( s.length() > 0 ) System.out.println( where() + " \"" + s + "\"" );
        //    }
    }

    public void parse(String filename) throws Exception {
        m_reader.parse(filename);
    }

    /**
     * Show how many warnings, errors and fatals were shown.
     *
     * @return true, if we should transmit an error exit code.
     */
    public boolean statistics() {
        System.out.println();
        System.out.print(m_warnings + " warnings, ");
        System.out.print(m_errors + " errors, and ");
        System.out.println(m_fatals + " fatal errors detected.");
        return (m_errors > 0 || m_fatals > 0);
    }

    // --- main ---

    public static void main(String args[]) throws Exception {
        boolean fail = true;

        if (args.length > 0) {
            try {
                DAXValidator validator = new DAXValidator(args.length > 1);
                validator.parse(args[0]);
                fail = validator.statistics();
            } catch (IOException ioe) {
                System.err.println(ioe);
            } catch (SAXException spe) {
                System.err.println(spe);
            }
        }

        if (fail) System.exit(1);
    }
}
