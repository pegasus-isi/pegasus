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

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Stack;
import javax.xml.parsers.*;
import org.griphyn.vdl.classes.LFN;
import org.griphyn.vdl.dax.*;
import org.griphyn.vdl.util.Logging;
import org.griphyn.vdl.util.VDLType;
import org.xml.sax.*;
import org.xml.sax.helpers.DefaultHandler;

/**
 * This class uses the xerces SAX2 parser to validate and parse an DAX document. This class extends
 * the xerces DefaultHandler so that we only need to override callbacks of interest.
 *
 * @author Kavitha Ranganathan
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 */
public class DAXParser extends DefaultHandler {
    /** class name of the SAX parser. */
    private static String vendorParserClass = "org.apache.xerces.parsers.SAXParser";

    /** our own instance of the SAX parser. */
    private XMLReader m_parser;

    /** maintain the hierarchy for some debug printing */
    private int m_depth;

    /** collects the information for a section 2 job element. */
    private Job m_job;

    /** collects the leaves for a profile that is part of a job. */
    private Profile m_profile;

    /** maintains the currently viewed section 3 child element. */
    private String m_child;

    /** collects the parents associated with a particular child. */
    private java.util.List m_parent;

    /**
     * maintains the indications which parent element to be used whenever a filename tag is being
     * encountered.
     */
    private int m_tag;

    // possible values for m_tag
    static final int TAG_ADAG = 0;
    static final int TAG_ARGUMENT = 1;
    static final int TAG_PROFILE = 2;
    static final int TAG_OTHER = 3;

    /** Keep the location within the document */
    private Locator m_location;

    /** A Hashmap to forward resolve namespaces that were encountered during parsing. */
    private Map m_forward;

    /** A Hashmap to reverse resolve namespaces that were encountered during parsing. */
    private Map m_reverse;

    /** Obtain our logger once for multiple uses. */
    private Logging m_log;

    /** Maintains the callback class to provide the information to. */
    private Callback m_callback;

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
     * The class constructor initializes the Xerces parser, sets the classes that hold the callback
     * functions, and the features that enable schema validation.
     *
     * @param schemaLocation is any URI pointing to the XML schema definition.
     */
    public DAXParser(String schemaLocation) {
        // member variables
        this.m_callback = null;
        this.m_child = null;
        this.m_parent = null;

        // parser related members
        this.m_forward = new HashMap();
        this.m_reverse = new HashMap();
        this.m_log = Logging.instance();

        try {
            m_parser = (XMLReader) Class.forName(vendorParserClass).newInstance();
            m_parser.setContentHandler(this);
            // m_parser.setErrorHandler(this);
            m_parser.setErrorHandler(new edu.isi.pegasus.planner.parser.XMLErrorHandler());

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
                setSchemaLocations(ADAG.SCHEMA_NAMESPACE + ' ' + schemaLocation);
                m_log.log("app", 2, "will use " + schemaLocation);
            } else {
                m_log.log("app", 2, "will use document schema hint");
            }
        } catch (ClassNotFoundException e) {
            m_log.log("default", 0, "The SAXParser class was not found: " + e);
        } catch (InstantiationException e) {
            m_log.log("default", 0, "The SAXParser class could not be instantiated: " + e);
        } catch (IllegalAccessException e) {
            m_log.log("default", 0, "The SAXParser class could not be accessed: " + e);
        }
    }

    /**
     * Obtains the current instance to be used for callbacks.
     *
     * @return the current callback instance object, or null.
     * @see #setCallback( Callback )
     */
    public Callback getCallback() {
        return this.m_callback;
    }

    /**
     * Sets a new callback object to use for future callbacks.
     *
     * @param callback is the new callback object.
     * @see #getCallback()
     */
    public void setCallback(Callback callback) {
        this.m_callback = callback;
    }

    /**
     * Set the list of external real locations where the XML schema may be found. Since this list
     * can be determined at run-time through properties etc., we expect this function to be called
     * between instantiating the parser, and using the parser.
     *
     * @param list is a list of strings representing schema locations. The content exists in pairs,
     *     one of the namespace URI, one of the location URL.
     */
    public void setSchemaLocations(String list) {
        // schema location handling
        try {
            m_parser.setProperty(
                    "http://apache.org/xml/properties/schema/external-schemaLocation", list);
        } catch (SAXException se) {
            m_log.log("default", 0, "The SAXParser reported an error: " + se);
        }
    }

    /**
     * This function parses a DAX source (could be a document, a stream, etc.), and creates java
     * class instances that correspond to the DAX. These will provided to the callback functions
     * instead of being collected here in memory.
     *
     * @param daxURI is the URI for the DAX source.
     * @return true for valid parsing, false if an error occurred.
     */
    public boolean parse(String daxURI) {
        boolean result = false;

        if (m_callback == null) {
            m_log.log("default", 0, "Error: Programmer forgot to provide a callback");
            return result;
        }

        try {
            InputSource inputSource = new InputSource(daxURI);
            m_parser.parse(inputSource);
            result = true;
        } catch (SAXException e) {
            m_log.log("default", 0, "SAX Error: " + e);
        } catch (IOException e) {
            m_log.log("default", 0, "IO Error: " + e);
        }

        return result;
    }

    /**
     * This function parses a DAX source (could be a document, a stream, etc.), and creates java
     * class instances that correspond to the DAX. These will provided to the callback functions
     * instead of being collected here in memory.
     *
     * @param stream is an input stream for the DAX source.
     * @return true for valid parsing, false if an error occurred.
     */
    public boolean parse(InputStream stream) {
        boolean result = false;

        if (m_callback == null) {
            m_log.log("default", 0, "Error: Programmer forgot to provide a callback");
            return result;
        }

        try {
            InputSource inputSource = new InputSource(stream);
            m_parser.parse(inputSource);
            result = true;
        } catch (SAXException e) {
            m_log.log("default", 0, "SAX Error: " + e);
        } catch (IOException e) {
            m_log.log("default", 0, "IO Error: " + e);
        }

        return result;
    }

    //
    // here starts the implementation to the Interface
    //

    /**
     * Obtains the document locator from the parser. The document location can be used to print
     * debug information, i.e the current location (line, column) in the document.
     *
     * @param locator is the externally set current position
     */
    public void setDocumentLocator(Locator locator) {
        this.m_location = locator;
    }

    /**
     * This method specifies what to do when the parser is at the beginning of the document. In this
     * case, we simply print a message for debugging.
     */
    public void startDocument() {
        m_depth = 0;
        m_log.log("parser", 1, "*** start of document ***");
    }

    /** The parser comes to the end of the document. */
    public void endDocument() {
        m_log.log("parser", 1, "*** end of document ***");
    }

    /**
     * There is a prefix or namespace defined, put the prefix and its URI in the HashMap. We can get
     * the URI when the prefix is used here after.
     *
     * @param prefix the Namespace prefix being declared.
     * @param uri the Namespace URI the prefix is mapped to.
     */
    public void startPrefixMapping(java.lang.String prefix, java.lang.String uri)
            throws SAXException {
        String p = prefix == null ? null : new String(prefix);
        String u = uri == null ? null : new String(uri);
        m_log.log("parser", 2, "adding \"" + p + "\" <=> " + u);

        if (!this.m_forward.containsKey(p)) this.m_forward.put(p, new Stack());
        ((Stack) this.m_forward.get(p)).push(u);

        if (!this.m_reverse.containsKey(u)) this.m_reverse.put(u, new Stack());
        ((Stack) this.m_reverse.get(u)).push(p);
    }

    /**
     * Out of the reach of the prefix, remove it from the HashMap.
     *
     * @param prefix is the prefix that was being mapped previously.
     */
    public void endPrefixMapping(java.lang.String prefix) throws SAXException {
        String u = (String) ((Stack) this.m_forward.get(prefix)).pop();
        String p = (String) ((Stack) this.m_reverse.get(u)).pop();
        m_log.log("parser", 2, "removed \"" + p + "\" <=> " + u);
    }

    /**
     * Helper function to map prefixes correctly onto the elements.
     *
     * @param uri is the parser-returned URI that needs translation.
     * @return the correct prefix for the URI
     */
    private String map(String uri) {
        if (uri == null || uri.length() == 0) return "";
        Stack stack = (Stack) this.m_reverse.get(uri);
        String result = stack == null ? null : (String) stack.peek();
        if (result == null || result.length() == 0) return "";
        else return result + ':';
    }

    /**
     * This method defines the action to take when the parser begins to parse an element.
     *
     * @param namespaceURI is the URI of the namespace for the element
     * @param localName is the element name without namespace
     * @param qName is the element name as it appears in the docment
     * @param atts has the names and values of all the attributes
     */
    public void startElement(
            java.lang.String namespaceURI,
            java.lang.String localName,
            java.lang.String qName,
            Attributes atts)
            throws SAXException {

        m_log.log(
                "parser",
                3,
                "<"
                        + map(namespaceURI)
                        + localName
                        + "> at "
                        + m_location.getLineNumber()
                        + ":"
                        + m_location.getColumnNumber());

        // yup, one more element level
        m_depth++;

        java.util.List names = new java.util.ArrayList();
        java.util.List values = new java.util.ArrayList();
        for (int i = 0; i < atts.getLength(); ++i) {
            String name = new String(atts.getLocalName(i));
            String value = new String(atts.getValue(i));

            m_log.log(
                    "parser", 2, "attribute " + map(atts.getURI(i)) + name + "=\"" + value + "\"");
            names.add(name);
            values.add(value);
        }

        createElementObject(localName, names, values);
    }

    /**
     * The parser is at the end of an element. Each successfully and completely parsed Definition
     * will trigger a callback to the registered DefinitionHandler.
     *
     * @param namespaceURI is the URI of the namespace for the element
     * @param localName is the element name without namespace
     * @param qName is the element name as it appears in the docment
     */
    public void endElement(
            java.lang.String namespaceURI, java.lang.String localName, java.lang.String qName)
            throws SAXException {
        // that's it for this level
        m_depth--;
        m_log.log(
                "parser",
                3,
                "</"
                        + map(namespaceURI)
                        + localName
                        + "> at "
                        + m_location.getLineNumber()
                        + ":"
                        + m_location.getColumnNumber());

        setElementRelation(localName);
    }

    /**
     * This method is the callback function for characters in an element. The element should be
     * mixed-content.
     *
     * @param ch are the characters from the XML document
     * @param start is the start position into the array
     * @param length is the amount of valid data in the array
     */
    public void characters(char[] ch, int start, int length) throws SAXException {
        String message = new String(ch, start, length);
        if (message.length() > 0) {
            if (message.trim().length() == 0)
                m_log.log("parser", 3, "Characters: whitespace x " + length);
            else m_log.log("parser", 3, "Characters: \"" + message + "\"");
            elementCharacters(message);
        }
    }

    /**
     * Currently, ignorable whitespace will be ignored.
     *
     * @param ch are the characters from the XML document
     * @param start is the start position into the array
     * @param length is the amount of valid data in the array
     */
    public void ignorableWhitespace(char[] ch, int start, int length) throws SAXException {
        m_log.log("parser", 3, "Ignoring " + length + " whitespaces");
    }

    /**
     * Receive a processing instruction. Currently, we are just printing a debug message that we
     * received a PI.
     *
     * @param target the processing instruction target
     * @param data the processing instruction data, or null if none was supplied. The data does not
     *     include any whitespace separating it from the target.
     */
    public void processingInstruction(java.lang.String target, java.lang.String data)
            throws SAXException {
        m_log.log(
                "parser", 2, "processing instruction " + target + "=\"" + data + "\" was skipped!");
    }

    /**
     * Receive a notification that an entity was skipped. Currently, we are just printing a debug
     * message to this fact.
     *
     * @param name The name of the skipped entity. If it is a parameter entity, the name will begin
     *     with '%', and if it is the external DTD subset, it will be the string "[dtd]".
     */
    public void skippedEntity(java.lang.String name) throws SAXException {
        m_log.log("parser", 2, "entity " + name + " was skipped!");
    }

    //
    // =================================================== our own stuff ===
    //

    /**
     * Small helper method to bundle repetitive parameters in a template for reporting progress.
     *
     * @param subject is the name of the XML element that is being scrutinized.
     * @param name is then name of the element we are working with.
     * @param value is the attribute value.
     */
    private void log(String subject, String name, String value) {
        if (value == null) value = new String();
        m_log.log("parser", 3, subject + "." + name + "=\"" + value + "\"");
    }

    /**
     * Small helper method to bundle repetitive complaints in a template for reporting progress.
     *
     * @param subject is the name of the XML element that is being scrutinized.
     * @param name is then name of the element we are working with.
     * @param value is the attribute value.
     */
    private void complain(String subject, String name, String value) {
        if (value == null) value = new String();
        m_log.log("default", 0, "ignoring " + subject + '@' + name + "=\"" + value + '"', true);
    }

    /**
     * This method finds out what is the current element, creates the java object that corresponds
     * to the element, and sets the member variables with the values of the attributes of the
     * element.
     *
     * @param e is the name of the element
     * @param names is a list of attribute names, as strings.
     * @param values is a list of attribute values, to match the key list.
     */
    public void createElementObject(String e, java.util.List names, java.util.List values)
            throws IllegalArgumentException {
        // invalid length
        if (e == null || e.length() < 1)
            throw new IllegalArgumentException("illegal element length");

        if (e.equals("adag")) {
            HashMap cbdata = new HashMap();
            m_tag = TAG_ADAG;

            for (int i = 0; i < names.size(); ++i) {
                String name = (String) names.get(i);
                String value = (String) values.get(i);

                if (name.equals("name")) {
                    this.log(e, name, value);
                    cbdata.put(name, value);
                } else if (name.equals("index")) {
                    this.log(e, name, value);
                    cbdata.put(name, value);
                } else if (name.equals("count")) {
                    this.log(e, name, value);
                    cbdata.put(name, value);
                } else if (name.equals("version")) {
                    this.log(e, name, value);
                    cbdata.put(name, value);
                } else if (name.equals("jobCount")) {
                    this.log(e, name, value);
                    cbdata.put(name, value);
                } else if (name.equals("fileCount")) {
                    this.log(e, name, value);
                    cbdata.put(name, value);
                } else if (name.equals("childCount")) {
                    this.log(e, name, value);
                    cbdata.put(name, value);
                } else if (name.equals("schemaLocation")) {
                    cbdata.put(name, value);
                } else {
                    this.complain(e, name, value);
                }
            }
            m_callback.cb_document(cbdata);
            return;
        }

        if (e.equals("filename")
                || e.equals("stdin")
                || e.equals("stdout")
                || e.equals("stderr")
                || e.equals("uses")) {
            Filename fn = new Filename();
            for (int i = 0; i < names.size(); ++i) {
                String name = (String) names.get(i);
                String value = (String) values.get(i);

                if (name.equals("file")) {
                    this.log(e, name, value);
                    fn.setFilename(value);
                } else if (name.equals("link")) {
                    this.log(e, name, value);
                    fn.setLink(VDLType.getLinkType(value));
                } else if (name.equals("optional")) {
                    this.log(e, name, value);
                    fn.setOptional(new Boolean(value).booleanValue());
                } else if (name.equals("dontRegister")) {
                    this.log(e, name, value);
                    fn.setDontRegister(new Boolean(value).booleanValue());
                }
                // handle the register flag
                else if (name.equals("register")) {
                    fn.setRegister(new Boolean(value).booleanValue());
                } else if (name.equals("dontTransfer")) {
                    // parse tri-state
                    if (value.equals("false")) {
                        this.log(e, name, value);
                        fn.setDontTransfer(LFN.XFER_MANDATORY);
                    } else if (value.equals("true")) {
                        this.log(e, name, value);
                        fn.setDontTransfer(LFN.XFER_NOT);
                    } else if (value.equals("optional")) {
                        this.log(e, name, value);
                        fn.setDontTransfer(LFN.XFER_OPTIONAL);
                    } else {
                        this.complain(e, name, value);
                    }
                }
                // handle the transfer flag
                else if (name.equals("transfer")) {
                    // parse tri-state
                    if (value.equals("false")) {
                        this.log(e, name, value);
                        fn.setTransfer(LFN.XFER_NOT);
                    } else if (value.equals("true")) {
                        this.log(e, name, value);
                        fn.setTransfer(LFN.XFER_MANDATORY);
                    } else if (value.equals("optional")) {
                        this.log(e, name, value);
                        fn.setTransfer(LFN.XFER_OPTIONAL);
                    } else {
                        this.complain(e, name, value);
                    }
                } else if (name.equals("isTemporary")) {
                    this.log(e, name, value);
                    boolean temp = (new Boolean(value)).booleanValue();
                    fn.setDontRegister(temp);
                    fn.setDontTransfer(temp ? LFN.XFER_NOT : LFN.XFER_MANDATORY);
                } else if (name.equals("temporaryHint")) {
                    this.log(e, name, value);
                    fn.setTemporary(value);
                } else if (name.equals("varname")) {
                    this.log(e, name, value);
                    fn.setVariable(value);
                } else if (name.equals("type")) {
                    this.log(e, name, value);
                    fn.setType(LFN.typeInt(value));
                } else {
                    this.complain(e, name, value);
                }
            } // for

            if (e.equals("filename")) {
                switch (m_tag) {
                    case TAG_ADAG:
                        m_callback.cb_filename(fn);
                        break;
                    case TAG_PROFILE:
                        m_profile.addLeaf(fn);
                        break;
                    case TAG_ARGUMENT:
                        m_job.addArgument(fn);
                }
            } else {
                m_tag = TAG_OTHER;

                if (e.equals("stdin")) m_job.setStdin(fn);
                else if (e.equals("stdout")) m_job.setStdout(fn);
                else if (e.equals("stderr")) m_job.setStderr(fn);
                else if (e.equals("uses")) m_job.addUses(fn);
            }
            return;
        }

        if (e.equals("job")) {
            m_job = new Job();
            for (int i = 0; i < names.size(); ++i) {
                String name = (String) names.get(i);
                String value = (String) values.get(i);

                if (name.equals("name")) {
                    this.log(e, name, value);
                    m_job.setName(value);
                } else if (name.equals("level")) {
                    this.log(e, name, value);
                    m_job.setLevel(Integer.parseInt(value));
                } else if (name.equals("namespace")) {
                    this.log(e, name, value);
                    m_job.setNamespace(value);
                } else if (name.equals("version")) {
                    this.log(e, name, value);
                    m_job.setVersion(value);
                } else if (name.equals("compound")) {
                    this.log(e, name, value);
                    m_job.setChain(value);
                } else if (name.equals("id")) {
                    this.log(e, name, value);
                    m_job.setID(value);
                } else if (name.equals("dv-namespace")) {
                    this.log(e, name, value);
                    m_job.setDVNamespace(value);
                } else if (name.equals("dv-name")) {
                    this.log(e, name, value);
                    m_job.setDVName(value);
                } else if (name.equals("dv-version")) {
                    this.log(e, name, value);
                    m_job.setDVVersion(value);
                } else {
                    this.complain(e, name, value);
                }
            }
            return;
        }

        if (e.equals("child")) {
            this.m_parent = new ArrayList();
            for (int i = 0; i < names.size(); ++i) {
                String name = (String) names.get(i);
                String value = (String) values.get(i);

                if (name.equals("ref")) {
                    this.log(e, name, value);
                    m_child = value;
                } else {
                    this.complain(e, name, value);
                }
            }
            return;
        }

        if (e.equals("parent")) {
            String parent = null;
            for (int i = 0; i < names.size(); ++i) {
                String name = (String) names.get(i);
                String value = (String) values.get(i);

                if (name.equals("ref")) {
                    this.log(e, name, value);
                    parent = value;
                } else {
                    this.complain(e, name, value);
                }
            }
            if (parent != null) m_parent.add(parent);
            return;
        }

        if (e.equals("argument")) {
            m_tag = TAG_ARGUMENT;
            return;
        }

        if (e.equals("profile")) {
            m_profile = new Profile();
            m_tag = TAG_PROFILE;
            for (int i = 0; i < names.size(); ++i) {
                String name = (String) names.get(i);
                String value = (String) values.get(i);

                if (name.equals("namespace")) {
                    this.log(e, name, value);
                    m_profile.setNamespace(value);
                } else if (name.equals("key")) {
                    this.log(e, name, value);
                    m_profile.setKey(value);
                } else if (name.equals("origin")) {
                    this.log(e, name, value);
                    m_profile.setOrigin(value);
                } else {
                    this.complain(e, name, value);
                }
            }
            return;
        }

        // FIXME: shouldn't this be an exception?
        m_log.log("filler", 0, "Error: No rules defined for element " + e);
    }

    /**
     * This method sets the relations between the current java object and its parent object
     * according to the element hierarchy. Usually it involves adding the object to the parent's
     * child object list.
     */
    public void setElementRelation(String elementName) {
        switch (elementName.charAt(0)) {
            case 'a':
                if (elementName.equals("argument")) {
                    m_tag = TAG_OTHER;
                } else if (elementName.equals("adag")) {
                    m_callback.cb_done();
                }
                break;

            case 'c':
                if (elementName.equals("child")) {
                    m_callback.cb_parents(m_child, m_parent);
                }
                break;

            case 'j':
                if (elementName.equals("job")) {
                    m_tag = TAG_ADAG;
                    m_callback.cb_job(m_job);
                    m_log.log("filler", 3, "Adding job " + m_job.getID());
                }
                break;

            case 'p':
                if (elementName.equals("profile")) {
                    m_job.addProfile(m_profile);
                    m_tag = TAG_OTHER;
                }
                break;

            default:
                // m_log.log( "filler", 0, "Cannot guess parent for " + elementName );
                break;
        }
    }

    /**
     * This method sets the content of the java object corresponding to the element "text", which
     * has mixed content.
     *
     * @see org.griphyn.vdl.classes.Text
     */
    public void elementCharacters(String elementChars) {
        PseudoText text = new PseudoText(elementChars);

        switch (m_tag) {
            case TAG_PROFILE:
                m_profile.addLeaf(text);
                this.log("profile", "text", elementChars);
                break;
            case TAG_ARGUMENT:
                m_job.addArgument(text);
                this.log("argument", "text", elementChars);
        }
    }
}
