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

import java.util.HashMap;
import java.util.Map;
import java.util.Stack;
import org.griphyn.vdl.classes.*;
import org.griphyn.vdl.util.*;
import org.xml.sax.*;

/**
 * This class establishes the in-memory construction of Definition objects read, and does the
 * callback on the storage interface. This class is the content handler for the XML document being
 * parsed.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 * @see Definition
 * @see DefinitionHandler
 */
public class VDLContentHandler implements ContentHandler {
    /** This is the callback handler for ready Definitions. */
    private DefinitionHandler m_callback;

    /** This is the callback handler for a Definition outside Definitions. */
    private FinalizerHandler m_finalize;

    /** Keep the location within the document */
    private Locator m_location;

    /** A Hashmap to forward resolve namespaces that were encountered during parsing. */
    private Map m_forward;

    /** A Hashmap to reverse resolve namespaces that were encountered during parsing. */
    private Map m_reverse;

    /** The meta elements needs some very special handling (only bypassed). */
    private int m_metamode = 0;

    /** Count the depths of elements in the document */
    private int m_depth = 0;

    /** */
    private Stack m_stack;

    /** ctor. */
    public VDLContentHandler() {
        this.m_callback = null;
        this.m_finalize = null;
        this.m_forward = new HashMap();
        this.m_reverse = new HashMap();
    }

    /**
     * Accessor: This function allows a different DefinitionHandler to be set and used.
     *
     * @param ds is the new callback object to handle each Definition as it becomes ready.
     */
    public void setDefinitionHandler(DefinitionHandler ds) {
        this.m_callback = ds;
    }

    /**
     * Accessor: This function allows a different FinalizerHandler to be set and used. Note, this
     * cannot be used for the Definitions elements. In order to reduce the memory footprint, the
     * Definitions element will not be maintained!
     *
     * @param fh is the new callback object to handle a single top-level VDL element as it becomes
     *     ready.
     */
    public void setFinalizerHandler(FinalizerHandler fh) {
        this.m_finalize = fh;
    }

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
    public void startDocument() throws SAXException {
        this.m_depth = 0;
        this.m_stack = new Stack();
        Logging.instance().log("parser", 1, ">>> start of document >>>");
    }

    /**
     * This method specifies what to do when the parser reached the end of the document. In this
     * case, we simply print a message for debugging.
     */
    public void endDocument() throws SAXException {
        Logging.instance().log("parser", 1, "<<< end of document <<<");
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
        Logging.instance().log("parser", 2, "adding \"" + p + "\" <=> " + u);

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
        Logging.instance().log("parser", 2, "removed \"" + p + "\" <=> " + u);
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
        Logging.instance()
                .log(
                        "parser",
                        3,
                        "<"
                                + map(namespaceURI)
                                + localName
                                + "> at "
                                + m_location.getLineNumber()
                                + ":"
                                + m_location.getColumnNumber());
        m_depth++;

        // if in meta mode, skip filling in new elements
        if (this.m_metamode == 0) {
            java.util.List names = new java.util.ArrayList();
            java.util.List values = new java.util.ArrayList();
            for (int i = 0; i < atts.getLength(); ++i) {
                String name = new String(atts.getLocalName(i));
                String value = new String(atts.getValue(i));

                Logging.instance()
                        .log(
                                "parser",
                                2,
                                "attribute " + map(atts.getURI(i)) + name + "=\"" + value + "\"");
                names.add(name);
                values.add(value);
            }

            VDL object = createObject(qName, names, values);
            if (object != null) m_stack.push(new StackElement(qName, object));
            else throw new SAXException("empty element while parsing");
        }

        // check for start of meta mode
        if (localName.equals("meta") && namespaceURI.equals(Definitions.SCHEMA_NAMESPACE)) {
            // increase meta level
            this.m_metamode++;
        }
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
        m_depth--;
        Logging.instance()
                .log(
                        "parser",
                        3,
                        "</"
                                + map(namespaceURI)
                                + localName
                                + "> at "
                                + m_location.getLineNumber()
                                + ":"
                                + m_location.getColumnNumber());

        // check for end of meta mode
        if (localName.equals("meta") && namespaceURI.equals(Definitions.SCHEMA_NAMESPACE)) {
            // decrease meta level
            --this.m_metamode;
        }

        // if in meta mode, skip filling in new elements, as they won't
        // belong to VDLx (or they might, but should be ignored).
        if (this.m_metamode == 0) {
            StackElement tos = (StackElement) m_stack.pop();
            if (!qName.equals(tos.m_name)) {
                Logging.instance().log("default", 0, "assertion failure");
                System.exit(1);
            }

            if (!m_stack.empty()) {
                // add pieces to lower levels
                StackElement peek = (StackElement) m_stack.peek();
                if (!setElementRelation(peek.m_name.charAt(0), peek.m_obj, tos.m_obj))
                    Logging.instance()
                            .log(
                                    "parser",
                                    0,
                                    "Element "
                                            + tos.m_name
                                            + " does not fit into element "
                                            + peek.m_name);
            } else {
                // run finalizer, if available
                if (m_finalize != null) m_finalize.store(tos.m_obj);
            }
        }
    }

    /**
     * This method is the callback function for characters in an element. The element is expected to
     * be of mixed content.
     *
     * @param ch are the characters from the XML document
     * @param start is the start position into the array
     * @param length is the amount of valid data in the array
     */
    public void characters(char[] ch, int start, int length) throws SAXException {
        String message = new String(ch, start, length);
        if (message.length() > 0) {
            if (message.trim().length() == 0)
                Logging.instance().log("parser", 3, "Characters: \' \' x " + message.length());
            else Logging.instance().log("parser", 3, "Characters: \"" + message + "\"");
            if (this.m_metamode == 0) {
                // insert text into the only elements possible to carry text
                StackElement tos = (StackElement) m_stack.peek();
                if (tos.m_obj instanceof Text) {
                    Text text = (Text) tos.m_obj;
                    String old = text.getContent();
                    if (old == null) text.setContent(message);
                    else text.setContent(old + message);
                    this.log("Text", tos.m_name, message);
                } else if (tos.m_obj instanceof Meta) {
                    Meta meta = (Meta) tos.m_obj;
                    String content = message.trim();
                    meta.addContent(content);
                    this.log("Meta", tos.m_name, content);
                }
            }
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
        // not implemented
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
        Logging.instance()
                .log(
                        "parser",
                        2,
                        "processing instruction " + target + "=\"" + data + "\" was skipped!");
    }

    /**
     * Receive a notification that an entity was skipped. Currently, we are just printing a debug
     * message to this fact.
     *
     * @param name The name of the skipped entity. If it is a parameter entity, the name will begin
     *     with '%', and if it is the external DTD subset, it will be the string "[dtd]".
     */
    public void skippedEntity(java.lang.String name) throws SAXException {
        Logging.instance().log("parser", 2, "entity " + name + " was skipped!");
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
        Logging.instance().log("filler", 3, subject + "." + name + "=\"" + value + "\"");
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
        Logging.instance()
                .log("default", 0, "ignoring " + subject + '@' + name + "=\"" + value + '"', true);
    }

    /**
     * This method determines the actively parsed element, creates the Java object that corresponds
     * to the element, and sets the member variables with the values of the attributes of the
     * element.
     *
     * @param e is the name of the element
     * @param names is a list of attribute names, as strings.
     * @param values is a list of attribute values, to match the key list.
     * @return A new VDL Java object, which may only be partly constructed.
     * @exception IllegalArgumentException if the element name is too short.
     */
    protected VDL createObject(String e, java.util.List names, java.util.List values)
            throws IllegalArgumentException {
        if (e == null || e.length() < 1)
            throw new IllegalArgumentException("illegal element length");

        // postcondition: string has content w/ length > 0
        switch (e.charAt(0)) {
                //
                // A
                //
            case 'a':
                if (e.equals("argument")) {
                    Argument argument = new Argument();
                    for (int i = 0; i < names.size(); ++i) {
                        String name = (String) names.get(i);
                        String value = (String) values.get(i);

                        if (name.equals("name")) {
                            this.log(e, name, value);
                            argument.setName(value);
                        } else {
                            this.complain(e, name, value);
                        }
                    }
                    return argument;
                }
                // unknown
                return null;

                //
                // C
                //
            case 'c':
                if (e.equals("call")) {
                    Call call = new Call();
                    for (int i = 0; i < names.size(); ++i) {
                        String name = (String) names.get(i);
                        String value = (String) values.get(i);

                        if (name.equals("minIncludeVersion")) {
                            this.log(e, name, value);
                            call.setMinIncludeVersion(value);
                        } else if (name.equals("maxIncludeVersion")) {
                            this.log(e, name, value);
                            call.setMaxIncludeVersion(value);
                        } else if (name.equals("usesspace")) {
                            this.log(e, name, value);
                            call.setUsesspace(value);
                        } else if (name.equals("uses")) {
                            this.log(e, name, value);
                            call.setUses(value);
                        } else {
                            this.complain(e, name, value);
                        }
                    }
                    return call;
                }

                // unknown
                return null;

                //
                // D
                //
            case 'd':
                if (e.equals("declare")) {
                    Declare declaration = new Declare("", 0);
                    for (int i = 0; i < names.size(); ++i) {
                        String name = (String) names.get(i);
                        String value = (String) values.get(i);

                        if (name.equals("name")) {
                            this.log(e, name, value);
                            declaration.setName(value);
                        } else if (name.equals("container")) {
                            this.log(e, name, value);
                            declaration.setContainerType(VDLType.getContainerType(value));
                        } else if (name.equals("link")) {
                            this.log(e, name, value);
                            declaration.setLink(VDLType.getLinkType(value));
                        } else {
                            this.complain(e, name, value);
                        }
                    }

                    return declaration;

                } else if (e.equals("definitions")) {
                    Definitions definitions = new Definitions();
                    for (int i = 0; i < names.size(); ++i) {
                        String name = (String) names.get(i);
                        String value = (String) values.get(i);

                        if (name.equals("vdlns")) {
                            this.log(e, name, value);
                            definitions.setVdlns(value);
                        } else if (name.equals("version")) {
                            this.log(e, name, value);
                            definitions.setVersion(value);
                        } else if (name.equals("schemaLocation")) {
                            // ignore
                        } else {
                            this.complain(e, name, value);
                        }
                    }

                    return definitions;

                } else if (e.equals("derivation")) {
                    Derivation derivation = new Derivation();
                    for (int i = 0; i < names.size(); ++i) {
                        String name = (String) names.get(i);
                        String value = (String) values.get(i);

                        if (name.equals("minIncludeVersion")) {
                            this.log(e, name, value);
                            derivation.setMinIncludeVersion(value);
                        } else if (name.equals("maxIncludeVersion")) {
                            this.log(e, name, value);
                            derivation.setMaxIncludeVersion(value);
                        } else if (name.equals("name")) {
                            this.log(e, name, value);
                            derivation.setName(value);
                        } else if (name.equals("namespace")) {
                            this.log(e, name, value);
                            derivation.setNamespace(value);
                        } else if (name.equals("description")) {
                            this.log(e, name, value);
                            derivation.setDescription(value);
                        } else if (name.equals("keyword")) {
                            this.log(e, name, value);
                            derivation.setKeyword(value);
                        } else if (name.equals("title")) {
                            this.log(e, name, value);
                            derivation.setTitle(value);
                        } else if (name.equals("url")) {
                            this.log(e, name, value);
                            derivation.setUrl(value);
                        } else if (name.equals("usesspace")) {
                            this.log(e, name, value);
                            derivation.setUsesspace(value);
                        } else if (name.equals("uses")) {
                            this.log(e, name, value);
                            derivation.setUses(value);
                        } else if (name.equals("version")) {
                            this.log(e, name, value);
                            derivation.setVersion(value);
                        } else {
                            this.complain(e, name, value);
                        }
                    }
                    return derivation;
                }

                // unknown
                return null;

                //
                // L
                //
            case 'l':
                if (e.equals("lfn")) { // remove const-lfn from here
                    LFN lfn = new LFN();
                    for (int i = 0; i < names.size(); ++i) {
                        String name = (String) names.get(i);
                        String value = (String) values.get(i);

                        if (name.equals("file")) {
                            this.log(e, name, value);
                            lfn.setFilename(value);
                        } else if (name.equals("temporaryHint")) {
                            this.log(e, name, value);
                            lfn.setTemporary(value);
                        } else if (name.equals("dontRegister")) {
                            this.log(e, name, value);
                            lfn.setDontRegister(Boolean.valueOf(value).booleanValue());
                        } else if (name.equals("optional")) {
                            this.log(e, name, value);
                            lfn.setOptional(new Boolean(value).booleanValue());
                        } else if (name.equals("dontTransfer")) {
                            if (value.equals("false")) {
                                this.log(e, name, value);
                                lfn.setDontTransfer(LFN.XFER_MANDATORY);
                            } else if (value.equals("true")) {
                                this.log(e, name, value);
                                lfn.setDontTransfer(LFN.XFER_NOT);
                            } else if (value.equals("optional")) {
                                this.log(e, name, value);
                                lfn.setDontTransfer(LFN.XFER_OPTIONAL);
                            } else {
                                this.complain(e, name, value);
                            }
                        } else if (name.equals("isTemporary")) {
                            // deprecated work-around until phased out.
                            this.log(e, name, value);
                            // FIXME: check for null
                            Logging.instance()
                                    .log(
                                            "app",
                                            0,
                                            "using deprecated attribute \"isTemporary\" in LFN "
                                                    + lfn.getFilename());
                            boolean temp = Boolean.valueOf(value).booleanValue();
                            lfn.setDontRegister(temp);
                            lfn.setDontTransfer(temp ? LFN.XFER_NOT : LFN.XFER_MANDATORY);
                        } else if (name.equals("link")) {
                            this.log(e, name, value);
                            int link = VDLType.getLinkType(value);
                            lfn.setLink(link);
                        } else {
                            this.complain(e, name, value);
                        }
                    } // for
                    return lfn;

                } else if (e.equals("local")) {
                    Local temp = new Local("", 0);
                    for (int i = 0; i < names.size(); ++i) {
                        String name = (String) names.get(i);
                        String value = (String) values.get(i);

                        if (name.equals("name")) {
                            this.log(e, name, value);
                            temp.setName(value);
                        } else if (name.equals("container")) {
                            this.log(e, name, value);
                            temp.setContainerType(VDLType.getContainerType(value));
                        } else if (name.equals("link")) {
                            this.log(e, name, value);
                            temp.setLink(VDLType.getLinkType(value));
                        } else {
                            this.complain(e, name, value);
                        }
                    }
                    return temp;

                } else if (e.equals("list")) {
                    org.griphyn.vdl.classes.List list = new org.griphyn.vdl.classes.List();
                    for (int i = 0; i < names.size(); ++i) {
                        String name = (String) names.get(i);
                        String value = (String) values.get(i);
                        this.complain(e, name, value);
                    }
                    return list;
                }

                // unknown
                return null;

                //
                // M
                //
            case 'm':
                if (e.equals("meta")) {
                    Logging.instance().log("app", 2, "entering meta mode");
                    return new Meta();
                }

                // unknown
                return null;

                //
                // P
                //
            case 'p':
                if (e.equals("profile")) {
                    Profile prof = new Profile();
                    for (int i = 0; i < names.size(); ++i) {
                        String name = (String) names.get(i);
                        String value = (String) values.get(i);

                        if (name.equals("key")) {
                            this.log(e, name, value);
                            prof.setKey(value);
                        } else if (name.equals("namespace")) {
                            this.log(e, name, value);
                            prof.setNamespace(value);
                        } else {
                            this.complain(e, name, value);
                        }
                    }

                    return prof;

                } else if (e.equals("pass")) {
                    Pass pass = new Pass();
                    for (int i = 0; i < names.size(); ++i) {
                        String name = (String) names.get(i);
                        String value = (String) values.get(i);

                        if (name.equals("bind")) {
                            this.log(e, name, value);
                            pass.setBind(value);
                        } else {
                            this.complain(e, name, value);
                        }
                    }
                    return pass;
                }

                // unknown
                return null;

                //
                // S
                //
            case 's':
                if (e.equals("scalar")) {
                    Scalar scalar = new Scalar();
                    for (int i = 0; i < names.size(); ++i) {
                        String name = (String) names.get(i);
                        String value = (String) values.get(i);
                        this.complain(e, name, value);
                    }
                    return scalar;
                }

                // unknown
                return null;

                //
                // T
                //
            case 't':
                if (e.equals("text")) {
                    return new Text();
                } else if (e.equals("transformation")) {
                    Transformation trans = new Transformation();
                    for (int i = 0; i < names.size(); ++i) {
                        String name = (String) names.get(i);
                        String value = (String) values.get(i);

                        if (name.equals("version")) {
                            this.log(e, name, value);
                            trans.setVersion(value);
                        } else if (name.equals("name")) {
                            this.log(e, name, value);
                            trans.setName(value);
                        } else if (name.equals("namespace")) {
                            this.log(e, name, value);
                            trans.setNamespace(value);
                        } else if (name.equals("description")) {
                            this.log(e, name, value);
                            trans.setDescription(value);
                        } else if (name.equals("keyword")) {
                            this.log(e, name, value);
                            trans.setKeyword(value);
                        } else if (name.equals("title")) {
                            this.log(e, name, value);
                            trans.setTitle(value);
                        } else if (name.equals("url")) {
                            this.log(e, name, value);
                            trans.setUrl(value);
                        } else if (name.equals("version")) {
                            this.log(e, name, value);
                            trans.setVersion(value);
                        } else if (name.equals("argumentSeparator")) {
                            this.log(e, name, value);
                            trans.setArgumentSeparator(value);
                        } else {
                            this.complain(e, name, value);
                        }
                    }
                    return trans;
                }

                // unknown
                return null;

                //
                // U
                //
            case 'u':
                if (e.equals("use")) {
                    Use use = new Use();
                    for (int i = 0; i < names.size(); ++i) {
                        String name = (String) names.get(i);
                        String value = (String) values.get(i);

                        if (name.equals("name")) {
                            this.log(e, name, value);
                            use.setName(value);
                        } else if (name.equals("prefix")) {
                            this.log(e, name, value);
                            use.setPrefix(value);
                        } else if (name.equals("separator")) {
                            this.log(e, name, value);
                            use.setSeparator(value);
                        } else if (name.equals("suffix")) {
                            this.log(e, name, value);
                            use.setSuffix(value);
                        } else if (name.equals("link")) {
                            this.log(e, name, value);
                            use.setLink(VDLType.getLinkType(value));
                        } else {
                            this.complain(e, name, value);
                        }
                    }
                    return use;
                }

                // unknown
                return null;

            default:
                // FIXME: shouldn't this be an exception?
                Logging.instance().log("filler", 0, "Error: No rules defined for element " + e);
                return null;
        }
    }

    /**
     * This method sets the relations between the currently finished XML element and its containing
     * element in terms of Java objects. Usually it involves adding the object to the parent's child
     * object list.
     *
     * @param initial is the first charactor of the parent element name
     * @param parent is a reference to the parent's Java object
     * @param child is the completed child object to connect to the parent
     * @return true if the element was added successfully, false, if the child does not match into
     *     the parent.
     */
    protected boolean setElementRelation(char initial, VDL parent, VDL child) {
        switch (initial) {
                //
                // A
                //
            case 'a':
                if (parent instanceof Argument && child instanceof Leaf) {
                    // addLeaf is self-checking
                    ((Argument) parent).addLeaf((Leaf) child);
                    return true;
                }
                return false;

                //
                // C
                //
            case 'c':
                if (parent instanceof Call) {
                    Call c = (Call) parent;
                    if (child instanceof Pass) {
                        ((Call) parent).addPass((Pass) child);
                        return true;
                    } else if (child instanceof Meta) {
                        // dunno
                        return true;
                    }
                }
                return false;

                //
                // D
                //
            case 'd':
                if (parent instanceof Declare && child instanceof Value) {
                    // setValue is self-checking
                    ((Declare) parent).setValue((Value) child);
                    return true;

                } else if (parent instanceof Definitions && child instanceof Definition) {
                    // again, self-checking
                    ((Definitions) parent).addDefinition((Definition) child);

                    // This means we have finished parsing a TR or DV. Invoke callback!
                    if (m_callback != null) {
                        // Logging.instance().log( "app", 3, "invoking callback for " +
                        // ((Definition) child).shortID() );
                        m_callback.store((Definition) child);
                    }

                    // FIXME: decrease memory footprint!
                    ((Definitions) parent).removeAllDefinition();
                    return true;

                } else if (parent instanceof Derivation) {
                    Derivation d = (Derivation) parent;
                    if (child instanceof Pass) {
                        d.addPass((Pass) child);
                        return true;
                    } else if (child instanceof Meta) {
                        // dunno
                        return true;
                    }
                }

                // unknown
                return false;

                //
                // L
                //
            case 'l':
                if (parent instanceof org.griphyn.vdl.classes.List && child instanceof Scalar) {
                    ((org.griphyn.vdl.classes.List) parent).addScalar((Scalar) child);
                    return true;
                } else if (parent instanceof Local && child instanceof Value) {
                    // setValue is self-checking
                    ((Local) parent).setValue((Value) child);
                    return true;
                }

                // LFN is a *leaf* !!

                // unknown
                return false;

                //
                // M
                //
            case 'm':
                if (parent instanceof Meta) {
                    // dunno whatta do
                    return true;
                }

                // unknown
                return false;

                //
                // P
                //
            case 'p':
                if (parent instanceof Profile && child instanceof Leaf) {
                    ((Profile) parent).addLeaf((Leaf) child);
                    return true;

                } else if (parent instanceof Pass && child instanceof Value) {
                    ((Pass) parent).setValue((Value) child);
                    return true;
                }

                // unknown
                return false;

                //
                // S
                //
            case 's':
                if (parent instanceof Scalar && child instanceof Leaf) {
                    ((Scalar) parent).addLeaf((Leaf) child);
                    return true;
                }

                // unknown
                return false;

                //
                // T
                //
            case 't':
                if (parent instanceof Transformation) {
                    Transformation trans = (Transformation) parent;
                    if (child instanceof Declare) {
                        trans.addDeclare((Declare) child);
                        return true;
                    } else if (child instanceof Argument) {
                        trans.addArgument((Argument) child);
                        return true;
                    } else if (child instanceof Call) {
                        trans.addCall((Call) child);
                        return true;
                    } else if (child instanceof Local) {
                        trans.addLocal((Local) child);
                        return true;
                    } else if (child instanceof Profile) {
                        trans.addProfile((Profile) child);
                        return true;
                    } else if (child instanceof Meta) {
                        // dunno
                        return true;
                    }
                }
                // Text is a *leaf* !!
                return false;

                //
                // U
                //
            case 'u':
                // Use is a *leaf* !!
                return false;

            default:
                // FIXME: shouldn't this be an exception?
                Logging.instance().log("filler", 0, "Error: unable to join child to parent");
                return false;
        }
    }
}
