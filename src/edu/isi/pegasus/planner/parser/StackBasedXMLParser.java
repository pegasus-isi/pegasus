/*
 *
 *   Copyright 2007-2008 University Of Southern California
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing,
 *   software distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *
 */

package edu.isi.pegasus.planner.parser;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.classes.PegasusBag;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Stack;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;

/**
 * An abstract base class that XML Parsers can use if they use stack internally to store the
 * elements encountered while parsing XML documents using SAX
 *
 * @author Karan Vahi vahi@isi.edu
 * @version $Revision$
 */
public abstract class StackBasedXMLParser extends Parser {

    /** Count the depths of elements in the document */
    protected int mDepth;

    /** The stack of objects kept around. */
    protected Stack mStack;

    /** A boolean indicating that parsing is done. */
    protected boolean mParsingDone;

    /** A set of containing the unsupported element attributes */
    protected Set<String> mUnsupportedElementAttributes;

    /** The default Constructor. */
    /* public StackBasedXMLParser(  ) {
        this( PegasusProperties.nonSingletonInstance() );
    }*/

    /**
     * The overloaded constructor.
     *
     * @param bag the <code>PegasusBag</code> to be used.
     */
    public StackBasedXMLParser(PegasusBag bag) {
        super(bag);
        mStack = new Stack();
        mDepth = 0;
        mUnsupportedElementAttributes = new HashSet();
    }

    /**
     * Sets the list of external real locations where the XML schema may be found. Since this list
     * can be determined at run-time through properties etc., we expect this function to be called
     * between instantiating the parser, and using the parser
     */
    public void setSchemaLocations() {
        // setting the schema Locations
        String schemaLoc = getSchemaLocation();
        mLogger.log("Picking schema " + schemaLoc, LogManager.CONFIG_MESSAGE_LEVEL);
        String list = getSchemaNamespace() + " " + schemaLoc;
        setSchemaLocations(list);
    }

    /**
     * Composes the <code>SiteData</code> object corresponding to the element name in the XML
     * document.
     *
     * @param element the element name encountered while parsing.
     * @param names is a list of attribute names, as strings.
     * @param values is a list of attribute values, to match the key list.
     * @return the relevant SiteData object, else null if unable to construct.
     * @exception IllegalArgumentException if the element name is too short.
     */
    public abstract Object createObject(String element, List names, List values);

    /**
     * This method sets the relations between the currently finished XML element(child) and its
     * containing element in terms of Java objects. Usually it involves adding the object to the
     * parent's child object list.
     *
     * @param childElement name is the the child element name
     * @param parent is a reference to the parent's Java object
     * @param child is the completed child object to connect to the parent
     * @return true if the element was added successfully, false, if the child does not match into
     *     the parent.
     */
    public abstract boolean setElementRelation(String childElement, Object parent, Object child);

    /** */
    public void endDocument() {
        mParsingDone = true;
    }

    /**
     * This method defines the action to take when the parser begins to parse an element.
     *
     * @param namespaceURI is the URI of the namespace for the element
     * @param localName is the element name without namespace
     * @param qName is the element name as it appears in the docment
     * @param atts has the names and values of all the attributes
     */
    public void startElement(String namespaceURI, String localName, String qName, Attributes atts)
            throws SAXException {

        // one more element level
        mDepth++;

        List names = new java.util.ArrayList();
        List values = new java.util.ArrayList();
        for (int i = 0; i < atts.getLength(); ++i) {
            String name = atts.getLocalName(i);
            String value = atts.getValue(i);
            names.add(name);
            values.add(value);
        }

        // System.out.println( "QNAME " + qName + " NAME " + names + "\t Values" + values );

        Object object = createObject(qName, names, values);
        if (object != null) {
            mStack.push(new ParserStackElement(qName, object));
        } else {
            mLogger.log(
                    "Unknown element in xml :" + namespaceURI + ":" + localName + ":" + qName,
                    LogManager.ERROR_MESSAGE_LEVEL);

            throw new SAXException("Unknown or Empty element while parsing ");
        }
    }

    /**
     * The parser is at the end of an element. Triggers the association of the child elements with
     * the appropriate parent elements.
     *
     * @param namespaceURI is the URI of the namespace for the element
     * @param localName is the element name without namespace
     * @param qName is the element name as it appears in the docment
     */
    public void endElement(String namespaceURI, String localName, String qName)
            throws SAXException {

        // that's it for this level
        mDepth--;
        mLogger.log(
                "</"
                        + localName
                        + "> at "
                        + this.mLocator.getLineNumber()
                        + ":"
                        + mLocator.getColumnNumber(),
                LogManager.TRACE_MESSAGE_LEVEL);

        ParserStackElement tos = (ParserStackElement) mStack.pop();
        if (!qName.equals(tos.getElementName())) {
            String error = "Top of Stack " + tos.getElementName() + " does not mactch " + qName;
            mLogger.log(error, LogManager.FATAL_MESSAGE_LEVEL);
            throw new SAXException(error);
        }

        // add pieces to lower levels
        ParserStackElement peek = mStack.empty() ? null : (ParserStackElement) mStack.peek();

        if (!setElementRelation(
                tos.getElementName(),
                peek == null ? null : peek.getElementObject(),
                tos.getElementObject())) {

            String element = peek == null ? "root-element" : peek.getElementName();
            mLogger.log(
                    "Element " + tos.getElementName() + " does not fit into element " + element,
                    LogManager.ERROR_MESSAGE_LEVEL);
        }

        // reinitialize our cdata handler at end of each element
        mTextContent.setLength(0);
    }

    /**
     * @param element
     * @param attribute
     * @param value
     */
    public void log(String element, String attribute, String value) {
        // to be enabled when logging per queue.
        mLogger.log(
                "For element " + element + " found " + attribute + " -> " + value,
                LogManager.TRACE_MESSAGE_LEVEL);
    }

    /**
     * This is called when an attribute is encountered for an element that is invalid from the
     * schema context and is not supported.
     *
     * @param element the element name
     * @param attribute the attribute name
     * @param value the attribute value
     */
    public void complain(String element, String attribute, String value) {
        mLogger.log(
                "For element " + element + " invalid attribute found " + attribute + " -> " + value,
                LogManager.ERROR_MESSAGE_LEVEL);
    }

    /**
     * This is called when an attribute is encountered for an element that is valid in the schema
     * context but not supported right now.
     *
     * @param element the element name
     * @param attribute the attribute name
     * @param value the attribute value
     */
    public void attributeNotSupported(String element, String attribute, String value) {
        StringBuffer sb = new StringBuffer();
        sb.append("element").append(":").append("attribute").append("->").append(value);
        String key = sb.toString();

        if (!this.mUnsupportedElementAttributes.contains(key)) {
            mLogger.log(
                    "For element "
                            + element
                            + " attribute currently not supported "
                            + attribute
                            + " -> "
                            + value,
                    LogManager.WARNING_MESSAGE_LEVEL);
            this.mUnsupportedElementAttributes.add(key);
        }
    }

    /**
     * Called when certain element nesting is allowed in the XML schema but is not supported in the
     * code as yet.
     *
     * @param parent parent element
     * @param child child element
     */
    public void unSupportedNestingOfElements(String parent, String child) {
        StringBuffer sb = new StringBuffer();
        sb.append("Unsupported nesting for element ")
                .append(child)
                .append(" in parent element ")
                .append(parent);
        mLogger.log(sb.toString(), LogManager.WARNING_MESSAGE_LEVEL);
    }
}
