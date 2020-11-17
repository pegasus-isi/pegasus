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
package org.griphyn.vdl.parser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import org.griphyn.vdl.classes.*;

/**
 * Parses the input stream and generates pool configuration map as output.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 * @see VDLtScanner
 * @see VDLtToken
 */
public class VDLtParser {
    /** The access to the lexical scanner is stored here. */
    private VDLtScanner m_scanner = null;

    /** Stores the look-ahead symbol. */
    private VDLtToken m_lookAhead = null;

    /**
     * Initializes the parser with an input stream to read from.
     *
     * @param r is the stream opened for reading.
     */
    public VDLtParser(java.io.Reader r) throws IOException, VDLtException {
        m_scanner = new VDLtScanner(r);
        m_lookAhead = m_scanner.nextToken();
    }

    protected LFN lfn() throws IOException, VDLtException {
        if (!(m_lookAhead instanceof VDLtAt))
            throw new VDLtParserException(m_scanner, "An LFN must start with an at symbol");
        m_lookAhead = m_scanner.nextToken();

        if (!(m_lookAhead instanceof VDLtOpenBrace))
            throw new VDLtParserException(m_scanner, "An LFN has a brace after the at symbol");
        m_lookAhead = m_scanner.nextToken();

        if (!(m_lookAhead instanceof VDLtIdentifier))
            throw new VDLtParserException(
                    m_scanner, "An LFN uses a direction identifier as first component");

        int direction = -1;
        String id = ((VDLtIdentifier) m_lookAhead).getValue();
        if (id.compareTo("in") == 0 || id.compareTo("input") == 0) {
            direction = LFN.INPUT;
            m_lookAhead = m_scanner.nextToken();
        } else if (id.compareTo("out") == 0 || id.compareTo("output") == 0) {
            direction = LFN.OUTPUT;
            m_lookAhead = m_scanner.nextToken();
        } else if (id.compareTo("io") == 0 || id.compareTo("inout") == 0) {
            direction = LFN.INOUT;
            m_lookAhead = m_scanner.nextToken();
        } else if (id.compareTo("none") == 0) {
            direction = LFN.NONE;
            m_lookAhead = m_scanner.nextToken();
        } else {
            // no keyword given
            throw new VDLtParserException(m_scanner, "A LFN needs to specify a direction");
        }

        if (!(m_lookAhead instanceof VDLtColon))
            throw new VDLtParserException(
                    m_scanner, "An LFN separates the filename with a colon from the direction");
        m_lookAhead = m_scanner.nextToken();

        if (!(m_lookAhead instanceof VDLtQuotedString))
            throw new VDLtParserException(m_scanner, "The filename of an LFN is a quoted string");
        id = ((VDLtQuotedString) m_lookAhead).getValue();
        m_lookAhead = m_scanner.nextToken();

        // check for temporary hint
        String hint = null;
        if (m_lookAhead instanceof VDLtColon) {
            m_lookAhead = m_scanner.nextToken();
            if (!(m_lookAhead instanceof VDLtQuotedString))
                throw new VDLtParserException(
                        m_scanner, "The temporary template of an LFN is a quoted string");
            hint = ((VDLtQuotedString) m_lookAhead).getValue();
            m_lookAhead = m_scanner.nextToken();
        }

        // check for (possibly empty) special section
        String rt = null;
        if (m_lookAhead instanceof VDLtVBar) {
            m_lookAhead = m_scanner.nextToken();
            if (m_lookAhead instanceof VDLtIdentifier) {
                rt = ((VDLtIdentifier) m_lookAhead).getValue();
                m_lookAhead = m_scanner.nextToken();
            } else if (m_lookAhead instanceof VDLtCloseBrace) {
                // empty section
                rt = new String();
            } else {
                throw new VDLtParserException(
                        m_scanner, "The LFN \"" + id + "\" has an invalid rt specification");
            }
        }

        // check for correct termination
        if (!(m_lookAhead instanceof VDLtCloseBrace))
            throw new VDLtParserException(
                    m_scanner, "The LFN \"" + id + "\" is not terminated correctly");
        m_lookAhead = m_scanner.nextToken();

        if (rt == null) {
            // backward compatibility. The constructor does the "right thing":
            // if hint is null, the rt will be false
            // if hint is not null, the rt will both be true
            return new LFN(id, direction, hint);
        } else {
            // new format, just do the full kit
            boolean no_t = (rt.indexOf('t') == -1);
            boolean no_T = (rt.indexOf('T') == -1);

            return new LFN(
                    id,
                    direction,
                    hint,
                    // if "r" is present, do register
                    rt.indexOf('r') == -1,
                    // if "t" is present, do transfer
                    // if "T" is present, optional transfer
                    no_t ? (no_T ? LFN.XFER_NOT : LFN.XFER_OPTIONAL) : LFN.XFER_MANDATORY,
                    rt.indexOf('o') != -1);
        }
    }

    protected Use use() throws IOException, VDLtException {
        Use result = new Use();

        if ((m_lookAhead instanceof VDLtDollar)) {
            m_lookAhead = m_scanner.nextToken();

            if (!(m_lookAhead instanceof VDLtOpenBrace))
                throw new VDLtParserException(
                        m_scanner, "An bound var has a brace after the dollar");
            m_lookAhead = m_scanner.nextToken();

            if (m_lookAhead instanceof VDLtQuotedString) {
                // rendering supplied
                String temp = ((VDLtQuotedString) m_lookAhead).getValue();
                m_lookAhead = m_scanner.nextToken();
                if (m_lookAhead instanceof VDLtColon) {
                    // triple supplied
                    result.setPrefix(temp);
                    m_lookAhead = m_scanner.nextToken();

                    if (!(m_lookAhead instanceof VDLtQuotedString))
                        throw new VDLtParserException(
                                m_scanner,
                                "The rending information is \"prefix\":\"separator\":\"suffix\"");
                    result.setSeparator(((VDLtQuotedString) m_lookAhead).getValue());
                    m_lookAhead = m_scanner.nextToken();

                    if (!(m_lookAhead instanceof VDLtColon))
                        throw new VDLtParserException(
                                m_scanner,
                                "The rending information is \"prefix\":\"separator\":\"suffix\"");
                    m_lookAhead = m_scanner.nextToken();

                    if (!(m_lookAhead instanceof VDLtQuotedString))
                        throw new VDLtParserException(
                                m_scanner,
                                "The rending information is \"prefix\":\"separator\":\"suffix\"");
                    result.setSuffix(((VDLtQuotedString) m_lookAhead).getValue());
                    m_lookAhead = m_scanner.nextToken();

                    if (!(m_lookAhead instanceof VDLtVBar))
                        throw new VDLtParserException(
                                m_scanner,
                                "The rending information is separated by a bar from the bound variable");
                    m_lookAhead = m_scanner.nextToken();

                } else if (m_lookAhead instanceof VDLtVBar) {
                    // just a separator
                    result.setSeparator(temp);
                    m_lookAhead = m_scanner.nextToken();
                } else
                    throw new VDLtParserException(
                            m_scanner, "The rending information for the bound variable is wrong");
            }

            if (!(m_lookAhead instanceof VDLtIdentifier))
                throw new VDLtParserException(
                        m_scanner, "An bound var has an optional direction or the identifier");

            String id = ((VDLtIdentifier) m_lookAhead).getValue();
            if (id.compareTo("in") == 0 || id.compareTo("input") == 0) {
                result.setLink(LFN.INPUT);
                m_lookAhead = m_scanner.nextToken();
            } else if (id.compareTo("out") == 0 || id.compareTo("output") == 0) {
                result.setLink(LFN.OUTPUT);
                m_lookAhead = m_scanner.nextToken();
            } else if (id.compareTo("io") == 0 || id.compareTo("inout") == 0) {
                result.setLink(LFN.INOUT);
                m_lookAhead = m_scanner.nextToken();
            } else if (id.compareTo("none") == 0) {
                result.setLink(LFN.NONE);
                m_lookAhead = m_scanner.nextToken();
            }

            if (result.getLink() != -1) {
                if (!(m_lookAhead instanceof VDLtColon))
                    throw new VDLtParserException(
                            m_scanner, "A colon separates the direction from the identifier");
                m_lookAhead = m_scanner.nextToken();
            }

            if (!(m_lookAhead instanceof VDLtIdentifier))
                throw new VDLtParserException(
                        m_scanner, "The bound variable must be named as identifier");
            result.setName(((VDLtIdentifier) m_lookAhead).getValue());
            m_lookAhead = m_scanner.nextToken();

            if (!(m_lookAhead instanceof VDLtCloseBrace))
                throw new VDLtParserException(
                        m_scanner,
                        "The bound var "
                                + result.getName()
                                + " is not terminated by a closing brace");
            m_lookAhead = m_scanner.nextToken();

        } else if ((m_lookAhead instanceof VDLtOpenParenthesis)) {
            //
            // abbreviated form 1, using a type-cast: (inout) var
            //
            m_lookAhead = m_scanner.nextToken();
            if (!(m_lookAhead instanceof VDLtIdentifier))
                throw new VDLtParserException(
                        m_scanner, "An bound var has an optional direction or the identifier");

            String id = ((VDLtIdentifier) m_lookAhead).getValue();
            if (id.compareTo("in") == 0 || id.compareTo("input") == 0) {
                result.setLink(LFN.INPUT);
                m_lookAhead = m_scanner.nextToken();
            } else if (id.compareTo("out") == 0 || id.compareTo("output") == 0) {
                result.setLink(LFN.OUTPUT);
                m_lookAhead = m_scanner.nextToken();
            } else if (id.compareTo("io") == 0 || id.compareTo("inout") == 0) {
                result.setLink(LFN.INOUT);
                m_lookAhead = m_scanner.nextToken();
            } else if (id.compareTo("none") == 0) {
                result.setLink(LFN.NONE);
                m_lookAhead = m_scanner.nextToken();
            }

            if (result.getLink() != -1) {
                if (!(m_lookAhead instanceof VDLtCloseParenthesis))
                    throw new VDLtParserException(
                            m_scanner, "A closeing parenthesis finished the type-cast");
                m_lookAhead = m_scanner.nextToken();
            }

            if (!(m_lookAhead instanceof VDLtIdentifier))
                throw new VDLtParserException(
                        m_scanner, "The bound variable must be named as identifier");
            result.setName(((VDLtIdentifier) m_lookAhead).getValue());
            m_lookAhead = m_scanner.nextToken();

        } else if ((m_lookAhead instanceof VDLtIdentifier)) {
            //
            // abbreviated form 2: justvar
            //
            result.setName(((VDLtIdentifier) m_lookAhead).getValue());
            m_lookAhead = m_scanner.nextToken();
        } else {
            throw new VDLtParserException(
                    m_scanner, "An bound variable starts with a dollar, type-cast or identifier");
        }

        return result;
    }

    protected Text text() throws IOException, VDLtException {
        if (m_lookAhead instanceof VDLtQuotedString) {
            Text result = new Text(((VDLtQuotedString) m_lookAhead).getValue());
            m_lookAhead = m_scanner.nextToken();
            return result;
        } else throw new VDLtParserException(m_scanner, "expecting a quoted string");
    }

    protected Leaf tr_leaf() throws IOException, VDLtException {
        if (m_lookAhead instanceof VDLtQuotedString) return text();
        else if ((m_lookAhead instanceof VDLtDollar)
                || (m_lookAhead instanceof VDLtOpenParenthesis)
                || (m_lookAhead instanceof VDLtIdentifier)) return use();
        else
            throw new VDLtParserException(
                    m_scanner, "the value is neither a quoted string nor a valid bound variable");
    }

    protected Scalar dv_leaf() throws IOException, VDLtException {
        if (m_lookAhead instanceof VDLtQuotedString) return new Scalar(text());
        else if (m_lookAhead instanceof VDLtAt) return new Scalar(lfn());
        else
            throw new VDLtParserException(
                    m_scanner, "the value is neither a quoted string nor a valid LFN");
    }

    /**
     * internal function to parse a profile inside a TR body.
     *
     * @return a memory representation of a profile instance.
     * @exception IOException if the reading from the stream fails,
     * @exception VDLtParserException if the parser detects a syntax error,
     * @exception VDLtScannerException if the scanner detects a lexical error.
     */
    protected Profile profile() throws IOException, VDLtException {
        if (!(m_lookAhead instanceof VDLtIdentifier)
                && ((VDLtIdentifier) m_lookAhead).getValue().toLowerCase().compareTo("profile")
                        != 0)
            throw new VDLtParserException(m_scanner, "Expecting keyword \"profile\"");
        m_lookAhead = m_scanner.nextToken();

        if (!(m_lookAhead instanceof VDLtIdentifier))
            throw new VDLtParserException(m_scanner, "The profile needs a profile namespace first");
        String namespace = ((VDLtIdentifier) m_lookAhead).getValue();
        m_lookAhead = m_scanner.nextToken();

        String name = null;
        int p1 = namespace.indexOf('.');
        int p2 = namespace.indexOf("..");
        if (p1 >= 0 && p1 < namespace.length()) {
            // read as one token what are three
            //      Logging.instance().log( "default", 0, "Line " + m_scanner.getLineNumber() +
            //			      ": Please use :: instead of . in profile " + namespace );
            name = namespace.substring(p1 + 1);
            namespace = namespace.substring(0, p1).toLowerCase();

        } else if (p2 >= 0 && p2 < namespace.length()) {
            // read as one token what are three
            name = namespace.substring(p2 + 2);
            namespace = namespace.substring(0, p2).toLowerCase();

        } else {
            namespace = namespace.toLowerCase();

            // read remaining tokens
            if (!(m_lookAhead instanceof VDLtPeriod || m_lookAhead instanceof VDLtDoubleColon))
                throw new VDLtParserException(
                        m_scanner,
                        "The profile namespace is separated by a . or :: from the profile key");
            //      if ( m_lookAhead instanceof VDLtPeriod )
            //	Logging.instance().log( "default", 0, "Line " + m_scanner.getLineNumber() +
            //				": Please use :: instead of . in profile" );
            m_lookAhead = m_scanner.nextToken();

            if (!(m_lookAhead instanceof VDLtIdentifier))
                throw new VDLtParserException(m_scanner, "The profile requires a name (key)");
            name = ((VDLtIdentifier) m_lookAhead).getValue();
            m_lookAhead = m_scanner.nextToken();
        }

        if (!(m_lookAhead instanceof VDLtEquals))
            throw new VDLtParserException(
                    m_scanner, "The profile value is separated by an equals from the key");
        m_lookAhead = m_scanner.nextToken();

        Collection children = new ArrayList();
        do {
            children.add(tr_leaf());
        } while (!(m_lookAhead instanceof VDLtSemicolon));

        return new Profile(namespace, name, children);
    }

    /**
     * internal function to parse a <code>argument</code> line.
     *
     * @return a memory representation of an TR argument.
     * @exception IOException if the reading from the stream fails,
     * @exception VDLtParserException if the parser detects a syntax error,
     * @exception VDLtScannerException if the scanner detects a lexical error.
     */
    protected Argument argument() throws IOException, VDLtException {
        Argument result = new Argument();
        if (!(m_lookAhead instanceof VDLtIdentifier)
                && ((VDLtIdentifier) m_lookAhead).getValue().toLowerCase().compareTo("argument")
                        != 0)
            throw new VDLtParserException(m_scanner, "Expecting keyword \"argument\"");
        m_lookAhead = m_scanner.nextToken();

        if ((m_lookAhead instanceof VDLtIdentifier)) {
            // optional identifier, only used for streams
            result.setName(((VDLtIdentifier) m_lookAhead).getValue());
            m_lookAhead = m_scanner.nextToken();
        }

        if (!(m_lookAhead instanceof VDLtEquals))
            throw new VDLtParserException(
                    m_scanner, "The argument value is separated by an equals from the argument");
        m_lookAhead = m_scanner.nextToken();

        do {
            result.addLeaf(tr_leaf());
        } while (!(m_lookAhead instanceof VDLtSemicolon));

        return result;
    }

    /**
     * internal function to parse a call inside a compound TR.
     *
     * @return an actual argument list as used by the <code>call</code> statement.
     * @exception IOException if the reading from the stream fails,
     * @exception VDLtParserException if the parser detects a syntax error,
     * @exception VDLtScannerException if the scanner detects a lexical error.
     */
    protected Pass carg() throws IOException, VDLtException {
        if (!(m_lookAhead instanceof VDLtIdentifier))
            throw new VDLtParserException(m_scanner, "Expecting an identifier in call arguments");
        Pass result = new Pass(((VDLtIdentifier) m_lookAhead).getValue());
        m_lookAhead = m_scanner.nextToken();

        if (!(m_lookAhead instanceof VDLtEquals))
            throw new VDLtParserException(
                    m_scanner, "Missing equals sign after call arg identifier");
        m_lookAhead = m_scanner.nextToken();

        if (m_lookAhead instanceof VDLtOpenBracket) {
            // list value
            m_lookAhead = m_scanner.nextToken();

            List list = new List();
            while (!(m_lookAhead instanceof VDLtCloseBracket)) {
                list.addScalar(new Scalar(tr_leaf()));
                if (m_lookAhead instanceof VDLtComma) m_lookAhead = m_scanner.nextToken();
            }
            // reached only for closing bracket
            m_lookAhead = m_scanner.nextToken();
            result.setValue(list);
        } else {
            // scalar value
            result.setValue(new Scalar(tr_leaf()));
        }

        return result;
    }

    /**
     * internal function to parse a call inside a compound TR.
     *
     * @return a memory representation of a call instance.
     * @exception IOException if the reading from the stream fails,
     * @exception VDLtParserException if the parser detects a syntax error,
     * @exception VDLtScannerException if the scanner detects a lexical error.
     */
    protected Call call() throws IOException, VDLtException {
        if (!(m_lookAhead instanceof VDLtIdentifier)
                && ((VDLtIdentifier) m_lookAhead).getValue().toLowerCase().compareTo("call") != 0)
            throw new VDLtParserException(m_scanner, "Expecting keyword \"call\"");
        m_lookAhead = m_scanner.nextToken();

        VDLtFQDN tr = trmap();
        Call result = new Call(tr.getValue(1), tr.getValue(2), tr.getValue(3));
        result.setUsesspace(tr.getValue(0));

        //
        // actual call argument list
        //
        if (!(m_lookAhead instanceof VDLtOpenParenthesis))
            throw new VDLtParserException(
                    m_scanner, "expecting an open parenthesis after the call mapping");
        m_lookAhead = m_scanner.nextToken();

        while (!(m_lookAhead instanceof VDLtCloseParenthesis)) {
            result.addPass(carg());
            if (m_lookAhead instanceof VDLtComma) {
                m_lookAhead = m_scanner.nextToken();
                if (!(m_lookAhead instanceof VDLtIdentifier))
                    throw new VDLtParserException(
                            m_scanner, "expecting more arguments after the comma");
            }
        }
        // reach this only with a closing parenthesis
        m_lookAhead = m_scanner.nextToken();

        return result;
    }

    /**
     * internal function to parse formal arguments employed by a TR.
     *
     * @return a single formal argument
     * @exception IOException if the reading from the stream fails,
     * @exception VDLtParserException if the parser detects a syntax error,
     * @exception VDLtScannerException if the scanner detects a lexical error.
     */
    protected Declare farg() throws IOException, VDLtException {
        if (!(m_lookAhead instanceof VDLtIdentifier))
            throw new VDLtParserException(m_scanner, "expecting a direction or an identifier");

        int direction = -1;
        String id = ((VDLtIdentifier) m_lookAhead).getValue().toLowerCase();
        if (id.compareTo("in") == 0 || id.compareTo("input") == 0) {
            direction = LFN.INPUT;
            m_lookAhead = m_scanner.nextToken();
        } else if (id.compareTo("out") == 0 || id.compareTo("output") == 0) {
            direction = LFN.OUTPUT;
            m_lookAhead = m_scanner.nextToken();
        } else if (id.compareTo("io") == 0 || id.compareTo("inout") == 0) {
            direction = LFN.INOUT;
            m_lookAhead = m_scanner.nextToken();
        } else if (id.compareTo("none") == 0) {
            direction = LFN.NONE;
            m_lookAhead = m_scanner.nextToken();
        } else {
            // no keyword given, assume none
            direction = LFN.NONE;
        }

        if (m_lookAhead instanceof VDLtOpenBracket)
            // common error I frequently commit myself
            throw new VDLtParserException(m_scanner, "[] not permitted after type");
        else if (!(m_lookAhead instanceof VDLtIdentifier))
            // otherwise erraneous
            throw new VDLtParserException(m_scanner, "expecting an identifier");

        id = ((VDLtIdentifier) m_lookAhead).getValue();
        m_lookAhead = m_scanner.nextToken();

        int containerType = -1;
        if (m_lookAhead instanceof VDLtOpenBracket) {
            // list container
            m_lookAhead = m_scanner.nextToken();
            if (!(m_lookAhead instanceof VDLtCloseBracket))
                throw new VDLtParserException(
                        m_scanner, "expecting a closing bracket after the open bracket");
            else m_lookAhead = m_scanner.nextToken();
            containerType = Value.LIST;
        } else {
            containerType = Value.SCALAR;
        }

        Declare result = new Declare(id, containerType, direction);
        if (m_lookAhead instanceof VDLtEquals) {
            // there is a default value associated with the variable
            m_lookAhead = m_scanner.nextToken();
            if (containerType == Value.SCALAR) {
                // scalar default
                result.setValue(dv_leaf());
            } else {
                // list default within brackets
                List list = new List();
                if (!(m_lookAhead instanceof VDLtOpenBracket))
                    throw new VDLtParserException(
                            m_scanner, "expecting an opening bracket for vector default values");
                m_lookAhead = m_scanner.nextToken();
                while (!(m_lookAhead instanceof VDLtCloseBracket)) {
                    list.addScalar(dv_leaf());
                    if (m_lookAhead instanceof VDLtComma) m_lookAhead = m_scanner.nextToken();
                }
                // reach this only with a closing bracket
                m_lookAhead = m_scanner.nextToken();
                result.setValue(list);
            }
        }
        return result;
    }

    /**
     * internal function to parse temporary variables employed n a TR.
     *
     * @return a single formal argument
     * @exception IOException if the reading from the stream fails,
     * @exception VDLtParserException if the parser detects a syntax error,
     * @exception VDLtScannerException if the scanner detects a lexical error.
     */
    protected Local targ() throws IOException, VDLtException {
        if (!(m_lookAhead instanceof VDLtIdentifier))
            throw new VDLtParserException(m_scanner, "expecting a direction or an identifier");

        int direction = -1;
        String id = ((VDLtIdentifier) m_lookAhead).getValue().toLowerCase();
        if (id.compareTo("in") == 0 || id.compareTo("input") == 0) {
            direction = LFN.INPUT;
            m_lookAhead = m_scanner.nextToken();
        } else if (id.compareTo("out") == 0 || id.compareTo("output") == 0) {
            direction = LFN.OUTPUT;
            m_lookAhead = m_scanner.nextToken();
        } else if (id.compareTo("io") == 0 || id.compareTo("inout") == 0) {
            direction = LFN.INOUT;
            m_lookAhead = m_scanner.nextToken();
        } else if (id.compareTo("none") == 0) {
            direction = LFN.NONE;
            m_lookAhead = m_scanner.nextToken();
        } else {
            // no keyword given, assume none
            direction = LFN.NONE;
        }

        if (!(m_lookAhead instanceof VDLtIdentifier))
            throw new VDLtParserException(m_scanner, "expecting an identifier");
        id = ((VDLtIdentifier) m_lookAhead).getValue();
        m_lookAhead = m_scanner.nextToken();

        int containerType = -1;
        if (m_lookAhead instanceof VDLtOpenBracket) {
            // list container
            m_lookAhead = m_scanner.nextToken();
            if (!(m_lookAhead instanceof VDLtCloseBracket))
                throw new VDLtParserException(
                        m_scanner, "expecting a closing bracket after the open bracket");
            else m_lookAhead = m_scanner.nextToken();
            containerType = Value.LIST;
        } else {
            containerType = Value.SCALAR;
        }

        // local variables must have a value to be bound at
        Local result = new Local(id, containerType, direction);
        if (!(m_lookAhead instanceof VDLtEquals))
            throw new VDLtParserException(m_scanner, "expecting an equal sign");

        m_lookAhead = m_scanner.nextToken();
        if (containerType == Value.SCALAR) {
            // scalar default
            result.setValue(dv_leaf());
        } else {
            // list default within brackets
            List list = new List();
            if (!(m_lookAhead instanceof VDLtOpenBracket))
                throw new VDLtParserException(
                        m_scanner, "expecting an opening bracket for vector default values");
            m_lookAhead = m_scanner.nextToken();
            while (!(m_lookAhead instanceof VDLtCloseBracket)) {
                list.addScalar(dv_leaf());
                if (m_lookAhead instanceof VDLtComma) m_lookAhead = m_scanner.nextToken();
            }
            // reach this only with a closing bracket
            m_lookAhead = m_scanner.nextToken();
            result.setValue(list);
        }
        return result;
    }

    /**
     * internal function to parse actual arguments employed by a DV.
     *
     * @return a single actual argument.
     * @exception IOException if the reading from the stream fails,
     * @exception VDLtParserException if the parser detects a syntax error,
     * @exception VDLtScannerException if the scanner detects a lexical error.
     */
    protected Pass aarg() throws IOException, VDLtException {
        if (!(m_lookAhead instanceof VDLtIdentifier))
            throw new VDLtParserException(m_scanner, "Expecting an identifier in actual arguments");
        Pass result = new Pass(((VDLtIdentifier) m_lookAhead).getValue());
        m_lookAhead = m_scanner.nextToken();

        if (!(m_lookAhead instanceof VDLtEquals))
            throw new VDLtParserException(
                    m_scanner, "Missing equals sign after actual arg identifier");
        m_lookAhead = m_scanner.nextToken();

        if (m_lookAhead instanceof VDLtOpenBracket) {
            // list value
            m_lookAhead = m_scanner.nextToken();

            List list = new List();
            while (!(m_lookAhead instanceof VDLtCloseBracket)) {
                list.addScalar(dv_leaf());
                if (m_lookAhead instanceof VDLtComma) m_lookAhead = m_scanner.nextToken();
            }
            // reached only for closing bracket
            m_lookAhead = m_scanner.nextToken();
            result.setValue(list);
        } else {
            // scalar value
            result.setValue(dv_leaf());
        }

        return result;
    }

    /**
     * internal function to parse the fully-qualified definition identifier into memory. This is the
     * name of a TR or DV.
     *
     * @return a parsed fully-qualified identifier.
     * @exception IOException if the reading from the stream fails,
     * @exception VDLtParserException if the parser detects a syntax error,
     * @exception VDLtScannerException if the scanner detects a lexical error.
     */
    protected VDLtFQDN fqdn() throws IOException, VDLtException {
        VDLtFQDN result = new VDLtFQDN();
        if (!(m_lookAhead instanceof VDLtIdentifier))
            throw new VDLtParserException(m_scanner, "A FQDN starts with an identifier");
        String temp = ((VDLtIdentifier) m_lookAhead).getValue();
        m_lookAhead = m_scanner.nextToken();

        if (m_lookAhead instanceof VDLtDoubleColon) {
            // first part was the namespace
            m_lookAhead = m_scanner.nextToken();
            result.setValue(0, temp);

            if (!(m_lookAhead instanceof VDLtIdentifier))
                throw new VDLtParserException(
                        m_scanner, "Expecting more identifiers after a double colon");
            temp = ((VDLtIdentifier) m_lookAhead).getValue();
            m_lookAhead = m_scanner.nextToken();
        }

        // set name (mandatory part)
        result.setValue(1, temp);

        if (m_lookAhead instanceof VDLtColon) {
            m_lookAhead = m_scanner.nextToken();
            if (!(m_lookAhead instanceof VDLtIdentifier))
                throw new VDLtParserException(m_scanner, "Expecting a version identifier");
            temp = ((VDLtIdentifier) m_lookAhead).getValue();
            m_lookAhead = m_scanner.nextToken();

            result.setValue(2, temp);
        }

        return result;
    }

    /**
     * internal function to parse the part after the arrow operator into memory. It is also used for
     * calls in compound transformations.
     *
     * <p>On popular demand, the syntax slightly changed to be more permissive with version maps.
     * The following short-cuts are permitted:
     *
     * @return a parsed mapping to a transformation.
     * @exception IOException if the reading from the stream fails,
     * @exception VDLtParserException if the parser detects a syntax error,
     * @exception VDLtScannerException if the scanner detects a lexical error.
     */
    protected VDLtFQDN trmap() throws IOException, VDLtException {
        VDLtFQDN result = new VDLtFQDN();
        if (!(m_lookAhead instanceof VDLtIdentifier))
            throw new VDLtParserException(m_scanner, "A TR mapping starts with an identifier");
        String temp = ((VDLtIdentifier) m_lookAhead).getValue();
        m_lookAhead = m_scanner.nextToken();

        if (m_lookAhead instanceof VDLtDoubleColon) {
            // first part was the namespace
            m_lookAhead = m_scanner.nextToken();
            result.setValue(0, temp);

            if (!(m_lookAhead instanceof VDLtIdentifier))
                throw new VDLtParserException(
                        m_scanner, "Expecting more identifiers after a double colon");
            temp = ((VDLtIdentifier) m_lookAhead).getValue();
            m_lookAhead = m_scanner.nextToken();
        }

        // set name (mandatory part)
        result.setValue(1, temp);

        //     :min,
        //     :,max
        //     :min,max
        // NEW :same
        //
        if (m_lookAhead instanceof VDLtColon) {
            // min and max versions as identifiers
            m_lookAhead = m_scanner.nextToken();
            if (m_lookAhead instanceof VDLtIdentifier) {
                // min version
                String minimum = ((VDLtIdentifier) m_lookAhead).getValue();
                result.setValue(2, minimum);
                m_lookAhead = m_scanner.nextToken();

                // NEW branch -- same version for both
                if (m_lookAhead instanceof VDLtOpenParenthesis) {
                    result.setValue(3, minimum);
                    return result;
                }
            }

            if (!(m_lookAhead instanceof VDLtComma))
                throw new VDLtParserException(
                        m_scanner, "Expecting a comma between min and max version");

            m_lookAhead = m_scanner.nextToken();
            if (m_lookAhead instanceof VDLtIdentifier) {
                // max version
                result.setValue(3, ((VDLtIdentifier) m_lookAhead).getValue());
                m_lookAhead = m_scanner.nextToken();
            } else if (!(m_lookAhead instanceof VDLtOpenParenthesis))
                throw new VDLtParserException(m_scanner, "Excepting a max version after the comma");
        }

        return result;
    }

    /**
     * internal function to parse a complete transformation.
     *
     * @return a derivation in memory
     * @exception IOException if the reading from the stream fails,
     * @exception VDLtParserException if the parser detects a syntax error,
     * @exception VDLtScannerException if the scanner detects a lexical error.
     */
    protected Derivation derivation() throws IOException, VDLtException {
        Derivation result = new Derivation();

        VDLtFQDN id = fqdn();
        result.setNamespace(id.getValue(0));
        result.setName(id.getValue(1));
        result.setVersion(id.getValue(2));

        if (!(m_lookAhead instanceof VDLtArrow))
            throw new VDLtParserException(m_scanner, "Expecting the map operator (arrow)");
        m_lookAhead = m_scanner.nextToken();

        id = trmap();
        result.setUsesspace(id.getValue(0));
        result.setUses(id.getValue(1));
        result.setMinIncludeVersion(id.getValue(2));
        result.setMaxIncludeVersion(id.getValue(3));

        //
        // actual argument list
        //
        if (!(m_lookAhead instanceof VDLtOpenParenthesis))
            throw new VDLtParserException(
                    m_scanner, "expecting an open parenthesis to start actual argument list");
        m_lookAhead = m_scanner.nextToken();

        while (!(m_lookAhead instanceof VDLtCloseParenthesis)) {
            result.addPass(aarg());
            if (m_lookAhead instanceof VDLtComma) {
                m_lookAhead = m_scanner.nextToken();
                if (!(m_lookAhead instanceof VDLtIdentifier))
                    throw new VDLtParserException(
                            m_scanner, "expecting more arguments after the comma");
            }
        }
        // reach this only with a closing parenthesis
        m_lookAhead = m_scanner.nextToken();

        if (!(m_lookAhead instanceof VDLtSemicolon))
            throw new VDLtParserException(m_scanner, "expecting a semicolon to terminate a DV");
        m_lookAhead = m_scanner.nextToken();

        return result;
    }

    /**
     * internal function to parse a complete transformation.
     *
     * @return a transformation in memory
     * @exception IOException if the reading from the stream fails,
     * @exception VDLtParserException if the parser detects a syntax error,
     * @exception VDLtScannerException if the scanner detects a lexical error.
     */
    protected Transformation transformation() throws IOException, VDLtException {
        Transformation result = new Transformation();

        VDLtFQDN id = fqdn();
        result.setNamespace(id.getValue(0));
        result.setName(id.getValue(1));
        result.setVersion(id.getValue(2));

        //
        // formal argument list
        //
        if (!(m_lookAhead instanceof VDLtOpenParenthesis))
            throw new VDLtParserException(
                    m_scanner, "expecting an open parenthesis after the TR identifier");
        m_lookAhead = m_scanner.nextToken();

        while (!(m_lookAhead instanceof VDLtCloseParenthesis)) {
            result.addDeclare(farg());
            if (m_lookAhead instanceof VDLtComma) {
                m_lookAhead = m_scanner.nextToken();
                if (!(m_lookAhead instanceof VDLtIdentifier))
                    throw new VDLtParserException(
                            m_scanner, "expecting more formal arguments after the comma");
            }
        }
        // reach this only with a closing parenthesis
        m_lookAhead = m_scanner.nextToken();

        //
        // transformation body
        //
        if (!(m_lookAhead instanceof VDLtOpenBrace) && !(m_lookAhead instanceof VDLtSemicolon))
            throw new VDLtParserException(m_scanner, "expecting the TR body");

        if (m_lookAhead instanceof VDLtOpenBrace) {
            // regular transformation body, skip brace
            m_lookAhead = m_scanner.nextToken();

            while (!(m_lookAhead instanceof VDLtCloseBrace)) {
                if (!(m_lookAhead instanceof VDLtIdentifier))
                    throw new VDLtParserException(
                            m_scanner,
                            "expecting \"profile\", \"call\", \"argument\", or "
                                    + "a temporary variable declaration inside TR body");
                String var = ((VDLtIdentifier) m_lookAhead).getValue().toLowerCase();
                if (var.compareTo("argument") == 0) {
                    result.addArgument(argument());
                } else if (var.compareTo("call") == 0) {
                    result.addCall(call());
                } else if (var.compareTo("profile") == 0) {
                    result.addProfile(profile());
                } else {
                    //	  throw new VDLtParserException( m_scanner,
                    //		"\"" + var + "\" is not a valid keyword for a TR body" );
                    result.addLocal(targ());
                }

                if (m_lookAhead instanceof VDLtSemicolon) m_lookAhead = m_scanner.nextToken();
            }
            // reach this only with a closing brace
            m_lookAhead = m_scanner.nextToken();
        } else {
            // transformation without a body, skip semicolon
            m_lookAhead = m_scanner.nextToken();
        }

        return result;
    }

    /**
     * Parses the a single definition from the input stream and returns just the definition. The
     * piece-by-piece parsing allows for a more memory-efficient parsing process of large input
     * streams.
     *
     * @return a Definition structure for one TR or DV
     * @exception IOException if the reading from the stream fails,
     * @exception VDLtParserException if the parser detects a syntax error,
     * @exception VDLtScannerException if the scanner detects a lexical error.
     * @see #parse()
     * @see #hasMoreTokens()
     */
    public Definition parseDefinition() throws IOException, VDLtException {
        if (!(m_lookAhead instanceof VDLtDefinition))
            throw new VDLtParserException(m_scanner, "expecting DV or TR");

        if (m_lookAhead instanceof VDLtDerivation) {
            // DV will be more frequently encountered, thus check first
            m_lookAhead = m_scanner.nextToken();
            return derivation();
        } else if (m_lookAhead instanceof VDLtTransformation) {
            // TR will not be as often
            m_lookAhead = m_scanner.nextToken();
            return transformation();
        } else {
            // this should not happen
            throw new VDLtParserException(m_scanner, "unknown definition");
        }
    }

    /**
     * Predicate to determine, if there are more Definition instances to be read.
     *
     * @return true, if there are potentially more tokens in the stream.
     * @exception IOException if the reading from the stream fails,
     * @exception VDLtParserException if the parser detects a syntax error,
     * @exception VDLtScannerException if the scanner detects a lexical error.
     */
    public boolean hasMoreTokens() throws IOException, VDLtException {
        return m_scanner.hasMoreTokens();
    }

    /**
     * Parses the complete input stream. This method will construct the complete stream as
     * Definitions in memory. It will gobble a lot of memory for large input.
     *
     * @return the Definitions in one structure
     * @exception IOException if the reading from the stream fails,
     * @exception VDLtParserException if the parser detects a syntax error,
     * @exception VDLtScannerException if the scanner detects a lexical error.
     * @see #parseDefinition()
     */
    public Definitions parse() throws IOException, VDLtException {
        Definitions result = new Definitions();

        do {
            if (m_lookAhead != null) result.addDefinition(parseDefinition());
        } while (m_scanner.hasMoreTokens());

        return result;
    }
}
