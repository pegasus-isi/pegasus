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
import java.io.LineNumberReader;

/**
 * Implements the scanner for reserved words and other tokens that are generated from the input
 * stream. This class is module-local on purpose.
 *
 * @author Jens-S. Vöckler
 * @version $Revision$
 */
class VDLtScanner {
    /** stores the stream from which we are currently scanning. */
    private LineNumberReader m_in;

    /** captures the look-ahead character; */
    private int m_lookAhead;

    /** Starts to scan the given stream. */
    public VDLtScanner(java.io.Reader reader) throws IOException {
        this.m_in = new LineNumberReader(reader);
        this.m_in.setLineNumber(1);
        this.m_lookAhead = m_in.read();
        // skipWhitespace();
    }

    /**
     * Obtains the current line number in the input stream from the outside.
     *
     * @return the current line number.
     */
    public int getLineNumber() {
        return m_in.getLineNumber();
    }

    /**
     * Skips any white space and comments in the input. This method stops either at the end of file,
     * or at any non-whitespace input character.
     */
    private void skipWhitespace() throws IOException {
        // end of file?
        if (m_lookAhead == -1) return;

        // skip over whitespace
        while (m_lookAhead != -1 && Character.isWhitespace((char) m_lookAhead))
            m_lookAhead = m_in.read();

        // skip over comments until eoln
        if (m_lookAhead == '#') {
            m_in.readLine();
            m_lookAhead = m_in.read();
            skipWhitespace(); // FIXME: reformulate end-recursion into loop
        }
    }

    /**
     * Checks for the availability of more input.
     *
     * @return true, if there is more to read, false for EOF.
     */
    public boolean hasMoreTokens() throws IOException {
        skipWhitespace();
        return (this.m_lookAhead != -1);
    }

    /**
     * Obtains the next token from the input stream.
     *
     * @return an instance conforming to the token interface, or null for eof.
     * @throws IOException if something went wrong while reading
     * @throws VDLtScannerException if a lexical error was encountered.
     */
    public VDLtToken nextToken() throws IOException, VDLtScannerException {
        // sanity check
        skipWhitespace();
        if (m_lookAhead == -1) return null;

        switch (m_lookAhead) {
            case '$':
                m_lookAhead = m_in.read();
                if (m_lookAhead == -1 || Character.isWhitespace((char) m_lookAhead))
                    throw new VDLtScannerException(m_in, "no whitespace allowed after dollar");
                else return new VDLtDollar();

            case ',':
                m_lookAhead = m_in.read();
                skipWhitespace();
                return new VDLtComma();

            case '|':
                m_lookAhead = m_in.read();
                skipWhitespace();
                return new VDLtVBar();

            case '.':
                m_lookAhead = m_in.read();
                if (m_lookAhead == -1 || Character.isWhitespace((char) m_lookAhead))
                    throw new VDLtScannerException(m_in, "no whitespace allowed after period");
                else return new VDLtPeriod();

            case '@':
                m_lookAhead = m_in.read();
                if (m_lookAhead == -1 || Character.isWhitespace((char) m_lookAhead))
                    throw new VDLtScannerException(m_in, "no whitespace allowed after at");
                else return new VDLtAt();

            case '-':
                m_lookAhead = m_in.read();
                if (m_lookAhead == '>') {
                    m_lookAhead = m_in.read();
                    skipWhitespace();
                    return new VDLtArrow();
                } else {
                    throw new VDLtScannerException(m_in, "a sole hyphen is not permitted");
                }

            case '=':
                m_lookAhead = m_in.read();
                skipWhitespace();
                return new VDLtEquals();

            case ';':
                m_lookAhead = m_in.read();
                skipWhitespace();
                return new VDLtSemicolon();

            case '(':
                m_lookAhead = m_in.read();
                skipWhitespace();
                return new VDLtOpenParenthesis();

            case ')':
                m_lookAhead = m_in.read();
                skipWhitespace();
                return new VDLtCloseParenthesis();

            case '{':
                m_lookAhead = m_in.read();
                skipWhitespace();
                return new VDLtOpenBrace();

            case '}':
                m_lookAhead = m_in.read();
                skipWhitespace();
                return new VDLtCloseBrace();

            case '[':
                m_lookAhead = m_in.read();
                skipWhitespace();
                return new VDLtOpenBracket();

            case ']':
                m_lookAhead = m_in.read();
                skipWhitespace();
                return new VDLtCloseBracket();

            case ':':
                m_lookAhead = m_in.read();
                if (m_lookAhead == ':') {
                    m_lookAhead = m_in.read();
                    if (m_lookAhead == -1 || Character.isWhitespace((char) m_lookAhead)) {
                        throw new VDLtScannerException(
                                m_in, "no whitespace allowed after double colon");
                    } else {
                        return new VDLtDoubleColon();
                    }
                } else if (m_lookAhead == -1 || Character.isWhitespace((char) m_lookAhead)) {
                    throw new VDLtScannerException(m_in, "no whitespace allowed after colon");
                } else {
                    return new VDLtColon();
                }

            case '"':
                // parse a quoted string
                StringBuffer result = new StringBuffer(16);

                do {
                    m_lookAhead = m_in.read();
                    if (m_lookAhead == -1 || m_lookAhead == '\r' || m_lookAhead == '\n') {
                        // eof is an unterminated string
                        throw new VDLtScannerException(m_in, "unterminated quoted string");
                    } else if (m_lookAhead == '\\') {
                        int temp = m_in.read();
                        if (temp == -1)
                            throw new VDLtScannerException(
                                    m_in, "unterminated escape in quoted string");
                        else
                            result.append(
                                    (char) temp); // always add whatever is after the backslash
                    } else if (m_lookAhead != '"') {
                        result.append((char) m_lookAhead);
                    }
                } while (m_lookAhead != '"');

                // skip over final quote
                m_lookAhead = m_in.read();
                skipWhitespace();
                return new VDLtQuotedString(result.toString());

            default:
                // are we parsing a reserved word or identifier
                if (Character.isLetterOrDigit((char) m_lookAhead)
                        || m_lookAhead == '_'
                        || m_lookAhead == '/') {
                    StringBuffer identifier = new StringBuffer(8);
                    identifier.append((char) m_lookAhead);
                    m_lookAhead = m_in.read();
                    while (m_lookAhead != -1
                            && (Character.isLetterOrDigit((char) m_lookAhead)
                                    || m_lookAhead == '_'
                                    || m_lookAhead == '-'
                                    || // <-- soon to be dropped !!!
                                    m_lookAhead == '/'
                                    || // <-- new for Mike
                                    m_lookAhead == '.')) {
                        if (m_lookAhead == '-') {
                            // terry kludge just for Jim, grumblftz
                            m_in.mark(2);
                            m_lookAhead = m_in.read();
                            if (m_lookAhead == '>') {
                                // this is part of the next token, reset stream
                                m_in.reset();
                                m_lookAhead = '-';
                                break;
                            } else {
                                identifier.append('-');
                            }
                        } else {
                            identifier.append((char) m_lookAhead);
                            m_lookAhead = m_in.read();
                        }
                    }

                    // done parsing identifier or reserved word
                    skipWhitespace();
                    String s = identifier.toString();
                    if (s.compareToIgnoreCase("tr") == 0)
                        // reserved word
                        return new VDLtTransformation();
                    else if (s.compareToIgnoreCase("dv") == 0)
                        // reserved word
                        return new VDLtDerivation();
                    else
                        // is a non-reserved identifier
                        return new VDLtIdentifier(s);

                } else {
                    // unknown material
                    throw new VDLtScannerException(m_in, "unknown character " + m_lookAhead);
                }
        } // switch
    }
}
