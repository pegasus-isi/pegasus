/*
 * Globus Toolkit Public License (GTPL)
 *
 * Copyright (c) 1999 University of Chicago and The University of
 * Southern California. All Rights Reserved.
 *
 *  1) The "Software", below, refers to the Globus Toolkit (in either
 *     source-code, or binary form and accompanying documentation) and a
 *     "work based on the Software" means a work based on either the
 *     Software, on part of the Software, or on any derivative work of
 *     the Software under copyright law: that is, a work containing all
 *     or a portion of the Software either verbatim or with
 *     modifications.  Each licensee is addressed as "you" or "Licensee."
 *
 *  2) The University of Southern California and the University of
 *     Chicago as Operator of Argonne National Laboratory are copyright
 *     holders in the Software.  The copyright holders and their third
 *     party licensors hereby grant Licensee a royalty-free nonexclusive
 *     license, subject to the limitations stated herein and
 *     U.S. Government license rights.
 *
 *  3) A copy or copies of the Software may be given to others, if you
 *     meet the following conditions:
 *
 *     a) Copies in source code must include the copyright notice and
 *        this license.
 *
 *     b) Copies in binary form must include the copyright notice and
 *        this license in the documentation and/or other materials
 *        provided with the copy.
 *
 *  4) All advertising materials, journal articles and documentation
 *     mentioning features derived from or use of the Software must
 *     display the following acknowledgement:
 *
 *     "This product includes software developed by and/or derived from
 *     the Globus project (http://www.globus.org/)."
 *
 *     In the event that the product being advertised includes an intact
 *     Globus distribution (with copyright and license included) then
 *     this clause is waived.
 *
 *  5) You are encouraged to package modifications to the Software
 *     separately, as patches to the Software.
 *
 *  6) You may make modifications to the Software, however, if you
 *     modify a copy or copies of the Software or any portion of it,
 *     thus forming a work based on the Software, and give a copy or
 *     copies of such work to others, either in source code or binary
 *     form, you must meet the following conditions:
 *
 *     a) The Software must carry prominent notices stating that you
 *        changed specified portions of the Software.
 *
 *     b) The Software must display the following acknowledgement:
 *
 *        "This product includes software developed by and/or derived
 *         from the Globus Project (http://www.globus.org/) to which the
 *         U.S. Government retains certain rights."
 *
 *  7) You may incorporate the Software or a modified version of the
 *     Software into a commercial product, if you meet the following
 *     conditions:
 *
 *     a) The commercial product or accompanying documentation must
 *        display the following acknowledgment:
 *
 *        "This product includes software developed by and/or derived
 *         from the Globus Project (http://www.globus.org/) to which the
 *         U.S. Government retains a paid-up, nonexclusive, irrevocable
 *         worldwide license to reproduce, prepare derivative works, and
 *         perform publicly and display publicly."
 *
 *     b) The user of the commercial product must be given the following
 *        notice:
 *
 *        "[Commercial product] was prepared, in part, as an account of
 *         work sponsored by an agency of the United States Government.
 *         Neither the United States, nor the University of Chicago, nor
 *         University of Southern California, nor any contributors to
 *         the Globus Project or Globus Toolkit nor any of their employees,
 *         makes any warranty express or implied, or assumes any legal
 *         liability or responsibility for the accuracy, completeness, or
 *         usefulness of any information, apparatus, product, or process
 *         disclosed, or represents that its use would not infringe
 *         privately owned rights.
 *
 *         IN NO EVENT WILL THE UNITED STATES, THE UNIVERSITY OF CHICAGO
 *         OR THE UNIVERSITY OF SOUTHERN CALIFORNIA OR ANY CONTRIBUTORS
 *         TO THE GLOBUS PROJECT OR GLOBUS TOOLKIT BE LIABLE FOR ANY
 *         DAMAGES, INCLUDING DIRECT, INCIDENTAL, SPECIAL, OR CONSEQUENTIAL
 *         DAMAGES RESULTING FROM EXERCISE OF THIS LICENSE AGREEMENT OR
 *         THE USE OF THE [COMMERCIAL PRODUCT]."
 *
 *  8) LICENSEE AGREES THAT THE EXPORT OF GOODS AND/OR TECHNICAL DATA
 *     FROM THE UNITED STATES MAY REQUIRE SOME FORM OF EXPORT CONTROL
 *     LICENSE FROM THE U.S. GOVERNMENT AND THAT FAILURE TO OBTAIN SUCH
 *     EXPORT CONTROL LICENSE MAY RESULT IN CRIMINAL LIABILITY UNDER U.S.
 *     LAWS.
 *
 *  9) Portions of the Software resulted from work developed under a
 *     U.S. Government contract and are subject to the following license:
 *     the Government is granted for itself and others acting on its
 *     behalf a paid-up, nonexclusive, irrevocable worldwide license in
 *     this computer software to reproduce, prepare derivative works, and
 *     perform publicly and display publicly.
 *
 * 10) The Software was prepared, in part, as an account of work
 *     sponsored by an agency of the United States Government.  Neither
 *     the United States, nor the University of Chicago, nor The
 *     University of Southern California, nor any contributors to the
 *     Globus Project or Globus Toolkit, nor any of their employees,
 *     makes any warranty express or implied, or assumes any legal
 *     liability or responsibility for the accuracy, completeness, or
 *     usefulness of any information, apparatus, product, or process
 *     disclosed, or represents that its use would not infringe privately
 *     owned rights.
 *
 * 11) IN NO EVENT WILL THE UNITED STATES, THE UNIVERSITY OF CHICAGO OR
 *     THE UNIVERSITY OF SOUTHERN CALIFORNIA OR ANY CONTRIBUTORS TO THE
 *     GLOBUS PROJECT OR GLOBUS TOOLKIT BE LIABLE FOR ANY DAMAGES,
 *     INCLUDING DIRECT, INCIDENTAL, SPECIAL, OR CONSEQUENTIAL DAMAGES
 *     RESULTING FROM EXERCISE OF THIS LICENSE AGREEMENT OR THE USE OF
 *     THE SOFTWARE.
 *
 *                               END OF LICENSE
 */
package org.griphyn.vdl.annotation;

import java.io.IOException;
import java.io.LineNumberReader;

/**
 * Implements the scanner for reserved words and other tokens that are generated from the input
 * stream. This class is module-local on purpose.
 *
 * @author Jens-S. Vöckler
 * @version $Revision$
 */
class QueryScanner {
    /** stores the stream from which we are currently scanning. */
    private LineNumberReader m_in;

    /** captures the look-ahead character; */
    private int m_lookAhead;

    /** Starts to scan the given stream. */
    public QueryScanner(java.io.Reader reader) throws IOException {
        this.m_in = new LineNumberReader(reader);
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
     * @throws QueryScannerException if a lexical error was encountered.
     */
    public String nextToken() throws IOException, QueryScannerException {
        // sanity check
        skipWhitespace();
        if (m_lookAhead == -1) return null;

        switch (m_lookAhead) {
            case '(':
                m_lookAhead = m_in.read();
                skipWhitespace();
                return "(";

            case ')':
                m_lookAhead = m_in.read();
                skipWhitespace();
                return ")";

            case '=':
                m_lookAhead = m_in.read();
                skipWhitespace();
                return "@EQ";

            case '>':
                m_lookAhead = m_in.read();
                if (m_lookAhead == '=') {
                    m_lookAhead = m_in.read();
                    skipWhitespace();
                    return "@GE";
                } else {
                    return "@GT";
                }

            case '<':
                m_lookAhead = m_in.read();
                if (m_lookAhead == '=') {
                    m_lookAhead = m_in.read();
                    skipWhitespace();
                    return "@GE";
                } else if (m_lookAhead == '>') {
                    m_lookAhead = m_in.read();
                    skipWhitespace();
                    return "@NE";
                } else {
                    return "@LT";
                }

            case '!':
                m_lookAhead = m_in.read();
                if (m_lookAhead == '=') {
                    m_lookAhead = m_in.read();
                    skipWhitespace();
                    return "@NE";
                } else {
                    // '!' alone is not allowed
                    throw new QueryScannerException(m_in, "found character '!' without '='");
                }

            case '\"':
            case '\'':
                int ch = m_lookAhead;
                // parse a quoted string
                StringBuffer result = new StringBuffer(16);

                do {
                    m_lookAhead = m_in.read();
                    if (m_lookAhead == -1 || m_lookAhead == '\r' || m_lookAhead == '\n') {
                        // eof is an unterminated string
                        throw new QueryScannerException(m_in, "unterminated quoted string");
                    } else if (m_lookAhead == '\\') {
                        m_lookAhead = m_in.read();
                        if (m_lookAhead == -1)
                            throw new QueryScannerException(
                                    m_in, "unterminated escape in quoted string");
                        else
                            result.append(
                                    m_lookAhead); // always add whatever is after the backslash
                    } else if (m_lookAhead != ch) {
                        result.append((char) m_lookAhead);
                    }
                } while (m_lookAhead != ch);

                // skip over final quote
                m_lookAhead = m_in.read();
                skipWhitespace();
                return ("$" + result.toString());

            default:
                // are we parsing a reserved word or identifier
                if (Character.isLetterOrDigit((char) m_lookAhead)
                        || m_lookAhead == '_'
                        || m_lookAhead == '-'
                        || m_lookAhead == '.') {
                    StringBuffer identifier = new StringBuffer(8);
                    identifier.append((char) m_lookAhead);
                    m_lookAhead = m_in.read();
                    while (m_lookAhead != -1
                            && (Character.isLetterOrDigit((char) m_lookAhead)
                                    || m_lookAhead == '_'
                                    || m_lookAhead == '-'
                                    || m_lookAhead == '.'
                                    || m_lookAhead == '('
                                    || m_lookAhead == ')')) {
                        identifier.append((char) m_lookAhead);
                        m_lookAhead = m_in.read();
                    }

                    // done parsing identifier or reserved word
                    skipWhitespace();
                    String s = identifier.toString();
                    if (s.compareToIgnoreCase("exists") == 0) return "@EX";
                    else if (s.compareToIgnoreCase("contains") == 0)
                        // reserved word
                        return "@CT";
                    else if (s.compareToIgnoreCase("like") == 0)
                        // reserved word
                        return "@LK";
                    else if (s.compareToIgnoreCase("between") == 0)
                        // reserved word
                        return "@BT";
                    else if (s.compareToIgnoreCase("and") == 0)
                        // reserved word
                        return "AND";
                    else if (s.compareToIgnoreCase("or") == 0)
                        // reserved word
                        return "OR";
                    else if (s.compareToIgnoreCase("not") == 0)
                        // reserved word
                        return "NOT";
                    else
                        // is a non-reserved identifier
                        return ("#" + s);

                } else {
                    // unknown material
                    throw new QueryScannerException(
                            m_in, "unknown character " + (char) m_lookAhead);
                }
        } // switch
    }
}
