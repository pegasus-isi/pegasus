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

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import org.griphyn.vdl.dbschema.Annotation;

/**
 * Parses the input stream and generates a query tree as output.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 * @see QueryScanner
 * @see QueryTree
 */
public class QueryParser {
    /** The access to the lexical scanner is stored here. */
    private QueryScanner m_scanner = null;

    /** Stores the look-ahead symbol. */
    private String m_lookAhead = null;

    /**
     * Initializes the parser with an input stream to read from.
     *
     * @param r is the stream opened for reading.
     */
    public QueryParser(java.io.Reader r) throws IOException, QueryParserException {
        m_scanner = new QueryScanner(r);
        m_lookAhead = m_scanner.nextToken();
    }

    /**
     * Parses the query stream
     *
     * @return the root node to the parsed query tree
     */
    public QueryTree parse() throws IOException, QueryParserException {
        QueryTree root = null;
        while (m_lookAhead != null) {
            if (root == null) {
                root = expression();
            } else expression();
        }
        return root;
    }

    /** The main function to test the parser. */
    public static void main(String[] args) throws IOException, QueryParserException {
        QueryParser parser;
        if (args.length == 0) parser = new QueryParser(new InputStreamReader(System.in));
        else parser = new QueryParser(new FileReader(args[0]));

        QueryTree root = parser.parse();

        if (root != null) {
            String sql = root.toSQL(Annotation.CLASS_FILENAME, null);
            System.out.println(sql);
        }
    }

    /** check a string and guess the type of the attribute from its value */
    protected int checkString(String value) {
        int type = Predicate.TYPE_STRING;
        int len = value.length();
        if (value == null || len == 0) return type;
        if (len == 1)
            if (value.equalsIgnoreCase("t") || value.equalsIgnoreCase("f"))
                return Predicate.TYPE_BOOL;
            else return type;

        // if (value.matches("\\d\\d[/.]\\d\\d[/.]\\d\\d(\\d\\d)?"))
        if (checkDate(value)) return Predicate.TYPE_DATE;

        return type;
    }

    /** check a string to see whether it represents a date value */
    protected boolean checkDate(String value) {
        SimpleDateFormat fmt[] = {
            new SimpleDateFormat("MM/dd/yy HH:mm:ss.SSS"),
            new SimpleDateFormat("MM/dd/yy HH:mm:ss"),
            new SimpleDateFormat("MM/dd/yy HH:mm"),
            new SimpleDateFormat("MM/dd/yy"),
            new SimpleDateFormat("MM.dd.yy HH:mm:ss.SSS"),
            new SimpleDateFormat("MM.dd.yy HH:mm:ss"),
            new SimpleDateFormat("MM.dd.yy HH:mm"),
            new SimpleDateFormat("MM.dd.yy"),
            new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS"),
            new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"),
            new SimpleDateFormat("yyyy-MM-dd HH:mm"),
            new SimpleDateFormat("yyyy-MM-dd"),
            new SimpleDateFormat("yyyy.MM.dd HH:mm:ss.SSS"),
            new SimpleDateFormat("yyyy.MM.dd HH:mm:ss"),
            new SimpleDateFormat("yyyy.MM.dd HH:mm"),
            new SimpleDateFormat("yyyy.MM.dd"),
        };
        for (int i = 0; i < fmt.length; i++) {
            try {
                fmt[i].parse(value);
                return true;
            } catch (ParseException e) {
                continue;
            }
        }
        return false;
    }

    /** check a string to see whether it represents a integer or float value. */
    protected int checkNumber(String value) {
        if (value.matches("^[+-]?\\d+$")) return Predicate.TYPE_INT;
        if (value.matches("^([+-]?)(?=\\d|\\.\\d)\\d*(\\.\\d*)?([Ee]([+-]?\\d+))?$"))
            return Predicate.TYPE_FLOAT;
        return -1;
    }

    /** Parses predicates and returns the corresponding Predicate objects */
    protected Predicate predicates() throws IOException, QueryParserException {
        Predicate p = null;
        if (m_lookAhead.startsWith("#")) {
            // identifier
            String name = m_lookAhead.substring(1);

            m_lookAhead = m_scanner.nextToken();
            if (!m_lookAhead.startsWith("@"))
                throw new QueryParserException(m_scanner, "A predicate must follow " + name);
            if (m_lookAhead.equals("@EX")) {
                // nothing following exists
                p = new Predicate(Predicate.EXISTS, name);
            } else if (m_lookAhead.equals("@CT")) {
                m_lookAhead = m_scanner.nextToken();
                if (!m_lookAhead.startsWith("$"))
                    throw new QueryParserException(
                            m_scanner, "CONTAINS should be followed by a string value");
                String value = m_lookAhead.substring(1);
                p = new Predicate(Predicate.CONTAINS, name, value);
            } else if (m_lookAhead.equals("@LK")) {
                m_lookAhead = m_scanner.nextToken();
                if (!m_lookAhead.startsWith("$"))
                    throw new QueryParserException(
                            m_scanner, "LIKE should be followed by a string value");
                String value = m_lookAhead.substring(1);
                p = new Predicate(Predicate.LIKE, name, value);
            } else if (m_lookAhead.equals("@BT")) {
                m_lookAhead = m_scanner.nextToken();

                int type;
                int numType1 = -1, numType2 = -1;
                int strType1 = -1, strType2 = -1;
                String value1;
                if (m_lookAhead.startsWith("#")) {
                    type = 0;
                    numType1 = -1;

                    value1 = m_lookAhead.substring(1);
                    if ((numType1 = checkNumber(value1)) == -1)
                        throw new QueryParserException(
                                m_scanner,
                                "the value " + value1 + " is not numeric, use quotes for a string");

                } else if (m_lookAhead.startsWith("$")) {
                    type = 1;
                    value1 = m_lookAhead.substring(1);
                    strType1 = checkString(value1);
                } else
                    throw new QueryParserException(
                            m_scanner, "BETWEEN should be followed by a value");
                m_lookAhead = m_scanner.nextToken();
                if (!m_lookAhead.equals("AND"))
                    throw new QueryParserException(m_scanner, "BETWEEN should be followed by AND");
                m_lookAhead = m_scanner.nextToken();
                if (!m_lookAhead.startsWith("#") && !m_lookAhead.startsWith("$")) {
                    throw new QueryParserException(
                            m_scanner, "BETWEEN should have another value after AND ");
                } else if ((m_lookAhead.startsWith("#") && type == 1)
                        || (m_lookAhead.startsWith("$") && type == 0))
                    throw new QueryParserException(m_scanner, "type mismatch in BETWEEN...AND ");

                String value2 = m_lookAhead.substring(1);
                numType2 = -1;
                if (m_lookAhead.startsWith("#")) {
                    if ((numType2 = checkNumber(value2)) == -1)
                        throw new QueryParserException(
                                m_scanner,
                                "the value " + value2 + " is not numeric, use quotes for a string");
                    if (numType1 != numType2) type = Predicate.TYPE_FLOAT;
                    else type = numType1;
                } else {
                    strType2 = checkString(value2);
                    if (strType1 != strType2) type = Predicate.TYPE_STRING;
                    else type = strType1;
                }
                p = new Predicate(Predicate.BETWEEN, name, type, value1, value2);
            } else {
                String op = m_lookAhead;
                m_lookAhead = m_scanner.nextToken();
                if (!m_lookAhead.startsWith("#") && !m_lookAhead.startsWith("$"))
                    throw new QueryParserException(
                            m_scanner, "Predicate should be followed by a value");
                String value = m_lookAhead.substring(1);

                int type = -1;
                if (m_lookAhead.startsWith("#")) {
                    if ((type = checkNumber(value)) == -1)
                        throw new QueryParserException(
                                m_scanner,
                                "the value " + value + " is not numeric, use quotes for a string");
                } else {
                    type = checkString(value);
                }

                int pt = -1;
                if (op.equals("@EQ")) pt = Predicate.EQ;
                else if (op.equals("@NE")) pt = Predicate.NE;
                else if (op.equals("@GT")) pt = Predicate.GT;
                else if (op.equals("@LT")) pt = Predicate.LT;
                else if (op.equals("@GE")) pt = Predicate.GE;
                else if (op.equals("@LE")) pt = Predicate.LE;

                p = new Predicate(pt, name, type, value);
            }
        }
        return p;
    }

    /** Parses the atoms of the query statement */
    public QueryTree atom() throws IOException, QueryParserException {
        QueryTree ret = null;
        QueryTree current = null;

        if (m_lookAhead.startsWith("#")) {
            Predicate p = predicates();
            current = new QueryTree(p);
        } else if (m_lookAhead.equals("NOT")) {
            m_lookAhead = m_scanner.nextToken();
            Predicate p = new Predicate(Predicate.NOT);
            current = new QueryTree(p);
            ret = atom();
            if (ret.getPredicate() == Predicate.NOT) current = ret.getRchild();
            else current.setRchild(ret);
        } else if (m_lookAhead.startsWith("(")) {
            m_lookAhead = m_scanner.nextToken();
            current = expression();

            if (m_lookAhead == null || !m_lookAhead.startsWith(")")) {
                throw new QueryParserException(m_scanner, "missing )");
            }
        } else
            throw new QueryParserException(m_scanner, "invalid token " + m_lookAhead.substring(1));

        return current;
    }

    /** Parses the expressions of the query statement */
    public QueryTree expression() throws IOException, QueryParserException {
        QueryTree ret = null;

        ret = atom();

        m_lookAhead = m_scanner.nextToken();
        while ((m_lookAhead != null) && (m_lookAhead.equals("AND") || m_lookAhead.equals("OR"))) {
            Predicate p;
            if (m_lookAhead.equals("AND")) {
                p = new Predicate(Predicate.AND);
            } else {
                p = new Predicate(Predicate.OR);
            }
            QueryTree tmp = new QueryTree(p);
            tmp.addChild(ret);

            m_lookAhead = m_scanner.nextToken();
            ret = atom();
            tmp.addChild(ret);

            m_lookAhead = m_scanner.nextToken();

            ret = tmp;
        }
        return ret;
    }
}
