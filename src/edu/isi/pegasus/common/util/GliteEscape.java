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
package edu.isi.pegasus.common.util;

/**
 * This class tries to define an interface to deal with quoting, escaping.
 *
 * <p>The quoting algorithm is safe to only itself. Thus,
 *
 * <p>
 *
 * <pre>
 * unescape( escape( s ) ) === s
 * </pre>
 *
 * <p>holds true, but
 *
 * <p>
 *
 * <pre>
 * escape( unescape( s ) ) =?= s
 * </pre>
 *
 * <p>does not necessarily hold.
 *
 * @author Rajiv Mayani
 * @version $Revision$
 */
public class GliteEscape {
    /** Defines the character used to escape characters. */
    private char m_escape;

    /** Defines the set of characters that require escaping. */
    private String m_escapable;

    /**
     * Defines the set of characters that are unescapable. Unescapable characters are those which
     * when processed by condor and written to a PBS script will cause failure, or be ignored
     * without warning.
     */
    private String m_unescapable;

    /**
     * Defines the default quoting and escaping rules, escaping the apostrophe, double quote and
     * backslash. The escape character is the backslash.
     */
    public GliteEscape() {
        m_escape = '\\';
        m_escapable = " \"\\";
        m_unescapable = "'\n\t\\";
    }

    /**
     * Constructs arbitrary escaping rules.
     *
     * @param escapable is the set of characters that require escaping
     * @param escape is the escape character itself.
     */
    public GliteEscape(String escapable, char escape) {
        m_escape = escape;
        m_escapable = escapable;
        m_unescapable = "'\\";

        // ensure that the escape character is part of the escapable char set
        if (escapable.indexOf(escape) == -1) m_escapable += m_escape;
    }

    /**
     * Checks to see if a string contains characters which cannot be escaped.
     *
     * @param s is the string to escape.
     * @return true if the string does not contain unescapable characters
     */
    private boolean isEscapable(String s) {

        for (char ch : s.toCharArray()) {
            if (m_unescapable.indexOf(ch) != -1) {
                return false;
            }
        }

        return true;
    }

    /**
     * Transforms a given string by escaping all characters inside the quotable characters set with
     * the escape character. The escape character itself is also escaped.
     *
     * @param s is the string to escape.
     * @return the quoted string
     */
    public String escape(String s) {
        // sanity check
        if (s == null) {
            return null;
        }

        if (!isEscapable(s)) {
            throw new RuntimeException(
                    "String "
                            + s
                            + " contains some un-escapable characters \""
                            + m_unescapable
                            + "\"");
        }

        StringBuilder result = new StringBuilder(s.length());

        for (int i = 0; i < s.length(); ++i) {
            char ch = s.charAt(i);

            if (m_escapable.indexOf(ch) != -1) {
                result.append(m_escape);

                if (ch == '\"') {
                    result.append(m_escape);
                    // result.append(m_escape);
                }
            }

            result.append(ch);
        }

        return result.toString();
    }

    /**
     * Test program.
     *
     * @param args are command-line arguments
     */
    public static void main(String args[]) {
        GliteEscape me = new GliteEscape(); // defaults

        for (int i = 0; i < args.length; ++i) {
            String e = me.escape(args[i]);
            System.out.println("raw s  > " + args[i]);
            System.out.println("e(s)   > " + e);
            System.out.println();
        }
    }
}
