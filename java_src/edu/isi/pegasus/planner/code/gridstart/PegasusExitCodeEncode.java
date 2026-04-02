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
package edu.isi.pegasus.planner.code.gridstart;

/**
 * This class tries to define a mechanism to encode arguments for pegasus-exitcode, as DAGMan does
 * not handle whitespaces correctly for postscript arguments.
 *
 * <p>The default rules are single space gets encoded to + + gets escaped to \+ non printing asci
 * characters are flagged
 *
 * <p>here are some examples of this encoding rule
 *
 * <pre>
 * Error Message      is encoded to Error+Message
 * Error   Message    is encoded to Error+++Message
 * Error + Message    is encoded to Error+\++Message
 * Error + \Message   is encoded to Error+\++\Message
 * Error + Message\   is encoded to Error+\++Message\
 * Error + \\ Message is encoded to Error+\++\\+Message
 * </pre>
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class PegasusExitCodeEncode {

    /** Defines the character used to escape characters. */
    private char mEscape;

    /** Defines the set of characters that require escaping. */
    private String mEscapable;

    /** Defines the character that requires encoding */
    private char mEncodeable;

    /** The value to encode to */
    private char mEncode;

    /** Defines the default encoding rules escape + with \+ encode single whitespace with + */
    public PegasusExitCodeEncode() {
        mEscapable = "+";
        mEscape = '\\';
        mEncodeable = ' ';
        mEncode = '+';
    }

    /**
     * Constructs arbitrary escaping rules.
     *
     * @param escapable is the set of characters that require escaping
     * @param escape is the escape character itself.
     */
    public PegasusExitCodeEncode(String escapable, char escape) {
        mEscape = escape;
        mEscapable = escapable;

        // ensure that the escape character is part of the escapable char set
        if (escapable.indexOf(escape) == -1) {
            mEscapable += mEscape;
        }
    }

    /**
     * Transforms a given string by encoding single whitespace with the escape
     * character set ( defaults to + ), and escapes the escape itself
     *
     * <pre>
     * error message is encoded to error+message
     * error +message is encoded to error+\+message
     * <>
     *
     * @param s is the string to encode.
     * @return the encoded string
     *
     * @see #unescape( String )
     */
    public String encode(String s) {
        // sanity check
        if (s == null) {
            return null;
        }

        StringBuilder result = new StringBuilder(s.length());
        for (int i = 0; i < s.length(); ++i) {
            char ch = s.charAt(i);

            if (Character.isWhitespace(ch)) {
                if (ch == mEncodeable) {
                    result.append(mEncode);
                } else {
                    throw new IllegalArgumentException(
                            "Invalid whitespace character \'" + ch + "\' passed for encoding " + s);
                }
                continue;
            }

            // after whitespace check for other non printing characters
            // to distinguish error between invalid whitespace and non printing character
            if (!this.isAsciiPrintable(ch)) {
                throw new IllegalArgumentException(
                        "Invalid non printing character \'" + ch + "\' passed for encoding " + s);
            } else if (mEscapable.indexOf(ch) != -1) {
                // we need to escape the character
                result.append(mEscape);
                result.append(ch);
            } else {
                result.append(ch);
            }
        }
        return result.toString();
    }

    /**
     * Transforms a given string by decoding all characters and unescaping where required.
     *
     * @param s is the string to remove escapes from.
     * @return the decoded string
     */
    public String decode(String s) {
        // sanity check
        if (s == null) {
            return null;
        }

        StringBuilder result = new StringBuilder(s.length());
        int state = 0;
        for (int i = 0; i < s.length(); ++i) {
            char ch = s.charAt(i);
            if (state == 0) {
                // default state
                if (ch == mEncode) {
                    result.append(mEncodeable);
                } else if (ch == mEscape) {
                    state = 1;

                    // fix for \ as last character
                    if (i == s.length() - 1) {
                        result.append(ch);
                    }
                } else {
                    result.append(ch);
                }
            } else {
                // "found escape" state
                if (mEscapable.indexOf(ch) == -1) {
                    result.append(mEscape);
                }
                result.append(ch);
                state = 0;
            }
        }

        return result.toString();
    }

    /**
     * Checks whether the character is ASCII 7 bit printable.
     *
     * <pre>
     *   CharUtils.isAsciiPrintable('a')  = true
     *   CharUtils.isAsciiPrintable('A')  = true
     *   CharUtils.isAsciiPrintable('3')  = true
     *   CharUtils.isAsciiPrintable('-')  = true
     *   CharUtils.isAsciiPrintable('\n') = false
     *   CharUtils.isAsciiPrintable('&copy;') = false
     * </pre>
     *
     * @param ch the character to check
     * @return true if between 32 and 126 inclusive
     */
    public boolean isAsciiPrintable(char ch) {
        return ch >= 32 && ch < 127;
    }

    public void test(String s) {
        String e = this.encode(s);
        String s1 = this.decode(e);
        System.out.println(s + " is encoded to " + e);
        System.out.println(e + " is decoded to " + s1);
        if (s.equals(s1)) {
            System.out.println("[Success] Encoding and decoding is symmetric ");
        } else {
            System.out.println("[Error] Encoding and decoding is asymmetric ");
        }
        System.out.println();
    }

    /**
     * Test program.
     *
     * @param args are command-line arguments
     */
    public static void main(String args[]) {
        PegasusExitCodeEncode me = new PegasusExitCodeEncode(); // defaults

        me.test("Error Message");
        me.test("Error   Message");
        me.test("Error + Message");
        me.test("Error + \\Message");
        me.test("Error + Message\\");
        me.test("Error + \\\\ Message");
        me.test("Error + Message\\\\");

        // should throw errors
        try {
            me.test("Error + " + "\t" + "Message");
        } catch (IllegalArgumentException e) {
            System.out.println(e.getMessage());
        }

        try {
            char ch = 163; // the pound sign
            me.test("Error Message " + ch);
        } catch (IllegalArgumentException e) {
            System.out.println(e.getMessage());
        }
    }
}
