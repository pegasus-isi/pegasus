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
package edu.isi.pegasus.planner.code.generator.condor;

import edu.isi.pegasus.planner.namespace.ENV;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * This class implements the rules defined by HTCondor for quoting environment values described here
 * http://research.cs.wisc.edu/htcondor/manual/v8.2/condor_submit.html
 *
 * <p>The rules are described below
 *
 * <pre>
 * The new syntax for specifying environment values:
 *
 * Put double quote marks around the entire argument string. This distinguishes the new syntax from the old.
 * The old syntax does not have double quote marks around it.
 * Any literal double quote marks within the string must be escaped by repeating the double quote mark.
 * 1) Each environment entry has the form <name>=<value>
 * 2) Use white space (space or tab characters) to separate environment entries.
 * 3) To put any white space in an environment entry, surround the space and as
 *    much of the surrounding entry as desired with single quote marks.
 * 4) To insert a literal single quote mark, repeat the single quote mark anywhere
 *    inside of a section surrounded by single quote marks.
 * Example:
 *
 *  environment = "one=1 two=""2"" three='spacey ''quoted'' value'"
 * Produces the following environment entries:
 *
 * one=1
 * two="2"
 * three=spacey 'quoted' value
 * </pre>
 *
 * @author Karan Vahi
 */
public class CondorEnvironmentEscape {

    /** Defines the set of characters that require escaping. */
    private char[] mEscapable = {'\'', '\"'};

    /** The character to use if whitespace is detected */
    private static final char WHITESPACE_ENCLOSING_CHARACTER = '\'';

    /**
     * Defines the default quoting and escaping rules, escaping the apostrophe, double quote and
     * backslash. The escape character is the backslash.
     */
    public CondorEnvironmentEscape() {}

    /**
     * Escapes all the profiles in the environment namespace per condor rules for escaping
     * environment variables
     *
     * <pre>
     * The new syntax for specifying environment values:
     *
     * Put double quote marks around the entire argument string. This distinguishes the new syntax from the old.
     * The old syntax does not have double quote marks around it.
     * Any literal double quote marks within the string must be escaped by repeating the double quote mark.
     * 1) Each environment entry has the form <name>=<value>
     * 2) Use white space (space or tab characters) to separate environment entries.
     * 3) To put any white space in an environment entry, surround the space and as
     *    much of the surrounding entry as desired with single quote marks.
     * 4) To insert a literal single quote mark, repeat the single quote mark anywhere
     *    inside of a section surrounded by single quote marks.
     * Example:
     *
     *  environment = "one=1 two=""2"" three='spacey ''quoted'' value'"
     * Produces the following environment entries:
     *
     * one=1
     * two="2"
     * three=spacey 'quoted' value
     * </pre>
     *
     * @param env
     * @return
     */
    public String escape(ENV env) {
        StringBuilder result = new StringBuilder();
        char whitespace = ' ';
        // whole environment is enclosed in double quotes
        result.append("\"");
        for (Iterator it = env.getProfileKeyIterator(); it.hasNext(); ) {
            String key = (String) it.next();
            result.append(key);
            result.append("=");
            result.append(this.escape((String) env.get(key)));
            result.append(whitespace);
        }

        // PM-1245 remove trailing whitespace
        if (result.charAt(result.length() - 1) == whitespace) {
            result.deleteCharAt(result.length() - 1);
        }

        // end enclosing double quotes
        result.append("\"");
        return result.toString();
    }

    /**
     * Transforms a given string by escaping all characters inside the quotable characters set with
     * the escape character. The rules followed are described below
     *
     * <pre>
     * 1) To put any white space in an environment entry, surround the space and as
     *    much of the surrounding entry as desired with single quote marks.
     * 2) To insert a literal single quote mark, repeat the single quote mark anywhere
     *    inside of a section surrounded by single quote marks.
     * </pre>
     *
     * @param s is the string to escape.
     * @return the quoted string
     */
    public String escape(String s) {
        // sanity check
        if (s == null) {
            return null;
        }

        // first check for existence of whitespace
        boolean encloseInSingleQuote = false;
        StringBuilder result = new StringBuilder(s.length());
        if (s.contains(" ")) {
            // we have to enclose the whole string with single quote marks
            encloseInSingleQuote = true;
            result.append(WHITESPACE_ENCLOSING_CHARACTER);
        }

        for (int i = 0; i < s.length(); ++i) {
            char ch = s.charAt(i);
            if (isEscapable(ch)) {
                // the escape characters are the ones
                // that are being escaped themselves!
                result.append(ch);
            }
            result.append(ch);
        }

        if (encloseInSingleQuote) {
            result.append(WHITESPACE_ENCLOSING_CHARACTER);
        }

        return result.toString();
    }

    /**
     * Test program.
     *
     * @param args are command-line arguments
     */
    public static void main(String args[]) {
        CondorEnvironmentEscape me = new CondorEnvironmentEscape(); // defaults
        Map<String, String> m = new LinkedHashMap();
        m.put("one", "1");
        m.put("two", "\"2\"");
        m.put("three", "spacey 'quoted' value");
        ENV env = new ENV(m);

        // should print out "one=1 two=""2"" three='spacey ''quoted'' value' "
        String expected = "\"one=1 two=\"\"2\"\" three='spacey ''quoted'' value' \"";
        String result = me.escape(env);
        System.out.println("escaping successful " + result.equals(expected));
        System.out.println(result);
    }

    /**
     * Returns whether the character is escapable or not
     *
     * @return
     */
    private boolean isEscapable(char ch) {
        return (ch == mEscapable[0] || ch == mEscapable[1]);
    }
}
