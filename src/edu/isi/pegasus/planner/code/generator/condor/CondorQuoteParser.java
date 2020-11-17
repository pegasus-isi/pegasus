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

/**
 * A utility class to correctly quote arguments strings before handing over to Condor.
 *
 * <p>The following Condor Quoting Rules are followed while quoting a String.
 *
 * <pre>
 * 1) \' => ''   e.g \'Test\' is converted to ''Test''
 * 2) \" => ""   e.g \"Test\" is converted to ""Test""
 * 3) '  => '    if not enclosed in surrounding double quotes
 *               e.g 'Test' is converted to 'Test'
 * 4) '  => ''   if enclosed in surrounding double quotes
 *               e.g "'Test'" is converted to ''Test''
 * 5) "  => '    if not enclosed in surrounding single quotes
 *               e.g Karan "Vahi" is converted to Karan 'Vahi'
 * 6) "  => ""   if enclosed in surrounding single quotes.
 *               e.g 'Karan "Vahi"' is converted to 'Karan ""Vahi""'.
 * 7) *  =>  *   if enclosed in single or double quotes, the enclosed characters
 *               are copied literally including \ (no escaping rules apply)
 * 8) \\ => \    escaping rules apply if not enclosed in single or double quotes.
 *               e.g \\\\ becomes \\, and \\\ throws error.
 * </pre>
 *
 * In order to pass \n etc in the arguments, either quote it or escape it. for e.g in the DAX the
 * following are valid ways to pass Karan\nVahi to the as arguments
 *
 * <pre>
 *   1) "Karan\nVahi"
 *   2) 'Karan\nVahi'
 *   3) Karan\\nVahi
 * </pre>
 *
 * In addition while writing out to the SubmitFile the whole argument String should be in enclosing
 * ". for e.g arguments = "Test";
 *
 * @author Karan Vahi
 * @author Gaurang Mehta
 * @version $Revision$
 */
public class CondorQuoteParser {

    /**
     * Table to contain the state transition diagram for the parser. The rows are defined as current
     * states 0 through 7. The columns is the current input character. The cell contains first the
     * action to be taken, followed by the new state to transition to:
     *
     * <pre>
     *      | EOS |  \  |  '  |  "  |other|
     *      |  0  |  1  |  2  |  3  |  4  |
     * -----+-----+-----+-----+-----+-----+
     *   0  | -,F |  -,1| A2,2| A2,3| A1,0|
     *   1  | -,E1| A1,0| A3,0| A4,0| A1,0|
     *   2  | -,E2| A1,2| A2,0| A4,2| A1,2|
     *   3  | -,E3| A1,3| A3,3| A2,0| A1,3|
     * -----+-----+-----+-----+-----+-----+
     *   F  |  4  | final state
     *   E1 |  5  | error1: unexpected end of input
     *   E2 |  6  | error2: unmatched single quotes
     *   E3 |  7  | error3: unmatched double quotes
     * </pre>
     *
     * The state variable collects the new state for a given state (rows) and input character set
     * (column) identifier.
     *
     * <p>The state diagram for the above table is shown as follows <br>
     * <a href="doc-files/CondorQuote.jpg"><img src="doc-files/CondorQuote.jpg" height="350"
     * width="400"></a>
     */
    private static final byte cState[][] = {
        // E   \   '   "   O
        {4, 1, 2, 3, 0}, // 0: starting state
        {5, 0, 0, 0, 0}, // 1: found a \
        {6, 2, 0, 2, 2}, // 2: found an opening single quote
        {7, 3, 3, 0, 3}, // 3: found an opening double quote
    };

    /**
     * There are five identified actions.
     *
     * <pre>
     *  -   | 0 | noop
     *  A1  | 1 | append input character to result
     *  A2  | 2 | append '  to result
     *  A3  | 3 | append '' to result
     *  A4  | 4 | append "" to result
     * </pre>
     *
     * The action variable collects the action to take for a given state (rows) and input character
     * set (column).
     */
    private static final byte cAction[][] = {
        // E   \   '   "   O
        {0, 0, 2, 2, 1}, // 0: starting state
        {0, 1, 3, 4, 1}, // 1: found a \
        {0, 1, 2, 4, 1}, // 2: found an opening single quote
        {0, 1, 3, 2, 1}, // 3: found an opening double quote
    };

    /**
     * Parses a string and condor quotes it. The enclosing quotes are not generated around the
     * String.
     *
     * @param s is the input string to parse and quote.
     * @return the quoted String.
     * @throws CondorQuoteParserException if the input cannot be recognized.
     */
    public static String quote(String s) throws CondorQuoteParserException {
        return quote(s, false);
    }

    /**
     * Parses a string and condor quotes it. Enclosing quotes are generated around the whole string
     * if boolean enclose parameter is set.
     *
     * @param s is the input string to parse and quote.
     * @param enclose boolean indicating whether to generate enclosing quotes or not.
     * @return the quoted String.
     * @throws CondorQuoteParserException if the input cannot be recognized.
     */
    public static String quote(String s, boolean enclose) throws CondorQuoteParserException {
        StringBuffer result = new StringBuffer();

        // enclose the string with mandatory " to start
        if (enclose) result.append("\"");

        int index = 0;
        byte charset, state = 0;
        char ch = '?';

        while (state < 4) {
            //
            // determine character class
            //
            switch ((ch = (index < s.length() ? s.charAt(index++) : '\0'))) {
                case '\0':
                    charset = 0;
                    break;

                case '\\':
                    charset = 1;
                    break;

                case '\'':
                    charset = 2;
                    break;

                case '\"':
                    charset = 3;
                    break;

                default:
                    charset = 4;
                    break;
            }

            //
            // perform action
            //
            switch (cAction[state][charset]) {
                case 0: // do nothing
                    break;

                case 1: // append the character to the result
                    result.append(ch);
                    break;

                case 2: // append \ to the result
                    result.append('\'');
                    break;

                case 3: // append '' to the result
                    result.append("\'\'");
                    break;

                case 4: // append "" to the result
                    result.append("\"\"");
                    break;
            }

            //
            // progress state
            //
            state = cState[state][charset];
        }

        if (state > 4) {
            switch (state) {
                case 5:
                    // we have unmatched single quotes
                    throw new CondorQuoteParserException(
                            "Unexpected end of input in string " + s, index);

                case 6:
                    // we have unmatched single quotes
                    throw new CondorQuoteParserException(
                            "Unmatched Single Quotes in string " + s, index);

                case 7:
                    // we have unmatched double quotes
                    throw new CondorQuoteParserException(
                            "Unmatched Double Quotes in string " + s, index);

                default:
                    throw new CondorQuoteParserException("Unknown error", index);
            }
        }

        // end the result with the mandatory closing " to end
        if (enclose) result.append("\"");

        return result.toString();
    }

    /** A Test program. */
    public static void main(String[] args) {
        test("Test Input"); // result should be Test Input
        test("'Test Input'"); // result should be 'Test Input'
        test("\"Test Input\""); // result should be 'Test Input'
        test("\\'Test Input\\'"); // result should be ''Test Input''
        test("\\\"Test Input\\\""); // result should be ""Test Input""
        test("\\\'Test Input\\\'"); // result should be ''Test Input''
        test("\"\'Test Input\'\""); // result should be '''Test Input'''
        test("\'\"Test Input\"\'"); // result should be '""Test Input""'
        test("\"\'Test \\Input\'\""); // result should be '''Test \Input'''
        test("\\\"Test Input\\\""); // result should be ""Test Input""
        test("\'Test \"Input\"\'"); // result should be 'Test ""Input""'
        test("Test \"Input\""); // result should be Test 'Input'
        test("\\\\Test Input"); // result should be \Test Input
        test("\\\\"); // result should be \
        test("\'\"Test Input\'"); // result should be '""Test Input'

        // errorneous inputs
        test("\'\"Test Input\"  ");
        test(" \"\"\" ");
        test(" '''  ");
    }

    /**
     * Helper test method that tries and catches exception
     *
     * @param s the string to be parsed.
     */
    private static void test(String s) {
        try {
            System.out.println(s + " condor quoted is " + quote(s));
        } catch (CondorQuoteParserException e) {
            System.out.println("Error " + e + " at position " + e.getPosition());
        }
        // System.out.println();
    }
}
