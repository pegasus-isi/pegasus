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
 * This class converts a boolean property specification (string) in various representations into a
 * booelan value. It is liberal in the representation it accepts, but strict in what it produces.
 *
 * <p>
 *
 * @author Gaurang Mehta
 * @author Karan Vahi
 * @author Jens-S. Vöckler
 * @version $Revision$
 */
public class Boolean {
    /** The character representation of a <code>true</code> value. */
    public static final String TRUE = "true";

    /** The character representation of a <code>false</code> value. */
    public static final String FALSE = "false";

    /**
     * Converts a boolean value into a strict representation of it.
     *
     * @param val is the boolean input value
     * @return a string representing the boolean value.
     */
    public static String print(boolean val) {
        return (val ? TRUE : FALSE);
    }

    /**
     * Converts a boolean string representation into a boolean value. Representations may include
     * non-negative integers, where only 0 means <code>false</code>. Other valid string
     * representations of <code>true</code> include:
     *
     * <pre>
     * true
     * yes
     * on
     * </pre>
     *
     * Any other string representation is taken to mean <code>false</code>
     *
     * @param rep is the input string representing a boolean value.
     * @return a boolean value from the representation.
     */
    public static boolean parse(String rep) {
        return parse(rep, false);
    }

    /**
     * Converts a boolean string representation into a boolean value. Representations may include
     * non-negative integers, where only 0 means <code>false</code>. Other valid string
     * representations of <code>true</code> include:
     *
     * <pre>
     * true
     * yes
     * on
     * </pre>
     *
     * Other valid string representations of <code>false</code> include, besides the numerical zero:
     *
     * <pre>
     * false
     * no
     * off
     * </pre>
     *
     * Any other string representation is taken to mean the boolean value indicated by the paramater
     * deflt.
     *
     * @param rep is the input string representing a boolean value.
     * @param deflt is the deflt value to use in case rep does not represent a valid boolean value.
     * @return a boolean value from the representation.
     */
    public static boolean parse(String rep, boolean deflt) {
        if (rep == null) return deflt;
        String s = rep.trim().toLowerCase();
        if (s.length() == 0) return deflt;

        if (Character.isDigit(s.charAt(0))) {
            // check for number
            long value;
            try {
                value = Long.parseLong(s);
            } catch (NumberFormatException nfe) {
                value = deflt ? 1 : 0;
            }
            return (value != 0);
        } else {
            // check for key words
            return ((s.equals("true") || s.equals("yes") || s.equals("on"))
                    ? true
                    : ((s.equals("false") || s.equals("no") || s.equals("off")) ? false : deflt));
        }
    }
}
