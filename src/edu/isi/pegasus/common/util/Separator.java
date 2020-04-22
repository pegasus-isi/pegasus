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
 * This class solely defines the separators used in the textual in- and output between namespace,
 * name and version(s). A textual representation of a definition looks like ns::name:version, and a
 * textual representation of a uses like ns::name:min,max.
 *
 * <p>
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 * @see org.griphyn.vdl.classes.Definition
 */
public class Separator {
    /** This constant defines the separator between a namespace and the identifier. */
    public static final String NAMESPACE = "::";

    /** This constant defines the separator between an identifier and its version. */
    public static final String NAME = ":";

    /**
     * This constant defines the separator that denotes a version range. Version ranges are only
     * used with the "uses" clause, which maps from a derivation to a transformation.
     */
    public static final String VERSION = ",";

    /**
     * Although not truly a separator, this is the name of the default namespace, which is used in
     * the absence of a namespace.
     *
     * @deprecated The default namespace is <code>null</code>.
     */
    public static final String DEFAULT = "default";

    /**
     * Combines the three components that constitute a fully-qualified definition identifier into a
     * single string.
     *
     * @param namespace is the namespace, may be empty or null.
     * @param name is the name to use, must not be empty nor null.
     * @param version is the version to attach, may be empty or null.
     * @return the combination of namespace, name and version with separators.
     * @exception NullPointerException will be thrown on an empty or null name, as no such
     *     identifier can be constructed.
     */
    public static String combine(String namespace, String name, String version) {
        StringBuffer result = new StringBuffer(32);

        if (namespace != null && namespace.length() > 0)
            result.append(namespace).append(Separator.NAMESPACE);
        // postcondition: no namespace, no double colon

        if (name != null && name.length() > 0) {
            result.append(name);
        } else {
            // gotta have a name
            throw new NullPointerException("Missing identifier for definition");
        }

        if (version != null && version.length() > 0) result.append(Separator.NAME).append(version);
        // postcondition: If there is a version, it will be appended

        return result.toString();
    }

    /**
     * Combines the four components that reference a fully-qualified definition identifier into a
     * single string.
     *
     * @param namespace is the namespace, may be empty or null.
     * @param name is the name to use, must not be empty nor null.
     * @param min is the lower version to attach, may be empty or null.
     * @param max is the upper version to attach, may be empty or null.
     * @return the combination of namespace, name and versions with appropriate separators.
     * @exception NullPointerException will be thrown on an empty or null name, as no such
     *     identifier can be constructed.
     */
    public static String combine(String namespace, String name, String min, String max) {
        StringBuffer result = new StringBuffer(32);

        if (namespace != null && namespace.length() > 0)
            result.append(namespace).append(Separator.NAMESPACE);
        // postcondition: no namespace, no double colon

        if (name != null && name.length() > 0) {
            result.append(name);
        } else {
            // gotta have a name
            throw new NullPointerException("Missing identifier for definition");
        }

        if (min != null && min.length() > 0) {
            // minimum version exists
            result.append(Separator.NAME).append(min).append(Separator.VERSION);
            if (max != null && max.length() > 0) result.append(max);
        } else {
            // minimum version does not exist
            if (max != null && max.length() > 0)
                result.append(Separator.NAME).append(Separator.VERSION).append(max);
        }

        return result.toString();
    }

    /**
     * Maps the action associated with a state and char class. The following actions were
     * determined:
     *
     * <table>
     *  <tr><th>0</th><td>no operation</td></tr>
     *  <tr><th>1</th><td>save character</td></tr>
     *  <tr><th>2</th><td>empty save into ns</td></tr>
     *  <tr><th>3</th><td>empty save into id</td></tr>
     *  <tr><th>4</th><td>empty save into vs</td></tr>
     *  <tr><th>5</th><td>empty save into id, save</td></tr>
     * </table>
     */
    private static short actionmap2[][] = {
        {3, 0, 1},
        {3, 2, 5},
        {3, 3, 1},
        {4, 0, 1}
    };

    /**
     * Maps the new state from current state and character class. The following character classes
     * are distinguished:
     *
     * <table>
     *  <tr><th>0</th><td>EOS</td></tr>
     *  <tr><th>1</th><td>colon (:)</td></tr>
     *  <tr><th>2</th><td>other (*)</td></tr>
     * </table>
     */
    private static short statemap2[][] = {
        {8, 1, 0},
        {3, 2, 3},
        {8, 3, 2},
        {8, 9, 3}
    };

    /**
     * Splits a fully-qualified definition identifier into separate namespace, name and version.
     * Certain extensions permit a spec to distinguish between an empty namespace or version and a
     * null (wildcard match) namespace and version.
     *
     * <p>There is a subtle distinction between a null value and an empty value for the namespace
     * and version. A null value is usually taken as a wildcard match. An empty string however is an
     * exact match of a definition without the namespace or version.
     *
     * <p>In order to enable the DAX generation function to distinguish these cases when specifying
     * user input, the following convention is supported, where * stands in for wild-card matches,
     * and (-) for a match of an empty element:
     *
     * <table>
     *  <tr><th>INPUT</th> <th>NS</th>  <th>ID</th> <th>VS</th></tr>
     *  <tr><td>id</td>    <td>*</td>   <td>id</td> <td>*</td></tr>
     *  <tr><td>::id</td>  <td>(-)</td> <td>id</td> <td>*</td></tr>
     *  <tr><td>::id:</td> <td>(-)</td> <td>id</td> <td>(-)</td></tr>
     *  <tr><td>id:</td>   <td>*</td>   <td>id</td> <td>(-)</td></tr>
     *  <tr><td>id:vs</td> <td>*</td>   <td>id</td> <td>vs</td></tr>
     *  <tr><td>n::id</td> <td>n</td>   <td>id</td> <td>*</td></tr>
     *  <tr><td>n::id:</td><td>n</td>   <td>id</td> <td>(-)</td></tr>
     *  <tr><td>n::i:v</td><td>n</td>   <td>i</td>  <td>v</td></tr>
     *  <tr><td>::i:v</td> <td>(-)</td> <td>i</td>  <td>v</td></tr>
     * </table>
     *
     * @param fqdi is the fully-qualified definition identifier.
     * @return an array with 3 entries representing namespace, name and version. Namespace and
     *     version may be empty or even null.
     */
    public static String[] splitFQDI(String fqdi) throws IllegalArgumentException {
        String[] result = new String[3];
        result[0] = result[1] = result[2] = null;
        StringBuffer save = new StringBuffer();

        short state = 0;
        int pos = 0;

        char ch;
        int chclass;
        do {
            // obtain next character and character class
            if (pos < fqdi.length()) {
                // regular char
                ch = fqdi.charAt(pos);
                chclass = (ch == ':') ? 1 : 2;
                ++pos;
            } else {
                // EOS
                ch = Character.MIN_VALUE;
                chclass = 0;
            }

            // perform the action appropriate for state transition
            switch (actionmap2[state][chclass]) {
                case 0: // no-op
                    break;
                case 5: // Vi+S
                    result[1] = save.toString();
                    save = new StringBuffer();
                    // NO break on purpose
                case 1: // S
                    save.append(ch);
                    break;
                case 2: // Vn
                    result[0] = save.toString();
                    save = new StringBuffer();
                    break;
                case 3: // Vi
                    result[1] = save.toString();
                    save = new StringBuffer();
                    break;
                case 4: // Vv
                    result[2] = save.toString();
                    save = new StringBuffer();
                    break;
            }

            // perform state transition
            state = statemap2[state][chclass];
        } while (state < 8);

        if (state == 9 || result[1] == null || result[1].trim().length() == 0)
            throw new IllegalArgumentException("Malformed fully-qualified definition identifier");

        // POSTCONDITION: state == 8
        return result;
    }

    /**
     * Maps the action associated with a state and a character class. The actions are as follows:
     *
     * <table>
     *  <tr><th>0</th><td>no operation</td></tr>
     *  <tr><th>1</th><td>save character</td></tr>
     *  <tr><th>2</th><td>empty save into ns</td></tr>
     *  <tr><th>3</th><td>empty save into name</td></tr>
     *  <tr><th>4</th><td>empty save into vs</td></tr>
     *  <tr><th>5</th><td>empty save into vs, 4args</td></tr>
     *  <tr><th>6</th><td>empty save into max</td></tr>
     *  <tr><th>7</th><td>empty save into max, 4args</td></tr>
     *  <tr><th>8</th><td>empty save into name, save</td></tr>
     * </table>
     */
    private static int actionmap[][] = {
        {0, 0, 0, 1}, // 0
        {3, 0, 0, 1}, // 1
        {0, 2, 0, 8}, // 2
        {0, 0, 0, 1}, // 3
        {3, 3, 0, 1}, // 4
        {4, 0, 5, 1}, // 5
        {7, 0, 0, 1} // 6
    };

    /**
     * Maps the state and character class to the follow-up state. The final state 16 is a regular
     * final state, and final state 17 is the error final state. All other states are intermediary
     * states.
     *
     * <p>Four character classes are distinguished:
     *
     * <table>
     *  <tr><th>0</th><td>end of string (EOS)</td>
     *  <tr><th>1</th><td>colon (:)</td>
     *  <tr><th>2</th><td>comma (,)</td>
     *  <tr><th>3</th><td>any other</td>
     * </table>
     */
    private static short statemap[][] = {
        {17, 17, 17, 1}, // 0
        {16, 2, 17, 1}, // 1
        {17, 3, 17, 5}, // 2
        {17, 17, 6, 4}, // 3
        {16, 5, 17, 4}, // 4
        {16, 17, 6, 5}, // 5
        {16, 17, 17, 6} // 6
    };

    /**
     * Splits a fully-qualified identifier into its components. Please note that you must check the
     * length of the result. If it contains three elements, it is a regular FQDN. If it contains
     * four results, it is a tranformation reference range. Note though, if the version portion is
     * not specified, a 3 argument string will always be returned, even if the context requires a 4
     * argument string.
     *
     * @param fqdn is the string to split into components.
     * @return a vector with three or four Strings, if it was parsable.
     *     <ol>
     *       <li>namespace, may be null
     *       <li>name, never null
     *       <li>version for 3arg, or minimum version for 4arg, may be null
     *       <li>maximum version for 4arg, may be null
     *     </ol>
     *
     * @exception IllegalArgumentException, if the identifier cannot be parsed correctly.
     */
    public static String[] split(String fqdn) throws IllegalArgumentException {
        String namespace = null;
        String name = null;
        String version = null;
        String max = null;

        short state = 0;
        int pos = 0;
        boolean is4args = false;
        StringBuffer save = new StringBuffer();

        char ch;
        int chclass;
        do {
            // obtain next character and character class
            if (pos < fqdn.length()) {
                // regular char
                ch = fqdn.charAt(pos);
                if (ch == ':') chclass = 1;
                else if (ch == ',') chclass = 2;
                else chclass = 3;
                ++pos;
            } else {
                // EOS
                ch = Character.MIN_VALUE;
                chclass = 0;
            }

            // perform the action appropriate for state transition
            switch (actionmap[state][chclass]) {
                case 0: // no-op
                    break;
                case 8:
                    if (save.length() > 0) name = save.toString();
                    save = new StringBuffer();
                    // NO break on purpose
                case 1: // save
                    save.append(ch);
                    break;
                case 2: // save(ns)
                    if (save.length() > 0) namespace = save.toString();
                    save = new StringBuffer();
                    break;
                case 3: // save(name)
                    if (save.length() > 0) name = save.toString();
                    save = new StringBuffer();
                    break;
                case 5: // save(version), 4args
                    is4args = true;
                    // NO break on purpose
                case 4: // save(version)
                    if (save.length() > 0) version = save.toString();
                    save = new StringBuffer();
                    break;
                case 7: // save(max), 4args
                    is4args = true;
                    // NO break on purpose
                case 6: // save(max)
                    if (save.length() > 0) max = save.toString();
                    save = new StringBuffer();
                    break;
            }

            // perform state transition
            state = statemap[state][chclass];
        } while (state < 16);

        if (state == 17 || (is4args && version == null && max == null))
            throw new IllegalArgumentException(
                    "Malformed fully-qualified definition identifier " + fqdn);

        // POSTCONDITION: state == 16
        // assemble result
        String[] result = new String[is4args ? 4 : 3];
        result[0] = namespace;
        result[1] = name;
        result[2] = version;
        if (is4args) result[3] = max;
        return result;
    }
}
