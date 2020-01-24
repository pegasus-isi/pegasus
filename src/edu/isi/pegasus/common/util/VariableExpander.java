/**
 * Copyright 2007-2015 University Of Southern California
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

import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.text.StrLookup;
import org.apache.commons.lang3.text.StrSubstitutor;

/**
 * Utility class to allow for variable expansions in strings.
 *
 * @author Karan Vahi
 */
public class VariableExpander {

    /** The list of values mMap that need to be expanded. */
    private Map<String, String> mValuesMap;

    private StrSubstitutor mExpander;

    /**
     * The default constructor, which intializes the values from the System environment variables,
     * and has case sensitivity turned off.
     */
    public VariableExpander() {
        this(false);
    }

    /**
     * Overloaded constructor
     *
     * @param caseSensitive boolean indicating whether you want lookups to be case sensitive or not.
     */
    public VariableExpander(boolean caseSensitive) {
        mValuesMap = new HashMap(System.getenv());
        mExpander = new StrSubstitutor(mValuesMap, "${", "}", '\\');
        mExpander.setVariableResolver(new CaseSensitiveStrLookup(this.mValuesMap, caseSensitive));
    }

    /**
     * Overloaded constructor. Constructs expander with case sensitivity turned off
     *
     * @param map containing variable names and values that need to be expanded
     */
    public VariableExpander(Map<String, String> map) {
        this(map, false);
    }

    /**
     * Overloaded constructor
     *
     * @param map containing variable names and values that need to be expanded
     * @param caseSensitive boolean indicating whether you want lookups to be case sensitive or not.
     */
    public VariableExpander(Map<String, String> map, boolean caseSensitive) {
        mValuesMap = map;
        mExpander = new StrSubstitutor(map);
        mExpander.setVariableResolver(new CaseSensitiveStrLookup(this.mValuesMap, caseSensitive));
    }

    /**
     * Expands the value passed with variable substitution
     *
     * @param text
     * @return expanded value
     */
    public String expand(String text) {
        return mExpander.replace(text);
    }

    public static void main(String[] args) {
        VariableExpander exp = new VariableExpander();
        System.out.println(exp.mValuesMap);

        System.out.println(exp.expand("Pegasus developer $(USER) rocks "));
        System.out.println(exp.expand("Pegasus developer $(USer) rocks "));
        // System.out.println( exp.expand( "Pegasus developer $(USER1) rocks "));
        System.out.println(exp.expand("Pegasus developer \\$(USER) rocks "));
    }
}

/**
 * A case sensitive look up class to use in StrSubstitutor
 *
 * @author vahi
 * @param <V>
 */
class CaseSensitiveStrLookup<V> extends StrLookup<V> {

    private final Map<String, V> mMap;

    private final boolean mCaseSensitive;

    CaseSensitiveStrLookup(final Map<String, V> map, boolean caseSensitive) {
        mCaseSensitive = caseSensitive;
        Map<String, V> setMap = new HashMap();
        // explicitly lower case all the key in there
        for (String key : map.keySet()) {
            V value = map.get(key);
            setMap.put(caseSensitive ? key : key.toLowerCase(), value);
        }
        this.mMap = setMap;
    }

    /**
     * Looks up the key and throws an exception if invalid value found
     *
     * @param key
     * @return
     */
    public String lookup(final String key) {
        String casedKey =
                mCaseSensitive ? key : key.toLowerCase(); // lowercase the key you're looking for
        if (mMap == null) {
            return null;
        }
        final Object obj = mMap.get(casedKey);
        if (obj == null) {
            throw new RuntimeException("Unable to expand variable " + key);
        }
        return obj.toString();
    }
}
