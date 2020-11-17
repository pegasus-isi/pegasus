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
package edu.isi.pegasus.planner.namespace.aggregator;

import java.util.HashSet;
import java.util.Set;

/**
 * Merges profile as a delimiter separated list. It ensures that only unique values are merged.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class UniqueMerge extends Abstract {

    /** Default delimiter */
    public static final String DEFAULT_DELIMITER = "@";

    private Set<String> mKeys;

    public UniqueMerge() {}

    /**
     * Sums up the values.
     *
     * @param oldValue the existing value for the profile.
     * @param newValue the new value being added to the profile.
     * @param dflt the default value to be used in case the values are not of the correct type.
     * @return the computed value as a String.
     */
    public String compute(String oldValue, String newValue, String dflt) {
        StringBuilder sb = new StringBuilder();

        if (oldValue == null) {
            mKeys = new HashSet();
            mKeys.add(newValue);
            sb.append(newValue);
            return sb.toString();
        } else {
            sb.append(oldValue);
            if (!mKeys.contains(newValue)) {
                // only add we have not previously merged this value
                sb.append(DEFAULT_DELIMITER);
                sb.append(newValue);
            }
        }

        return sb.toString();
    }
}
