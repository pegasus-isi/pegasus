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

/**
 * An implementation of the Aggregator interface that takes the minimum of the profile values. In
 * the case of either of the profile values not valid integers, the default value is picked up.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class MIN extends Abstract {

    /**
     * Returns the minimum of two values.
     *
     * @param oldValue the existing value for the profile.
     * @param newValue the new value being added to the profile.
     * @param dflt the default value to be used in case the values are not of the correct type.
     * @return the computed value as a String.
     */
    public String compute(String oldValue, String newValue, String dflt) {
        int val1 = parseInt(oldValue, dflt);
        int val2 = parseInt(newValue, dflt);

        return (val2 < val1) ? Integer.toString(val2) : Integer.toString(val1);
    }
}
