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
 * An abstract implementation of the Profile Aggregators.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public abstract class Abstract implements Aggregator {

    /**
     * Formats the String value as an integer. If the String is NaN then the default value is
     * assigned.
     *
     * @param value the value to be converted to integer.
     * @param dflt the default value to be used in case value is NaN or null.
     * @return the integer value
     * @throws NumberFormatException in the case when default value cannot be converted to an int.
     */
    protected int parseInt(String value, String dflt) throws NumberFormatException {
        int val = Integer.parseInt(dflt);

        // check for null and apply default
        if (value == null) return val;

        // try to parse the value
        try {
            val = Integer.parseInt(value);
        } catch (Exception e) {
            /*ignore for now*/
        }

        return val;
    }
}
