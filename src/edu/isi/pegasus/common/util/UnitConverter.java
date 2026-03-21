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

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A convenience class to convert string formatted values with units to their numeric counterparts
 *
 * @author Karan Vahi
 */
public class UnitConverter {

    /** Store the regular expressions necessary to parse the memory values */
    private static final String mRegexExpression =
            "^\\s*(\\d+([Ee][+-]?\\d+)?)\\s*([MmGgPpEeZzYy][Bb]?)?\\s*$";

    /** Stores compiled patterns at first use, quasi-Singleton. */
    private static Pattern mPattern = null;

    private static Map<String, Long> mUnitsToMBMultiplier = null;

    /** initialize just once */
    static {
        mPattern = Pattern.compile(mRegexExpression);

        mUnitsToMBMultiplier = new HashMap();
        // 1MB is 1L<<20 Bytes
        // our base is MB
        mUnitsToMBMultiplier.put("mb", 1L << (20 - 20));
        mUnitsToMBMultiplier.put("gb", 1L << (30 - 20));
        mUnitsToMBMultiplier.put("tb", 1L << (40 - 20));
        mUnitsToMBMultiplier.put("pb", 1L << (50 - 20));
    }

    /**
     * Converts the value passed into MB. If no unit is specified, the passed value is assumed to be
     * in MB. If a value can't be converted then returns -1.
     *
     * @param value value to be converted optionally with a suffix
     * @return
     */
    public static long toMB(String value) {
        Matcher matcher = mPattern.matcher(value);
        long result = -1;
        if (!matcher.matches()) {
            return -1;
        }

        // default unit is mb
        String unit = "mb";
        int amt = Integer.parseInt(matcher.group(1));

        if (matcher.groupCount() == 3) {
            if (matcher.group(3) != null) {
                unit = matcher.group(3).toLowerCase();
            }
        }

        if (!unit.endsWith("b")) {
            // add a b suffix
            unit += "b";
        }
        result = amt * UnitConverter.mUnitsToMBMultiplier.get(unit);

        // System.out.println(
        //        "Unit computed for " + value + " is " + unit + " and converted to MB is " +
        // result);
        return result;
    }

    /**
     * Converts the value passed into MB. If no unit is specified, the passed value is assumed to be
     * in KB. If a value can't be converted then returns -1.
     *
     * @param value value to be converted optionally with a suffix
     * @return
     */
    public static long toKB(String value) {
        long val = UnitConverter.toMB(value);

        val = (val == -1) ? -1 : val * 1024;
        // System.out.println(
        //        "Unit computed for " + value +  " and converted to KB is " + val);
        return val;
    }

    public static void main(String[] args) {
        // UnitConverter uc = new UnitConverter();
        UnitConverter.toMB("1 M");
        UnitConverter.toMB("1 MB");
        UnitConverter.toMB("1");
        UnitConverter.toMB("1 G");
        UnitConverter.toMB("10 GB");
        UnitConverter.toKB("1 MB");
    }
}
