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
package edu.isi.pegasus.planner.common;

import java.util.Random;

/**
 * A Helper class that returns the Random values using java.util.Random class. It is a singleton
 * instance, and all functions in Pegasus call it to get the random value. The singleton ensures
 * that the number returned is random as compared to previous values. If this is not used and one
 * ends up doing a new Random(), all the calls effectively end up setting the same seed value, nad
 * by extension one gets the same value. Using just one Random object we hope to bypass the problem.
 *
 * <p>Copyright: Copyright (c) 2002
 *
 * <p>Company: USC/ISI
 *
 * @author Gaurang Mehta
 * @author Karan Vahi
 * @version $Revision$
 */
public class PegRandom {

    /** The object containing the instance of the java.util.Random class. */
    private static Random mRandom;

    /** This is called only once when the class is first loaded. */
    static {
        mRandom = new Random();
    }

    /** Returns a double value between 0.0 and 1.0. */
    public static double nextDouble() {
        return mRandom.nextDouble();
    }

    /** Returns a normally distributed (gaussian) random variable between 0.0 and 1.0. */
    public static double nextGaussian() {
        return mRandom.nextGaussian();
    }

    /** This calls the next double function and returns an integer between the 0 and upper index. */
    public static int getInteger(int upperIndex) {
        return getInteger(0, upperIndex);
    }

    /**
     * This calls the next double function and returns an integer between the lower index and upper
     * index.
     */
    public static int getInteger(int lowerIndex, int upperIndex) {
        double value = nextDouble();
        int val = 0;

        // adding one as intValue() function
        // truncates the value instead of
        // rounding it off.
        upperIndex += 1;
        value = lowerIndex + ((upperIndex - lowerIndex) * value);
        val = new Double(value).intValue();

        if (val == upperIndex)
            // get the one lower value
            val -= 1;

        return val;
    }
}
