/**
 * Copyright 2007-2013 University Of Southern California
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

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Unit tests for the UniqueMerge aggregator. */
public class UniqueMergeTest {

    private UniqueMerge mAggregator;

    @BeforeEach
    public void setUp() {
        mAggregator = new UniqueMerge();
    }

    @AfterEach
    public void tearDown() {
        mAggregator = null;
    }

    @Test
    public void testFirstMergeWithNullOldValue() {
        // when oldValue is null the newValue becomes the first entry
        String result = mAggregator.compute(null, "alpha", "");
        assertEquals(
                "alpha", result, "First compute with null old value should return just newValue");
    }

    @Test
    public void testSecondMergeAddsDelimiter() {
        mAggregator.compute(null, "alpha", "");
        String result = mAggregator.compute("alpha", "beta", "");
        assertEquals(
                "alpha" + UniqueMerge.DEFAULT_DELIMITER + "beta",
                result,
                "Second distinct value should be appended with delimiter");
    }

    @Test
    public void testDuplicateValueIsNotMergedAgain() {
        mAggregator.compute(null, "alpha", "");
        String result = mAggregator.compute("alpha", "alpha", "");
        assertEquals("alpha", result, "Duplicate value should not be appended again");
    }

    @Test
    public void testDefaultDelimiterIsAtSign() {
        assertEquals("@", UniqueMerge.DEFAULT_DELIMITER, "Default delimiter should be @");
    }

    @Test
    public void testThreeUniqueValues() {
        mAggregator.compute(null, "a", "");
        mAggregator.compute("a", "b", "");
        String result = mAggregator.compute("a" + UniqueMerge.DEFAULT_DELIMITER + "b", "c", "");
        assertTrue(result.contains("c"), "Third unique value should be appended");
    }

    @Test
    public void testImplementsAggregatorInterface() {
        assertTrue(mAggregator instanceof Aggregator, "UniqueMerge should implement Aggregator");
    }

    @Test
    public void testExtendsAbstract() {
        assertTrue(mAggregator instanceof Abstract, "UniqueMerge should extend Abstract");
    }
}
