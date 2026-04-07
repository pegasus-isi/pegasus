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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

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
        assertThat(result, is("alpha"));
    }

    @Test
    public void testSecondMergeAddsDelimiter() {
        mAggregator.compute(null, "alpha", "");
        String result = mAggregator.compute("alpha", "beta", "");
        assertThat(result, is("alpha" + UniqueMerge.DEFAULT_DELIMITER + "beta"));
    }

    @Test
    public void testDuplicateValueIsNotMergedAgain() {
        mAggregator.compute(null, "alpha", "");
        String result = mAggregator.compute("alpha", "alpha", "");
        assertThat(result, is("alpha"));
    }

    @Test
    public void testDefaultDelimiterIsAtSign() {
        assertThat(UniqueMerge.DEFAULT_DELIMITER, is("@"));
    }

    @Test
    public void testThreeUniqueValues() {
        mAggregator.compute(null, "a", "");
        mAggregator.compute("a", "b", "");
        String result = mAggregator.compute("a" + UniqueMerge.DEFAULT_DELIMITER + "b", "c", "");
        assertThat(result.contains("c"), is(true));
    }

    @Test
    public void testFirstMergeWithNullNewValueUsesStringBuilderNullText() {
        String result = mAggregator.compute(null, null, "");
        assertThat(result, is("null"));
    }

    @Test
    public void testDuplicateOfLaterValueIsCurrentlyMergedAgain() {
        mAggregator.compute(null, "alpha", "");
        String second = mAggregator.compute("alpha", "beta", "");
        String result = mAggregator.compute(second, "beta", "");
        assertThat(
                result,
                is(
                        "alpha"
                                + UniqueMerge.DEFAULT_DELIMITER
                                + "beta"
                                + UniqueMerge.DEFAULT_DELIMITER
                                + "beta"));
    }

    @Test
    public void testNullOldValueResetsTrackedKeys() {
        mAggregator.compute(null, "alpha", "");
        mAggregator.compute("alpha", "beta", "");
        String result = mAggregator.compute(null, "beta", "");
        assertThat(result, is("beta"));
    }

    @Test
    public void testImplementsAggregatorInterface() {
        assertThat(mAggregator instanceof Aggregator, is(true));
    }

    @Test
    public void testExtendsAbstract() {
        assertThat(mAggregator instanceof Abstract, is(true));
    }
}
