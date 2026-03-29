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

/** Unit tests for the Update aggregator. */
public class UpdateTest {

    private Update mAggregator;

    @BeforeEach
    public void setUp() {
        mAggregator = new Update();
    }

    @AfterEach
    public void tearDown() {
        mAggregator = null;
    }

    @Test
    public void testAlwaysReturnsNewValue() {
        String result = mAggregator.compute("old", "new", "default");
        assertEquals("new", result, "Update should always return the new value");
    }

    @Test
    public void testReturnsNewValueWhenOldIsNull() {
        String result = mAggregator.compute(null, "new", "default");
        assertEquals("new", result, "Update should return new value even when old is null");
    }

    @Test
    public void testReturnsNewValueWhenOldIsEmpty() {
        String result = mAggregator.compute("", "replacement", "default");
        assertEquals("replacement", result, "Update should return new value when old is empty");
    }

    @Test
    public void testIgnoresDefaultValue() {
        String result = mAggregator.compute("old", "new", "should-be-ignored");
        assertEquals("new", result, "Update should ignore the default parameter");
    }

    @Test
    public void testReturnsNullWhenNewValueIsNull() {
        String result = mAggregator.compute("old", null, "default");
        assertNull(result, "Update should return null when new value is null");
    }

    @Test
    public void testImplementsAggregatorInterface() {
        assertTrue(mAggregator instanceof Aggregator, "Update should implement Aggregator");
    }

    @Test
    public void testExtendsAbstract() {
        assertTrue(mAggregator instanceof Abstract, "Update should extend Abstract");
    }

    @Test
    public void testReturnsNumericNewValue() {
        String result = mAggregator.compute("100", "200", "0");
        assertEquals("200", result, "Update should return new numeric value 200");
    }
}
