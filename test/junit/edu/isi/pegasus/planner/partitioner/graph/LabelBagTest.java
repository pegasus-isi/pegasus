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
package edu.isi.pegasus.planner.partitioner.graph;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Unit tests for the LabelBag class. */
public class LabelBagTest {

    private LabelBag mBag;

    @BeforeEach
    public void setUp() {
        mBag = new LabelBag();
    }

    @Test
    public void testDefaultLabelKeyConstant() {
        assertEquals("label", LabelBag.LABEL_KEY, "Default LABEL_KEY should be 'label'");
    }

    @Test
    public void testPartitionKeyConstant() {
        assertEquals("partition", LabelBag.PARTITION_KEY, "PARTITION_KEY should be 'partition'");
    }

    @Test
    public void testAddAndRetrieveLabelValue() {
        mBag.add(LabelBag.LABEL_KEY, "my-partition");
        assertEquals(
                "my-partition",
                mBag.get(LabelBag.LABEL_KEY),
                "Should retrieve the added label value");
    }

    @Test
    public void testAddAndRetrievePartitionValue() {
        mBag.add(LabelBag.PARTITION_KEY, "part-42");
        assertEquals(
                "part-42",
                mBag.get(LabelBag.PARTITION_KEY),
                "Should retrieve the added partition ID");
    }

    @Test
    public void testContainsKeyReturnsTrueAfterAdd() {
        mBag.add(LabelBag.LABEL_KEY, "val");
        assertTrue(
                mBag.containsKey(LabelBag.LABEL_KEY),
                "containsKey should return true after adding a value");
    }

    @Test
    public void testContainsKeyReturnsFalseForUnknownKey() {
        assertFalse(
                mBag.containsKey("unknown-key"),
                "containsKey should return false for keys that have not been added");
    }

    @Test
    public void testGetReturnsNullInitially() {
        assertNull(
                mBag.get(LabelBag.LABEL_KEY),
                "get should return null before any value is added for LABEL_KEY");
    }

    @Test
    public void testLabelBagImplementsBag() {
        assertTrue(mBag instanceof Bag, "LabelBag should implement the Bag interface");
    }
}
