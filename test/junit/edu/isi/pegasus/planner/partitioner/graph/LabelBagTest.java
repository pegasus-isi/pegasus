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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
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
        assertThat(LabelBag.LABEL_KEY, is("label"));
    }

    @Test
    public void testPartitionKeyConstant() {
        assertThat(LabelBag.PARTITION_KEY, is("partition"));
    }

    @Test
    public void testAddAndRetrieveLabelValue() {
        mBag.add(LabelBag.LABEL_KEY, "my-partition");
        assertThat(mBag.get(LabelBag.LABEL_KEY), is("my-partition"));
    }

    @Test
    public void testAddAndRetrievePartitionValue() {
        mBag.add(LabelBag.PARTITION_KEY, "part-42");
        assertThat(mBag.get(LabelBag.PARTITION_KEY), is("part-42"));
    }

    @Test
    public void testContainsKeyReturnsTrueAfterAdd() {
        mBag.add(LabelBag.LABEL_KEY, "val");
        assertThat(mBag.containsKey(LabelBag.LABEL_KEY), is(true));
    }

    @Test
    public void testContainsKeyReturnsFalseForUnknownKey() {
        assertThat(mBag.containsKey("unknown-key"), is(false));
    }

    @Test
    public void testGetReturnsNullInitially() {
        assertThat(mBag.get(LabelBag.LABEL_KEY), is(nullValue()));
    }

    @Test
    public void testLabelBagImplementsBag() {
        assertThat(mBag instanceof Bag, is(true));
    }

    @Test
    public void testAddReturnsFalseForUnknownKey() {
        assertThat(mBag.add("unknown-key", "value"), is(false));
        assertThat(mBag.get("unknown-key"), is(nullValue()));
    }

    @Test
    public void testSetLabelKeyChangesLookupKey() {
        String originalKey = LabelBag.LABEL_KEY;
        try {
            LabelBag.setLabelKey("cluster");
            mBag.add(LabelBag.LABEL_KEY, "group-a");
            assertThat(mBag.get("cluster"), is("group-a"));
            assertThat(mBag.get(originalKey), is(nullValue()));
        } finally {
            LabelBag.setLabelKey(originalKey);
        }
    }

    @Test
    public void testContainsKeyForPartitionKeyIsTrueEvenBeforeAdd() {
        assertThat(mBag.containsKey(LabelBag.PARTITION_KEY), is(true));
    }

    @Test
    public void testToStringIncludesLabelAndPartitionValues() {
        mBag.add(LabelBag.LABEL_KEY, "label-1");
        mBag.add(LabelBag.PARTITION_KEY, "partition-9");
        assertThat(mBag.toString(), is("{label-1,partition-9}"));
    }
}
