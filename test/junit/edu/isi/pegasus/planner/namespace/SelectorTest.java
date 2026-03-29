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
package edu.isi.pegasus.planner.namespace;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** @author Rajiv Mayani */
public class SelectorTest {

    private Selector selector;

    @BeforeAll
    public static void setUpClass() {}

    @AfterAll
    public static void tearDownClass() {}

    @BeforeEach
    public void setUp() {
        selector = new Selector();
    }

    @AfterEach
    public void tearDown() {}

    @Test
    public void testDefaultConstructorIsEmpty() {
        assertTrue(selector.isEmpty(), "Newly constructed Selector namespace should be empty");
    }

    @Test
    public void testNamespaceName() {
        assertThat(selector.namespaceName(), is(Selector.NAMESPACE_NAME));
    }

    @Test
    public void testConstructAndGet() {
        selector.construct(Selector.EXECUTION_SITE_KEY, "condorpool");
        assertThat((String) selector.get(Selector.EXECUTION_SITE_KEY), is("condorpool"));
    }

    @Test
    public void testConstructInitializesMapLazily() {
        selector.construct(Selector.EXECUTION_SITE_KEY, "local");
        assertTrue(selector.containsKey(Selector.EXECUTION_SITE_KEY));
    }

    @Test
    public void testCheckKeyValidForAnyNonNullKeyAndValue() {
        assertThat(
                selector.checkKey(Selector.EXECUTION_SITE_KEY, "condorpool"),
                is(Namespace.VALID_KEY));
        assertThat(
                selector.checkKey(Selector.GRID_JOB_TYPE_KEY, "compute"), is(Namespace.VALID_KEY));
        assertThat(
                selector.checkKey(Selector.PFN_HINT_KEY, "/path/to/exec"), is(Namespace.VALID_KEY));
    }

    @Test
    public void testCheckKeyNotPermittedForNullKey() {
        assertThat(selector.checkKey(null, "condorpool"), is(Namespace.NOT_PERMITTED_KEY));
    }

    @Test
    public void testCheckKeyNotPermittedForNullValue() {
        assertThat(
                selector.checkKey(Selector.EXECUTION_SITE_KEY, null),
                is(Namespace.NOT_PERMITTED_KEY));
    }

    @Test
    public void testCheckKeyValidForArbitraryKey() {
        assertThat(selector.checkKey("custom_key", "custom_value"), is(Namespace.VALID_KEY));
    }

    @Test
    public void testToCondorReturnsEmptyString() {
        selector.construct(Selector.EXECUTION_SITE_KEY, "condorpool");
        assertThat(selector.toCondor(), is(""));
    }

    @Test
    public void testToStringDelegatesToToCondor() {
        selector.construct(Selector.EXECUTION_SITE_KEY, "local");
        assertThat(selector.toString(), is(selector.toCondor()));
    }

    @Test
    public void testCloneOnEmptyReturnsEmptySelector() {
        Selector clone = (Selector) selector.clone();
        assertTrue(clone.isEmpty());
    }

    @Test
    public void testClonePreservesValues() {
        selector.construct(Selector.EXECUTION_SITE_KEY, "condorpool");
        Selector clone = (Selector) selector.clone();
        assertThat((String) clone.get(Selector.EXECUTION_SITE_KEY), is("condorpool"));
    }

    @Test
    public void testCloneIsIndependent() {
        selector.construct(Selector.EXECUTION_SITE_KEY, "condorpool");
        Selector clone = (Selector) selector.clone();
        clone.construct(Selector.PFN_HINT_KEY, "/new/path");
        assertFalse(
                selector.containsKey(Selector.PFN_HINT_KEY),
                "Clone modification should not affect original");
    }

    @Test
    public void testMergeAddsKeysFromOtherNamespace() {
        selector.construct(Selector.EXECUTION_SITE_KEY, "condorpool");
        Selector other = new Selector();
        other.construct(Selector.PFN_HINT_KEY, "/path/to/exec");
        selector.merge(other);
        assertTrue(selector.containsKey(Selector.PFN_HINT_KEY));
    }

    @Test
    public void testMergeOverridesExistingKey() {
        selector.construct(Selector.EXECUTION_SITE_KEY, "condorpool");
        Selector other = new Selector();
        other.construct(Selector.EXECUTION_SITE_KEY, "local");
        selector.merge(other);
        assertThat((String) selector.get(Selector.EXECUTION_SITE_KEY), is("local"));
    }

    @Test
    public void testMergeThrowsOnWrongType() {
        assertThrows(
                IllegalArgumentException.class,
                () -> selector.merge(new ENV()),
                "Merging incompatible namespace types should throw");
    }

    @Test
    public void testSizeAfterMultipleConstructs() {
        selector.construct("key1", "val1");
        selector.construct("key2", "val2");
        selector.construct("key3", "val3");
        assertThat(selector.size(), is(3));
    }

    @Test
    public void testCheckKeyInNSFromSemicolonString() {
        selector.checkKeyInNS(Selector.EXECUTION_SITE_KEY + "=condorpool");
        assertTrue(selector.containsKey(Selector.EXECUTION_SITE_KEY));
    }
}
