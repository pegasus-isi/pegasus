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

import java.util.Iterator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for the Namespace abstract base class via the concrete ENV implementation. */
public class NamespaceTest {

    private ENV ns;

    @BeforeAll
    public static void setUpClass() {}

    @AfterAll
    public static void tearDownClass() {}

    @BeforeEach
    public void setUp() {
        ns = new ENV();
    }

    @AfterEach
    public void tearDown() {}

    @Test
    public void testDefaultConstructorIsEmpty() {
        assertTrue(ns.isEmpty(), "Newly constructed namespace should be empty");
    }

    @Test
    public void testConstructAndGet() {
        ns.construct("PATH", "/usr/bin");
        assertThat((String) ns.get("PATH"), is("/usr/bin"));
    }

    @Test
    public void testContainsKeyAfterConstruct() {
        ns.construct("HOME", "/home/user");
        assertTrue(ns.containsKey("HOME"));
    }

    @Test
    public void testContainsKeyReturnsFalseForMissingKey() {
        assertFalse(ns.containsKey("MISSING_KEY"));
    }

    @Test
    public void testSizeIncrementsOnConstruct() {
        ns.construct("A", "1");
        ns.construct("B", "2");
        assertThat(ns.size(), is(2));
    }

    @Test
    public void testGetReturnsNullForMissingKey() {
        assertNull(ns.get("NO_SUCH_KEY"));
    }

    @Test
    public void testRemoveKey() {
        ns.construct("TEMP", "value");
        ns.removeKey("TEMP");
        assertFalse(ns.containsKey("TEMP"));
    }

    @Test
    public void testReset() {
        ns.construct("K1", "v1");
        ns.construct("K2", "v2");
        ns.reset();
        assertTrue(ns.isEmpty());
    }

    @Test
    public void testGetIntValueReturnsDefault() {
        int defaultVal = 42;
        assertThat(ns.getIntValue("nonexistent", defaultVal), is(defaultVal));
    }

    @Test
    public void testGetIntValueParsesStoredValue() {
        ns.construct("COUNT", "10");
        assertThat(ns.getIntValue("COUNT", 0), is(10));
    }

    @Test
    public void testGetLongValueReturnsDefault() {
        long defaultVal = 100L;
        assertThat(ns.getLongValue("nonexistent", defaultVal), is(defaultVal));
    }

    @Test
    public void testGetLongValueParsesStoredValue() {
        ns.construct("SIZE", "9999999999");
        assertThat(ns.getLongValue("SIZE", 0L), is(9999999999L));
    }

    @Test
    public void testProfileKeyIteratorOnEmptyNamespace() {
        Iterator it = ns.getProfileKeyIterator();
        assertFalse(it.hasNext(), "Iterator on empty namespace should have no elements");
    }

    @Test
    public void testProfileKeyIteratorOnPopulatedNamespace() {
        ns.construct("VAR1", "val1");
        Iterator it = ns.getProfileKeyIterator();
        assertTrue(it.hasNext(), "Iterator on populated namespace should have elements");
    }

    @Test
    public void testIsNamespaceValid() {
        assertTrue(Namespace.isNamespaceValid("env"));
        assertTrue(Namespace.isNamespaceValid("condor"));
        assertFalse(Namespace.isNamespaceValid("invalid_ns_xyz"));
    }

    @Test
    public void testNamespaceConstants() {
        assertThat(Namespace.VALID_KEY, is(0));
        assertThat(Namespace.MALFORMED_KEY, is(-1));
        assertThat(Namespace.UNKNOWN_KEY, is(1));
        assertThat(Namespace.NOT_PERMITTED_KEY, is(2));
        assertThat(Namespace.DEPRECATED_KEY, is(3));
        assertThat(Namespace.EMPTY_KEY, is(4));
        assertThat(Namespace.MERGE_KEY, is(5));
    }
}
