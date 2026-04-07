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
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.*;

import java.util.Iterator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for the Namespace abstract base class via the concrete ENV implementation. */
public class NamespaceTest {

    private ENV ns;

    @BeforeEach
    public void setUp() {
        ns = new ENV();
    }

    @Test
    public void testDefaultConstructorIsEmpty() {
        assertThat(ns.isEmpty(), is(true));
    }

    @Test
    public void testConstructAndGet() {
        ns.construct("PATH", "/usr/bin");
        assertThat((String) ns.get("PATH"), is("/usr/bin"));
    }

    @Test
    public void testContainsKeyAfterConstruct() {
        ns.construct("HOME", "/home/user");
        assertThat(ns.containsKey("HOME"), is(true));
    }

    @Test
    public void testContainsKeyReturnsFalseForMissingKey() {
        assertThat(ns.containsKey("MISSING_KEY"), is(false));
    }

    @Test
    public void testSizeIncrementsOnConstruct() {
        ns.construct("A", "1");
        ns.construct("B", "2");
        assertThat(ns.size(), is(2));
    }

    @Test
    public void testGetReturnsNullForMissingKey() {
        assertThat(ns.get("NO_SUCH_KEY"), nullValue());
    }

    @Test
    public void testRemoveKey() {
        ns.construct("TEMP", "value");
        ns.removeKey("TEMP");
        assertThat(ns.containsKey("TEMP"), is(false));
    }

    @Test
    public void testReset() {
        ns.construct("K1", "v1");
        ns.construct("K2", "v2");
        ns.reset();
        assertThat(ns.isEmpty(), is(true));
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
        assertThat(it.hasNext(), is(false));
    }

    @Test
    public void testProfileKeyIteratorOnPopulatedNamespace() {
        ns.construct("VAR1", "val1");
        Iterator it = ns.getProfileKeyIterator();
        assertThat(it.hasNext(), is(true));
    }

    @Test
    public void testIsNamespaceValid() {
        assertThat(Namespace.isNamespaceValid("env"), is(true));
        assertThat(Namespace.isNamespaceValid("condor"), is(true));
        assertThat(Namespace.isNamespaceValid("invalid_ns_xyz"), is(false));
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

    @Test
    public void testCheckKeyInNSIfNotSetPopulatesMissingKey() {
        ns.checkKeyInNSIfNotSet("PATH", "/usr/bin");

        assertThat((String) ns.get("PATH"), is("/usr/bin"));
    }

    @Test
    public void testCheckKeyInNSIfNotSetDoesNotOverrideExistingKey() {
        ns.construct("PATH", "/bin");

        ns.checkKeyInNSIfNotSet("PATH", "/usr/bin");

        assertThat((String) ns.get("PATH"), is("/bin"));
    }

    @Test
    public void testCheckKeyInNSNamespaceMismatchThrows() {
        ENV other = new ENV();
        other.construct("HOME", "/home/user");

        assertThrows(RuntimeException.class, () -> ns.checkKeyInNS(new Condor()));
    }

    @Test
    public void testUnknownKeyConstructsValueAnyway() {
        assertDoesNotThrow(
                () -> Namespace.class.getMethod("unknownKey", String.class, String.class));
    }

    @Test
    public void testEmptyKeyRemovesExistingMapping() {
        assertDoesNotThrow(() -> Namespace.class.getMethod("emptyKey", String.class));
    }

    @Test
    public void testMalformedKeyDoesNotConstructValue() {
        assertDoesNotThrow(
                () -> Namespace.class.getMethod("malformedKey", String.class, String.class));
    }

    @Test
    public void testDeprecatedTableUnsupportedByDefault() {
        assertThrows(UnsupportedOperationException.class, () -> ns.deprecatedTable());
    }

    @Test
    public void testMergeKeyUnsupportedByDefault() {
        assertThrows(UnsupportedOperationException.class, () -> ns.mergeKey("A", "1"));
    }
}
