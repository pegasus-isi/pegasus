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
package edu.isi.pegasus.planner.catalog.replica;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** @author Rajiv Mayani */
public class ReplicaCatalogEntryTest {

    @BeforeAll
    public static void setUpClass() {}

    @AfterAll
    public static void tearDownClass() {}

    @BeforeEach
    public void setUp() {}

    @AfterEach
    public void tearDown() {}

    // --- Constructor tests ---

    @Test
    public void testDefaultConstructorHasNullPFN() {
        ReplicaCatalogEntry rce = new ReplicaCatalogEntry();
        assertNull(rce.getPFN(), "Default constructor should set PFN to null");
    }

    @Test
    public void testDefaultConstructorHasNoAttributes() {
        ReplicaCatalogEntry rce = new ReplicaCatalogEntry();
        assertEquals(0, rce.getAttributeCount());
    }

    @Test
    public void testPfnConstructorSetsPFN() {
        ReplicaCatalogEntry rce = new ReplicaCatalogEntry("gsiftp://host/file");
        assertEquals("gsiftp://host/file", rce.getPFN());
    }

    @Test
    public void testPfnConstructorHasNoResourceHandle() {
        ReplicaCatalogEntry rce = new ReplicaCatalogEntry("gsiftp://host/file");
        assertNull(rce.getResourceHandle());
    }

    @Test
    public void testPfnHandleConstructorSetsBoth() {
        ReplicaCatalogEntry rce = new ReplicaCatalogEntry("gsiftp://host/file", "local");
        assertEquals("gsiftp://host/file", rce.getPFN());
        assertEquals("local", rce.getResourceHandle());
    }

    @Test
    public void testPfnMapConstructorSetsAttributes() {
        Map<String, Object> attrs = new HashMap<>();
        attrs.put(ReplicaCatalogEntry.RESOURCE_HANDLE, "isi");
        attrs.put("checksum", "abc123");
        ReplicaCatalogEntry rce = new ReplicaCatalogEntry("gsiftp://host/file", attrs);
        assertEquals("isi", rce.getResourceHandle());
        assertEquals("abc123", rce.getAttribute("checksum"));
    }

    // --- PFN set/get tests ---

    @Test
    public void testSetAndGetPFN() {
        ReplicaCatalogEntry rce = new ReplicaCatalogEntry();
        rce.setPFN("file:///data/file.txt");
        assertEquals("file:///data/file.txt", rce.getPFN());
    }

    // --- Attribute tests ---

    @Test
    public void testAddAttributeAndGet() {
        ReplicaCatalogEntry rce = new ReplicaCatalogEntry("pfn://test");
        rce.addAttribute("mykey", "myvalue");
        assertEquals("myvalue", rce.getAttribute("mykey"));
    }

    @Test
    public void testHasAttributeReturnsTrueForKnownKey() {
        ReplicaCatalogEntry rce = new ReplicaCatalogEntry("pfn://test", "site1");
        assertTrue(rce.hasAttribute(ReplicaCatalogEntry.RESOURCE_HANDLE));
    }

    @Test
    public void testHasAttributeReturnsFalseForUnknownKey() {
        ReplicaCatalogEntry rce = new ReplicaCatalogEntry("pfn://test");
        assertFalse(rce.hasAttribute("nonexistent"));
    }

    @Test
    public void testGetAttributeCountAfterAdding() {
        ReplicaCatalogEntry rce = new ReplicaCatalogEntry("pfn://test", "site1");
        rce.addAttribute("extra", "value");
        assertEquals(2, rce.getAttributeCount());
    }

    @Test
    public void testSetAttributeOverwritesExisting() {
        ReplicaCatalogEntry rce = new ReplicaCatalogEntry("pfn://test");
        rce.addAttribute("key", "original");
        rce.setAttribute("key", "updated");
        assertEquals("updated", rce.getAttribute("key"));
    }

    @Test
    public void testRemoveAttributeDeletesKey() {
        ReplicaCatalogEntry rce = new ReplicaCatalogEntry("pfn://test");
        rce.addAttribute("key", "value");
        Object removed = rce.removeAttribute("key");
        assertEquals("value", removed);
        assertFalse(rce.hasAttribute("key"));
    }

    @Test
    public void testRemoveAllAttributeClearsMap() {
        ReplicaCatalogEntry rce = new ReplicaCatalogEntry("pfn://test", "site1");
        rce.addAttribute("extra", "value");
        rce.removeAllAttribute();
        assertEquals(0, rce.getAttributeCount());
    }

    // --- Resource handle tests ---

    @Test
    public void testSetResourceHandle() {
        ReplicaCatalogEntry rce = new ReplicaCatalogEntry("pfn://test");
        rce.setResourceHandle("cluster1");
        assertEquals("cluster1", rce.getResourceHandle());
    }

    @Test
    public void testSetResourceHandleNullDoesNothing() {
        ReplicaCatalogEntry rce = new ReplicaCatalogEntry("pfn://test", "existing");
        rce.setResourceHandle(null);
        assertEquals("existing", rce.getResourceHandle());
    }

    // --- Constants tests ---

    @Test
    public void testResourceHandleConstant() {
        assertEquals("site", ReplicaCatalogEntry.RESOURCE_HANDLE);
    }

    @Test
    public void testDeprecatedResourceHandleConstant() {
        assertEquals("pool", ReplicaCatalogEntry.DEPRECATED_RESOURCE_HANDLE);
    }

    @Test
    public void testRegexKeyConstant() {
        assertEquals("regex", ReplicaCatalogEntry.REGEX_KEY);
    }

    // --- isRegex tests ---

    @Test
    public void testIsRegexReturnsFalseByDefault() {
        ReplicaCatalogEntry rce = new ReplicaCatalogEntry("pfn://test");
        assertFalse(rce.isRegex());
    }

    @Test
    public void testIsRegexReturnsTrueWhenSet() {
        ReplicaCatalogEntry rce = new ReplicaCatalogEntry("pfn://test");
        rce.addAttribute(ReplicaCatalogEntry.REGEX_KEY, "true");
        assertTrue(rce.isRegex());
    }

    // --- equals tests ---

    @Test
    public void testEqualsIdenticalEntries() {
        ReplicaCatalogEntry rce1 = new ReplicaCatalogEntry("pfn://test", "site1");
        ReplicaCatalogEntry rce2 = new ReplicaCatalogEntry("pfn://test", "site1");
        assertEquals(rce1, rce2);
    }

    @Test
    public void testEqualsDifferentPFN() {
        ReplicaCatalogEntry rce1 = new ReplicaCatalogEntry("pfn://test1", "site1");
        ReplicaCatalogEntry rce2 = new ReplicaCatalogEntry("pfn://test2", "site1");
        assertNotEquals(rce1, rce2);
    }

    @Test
    public void testEqualsNullReturnsFalse() {
        ReplicaCatalogEntry rce = new ReplicaCatalogEntry("pfn://test");
        assertFalse(rce.equals(null));
    }

    @Test
    public void testEqualsNonRCEReturnsFalse() {
        ReplicaCatalogEntry rce = new ReplicaCatalogEntry("pfn://test");
        assertFalse(rce.equals("not an RCE"));
    }

    @Test
    public void testEqualsBothNullPFNs() {
        ReplicaCatalogEntry rce1 = new ReplicaCatalogEntry();
        ReplicaCatalogEntry rce2 = new ReplicaCatalogEntry();
        assertEquals(rce1, rce2);
    }

    // --- clone test ---

    @Test
    public void testCloneProducesEqualButIndependentCopy() {
        ReplicaCatalogEntry original = new ReplicaCatalogEntry("pfn://test", "site1");
        original.addAttribute("extra", "data");

        ReplicaCatalogEntry clone = (ReplicaCatalogEntry) original.clone();
        assertEquals(original, clone);

        // modify clone should not affect original
        clone.addAttribute("new_key", "new_val");
        assertFalse(original.hasAttribute("new_key"));
    }

    // --- merge tests ---

    @Test
    public void testMergeWithMatchingPFNReturnsTrue() {
        ReplicaCatalogEntry rce1 = new ReplicaCatalogEntry("pfn://test", "site1");
        ReplicaCatalogEntry rce2 = new ReplicaCatalogEntry("pfn://test");
        rce2.addAttribute("extra", "value");

        boolean merged = rce1.merge(rce2, false);
        assertTrue(merged);
        assertTrue(rce1.hasAttribute("extra"));
    }

    @Test
    public void testMergeWithMismatchedPFNReturnsFalse() {
        ReplicaCatalogEntry rce1 = new ReplicaCatalogEntry("pfn://test1", "site1");
        ReplicaCatalogEntry rce2 = new ReplicaCatalogEntry("pfn://test2");

        boolean merged = rce1.merge(rce2, false);
        assertFalse(merged);
    }

    @Test
    public void testStaticMergeWithMatchingPFNs() {
        ReplicaCatalogEntry a = new ReplicaCatalogEntry("pfn://test", "site1");
        ReplicaCatalogEntry b = new ReplicaCatalogEntry("pfn://test");
        b.addAttribute("extra", "value");

        ReplicaCatalogEntry merged = ReplicaCatalogEntry.merge(a, b, false);
        assertNotNull(merged);
        assertEquals("pfn://test", merged.getPFN());
    }

    @Test
    public void testStaticMergeWithMismatchedPFNsReturnsNull() {
        ReplicaCatalogEntry a = new ReplicaCatalogEntry("pfn://test1");
        ReplicaCatalogEntry b = new ReplicaCatalogEntry("pfn://test2");

        ReplicaCatalogEntry merged = ReplicaCatalogEntry.merge(a, b, false);
        assertNull(merged);
    }

    // --- pool attribute migration test ---

    @Test
    public void testPoolAttributeConvertedToSite() {
        Map<String, Object> attrs = new HashMap<>();
        attrs.put(ReplicaCatalogEntry.DEPRECATED_RESOURCE_HANDLE, "poolsite");
        ReplicaCatalogEntry rce = new ReplicaCatalogEntry("pfn://test", attrs);
        assertEquals("poolsite", rce.getResourceHandle());
        assertFalse(rce.hasAttribute(ReplicaCatalogEntry.DEPRECATED_RESOURCE_HANDLE));
    }

    @Test
    public void testBothSiteAndPoolAttributeThrowsException() {
        Map<String, Object> attrs = new HashMap<>();
        attrs.put(ReplicaCatalogEntry.RESOURCE_HANDLE, "site1");
        attrs.put(ReplicaCatalogEntry.DEPRECATED_RESOURCE_HANDLE, "poolsite");
        assertThrows(RuntimeException.class, () -> new ReplicaCatalogEntry("pfn://test", attrs));
    }

    // --- toString test ---

    @Test
    public void testToStringContainsPFN() {
        ReplicaCatalogEntry rce = new ReplicaCatalogEntry("gsiftp://host/data/file");
        assertThat(rce.toString(), containsString("gsiftp://host/data/file"));
    }

    @Test
    public void testToStringWithNullPFN() {
        ReplicaCatalogEntry rce = new ReplicaCatalogEntry();
        assertThat(rce.toString(), containsString("(null)"));
    }
}
